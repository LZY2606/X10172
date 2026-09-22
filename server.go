/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package ttrpc

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/containerd/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	config   *serverConfig
	services *serviceSet
	codec    codec

	mu          sync.Mutex
	listeners   map[net.Listener]struct{}
	connections map[*serverConn]struct{} // all connections to current state
	done        chan struct{}            // marks point at which we stop serving requests
	idleCh      chan struct{}            // signaled (non-blocking) when a connection becomes idle
}

func NewServer(opts ...ServerOpt) (*Server, error) {
	config := &serverConfig{}
	for _, opt := range opts {
		if err := opt(config); err != nil {
			return nil, err
		}
	}
	if config.interceptor == nil {
		config.interceptor = defaultServerInterceptor
	}

	return &Server{
		config:      config,
		services:    newServiceSet(config.interceptor),
		done:        make(chan struct{}),
		idleCh:      make(chan struct{}, 1),
		listeners:   make(map[net.Listener]struct{}),
		connections: make(map[*serverConn]struct{}),
	}, nil
}

// Register registers a map of methods to method handlers
// TODO: Remove in 2.0, does not support streams
func (s *Server) Register(name string, methods map[string]Method) {
	s.services.register(name, &ServiceDesc{Methods: methods})
}

func (s *Server) RegisterService(name string, desc *ServiceDesc) {
	s.services.register(name, desc)
}

func (s *Server) Serve(ctx context.Context, l net.Listener) error {
	s.mu.Lock()
	s.addListenerLocked(l)
	defer s.closeListener(l)

	select {
	case <-s.done:
		s.mu.Unlock()
		return ErrServerClosed
	default:
	}
	s.mu.Unlock()

	var (
		backoff    time.Duration
		handshaker = s.config.handshaker
	)

	if handshaker == nil {
		handshaker = handshakerFunc(noopHandshake)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-s.done:
				return ErrServerClosed
			default:
			}

			if terr, ok := err.(interface {
				Temporary() bool
			}); ok && terr.Temporary() {
				if backoff == 0 {
					backoff = time.Millisecond
				} else {
					backoff *= 2
				}

				backoff = min(time.Second, backoff)

				sleep := time.Duration(rand.Int63n(int64(backoff)))
				log.G(ctx).WithError(err).Errorf("ttrpc: failed accept; backoff %v", sleep)
				time.Sleep(sleep)
				continue
			}

			return err
		}

		backoff = 0

		approved, handshake, err := handshaker.Handshake(ctx, conn)
		if err != nil {
			log.G(ctx).WithError(err).Error("ttrpc: refusing connection after handshake")
			conn.Close()
			continue
		}

		sc, err := s.newConn(approved, handshake)
		if err != nil {
			log.G(ctx).WithError(err).Error("ttrpc: create connection failed")
			conn.Close()
			continue
		}

		go sc.run(ctx)
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	select {
	case <-s.done:
	default:
		// protected by mutex
		close(s.done)
	}
	lnerr := s.closeListeners()
	s.mu.Unlock()

	return s.waitConnections(ctx, lnerr, false)
}

// Drain gracefully drains the server. It stops accepting new connections,
// announces a per-connection drain boundary on every connection whose peer
// negotiated the graceful drain feature, lets every request accepted before
// the boundary finish, and waits until no connections remain or ctx is done.
//
// Requests past a boundary are rejected with a stable Unavailable status
// (ErrConnectionDrain on capable ttrpc clients) rather than being dropped by a
// connection close.
//
// Drain is idempotent: repeated calls and concurrent calls with Shutdown or
// Close share the same shutdown point and wait for the same conditions. On a
// server created without WithGracefulDrain, Drain is equivalent to Shutdown
// except that per-connection requests are still rejected after the boundary;
// only the announcement to the peer is skipped.
func (s *Server) Drain(ctx context.Context) error {
	s.mu.Lock()
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	lnerr := s.closeListeners()
	conns := make([]*serverConn, 0, len(s.connections))
	for c := range s.connections {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	// Announce the boundary on every connection before closing idle
	// connections, so even an idle connection receives the DRAIN frame
	// before its socket is shut.
	for _, c := range conns {
		c.signalDrain()
	}
	for _, c := range conns {
		select {
		case <-c.drainAck:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return s.waitConnections(ctx, lnerr, true)
}

// waitConnections closes idle connections until none remain. When draining is
// true, the periodic ticker is retained as a fallback but wakeups are driven
// by connection idle notifications; otherwise this preserves the historical
// 200ms polling behavior of Shutdown.
func (s *Server) waitConnections(ctx context.Context, lnerr error, draining bool) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		s.closeIdleConns()

		if s.countConnection() == 0 {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.idleCh:
			if !draining {
				continue
			}
		case <-ticker.C:
		}
	}

	return lnerr
}

// notifyIdle wakes a draining wait loop when a connection transitioned to
// idle. It never blocks.
func (s *Server) notifyIdle() {
	select {
	case s.idleCh <- struct{}{}:
	default:
	}
}

// Close the server without waiting for active connections.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-s.done:
	default:
		// protected by mutex
		close(s.done)
	}

	err := s.closeListeners()
	for c := range s.connections {
		c.close()
		delete(s.connections, c)
	}

	return err
}

func (s *Server) addListenerLocked(l net.Listener) {
	s.listeners[l] = struct{}{}
}

func (s *Server) closeListener(l net.Listener) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closeListenerLocked(l)
}

func (s *Server) closeListenerLocked(l net.Listener) error {
	defer delete(s.listeners, l)
	return l.Close()
}

func (s *Server) closeListeners() error {
	var err error
	for l := range s.listeners {
		if cerr := s.closeListenerLocked(l); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}

func (s *Server) addConnection(c *serverConn) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-s.done:
		return ErrServerClosed
	default:
	}

	s.connections[c] = struct{}{}
	return nil
}

func (s *Server) delConnection(c *serverConn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.connections, c)
}

func (s *Server) countConnection() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.connections)
}

func (s *Server) closeIdleConns() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for c := range s.connections {
		if st, ok := c.getState(); !ok || st == connStateActive {
			continue
		}
		// A drain may be in flight before the DRAIN frame has been
		// written; closing then would drop the announcement. Only
		// close once the boundary snapshot is taken AND the ack closed,
		// which happens after the frame send (or when no frame is
		// required).
		if c.drainTaken.Load() {
			select {
			case <-c.drainAck:
			default:
				continue
			}
			// The drain boundary was announced and every accepted call
			// finished. Half-close the write side to flush buffered final
			// frames toward the peer before the full close below makes the
			// peer observe EOF. Connections without CloseWrite fall back
			// to a full close.
			if cw, ok := c.conn.(interface{ CloseWrite() error }); ok {
				_ = cw.CloseWrite()
			}
		}
		c.close()
		delete(s.connections, c)
	}
}

type connState int

// unaryStreamSentinel marks an accepted unary stream in the streams map.
type unaryStreamSentinel struct{}

const (
	connStateActive = iota + 1 // outstanding requests
	connStateIdle              // no requests
	connStateClosed            // closed connection
)

func (cs connState) String() string {
	switch cs {
	case connStateActive:
		return "active"
	case connStateIdle:
		return "idle"
	case connStateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

func (s *Server) newConn(conn net.Conn, handshake any) (*serverConn, error) {
	c := &serverConn{
		server:             s,
		conn:               conn,
		handshake:          handshake,
		shutdown:           make(chan struct{}),
		drainSignal:        make(chan struct{}),
		drainAck:           make(chan struct{}),
		localGracefulDrain: s.config.gracefulDrain,
	}
	c.setState(connStateIdle)
	if err := s.addConnection(c); err != nil {
		c.close()
		return nil, err
	}
	return c, nil
}

type serverConn struct {
	server    *Server
	conn      net.Conn
	handshake any // data from handshake, not used for now
	state     atomic.Value

	shutdownOnce sync.Once
	shutdown     chan struct{} // forced shutdown, used by close

	// drainSignal is closed once by signalDrain. The receive goroutine and
	// run loop select on it to take the drain boundary.
	drainSignal chan struct{}
	drainOnce   sync.Once

	// drainAck is closed by the run loop after the DRAIN frame has been
	// handed to the channel, synchronizing the announcement with idle
	// connection teardown.
	drainAck     chan struct{}
	drainAckOnce sync.Once

	// drainMu guards the mutable per-connection drain state.
	drainMu      sync.Mutex
	lastStreamID uint32 // highest client stream id seen
	drained      bool   // a boundary has been taken
	drainBound   uint32 // the announced boundary (lastStreamID at take)

	// peerGracefulDrain records whether the peer advertised the feature.
	peerGracefulDrain atomic.Bool

	// drainTaken is set the moment the boundary snapshot is taken. Together
	// with drainAck it lets closeIdleConns distinguish "no drain yet" from
	// "drain boundary in flight".
	drainTaken atomic.Bool

	// localGracefulDrain is the server configuration copied at creation.
	localGracefulDrain bool
}

func (c *serverConn) getState() (connState, bool) {
	cs, ok := c.state.Load().(connState)
	return cs, ok
}

func (c *serverConn) setState(newstate connState) {
	c.state.Store(newstate)
}

func (c *serverConn) close() error {
	c.shutdownOnce.Do(func() {
		close(c.shutdown)
	})
	c.drainAckOnce.Do(func() {
		close(c.drainAck)
	})

	return nil
}

// signalDrain marks the connection for draining. Idempotent: repeat calls and
// concurrent calls with close do nothing after the first.
func (c *serverConn) signalDrain() {
	c.drainOnce.Do(func() {
		close(c.drainSignal)
	})
}

// takeDrain atomically snapshots the current high water mark of accepted
// stream ids and records the boundary. It returns the boundary and true only
// for the first drain signal.
func (c *serverConn) takeDrain() (uint32, bool) {
	c.drainMu.Lock()
	defer c.drainMu.Unlock()
	if c.drained {
		return c.drainBound, false
	}
	c.drained = true
	c.drainBound = c.lastStreamID
	c.drainTaken.Store(true)
	return c.drainBound, true
}

// newStreamAllowed enforces odd/monotonic ids and the drain boundary. It
// returns a non-nil gRPC status when the request must be rejected. On success
// it records the stream id as the new high water mark.
func (c *serverConn) newStreamAllowed(id uint32) *status.Status {
	c.drainMu.Lock()
	defer c.drainMu.Unlock()
	if id <= c.lastStreamID {
		return status.Newf(codes.InvalidArgument, "StreamID cannot be re-used and must increment")
	}
	if c.drained && id > c.drainBound {
		return mustDrainingStatus(c.drainBound)
	}
	c.lastStreamID = id
	return nil
}

func (c *serverConn) run(sctx context.Context) {
	type (
		response struct {
			id          uint32
			status      *status.Status
			data        []byte
			closeStream bool
			streaming   bool
			control     []byte
			drainAck    bool
		}
	)

	var (
		ch          = newChannel(c.conn)
		ctx, cancel = context.WithCancel(sctx)
		state       connState
		responses   = make(chan response)
		recvErr     = make(chan error, 1)
		done        = make(chan struct{})
		streams     = sync.Map{}
		active      int32
	)
	state = connStateIdle
	// drainHandled ensures the main loop acts on the one-shot drain
	// signal at most once, even if it was already consumed at the start
	// of an iteration.
	var drainHandled bool

	defer c.conn.Close()
	defer cancel()
	defer close(done)
	defer c.server.delConnection(c)

	sendStatus := func(id uint32, st *status.Status) bool {
		select {
		case responses <- response{
			// even though we've had an invalid stream id, we send it
			// back on the same stream id so the client knows which
			// stream id was bad.
			id:          id,
			status:      st,
			closeStream: true,
		}:
			return true
		case <-c.shutdown:
			return false
		case <-done:
			return false
		}
	}

	type incoming struct {
		header  messageHeader
		payload []byte
		err     error
	}

	// The reader owns the blocking channel recv so that a drain signal can
	// be observed even while parked on a read. Messages are handed to the
	// processor goroutine which performs all per-stream state changes.
	incomingCh := make(chan incoming)
	go func() {
		for {
			mh, p, err := ch.recv()
			select {
			case incomingCh <- incoming{header: mh, payload: p, err: err}:
			case <-c.shutdown:
				return
			case <-done:
				return
			}
			if err != nil {
				return
			}
		}
	}()

	go func(recvErr chan error) {
		defer close(recvErr)
		// drainCh is a local reference that is nilled after the one-shot
		// drain signal is consumed, so a closed channel does not spin the
		// loop.
		var drainCh <-chan struct{} = c.drainSignal
		announceDrain := func(bound uint32) bool {
			if !c.localGracefulDrain || !c.peerGracefulDrain.Load() {
				c.drainAckOnce.Do(func() { close(c.drainAck) })
				return true
			}
			select {
			case responses <- response{id: controlStreamID, control: marshalDrainControl(bound), drainAck: true}:
				// The run loop closes drainAck once the frame has been
				// written to the wire.
				return true
			case <-c.shutdown:
				return false
			case <-done:
				return false
			}
		}
		for {
			select {
			case <-c.shutdown:
				return
			case <-done:
				return
			case <-drainCh:
				drainCh = nil // consume the one-shot signal
				bound, first := c.takeDrain()
				if first && !announceDrain(bound) {
					return
				}
			case in := <-incomingCh:
				mh, p, err := in.header, in.payload, in.err
				if err != nil {
					st, ok := status.FromError(err)
					if !ok {
						recvErr <- err
						return
					}

					// in this case, we send an error for that particular message
					// when the status is defined.
					if !sendStatus(mh.StreamID, st) {
						return
					}

					continue
				}

				if mh.Type == messageTypeControl {
					if mh.StreamID != controlStreamID {
						if !sendStatus(mh.StreamID, status.Newf(codes.InvalidArgument, "control frames must use stream id 0")) {
							return
						}
						continue
					}
					ctrl, cerr := unmarshalControl(p)
					if p != nil {
						ch.putmbuf(p)
					}
					if cerr != nil {
						// Malformed or unknown control frames must never
						// poison RPC traffic; ignore them.
						log.G(ctx).WithError(cerr).Error("ttrpc: failed to unmarshal control frame")
						continue
					}
					switch ctrl.GetType() {
					case Control_SETTINGS:
						if supportsGracefulDrain(ctrl) {
							c.peerGracefulDrain.Store(true)
							if c.localGracefulDrain {
								select {
								case responses <- response{
									id:      controlStreamID,
									control: marshalSettingsControl(featureGracefulDrain),
								}:
								case <-c.shutdown:
									return
								case <-done:
									return
								}
							}
						}
					case Control_DRAIN:
						// Only the server sends DRAIN; ignore frames from the
						// client to remain forward compatible.
					}
					continue
				}

				if mh.StreamID%2 != 1 {
					// enforce odd client initiated identifiers.
					if !sendStatus(mh.StreamID, status.Newf(codes.InvalidArgument, "StreamID must be odd for client initiated streams")) {
						return
					}
					continue
				}

				if mh.Type == messageTypeData {
					i, ok := streams.Load(mh.StreamID)
					if !ok {
						if !sendStatus(mh.StreamID, status.Newf(codes.InvalidArgument, "StreamID is no longer active")) {
							return
						}
						continue
					}
					sh, isStreamHandler := i.(*streamHandler)
					if !isStreamHandler {
						if !sendStatus(mh.StreamID, status.Newf(codes.InvalidArgument, "data is not allowed on a unary stream")) {
							return
						}
						continue
					}
					if mh.Flags&flagNoData != flagNoData {
						unmarshal := func(obj any) error {
							err := protoUnmarshal(p, obj)
							ch.putmbuf(p)
							return err
						}

						if err := sh.data(unmarshal); err != nil {
							if !sendStatus(mh.StreamID, status.Newf(codes.InvalidArgument, "data handling error: %v", err)) {
								return
							}
							continue
						}
					}

					if mh.Flags&flagRemoteClosed == flagRemoteClosed {
						sh.closeSend()
						if len(p) > 0 {
							if !sendStatus(mh.StreamID, status.Newf(codes.InvalidArgument, "data close message cannot include data")) {
								return
							}
							continue
						}
					}
				} else if mh.Type == messageTypeRequest {
					if rejection := c.newStreamAllowed(mh.StreamID); rejection != nil {
						if !sendStatus(mh.StreamID, rejection) {
							return
						}
						if p != nil {
							ch.putmbuf(p)
						}
						continue
					}

					// TODO: Make request type configurable
					// Unmarshaller which takes in a byte array and returns an interface?
					var req Request
					if err := c.server.codec.Unmarshal(p, &req); err != nil {
						ch.putmbuf(p)
						if !sendStatus(mh.StreamID, status.Newf(codes.InvalidArgument, "unmarshal request error: %v", err)) {
							return
						}
						continue
					}
					ch.putmbuf(p)

					id := mh.StreamID
					respond := func(status *status.Status, data []byte, streaming, closeStream bool) error {
						select {
						case responses <- response{
							id:          id,
							status:      status,
							data:        data,
							closeStream: closeStream,
							streaming:   streaming,
						}:
						case <-done:
							return ErrClosed
						}
						return nil
					}
					sh, err := c.server.services.handle(ctx, &req, respond)
					if err != nil {
						status, _ := status.FromError(err)
						if !sendStatus(mh.StreamID, status) {
							return
						}
						continue
					}

					// Track every accepted request so draining waits for
					// completion. Unary handlers return a nil stream
					// handler; use a sentinel so a stray data frame cannot
					// trigger a nil type assertion.
					tracked := any(sh)
					if sh == nil {
						tracked = unaryStreamSentinel{}
					}
					streams.Store(id, tracked)
					atomic.AddInt32(&active, 1)
				}
				// TODO: else we must ignore this for future compat. log this?
			}
		}
	}(recvErr)

	for {
		var (
			newstate connState
			shutdown chan struct{}
			drainCh  <-chan struct{}
		)

		wasActive := state == connStateActive
		activeN := atomic.LoadInt32(&active)
		if activeN > 0 {
			newstate = connStateActive
			shutdown = nil
		} else {
			newstate = connStateIdle
			shutdown = c.shutdown // only enable this branch in idle mode
			// When idle and draining, take priority over shutdown so the
			// DRAIN frame is sent before the connection can be closed idle.
			if !drainHandled && isDraining(c.drainSignal) {
				drainCh = c.drainSignal
			}
		}
		if newstate != state {
			c.setState(newstate)
			state = newstate
			if wasActive && newstate == connStateIdle {
				c.server.notifyIdle()
			}
		}

		// Priority processing of a drain signal while idle: it is
		// drained from the channel once and, when the receive processor
		// has not already emitted a DRAIN frame, take the boundary and
		// announce it before shutdown can close the idle connection.
		if drainCh != nil {
			select {
			case <-drainCh:
				drainCh = nil
				drainHandled = true
				if bound, first := c.takeDrain(); first &&
					c.localGracefulDrain && c.peerGracefulDrain.Load() {
					if err := ch.send(controlStreamID, messageTypeControl, 0,
						marshalDrainControl(bound)); err != nil {
						log.G(ctx).WithError(err).Error("ttrpc: failed sending drain control frame")
						return
					}
				}
				c.drainAckOnce.Do(func() { close(c.drainAck) })
				continue
			default:
			}
		}

		select {
		case response := <-responses:
			if response.control != nil {
				if err := ch.send(response.id, messageTypeControl, 0, response.control); err != nil {
					log.G(ctx).WithError(err).Error("ttrpc: failed sending control frame")
					return
				}
				if response.drainAck {
					c.drainAckOnce.Do(func() { close(c.drainAck) })
				}
				continue
			}
			if !response.streaming || response.status.Code() != codes.OK {
				p, err := c.server.codec.Marshal(&Response{
					Status:  response.status.Proto(),
					Payload: response.data,
				})
				if err != nil {
					log.G(ctx).WithError(err).Error("failed marshaling response")
					return
				}

				if err := ch.send(response.id, messageTypeResponse, 0, p); err != nil {
					log.G(ctx).WithError(err).Error("failed sending message on channel")
					return
				}
			} else {
				var flags uint8
				if response.closeStream {
					flags = flagRemoteClosed
				}
				if response.data == nil {
					flags = flags | flagNoData
				}
				if err := ch.send(response.id, messageTypeData, flags, response.data); err != nil {
					log.G(ctx).WithError(err).Error("failed sending message on channel")
					return
				}
			}

			if response.closeStream {
				// The ttrpc protocol currently does not support the case where
				// the server is localClosed but not remoteClosed. Once the server
				// is closing, the whole stream may be considered finished
				if _, loaded := streams.LoadAndDelete(response.id); loaded {
					atomic.AddInt32(&active, -1)
				}
			}
		case err := <-recvErr:
			// TODO(stevvooe): Not wildly clear what we should do in this
			// branch. Basically, it means that we are no longer receiving
			// requests due to a terminal error.
			recvErr = nil // connection is now "closing"
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET) {
				// The client went away and we should stop processing
				// requests, so that the client connection is closed
				return
			}
			log.G(ctx).WithError(err).Error("error receiving message")
			// else, initiate shutdown
		case <-shutdown:
			return
		}
	}
}

func getRequestContext(ctx context.Context, req *Request) (retCtx context.Context, cancel func()) {
	if len(req.Metadata) > 0 {
		md := MD{}
		md.fromRequest(req)
		ctx = WithMetadata(ctx, md)
	}

	if req.TimeoutNano == 0 {
		// Cancellable so handlers' deferred cancel propagates to RecvMsg.
		return context.WithCancel(ctx)
	}

	return context.WithTimeout(ctx, time.Duration(req.TimeoutNano))
}

// isDraining reports whether the one-shot drain signal channel has been
// closed.
func isDraining(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

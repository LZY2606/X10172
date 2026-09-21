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

	mu           sync.Mutex
	listeners    map[net.Listener]struct{}
	connections  map[*serverConn]struct{} // all connections to current state
	done         chan struct{}            // marks point at which we stop serving requests
	shutdownOnce sync.Once                // guards close of done
	connChanged  chan struct{}            // signaled when the connection set may have changed
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
		listeners:   make(map[net.Listener]struct{}),
		connections: make(map[*serverConn]struct{}),
		connChanged: make(chan struct{}, 1),
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
	s.beginShutdown()

	if s.config.gracefulDrain {
		s.announceDrain()
	}

	s.mu.Lock()
	lnerr := s.closeListeners()
	s.mu.Unlock()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		s.closeIdleConns()

		if s.countConnection() == 0 {
			break
		}

		select {
		case <-s.connChangedSignal():
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}

	return lnerr
}

// Drain performs a graceful, connection-level shutdown of the server. It
// stops accepting new connections and announces a per-connection last
// accepted stream ID to every peer that negotiated the drain protocol, then
// waits until all RPCs started before each boundary have completed (or ctx is
// cancelled). Calls with stream IDs after a boundary fail with a stable
// codes.Unavailable rejection instead of racing with connection closure.
//
// Drain is idempotent: repeated and concurrent invocations share a single
// drain and return once it has completed.
func (s *Server) Drain(ctx context.Context) error {
	return s.Shutdown(ctx)
}

// beginShutdown marks the server as shutting down exactly once.
func (s *Server) beginShutdown() {
	s.shutdownOnce.Do(func() {
		s.mu.Lock()
		close(s.done)
		s.mu.Unlock()
	})
}

// isShutdown reports whether shutdown (including Drain) has begun.
func (s *Server) isShutdown() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// notifyConnChanged wakes any Shutdown waiter after the connection set may
// have changed. Sends are non-blocking and a fresh signal is installed by the
// waiter when it drains the channel, so notifications cannot be lost.
func (s *Server) notifyConnChanged() {
	select {
	case s.connChanged <- struct{}{}:
	default:
	}
}

// connChangedSignal returns a receive channel drained after a wakeup so that
// signals arriving between the connection count check and the select are not
// lost.
func (s *Server) connChangedSignal() <-chan struct{} {
	ch := s.connChanged
	select {
	case <-ch:
	default:
	}
	return ch
}

// announceDrain asks every active connection to publish its drain boundary.
// Connections whose peer did not negotiate the capability are left untouched
// and continue to be handled by the regular idle-connection shutdown path.
func (s *Server) announceDrain() {
	s.mu.Lock()
	conns := make([]*serverConn, 0, len(s.connections))
	for c := range s.connections {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	for _, c := range conns {
		c.beginDrain()
	}
}

// Close the server without waiting for active connections.
func (s *Server) Close() error {
	s.beginShutdown()

	s.mu.Lock()
	defer s.mu.Unlock()

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
	delete(s.connections, c)
	s.mu.Unlock()
	s.notifyConnChanged()
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
		// Connections with a drain-capable peer close themselves once idle
		// after publishing their boundary; force-closing here would cut
		// streams whose final responses are still being delivered.
		if c.drainStarted.Load() && c.peerDrain.Load() {
			continue
		}
		c.close()
		delete(s.connections, c)
	}
}

type connState int

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
		server:       s,
		conn:         conn,
		handshake:    handshake,
		shutdown:     make(chan struct{}),
		drainSignals: make(chan struct{}, 1),
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

	// drainSignals carries publish requests for the graceful drain
	// boundary to the connection's frame processing goroutine.
	drainSignals chan struct{}

	// peerDrain records whether the graceful drain protocol was
	// negotiated with the peer; only capable peers receive drain frames.
	peerDrain    atomic.Bool
	drainStarted atomic.Bool
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

	return nil
}

// beginDrain asks this connection to publish its drain boundary exactly once.
// It is idempotent: repeated starts and concurrent Shutdown/Drain calls
// coalesce onto a single boundary publication.
func (c *serverConn) beginDrain() {
	if !c.drainStarted.CompareAndSwap(false, true) {
		return
	}
	select {
	case c.drainSignals <- struct{}{}:
	default:
	}
}

func (c *serverConn) run(sctx context.Context) {
	type (
		response struct {
			id          uint32
			status      *status.Status
			data        []byte
			closeStream bool
			streaming   bool
		}

		controlOut struct {
			payload []byte
		}

		// rawFrame is a frame read from the wire that still needs protocol
		// processing. Splitting reading from processing lets the frame owner
		// observe drain signals without abandoning ordered frame delivery.
		rawFrame struct {
			header messageHeader
			p      []byte
		}
	)

	var (
		ch                    = newChannel(c.conn)
		ctx, cancel           = context.WithCancel(sctx)
		state       connState = connStateIdle
		outbound              = make(chan any)
		frames                = make(chan rawFrame)
		recvErr               = make(chan error, 1)
		done                  = make(chan struct{})
		streams               = sync.Map{}
		active      int32
	)

	defer c.conn.Close()
	defer cancel()
	defer close(done)
	defer c.server.delConnection(c)

	sendStatus := func(id uint32, st *status.Status) bool {
		select {
		case outbound <- response{
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

	// reader owns the connection read side. It only parses frames; all
	// protocol decisions, including stream-id boundary enforcement and drain
	// publication, happen in the processor so they stay strictly ordered with
	// request frames.
	go func() {
		defer close(recvErr)
		for {
			select {
			case <-c.shutdown:
				return
			case <-done:
				return
			default: // proceed
			}

			header, p, err := ch.recv()
			if err != nil {
				st, ok := status.FromError(err)
				if !ok {
					recvErr <- err
					return
				}

				// in this case, we send an error for that particular message
				// when the status is defined.
				if !sendStatus(header.StreamID, st) {
					return
				}

				continue
			}

			select {
			case frames <- rawFrame{header: header, p: p}:
			case <-c.shutdown:
				return
			case <-done:
				return
			}
		}
	}()

	// processor owns the streams map, last accepted stream id and the drain
	// boundary. Because request handling and drain publication run on this same
	// goroutine, the boundary is always observed at a precise point in the
	// ordered frame stream and can never move backwards.
	go func() {
		var lastStreamID uint32

		sendControl := func(payload []byte) bool {
			select {
			case outbound <- controlOut{payload: payload}:
				return true
			case <-c.shutdown:
				return false
			case <-done:
				return false
			}
		}

		publishDrain := func() {
			if !c.peerDrain.Load() {
				return
			}
			sendControl(encodeControlDrain(lastStreamID))
		}

		for {
			var f rawFrame
			select {
			case <-c.shutdown:
				return
			case <-done:
				return
			case <-c.drainSignals:
				publishDrain()
				continue
			case f = <-frames:
			}
			mh, p := f.header, f.p

			// Control frames are connection scoped and never carry RPC
			// state. Unknown types and frames on RPC streams are ignored so
			// older and newer peers interoperate safely.
			if mh.Type == messageTypeControl {
				if mh.StreamID == controlStreamID && c.server.config.gracefulDrain {
					if kind, fields, derr := decodeControlPayload(p); derr == nil && kind == controlHelloKind {
						features := controlFeature(fields[controlFieldU32])
						if features&controlFeatureGracefulDrain == controlFeatureGracefulDrain &&
							c.peerDrain.CompareAndSwap(false, true) {
							if !sendControl(encodeControlHello(controlFeatureGracefulDrain)) {
								return
							}
							// The server may already be draining when the
							// peer's Hello arrives; publish the boundary
							// immediately in that case.
							if c.drainStarted.Load() {
								publishDrain()
							}
						}
					}
				}
				ch.putmbuf(p)
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
				sh := i.(*streamHandler)
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
				if mh.StreamID <= lastStreamID {
					// enforce odd client initiated identifiers.
					if !sendStatus(mh.StreamID, status.Newf(codes.InvalidArgument, "StreamID cannot be re-used and must increment")) {
						return
					}
					continue
				}

				// Graceful drain boundary: requests already received when
				// the boundary was published complete normally; requests
				// after it get a stable Unavailable rejection.
				if c.drainStarted.Load() && c.peerDrain.Load() && mh.StreamID > lastStreamID {
					if !sendStatus(mh.StreamID, status.New(codes.Unavailable, drainingMessage)) {
						return
					}
					continue
				}

				lastStreamID = mh.StreamID

				// TODO: Make request type configurable
				// Unmarshaller which takes in a byte array to return an interface?
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
				respond := func(rst *status.Status, data []byte, streaming, closeStream bool) error {
					select {
					case outbound <- response{
						id:          id,
						status:      rst,
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
					rst, _ := status.FromError(err)
					if !sendStatus(mh.StreamID, rst) {
						return
					}
					continue
				}

				streams.Store(id, sh)
				atomic.AddInt32(&active, 1)
			}
			// Unknown message types are intentionally ignored for forward
			// compatibility with future protocol extensions.
		}
	}()

	for {
		var (
			newstate connState
			shutdown chan struct{}
		)

		activeN := atomic.LoadInt32(&active)
		if activeN > 0 {
			newstate = connStateActive
			shutdown = nil
		} else {
			newstate = connStateIdle
			shutdown = c.shutdown // only enable this branch in idle mode
		}
		// Once the drain boundary has been published to a capable peer, an
		// idle connection has no outstanding pre-boundary RPCs and can be
		// closed cleanly. Closing here (instead of from Shutdown) guarantees
		// that every already-accepted stream delivered its final status first.
		if activeN == 0 && c.drainStarted.Load() && c.peerDrain.Load() {
			return
		}
		if newstate != state {
			c.setState(newstate)
			state = newstate
		}

		select {
		case out := <-outbound:
			switch msg := out.(type) {
			case controlOut:
				if err := ch.send(controlStreamID, messageTypeControl, 0, msg.payload); err != nil {
					log.G(ctx).WithError(err).Error("failed sending control message on channel")
					return
				}
			case response:
				if !msg.streaming || msg.status.Code() != codes.OK {
					p, err := c.server.codec.Marshal(&Response{
						Status:  msg.status.Proto(),
						Payload: msg.data,
					})
					if err != nil {
						log.G(ctx).WithError(err).Error("failed marshaling response")
						return
					}

					if err := ch.send(msg.id, messageTypeResponse, 0, p); err != nil {
						log.G(ctx).WithError(err).Error("failed sending message on channel")
						return
					}
				} else {
					var flags uint8
					if msg.closeStream {
						flags = flagRemoteClosed
					}
					if msg.data == nil {
						flags = flags | flagNoData
					}
					if err := ch.send(msg.id, messageTypeData, flags, msg.data); err != nil {
						log.G(ctx).WithError(err).Error("failed sending message on channel")
						return
					}
				}

				if msg.closeStream {
					// The ttrpc protocol currently does not support the case where
					// the server is localClosed but not remoteClosed. Once the server
					// is closing, the whole stream may be considered finished
					streams.Delete(msg.id)
					if atomic.AddInt32(&active, -1) == 0 {
						c.setState(connStateIdle)
						state = connStateIdle
					}
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

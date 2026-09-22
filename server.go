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

	return s.waitConnections(ctx, lnerr)
}

// Drain initiates a graceful drain of all connections and waits until
// they have finished draining, or until ctx is done.
//
// Drain stops accepting new connections, like Shutdown. On each
// connection whose client negotiated the graceful drain capability,
// the server announces the last accepted client stream ID. Streams at
// or before that boundary are allowed to complete in their normal
// order; new calls after the boundary receive a stable Unavailable
// status (ErrConnectionDraining on capable clients) rather than
// depending on connection close timing. The connection is closed once
// every in-flight stream has finished.
//
// Connections whose client did not negotiate drain keep the legacy
// Shutdown behavior: active connections are left alone and idle
// connections are closed.
//
// Drain is idempotent and safe to call concurrently with itself, with
// Shutdown and with Close. Repeated calls share a single drain and
// return the same outcome.
func (s *Server) Drain(ctx context.Context) error {
	s.mu.Lock()
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	lnerr := s.closeListeners()
	s.drainConnectionsLocked()
	s.mu.Unlock()

	return s.waitConnections(ctx, lnerr)
}

// drainConnectionsLocked asks every registered connection to start
// draining. Connections added racing with shutdown are rejected by
// addConnection.
func (s *Server) drainConnectionsLocked() {
	for c := range s.connections {
		c.beginDrain()
	}
}

// waitConnections closes idle (legacy) connections and blocks until no
// tracked connections remain. Drain-announced connections are never
// force closed while idle so the client always receives the boundary.
func (s *Server) waitConnections(ctx context.Context, lnerr error) error {
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
		case <-ticker.C:
		}
	}

	return lnerr
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
		if c.isDraining() && !c.legacyPeer() {
			// Drain-capable connections must not be force closed
			// while idle: the client has to observe the drain
			// boundary before the connection goes away. Such a
			// connection closes itself once its last stream ends.
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
		server:    s,
		conn:      conn,
		handshake: handshake,
		shutdown:  make(chan struct{}),
		drainReq:  make(chan struct{}, 1),
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

	// drainReq is signaled to ask the receive goroutine to announce a
	// drain boundary. It is buffered so beginDrain never blocks even if
	// the connection is already exiting.
	drainReq chan struct{}

	// draining marks that drain was requested on this connection and
	// peerDrain that the peer advertised the drain capability.
	draining  atomic.Bool
	peerDrain atomic.Bool
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

// beginDrain asks the connection to announce a drain boundary and stop
// accepting new streams. It is a no-op after the first request and is
// safe to invoke any number of times concurrently.
func (c *serverConn) beginDrain() {
	if !c.draining.CompareAndSwap(false, true) {
		return
	}
	select {
	case c.drainReq <- struct{}{}:
	default:
	}
}

func (c *serverConn) isDraining() bool {
	return c.draining.Load()
}

func (c *serverConn) legacyPeer() bool {
	return !c.peerDrain.Load()
}

// drainComplete reports whether a drain-capable connection has
// announced its boundary and finished every in-flight call. It is
// evaluated in the main loop only after the boundary control frame and
// the terminal responses have been written to the wire.
func (c *serverConn) drainComplete(active, inFlight int32) bool {
	return c.isDraining() && c.peerDrain.Load() && active == 0 &&
		inFlight == 0
}

func (c *serverConn) run(sctx context.Context) {
	var (
		ch                      = newChannel(c.conn)
		ctx, cancel             = context.WithCancel(sctx)
		state         connState = connStateIdle
		responses               = make(chan connResponse)
		controls                = make(chan []byte)
		frames                  = make(chan connFrame)
		recvErr                 = make(chan error, 1)
		done                    = make(chan struct{})
		processorDone           = make(chan struct{})
		streams                 = sync.Map{}
		active        int32
		inFlight      int32
		lastStreamID  uint32
	)

	defer c.conn.Close()
	defer cancel()
	defer close(done)
	defer func() { <-processorDone }()
	defer c.server.delConnection(c)

	sendControl := func(p []byte) bool {
		select {
		case controls <- p:
			return true
		case <-c.shutdown:
			return false
		case <-done:
			return false
		}
	}

	sendStatus := func(id uint32, st *status.Status) bool {
		select {
		case responses <- connResponse{
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

	// reader only pulls frames off the wire. All protocol decisions,
	// including the drain boundary, happen in the processor so that a
	// frame already read is always ordered before a drain arm.
	go func() {
		for {
			select {
			case <-c.shutdown:
				return
			case <-done:
				return
			default: // proceed
			}

			mh, p, err := ch.recv()
			if err != nil {
				if _, ok := status.FromError(err); ok {
					// A grpc status error (e.g. oversized frame) is
					// reported back on the offending stream id by the
					// processor, matching the per-stream failure path.
					select {
					case frames <- connFrame{header: mh, payload: p, recvErr: err}:
					case <-done:
					}
					continue
				}

				select {
				case recvErr <- err:
				case <-done:
				}
				return
			}

			select {
			case frames <- connFrame{header: mh, payload: p}:
			case <-done:
				return
			}
		}
	}()

	// processor serializes stream id checks, drain boundary handling
	// and stream bookkeeping.
	go func() {
		defer close(processorDone)

		// drainBoundary is zero until draining is armed; new streams
		// with an id strictly greater than the boundary are refused.
		var drainBoundary uint32
		draining := false
		legacyDrain := false

		armDrain := func() {
			if draining {
				return
			}
			draining = true
			if !c.peerDrain.Load() {
				// The peer did not negotiate drain: preserve the
				// legacy Shutdown behavior, force closing only once
				// the connection is idle.
				legacyDrain = true
				return
			}
			drainBoundary = lastStreamID
			sendControl(marshalControl(controlV1{
				typ:          controlMessageDrainBegin,
				lastStreamID: drainBoundary,
			}))
		}

		handle := func(frame connFrame) {
			c.processFrame(ctx, frame, ch, &streams, &active,
				&inFlight, &lastStreamID, &drainBoundary, draining,
				sendStatus, responses)
		}

		for {
			select {
			case <-c.shutdown:
				return
			case <-done:
				return
			case <-c.drainReq:
				// A frame already pulled off the wire takes
				// precedence, so a received request can never end up
				// behind the boundary it should be part of. Drain is
				// armed in the same iteration after such a frame.
				select {
				case frame := <-frames:
					handle(frame)
				default:
				}
				armDrain()
			case frame := <-frames:
				handle(frame)
				if draining && legacyDrain && atomic.LoadInt32(&active) == 0 {
					return
				}
			}
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
		if newstate != state {
			c.setState(newstate)
			state = newstate
		}

		select {
		case response := <-responses:
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
				streams.Delete(response.id)
				activeRemaining := atomic.AddInt32(&active, -1)
				inFlightRemaining := atomic.AddInt32(&inFlight, -1)
				if c.drainComplete(activeRemaining, inFlightRemaining) {
					return
				}
			}
		case ctrl := <-controls:
			if err := ch.send(controlStreamID, messageTypeControl, 0, ctrl); err != nil {
				log.G(ctx).WithError(err).Error("failed sending control message on channel")
				return
			}
			if c.drainComplete(atomic.LoadInt32(&active), atomic.LoadInt32(&inFlight)) {
				return
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
	if userMetadata := withoutInternalMetadata(req.Metadata); len(userMetadata) > 0 {
		md := MD{}
		md.fromRequest(&Request{Metadata: userMetadata})
		ctx = WithMetadata(ctx, md)
	}

	if req.TimeoutNano == 0 {
		// Cancellable so handlers' deferred cancel propagates to RecvMsg.
		return context.WithCancel(ctx)
	}

	return context.WithTimeout(ctx, time.Duration(req.TimeoutNano))
}

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
	draining    chan struct{}            // marks the point at which graceful draining started

	// cond is signaled (Broadcast) whenever a connection is added or
	// removed, allowing Shutdown/Drain to wait for quiescence without
	// polling.
	cond *sync.Cond
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

	srv := &Server{
		config:      config,
		services:    newServiceSet(config.interceptor),
		done:        make(chan struct{}),
		draining:    make(chan struct{}),
		listeners:   make(map[net.Listener]struct{}),
		connections: make(map[*serverConn]struct{}),
	}
	srv.cond = sync.NewCond(&srv.mu)
	return srv, nil
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
	// Connections already idle have nothing left to drain; close them
	// once so they do not have to wait for their next state transition.
	s.closeIdleConnsLocked()
	waitErr := s.waitConnectionsLocked(ctx)
	s.mu.Unlock()

	if waitErr != nil {
		return waitErr
	}
	return lnerr
}

// Drain starts graceful draining of the server.
//
// Listeners are closed so no new connections are accepted. On every
// connection that negotiated graceful drain, the server announces the
// last stream id it accepts: streams at or before that boundary are
// allowed to finish with their normal request, data and response
// ordering, while later calls fail with a stable Unavailable drain
// rejection. Drain waits for the remaining connections to complete; it
// never forcibly cancels an in-flight rpc. If ctx is cancelled before
// quiescence, Drain returns the context error and leaves the remaining
// connections running.
//
// Starting a drain more than once, or calling Drain concurrently with
// Shutdown, is idempotent: all calls wait for the same quiescence point.
// On a server created without WithServerGracefulDrain, Drain behaves
// exactly like Shutdown, so callers do not need to know whether the
// feature was enabled.
func (s *Server) Drain(ctx context.Context) error {
	if !s.config.gracefulDrain {
		return s.Shutdown(ctx)
	}

	s.mu.Lock()
	select {
	case <-s.draining:
	default:
		close(s.draining)
	}
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	lnerr := s.closeListeners()
	for c := range s.connections {
		// Trigger the per-connection drain announcement. beginDrain is
		// idempotent, so concurrent Shutdown/Close races are harmless.
		c.beginDrain()
	}
	waitErr := s.waitConnectionsLocked(ctx)
	s.mu.Unlock()

	if waitErr != nil {
		return waitErr
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
	s.cond.Broadcast()

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
	s.cond.Broadcast()
	return nil
}

func (s *Server) delConnection(c *serverConn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.connections, c)
	s.cond.Broadcast()
}

func (s *Server) closeIdleConnsLocked() {
	for c := range s.connections {
		if st, ok := c.getState(); !ok || st == connStateActive {
			continue
		}
		c.close()
		delete(s.connections, c)
	}
}

// waitConnectionsLocked blocks until no connections remain. The caller
// must hold s.mu; the mutex is released while waiting and reacquired
// before returning. Connection additions and removals broadcast on
// s.cond.
func (s *Server) waitConnectionsLocked(ctx context.Context) error {
	if ctx.Done() == nil {
		for len(s.connections) > 0 {
			s.cond.Wait()
		}
		return nil
	}

	// Wake the waiter when ctx is cancelled. The goroutine exits once
	// the wait returns (either quiescence or ctx cancellation).
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			s.mu.Lock()
			s.cond.Broadcast()
			s.mu.Unlock()
		case <-stop:
		}
	}()

	for len(s.connections) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		s.cond.Wait()
	}
	return nil
}

// isDraining reports whether a graceful drain has been started.
func (s *Server) isDraining() bool {
	select {
	case <-s.draining:
		return true
	default:
		return false
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

	// drainReq is signaled at most once (see beginDrain) to ask the
	// connection's run loop to announce the drain boundary.
	drainReq chan struct{}
	drainOnce sync.Once

	// drainCapable is set from the receive goroutine when the client
	// advertises the drain feature in request metadata. Only then may a
	// control frame be sent.
	drainCapable atomic.Bool

	// lastStreamID is the highest request stream id received so far and
	// drainBoundary is the announced last accepted stream id. Both are
	// accessed by the receive goroutine and the run loop.
	lastStreamID  atomic.Uint32
	drainBoundary atomic.Uint32
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

// beginDrain requests the connection to announce a drain boundary. It is
// idempotent: repeated calls, including concurrent Shutdown and Drain,
// collapse onto a single announcement.
func (c *serverConn) beginDrain() {
	c.drainOnce.Do(func() {
		select {
		case c.drainReq <- struct{}{}:
		default:
		}
	})
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
	)

	var (
		ch                     = newChannel(c.conn)
		ctx, cancel            = context.WithCancel(sctx)
		state        connState = connStateIdle
		responses              = make(chan response)
		recvErr                = make(chan error, 1)
		done                   = make(chan struct{})
		streams                = sync.Map{}
		active int32
	)
	var draining bool

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

	go func(recvErr chan error) {
		defer close(recvErr)
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
				status, ok := status.FromError(err)
				if !ok {
					recvErr <- err
					return
				}

				// in this case, we send an error for that particular message
				// when the status is defined.
				if !sendStatus(mh.StreamID, status) {
					return
				}

				continue
			}

			if mh.StreamID%2 != 1 {
				// Control frames are connection scoped and use stream id 0,
				// which is even.
				if mh.Type == messageTypeControl && mh.StreamID == controlStreamID {
					// A client never initiates control frames in this
					// version; discard the payload and ignore the frame so
					// peers with other or future control items do not poison
					// the connection.
					if p != nil {
						ch.putmbuf(p)
					}
					continue
				}
				// enforce odd client initiated identifiers.
				if !sendStatus(mh.StreamID, status.Newf(codes.InvalidArgument, "StreamID must be odd for client initiated streams")) {
					return
				}
				continue
			}

			if mh.Type == messageTypeControl {
				// Control frames on an rpc stream id are invalid.
				if p != nil {
					ch.putmbuf(p)
				}
				if !sendStatus(mh.StreamID, status.Newf(codes.InvalidArgument, "control messages are connection scoped")) {
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
				if mh.StreamID <= c.lastStreamID.Load() {
					// enforce odd client initiated identifiers.
					if !sendStatus(mh.StreamID, status.Newf(codes.InvalidArgument, "StreamID cannot be re-used and must increment")) {
						return
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

				if c.server.config.gracefulDrain && requestAdvertisesDrain(&req) {
					c.drainCapable.Store(true)
				}

				// Reject streams created beyond the drain boundary. The
				// boundary is a snapshot of lastStreamID taken by
				// beginDrain, so this frame was either accepted before the
				// announcement or it deterministically fails here with the
				// stable drain status; acceptance never depends on close
				// timing.
				if boundary, draining := c.drainState(); draining && mh.StreamID > boundary {
					if !sendStatus(mh.StreamID, drainingStatus(boundary)) {
						return
					}
					continue
				}
				c.lastStreamID.Store(mh.StreamID)

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

				streams.Store(id, sh)
				atomic.AddInt32(&active, 1)
			}
			// TODO: else we must ignore this for future compat. log this?
		}
	}(recvErr)

	for {
		var (
			newstate connState
			shutdown chan struct{}
		)

		// Once draining has been announced and no stream is active, this
		// connection has nothing left to do.
		if draining && atomic.LoadInt32(&active) == 0 {
			return
		}

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
		case <-c.drainReq:
			announced, err := c.announceDrain(ch)
			if err != nil {
				log.G(ctx).WithError(err).Error("failed sending drain control message")
				return
			}
			// Only drain-aware clients are rejected at the boundary;
			// connections that did not negotiate the feature keep the
			// historical shutdown semantics.
			if announced {
				draining = true
			} else if activeN == 0 {
				// Non-negotiated idle connection: nothing to wait for,
				// behave like an idle connection under Shutdown.
				return
			}
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
				atomic.AddInt32(&active, -1)
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
		case <-c.server.done:
			// The server has been asked to stop. An idle connection has
			// no outstanding work and exits on its own; active
			// connections keep completing their streams and exit when
			// they next become idle.
			if activeN == 0 {
				return
			}
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

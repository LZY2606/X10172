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
	draining    bool                     // marks point at which connections are drained
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

// Drain gracefully drains all current and future connections to the server.
//
// For each connection, the server publishes the stream id of the last
// request it has accepted. Requests already received at or below that
// boundary are allowed to complete; any new request past the boundary is
// rejected with a stable Unavailable status which drain-capable clients
// observe as ErrDraining. Once a connection has no outstanding requests it
// is closed. Drain blocks until every connection has finished or ctx is
// cancelled.
//
// Drain is idempotent and safe to call concurrently with Shutdown: the
// drain boundary of a connection is fixed the first time drain takes
// effect and never moves backwards. Connections accepted after Drain has
// started are drained immediately.
func (s *Server) Drain(ctx context.Context) error {
	s.mu.Lock()
	s.draining = true
	conns := make([]*serverConn, 0, len(s.connections))
	for c := range s.connections {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	for _, c := range conns {
		c.drain()
	}

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		if s.countConnection() == 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
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
	if s.draining {
		// Connections accepted after drain started are born drained:
		// their boundary is empty and every request is rejected.
		c.drain()
	}
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
		drainCh:   make(chan struct{}),
		controls:  make(chan []byte, 4),
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

	lastStreamID atomic.Uint32 // highest client initiated stream id received

	drainOnce sync.Once
	drainCh   chan struct{}  // closed once drain takes effect on this connection
	boundary  atomic.Uint32  // last accepted stream id, valid once drainCh is closed
	controls  chan []byte    // outbound control frames queued for the run loop
	peerDrain atomic.Bool    // peer announced drain capability
}

// drain initiates a graceful drain of the connection. The drain boundary is
// fixed to the highest stream id received so far and never moves afterwards,
// making repeated calls idempotent.
func (c *serverConn) drain() {
	c.drainOnce.Do(func() {
		c.boundary.Store(c.lastStreamID.Load())
		close(c.drainCh)
	})
}

func (c *serverConn) isDraining() bool {
	select {
	case <-c.drainCh:
		return true
	default:
		return false
	}
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
		active       int32
		drainNotify            = c.drainCh
	)

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

			if mh.Type == messageTypeControl {
				// Connection-level control frames live on stream id 0 and
				// are handled out of band. Unknown kinds are ignored for
				// forward compatibility.
				kind, body, err := decodeControl(p)
				if p != nil {
					ch.putmbuf(p)
				}
				if err == nil && kind == controlKindCapability && len(body) >= 1 && body[0]&capabilityDrain != 0 {
					c.peerDrain.Store(true)
					if c.server.config.drain {
						select {
						case c.controls <- encodeControlCapability(capabilityDrain):
						default:
						}
					}
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
				c.lastStreamID.Store(mh.StreamID)

				if c.isDraining() && mh.StreamID > c.boundary.Load() {
					// The request arrived after the drain boundary: reject
					// it with a stable status instead of dropping the
					// connection, so the client does not have to guess.
					if !sendStatus(mh.StreamID, status.New(codes.Unavailable, drainStatusMessage)) {
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
		if newstate == connStateIdle && c.isDraining() {
			// The connection is drained and has no outstanding requests:
			// close it so the peer observes a clean end of the drain.
			return
		}

		select {
		case cf := <-c.controls:
			if err := ch.send(0, messageTypeControl, 0, cf); err != nil {
				log.G(ctx).WithError(err).Error("failed sending control frame")
				return
			}
		case <-drainNotify:
			// Publish the drain boundary to capable peers exactly once.
			if c.peerDrain.Load() {
				if err := ch.send(0, messageTypeControl, 0, encodeControlDrain(c.boundary.Load())); err != nil {
					log.G(ctx).WithError(err).Error("failed sending drain control frame")
					return
				}
			}
			drainNotify = nil
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

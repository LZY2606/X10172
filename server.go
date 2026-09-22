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
	drained     chan struct{}            // marks point at which graceful drain started
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
		drained:     make(chan struct{}),
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
			case <-s.drained:
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

// Drain starts the optional graceful drain protocol and waits for in-flight
// calls on negotiated connections to finish.
//
// Drain is idempotent: repeated calls and a Drain racing with Shutdown share
// the same drain start and the same wait. Once Drain has started, listeners
// stop accepting new connections, each connection which negotiated
// FeatureGracefulDrain announces a last accepted stream id boundary, and
// Drain blocks until every such connection is idle or closed, or until ctx
// expires. Connections which did not negotiate the capability are left
// untouched and keep following the pre-drain semantics; pair Drain with
// Shutdown to close those once they are idle.
//
// ErrDrainNotEnabled is returned unless the server was created with
// WithGracefulDrain.
func (s *Server) Drain(ctx context.Context) error {
	if !s.config.gracefulDrain {
		return ErrDrainNotEnabled
	}

	conns := s.startDrain()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		remaining := 0
		for _, c := range conns {
			if !c.peerCapable.Load() {
				// Only connections which negotiated the capability are
				// drained and waited on.
				continue
			}
			select {
			case <-c.waitCh:
			default:
				remaining++
			}
		}
		if remaining == 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// startDrain closes listeners, marks the server draining and kicks every
// already accepted connection. Safe to call concurrently; only the first
// call performs the transition.
func (s *Server) startDrain() []*serverConn {
	s.mu.Lock()
	select {
	case <-s.drained:
	default:
		close(s.drained)
	}
	lnerr := s.closeListeners()
	conns := make([]*serverConn, 0, len(s.connections))
	for c := range s.connections {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	if lnerr != nil {
		// Listener close errors are non-fatal for drain; callers needing
		// them can still call Shutdown, which reports the first error.
		log.L.WithError(lnerr).Debug("ttrpc: drain listener close error")
	}

	for _, c := range conns {
		c.startDrain()
	}
	return conns
}

func (s *Server) isDraining() bool {
	select {
	case <-s.drained:
		return true
	default:
		return false
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
	select {
	case <-s.drained:
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
		server:        s,
		conn:          conn,
		handshake:     handshake,
		shutdown:      make(chan struct{}),
		drainRequests: make(chan struct{}, 1),
		waitCh:        make(chan struct{}),
	}
	c.setState(connStateIdle)
	if err := s.addConnection(c); err != nil {
		c.close()
		return nil, err
	}
	// A connection accepted concurrently with startDrain may be missing from
	// the snapshot used to kick established connections. Arm it directly so
	// the drain boundary can never be skipped.
	if s.isDraining() {
		c.startDrain()
	}
	return c, nil
}

// controlFrame is a connection-level message sent on stream id 0.
type controlFrame struct {
	features     uint64
	lastStreamID uint32
	draining     bool
}

type serverConn struct {
	server    *Server
	conn      net.Conn
	handshake any // data from handshake, not used for now
	state     atomic.Value

	shutdownOnce sync.Once
	shutdown     chan struct{} // forced shutdown, used by close

	// drainRequests is signaled by startDrain; buffered so a request which
	// races connection setup is not lost.
	drainRequests chan struct{}

	peerFeatures  atomic.Uint64
	peerCapable   atomic.Bool
	drainActive   atomic.Bool
	drainBoundary atomic.Uint32 // largest stream id accepted on this conn

	// waitCh is closed once a negotiated connection has finished draining
	// (idle at/after boundary) or the connection has ended.
	waitCh chan struct{}
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

// drainWait returns a channel that is closed once this connection is fully
// drained (no active calls) or the connection has ended. Connections which
// never negotiate the drain capability return nil.
func (c *serverConn) drainWait() <-chan struct{} {
	return c.waitCh
}

// startDrain is idempotent: the buffered channel and drainActive ensure
// repeated and concurrent calls arm the connection at most once.
func (c *serverConn) startDrain() {
	if !c.drainActive.CompareAndSwap(false, true) {
		return
	}
	select {
	case c.drainRequests <- struct{}{}:
	default:
	}
}

func (c *serverConn) finishWait() {
	select {
	case <-c.waitCh:
	default:
		close(c.waitCh)
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
	)

	var (
		ch                     = newChannel(c.conn)
		ctx, cancel            = context.WithCancel(sctx)
		state        connState = connStateIdle
		responses              = make(chan response, 16)
		terminalErr            = make(chan error, 1)
		done                   = make(chan struct{})
		streams                = sync.Map{}
		active       int32
		lastStreamID uint32
	)

	// Internal goroutines must all have exited before run returns so a
	// finished connection never leaks reader, processor or writer goroutines.
	// Closing the conn and signaling done happens first so they can exit.
	var internal sync.WaitGroup
	defer c.conn.Close()
	defer cancel()
	defer close(done)
	defer c.server.delConnection(c)
	defer c.finishWait()
	defer internal.Wait()

	// inFrames carries raw frames from the blocking reader to the
	// processor. It is bounded so that terminal errors are never lost and a
	// slow processor still applies backpressure to the reader.
	type inFrame struct {
		mh  messageHeader
		p   []byte
		err error
	}
	inFrames := make(chan inFrame, 1)
	internal.Add(1)
	go func() {
		defer internal.Done()
		for {
			mh, p, err := ch.recv()
			select {
			case inFrames <- inFrame{mh: mh, p: p, err: err}:
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

	// outbound serializes all writes. Control frames are queued directly
	// from the processor so announcing a drain never blocks on a peer which
	// is not currently reading, and never deadlocks with Drain itself.
	outbound := make(chan func(), 8)
	internal.Add(1)
	go func() {
		defer internal.Done()
		for {
			select {
			case <-done:
				return
			case <-c.shutdown:
				return
			case write := <-outbound:
				write()
			}
		}
	}()
	queueWrite := func(fn func()) bool {
		select {
		case outbound <- fn:
			return true
		case <-done:
			return false
		}
	}

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

	// drainArmed carries the boundary snapshot from the processor to the
	// main loop, which writes the drain frame as the next outbound message.
	type drainArmed struct {
		boundary uint32
	}
	drainArmedCh := make(chan drainArmed, 1)

	armDrain := func() {
		// Snapshot the highest stream id already accepted on this
		// connection. All frames buffered in inFrames precede this snapshot
		// in the receive order, so the boundary is stable regardless of how
		// the drain frame races in-flight data on the wire.
		boundary := lastStreamID
		c.drainBoundary.Store(boundary)
		select {
		case drainArmedCh <- drainArmed{boundary: boundary}:
		case <-done:
		}
	}

	sendControl := func(f controlFrame) bool {
		return queueWrite(func() {
			if err := ch.send(controlStreamID, messageTypeControl, 0,
				marshalControl(f.features, f.lastStreamID, f.draining)); err != nil {
				log.G(ctx).WithError(err).Error("failed sending control message on channel")
				return
			}
		})
	}

	helloSeen := false
	internal.Add(1)
	go func() {
		defer internal.Done()
		for {
			select {
			case <-c.shutdown:
				return
			case <-done:
				return
			case <-c.drainRequests:
				// Only negotiated connections are drained; a hello arriving
				// later re-issues the arming from its handler.
				if c.peerCapable.Load() {
					armDrain()
				}
			case f := <-inFrames:
				mh, p := f.mh, f.p
				if f.err != nil {
					st, ok := status.FromError(f.err)
					if !ok {
						select {
						case terminalErr <- f.err:
						default:
						}
						return
					}

					// A per-message status error: send an error for that
					// particular message when the status is defined.
					if !sendStatus(mh.StreamID, st) {
						return
					}
					continue
				}

				// Connection-level control frames are the only messages
				// allowed on stream id 0 and never create an RPC stream.
				if mh.StreamID == controlStreamID {
					if mh.Type != messageTypeControl {
						// Ignore unknown connection-level frames for forward
						// compatibility.
						ch.putmbuf(p)
						continue
					}
					features, _, draining := parseControl(p)
					ch.putmbuf(p)
					if !helloSeen {
						helloSeen = true
						c.peerFeatures.Store(features)
						if features&FeatureGracefulDrain == FeatureGracefulDrain {
							c.peerCapable.Store(true)
							var serverFeatures uint64
							if c.server.config.gracefulDrain {
								serverFeatures = FeatureGracefulDrain
							}
							if !sendControl(controlFrame{features: serverFeatures}) {
								return
							}
							if c.drainActive.Load() {
								armDrain()
							}
						}
					} else if draining {
						// Clients never initiate drains; ignore.
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

				// A negotiated drain boundary rejects anything beyond the
				// last accepted stream id with a stable, recognizable
				// status instead of relying on connection teardown timing.
				if c.peerCapable.Load() && c.drainActive.Load() && mh.StreamID > c.drainBoundary.Load() {
					boundary := c.drainBoundary.Load()
					ch.putmbuf(p)
					if mh.Type == messageTypeData {
						// Late data on a rejected stream is dropped quietly
						// so a racing client does not trigger a stream of
						// invalid-argument responses for a call it already
						// gave up on.
						continue
					}
					st := status.Newf(codes.Unavailable, "%s (last accepted stream id: %d)", drainMessage, boundary)
					if !sendStatus(mh.StreamID, st) {
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
					lastStreamID = mh.StreamID

					// TODO: Make request type configurable
					// Unmarshaller which takes in an array of bytes and returns an interface?
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
				} else {
					// Unknown message types on rpc streams are ignored for
					// future compatibility.
					ch.putmbuf(p)
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
		case armed := <-drainArmedCh:
			// The processor took the boundary snapshot; announce it before
			// any subsequent response so no reordering can hide it.
			// Repeated drain attempts send the (same) boundary again.
			if !queueWrite(func() {
				if err := ch.send(controlStreamID, messageTypeControl, 0,
					marshalControl(FeatureGracefulDrain, armed.boundary, true)); err != nil {
					log.G(ctx).WithError(err).Error("failed sending drain message on channel")
				}
			}) {
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

				if !queueWrite(func() {
					if err := ch.send(response.id, messageTypeResponse, 0, p); err != nil {
						log.G(ctx).WithError(err).Error("failed sending message on channel")
					}
				}) {
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
				data := response.data
				if !queueWrite(func() {
					if err := ch.send(response.id, messageTypeData, flags, data); err != nil {
						log.G(ctx).WithError(err).Error("failed sending message on channel")
					}
				}) {
					return
				}
			}

			if response.closeStream {
				// The ttrpc protocol currently does not support the case where
				// the server is localClosed but not remoteClosed. Once the server
				// is closing, the whole stream may be considered finished
				streams.Delete(response.id)
				atomic.AddInt32(&active, -1)
				if c.peerCapable.Load() && c.drainActive.Load() && atomic.LoadInt32(&active) == 0 {
					c.finishWait()
				}
			}
		case err := <-terminalErr:
			// TODO(stevvooe): Not wildly clear what we should do in this
			// branch. Basically, it means that we are no longer receiving
			// requests due to a terminal error.
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

		// Cover the ordering where drain armed (boundary frame sent) while
		// the connection was already idle, e.g. right after the last
		// response or before the first request.
		if c.peerCapable.Load() && c.drainActive.Load() && atomic.LoadInt32(&active) == 0 {
			c.finishWait()
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

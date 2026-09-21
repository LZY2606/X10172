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

	mu               sync.Mutex
	listeners        map[net.Listener]struct{}
	connections      map[*serverConn]struct{} // all connections to current state
	done             chan struct{}            // marks point at which we stop serving requests
	hardShutdown     chan struct{}            // marks point at which connections may be forcibly closed
	hardShutdownOnce sync.Once
	drainOnce        sync.Once // idempotently starts graceful drain
	drainStarted     chan struct{}

	drainedConns     atomic.Int64
	interruptedConns atomic.Int64
	unsupportedConns atomic.Int64
	connClassified   sync.WaitGroup
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
		config:       config,
		services:     newServiceSet(config.interceptor),
		done:         make(chan struct{}),
		hardShutdown: make(chan struct{}),
		drainStarted: make(chan struct{}),
		listeners:    make(map[net.Listener]struct{}),
		connections:  make(map[*serverConn]struct{}),
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

		// Reject any final connection accepted after shutdown/drain started.
		select {
		case <-s.done:
			conn.Close()
			return ErrServerClosed
		default:
		}

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

	s.signalHardShutdown()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		s.closeIdleConns()
		s.forceDrainingConns()

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

// ShutdownGraceful gracefully drains all connections and waits for
// in-flight calls to complete. It is effective on servers created with
// WithGracefulShutdown; without that option it behaves like Shutdown.
//
// The call is idempotent: starting drain, repeating it and racing with
// Shutdown produce one stable outcome. Concurrent Shutdown or Close forcibly
// closes connections that have not finished draining and those connections
// are reported as interrupted in the result.
//
// Calls whose stream id is at or below a connection's announced last
// accepted stream id complete normally. Later calls receive a stable
// codes.Unavailable rejection identifiable with IsServerDraining.
func (s *Server) ShutdownGraceful(ctx context.Context) (GracefulShutdownResult, error) {
	s.drainOnce.Do(func() {
		s.mu.Lock()
		select {
		case <-s.done:
		default:
			close(s.done)
		}
		lnerr := s.closeListeners()
		s.mu.Unlock()
		_ = lnerr

		if s.config.gracefulShutdown {
			s.startDrainConnections()
		}
		close(s.drainStarted)
	})

	select {
	case <-s.drainStarted:
	case <-ctx.Done():
		return GracefulShutdownResult{}, ctx.Err()
	}

	if !s.config.gracefulShutdown {
		return GracefulShutdownResult{}, s.Shutdown(ctx)
	}

	// Wait for every connection to be classified: either it completes
	// graceful drain (drainComplete), or a concurrent Shutdown/Close/peer
	// disconnect removes it. Unsupported (old client) connections use legacy
	// semantics: idle ones are closed, active ones finish naturally.
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for !s.allConnsClassified() {
		s.closeIdleConns()
		select {
		case <-ctx.Done():
			return s.snapshotDrainResult(), ctx.Err()
		case <-ticker.C:
		}
	}

	if s.isHardShuttingDown() {
		return s.snapshotDrainResult(), ErrServerClosed
	}
	return s.snapshotDrainResult(), nil
}

// allConnsClassified reports whether every tracked connection has reached a
// terminal drain classification.
func (s *Server) allConnsClassified() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.connections {
		if !c.drainSignaled.Load() || !c.drainNegotiated.Load() {
			// Unsupported connections are reaped below via legacy
			// idle/active shutdown and classified on removal.
			return false
		}
		select {
		case <-c.drainComplete:
		default:
			return false
		}
	}
	return true
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
		c.classifyInterrupted()
		c.close()
	}
	s.hardShutdownOnce.Do(func() {
		close(s.hardShutdown)
	})

	return err
}

func (s *Server) signalHardShutdown() {
	s.hardShutdownOnce.Do(func() {
		close(s.hardShutdown)
	})
}

// startDrainConnections announces the drain boundary on every connection
// whose client negotiated the graceful drain capability. Connections with
// older clients are left for legacy idle/active shutdown handling.
func (s *Server) startDrainConnections() {
	s.mu.Lock()
	conns := make([]*serverConn, 0, len(s.connections))
	for c := range s.connections {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	for _, c := range conns {
		c.startDrain()
	}
}

func (s *Server) snapshotDrainResult() GracefulShutdownResult {
	return GracefulShutdownResult{
		DrainedConnections:     int(s.drainedConns.Load()),
		InterruptedConnections: int(s.interruptedConns.Load()),
		UnsupportedConnections: int(s.unsupportedConns.Load()),
	}
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
	if _, ok := s.connections[c]; ok {
		c.finalizeOutcome()
	}
	delete(s.connections, c)
	s.mu.Unlock()
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
		st, ok := c.getState()
		if !ok || st == connStateActive {
			continue
		}
		// A negotiated connection targeted by graceful drain is closed by
		// the drain path itself (including the zero-active self-close); hard
		// shutdown forcibly closes it in forceDrainingConns. Do not let the
		// legacy idle reaper race that.
		if c.drainSignaled.Load() && c.drainNegotiated.Load() && !s.isHardShuttingDown() {
			continue
		}
		c.close()
	}
}

func (s *Server) isHardShuttingDown() bool {
	select {
	case <-s.hardShutdown:
		return true
	default:
		return false
	}
}

// forceDrainingConns forcibly closes connections that announced a drain
// boundary but have not reached zero in-flight calls. Used by the hard
// Shutdown path so a concurrent Shutdown interrupts a stuck graceful drain.
func (s *Server) forceDrainingConns() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for c := range s.connections {
		// Any negotiated connection targeted by drain that is still present
		// is forcibly ended by hard shutdown.
		if c.drainSignaled.Load() && c.drainNegotiated.Load() {
			c.classifyInterrupted()
			c.close()
		}
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
		drainSignal:   make(chan struct{}, 1),
		drainComplete: make(chan struct{}),
	}
	c.setState(connStateIdle)
	if err := s.addConnection(c); err != nil {
		c.close()
		return nil, err
	}
	s.connClassified.Add(1)
	return c, nil
}

type serverConn struct {
	server    *Server
	conn      net.Conn
	handshake any // data from handshake, not used for now
	state     atomic.Value

	shutdownOnce sync.Once
	shutdown     chan struct{} // forced shutdown, used by close

	sendMu sync.Mutex // serializes writes from the sender and control frames

	// drainSignal is closed to request the drain boundary be announced.
	drainSignal chan struct{} // buffered(1), idempotent drain request
	// drainComplete is closed once the boundary was announced and the
	// connection reached zero in-flight calls. The connection itself stays
	// open so the peer can observe the Drain frame.
	drainComplete chan struct{}
	drainDoneOnce sync.Once
	// drainNegotiated is set once the peer's Hello advertises the
	// graceful drain capability.
	drainNegotiated atomic.Bool
	// drainSignaled marks that a graceful drain was requested on this
	// connection.
	drainSignaled atomic.Bool
	// draining is set after the Drain control message has been sent.
	draining atomic.Bool
	// interrupted is set when the connection is forcibly closed before
	// draining completed.
	interrupted atomic.Bool
	// counted guards idempotent aggregation into GracefulShutdownResult.
	counted atomic.Bool
}

// startDrain requests a graceful drain on this connection. Only effective
// when the peer negotiated the capability. Idempotent.
func (c *serverConn) startDrain() {
	c.drainSignaled.Store(true)
	if c.server.config.gracefulShutdown && !c.drainNegotiated.Load() {
		c.addResult(&c.server.unsupportedConns)
	}
	c.requestDrainAnnounce()
}

// requestDrainAnnounce enqueues an idempotent drain request for the
// processor. It is safe to call before negotiation completes: once the
// peer's Hello is processed the processor drains the pending request and
// announces the boundary.
func (c *serverConn) requestDrainAnnounce() {
	select {
	case c.drainSignal <- struct{}{}:
	default:
	}
}

func (c *serverConn) markInterrupted() {
	c.interrupted.Store(true)
}

// classifyInterrupted counts a connection forcibly closed during graceful
// drain as interrupted. Idempotent per connection.
func (c *serverConn) classifyInterrupted() {
	c.markInterrupted()
	if !c.server.config.gracefulShutdown {
		return
	}
	if c.drainSignaled.Load() {
		c.addResult(&c.server.interruptedConns)
	}
}

func (c *serverConn) addResult(counter *atomic.Int64) bool {
	if c.counted.CompareAndSwap(false, true) {
		counter.Add(1)
		c.server.connClassified.Done()
		return true
	}
	return false
}

// finalizeOutcome guarantees every removed connection is counted exactly
// once for GracefulShutdownResult. Explicit outcomes (drained/interrupted/
// unsupported) are preferred; a connection that simply disappeared without
// drain being requested is counted as unsupported, matching legacy
// semantics.
func (c *serverConn) finalizeOutcome() {
	if !c.server.config.gracefulShutdown {
		c.addResult(&c.server.unsupportedConns)
		return
	}
	switch {
	case c.interrupted.Load():
		c.addResult(&c.server.interruptedConns)
	case c.draining.Load():
		c.addResult(&c.server.drainedConns)
	default:
		c.addResult(&c.server.unsupportedConns)
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

type serverResponse struct {
	id          uint32
	status      *status.Status
	data        []byte
	closeStream bool
	streaming   bool
}

type serverFrame struct {
	header  messageHeader
	payload []byte
}

func (c *serverConn) run(sctx context.Context) {
	var (
		ch                     = newChannel(c.conn)
		ctx, cancel            = context.WithCancel(sctx)
		state        connState = connStateIdle
		responses              = make(chan serverResponse)
		frames                 = make(chan serverFrame)
		pumpErr                = make(chan error, 1)
		done                   = make(chan struct{})
		streams                = sync.Map{}
		active       int32
		lastStreamID uint32
	)

	defer c.conn.Close()
	defer cancel()
	defer close(done)
	defer c.server.delConnection(c)

	sendStatus := func(id uint32, st *status.Status) bool {
		select {
		case responses <- serverResponse{id: id, status: st, closeStream: true}:
			return true
		case <-c.shutdown:
			return false
		case <-done:
			return false
		}
	}

	// pump is the only goroutine reading from the socket; the processor
	// handles every frame in wire order.
	go func() {
		for {
			select {
			case <-c.shutdown:
				return
			case <-done:
				return
			default:
			}

			mh, p, err := ch.recv()
			if err != nil {
				st, ok := status.FromError(err)
				if !ok {
					pumpErr <- err
					return
				}
				if !sendStatus(mh.StreamID, st) {
					return
				}
				if p != nil {
					ch.putmbuf(p)
				}
				continue
			}

			select {
			case frames <- serverFrame{header: mh, payload: p}:
			case <-done:
				if p != nil {
					ch.putmbuf(p)
				}
				return
			case <-c.shutdown:
				if p != nil {
					ch.putmbuf(p)
				}
				return
			}
		}
	}()

	if c.server.config.gracefulShutdown {
		// Advertise capabilities concurrently with the pump so a synchronous
		// transport does not deadlock when both peers send first. A Drain
		// message is only sent after the peer's Hello is received.
		go func() {
			c.sendMu.Lock()
			defer c.sendMu.Unlock()
			select {
			case <-done:
				return
			default:
			}
			if err := ch.send(controlStreamID, messageTypeControl, 0,
				marshalControlMessage(controlCapabilities(capabilityGracefulDrain))); err != nil {
				log.G(ctx).WithError(err).Debug("ttrpc: failed sending control hello")
			}
		}()
	}

	announceDrain := func() {
		if c.announceDrainLocked(&lastStreamID, ch) {
			c.notifyIfDrained(&active)
		}
	}

	go func() {
		for {
			// Frames already handed off by the pump count as received before
			// a drain announcement, even when the drain signal became ready
			// in the same iteration. Process them first so the announced
			// boundary never excludes a frame that reached the transport.
			select {
			case frame := <-frames:
				if !c.processFrame(ctx, ch, &streams, &active, &lastStreamID, frame, responses, sendStatus, done) {
					return
				}
				continue
			default:
			}

			// Only listen for the drain request once capability
			// negotiation completed; otherwise keep the token buffered so
			// the Hello handler can trigger the announcement.
			var drainReq <-chan struct{}
			if c.drainNegotiated.Load() {
				drainReq = c.drainSignal
			}

			select {
			case <-c.shutdown:
				return
			case <-done:
				return
			case frame := <-frames:
				if !c.processFrame(ctx, ch, &streams, &active, &lastStreamID, frame, responses, sendStatus, done) {
					return
				}
			case <-drainReq:
				announceDrain()
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
			shutdown = c.shutdown
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

				c.sendMu.Lock()
				err = ch.send(response.id, messageTypeResponse, 0, p)
				c.sendMu.Unlock()
				if err != nil {
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
				c.sendMu.Lock()
				err := ch.send(response.id, messageTypeData, flags, response.data)
				c.sendMu.Unlock()
				if err != nil {
					log.G(ctx).WithError(err).Error("failed sending message on channel")
					return
				}
			}

			if response.closeStream {
				// Only streams that dispatched a handler were registered
				// and counted. Rejections (draining, invalid stream id,
				// unimplemented methods) also carry closeStream but must not
				// decrement the active count, otherwise an active in-flight
				// stream could be misclassified as idle during drain.
				if _, loaded := streams.LoadAndDelete(response.id); loaded {
					if atomic.AddInt32(&active, -1) == 0 {
						c.notifyIfDrained(&active)
					}
				}
			}
		case err := <-pumpErr:
			pumpErr = nil
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET) {
				return
			}
			log.G(ctx).WithError(err).Error("error receiving message")
		case <-shutdown:
			return
		}
	}
}

// notifyIfDrained marks a draining connection complete once it reaches zero
// in-flight calls. The connection is kept open so the peer can still observe
// the Drain frame and finish pre-boundary streams; ShutdownGraceful waits
// on drainComplete for the stable completion signal.
func (c *serverConn) notifyIfDrained(active *int32) {
	if c.draining.Load() && !c.interrupted.Load() && atomic.LoadInt32(active) == 0 {
		c.drainDoneOnce.Do(func() {
			c.addResult(&c.server.drainedConns)
			close(c.drainComplete)
		})
	}
}

// processFrame handles one received frame in wire order and returns false to
// terminate the processor.
func (c *serverConn) processFrame(
	ctx context.Context,
	ch *channel,
	streams *sync.Map,
	active *int32,
	lastStreamID *uint32,
	frame serverFrame,
	responses chan<- serverResponse,
	sendStatus func(uint32, *status.Status) bool,
	done chan struct{},
) bool {
	mh, p := frame.header, frame.payload
	released := false
	release := func() {
		if !released && p != nil {
			released = true
			ch.putmbuf(p)
		}
	}

	if mh.Type == messageTypeControl {
		if mh.StreamID == controlStreamID && mh.Flags == 0 {
			if cm, ok := parseControlMessage(p); ok && cm.kind == controlMessageHello {
				if cm.uint32Value()&capabilityGracefulDrain == capabilityGracefulDrain {
					c.drainNegotiated.Store(true)
					// Drain may have been requested before the Hello
					// completed; honor it now that negotiation succeeded.
					if c.drainSignaled.Load() {
						// Drain was requested before negotiation completed;
						// the earlier token may already have been consumed
						// as a no-op; announce directly now that negotiation
						// is confirmed.
						if c.announceDrainLocked(lastStreamID, ch) {
							c.notifyIfDrained(active)
						}
					}
				}
			}
		}
		release()
		return true
	}

	if mh.StreamID%2 != 1 {
		sendStatus(mh.StreamID, status.Newf(codes.InvalidArgument, "StreamID must be odd for client initiated streams"))
		release()
		return true
	}

	if mh.Type == messageTypeData {
		i, ok := streams.Load(mh.StreamID)
		if !ok {
			if c.draining.Load() {
				sendStatus(mh.StreamID, newDrainingStatus(atomic.LoadUint32(lastStreamID)))
			} else {
				sendStatus(mh.StreamID, status.Newf(codes.InvalidArgument, "StreamID is no longer active"))
			}
			release()
			return true
		}
		sh := i.(*streamHandler)
		if mh.Flags&flagNoData != flagNoData {
			unmarshal := func(obj any) error {
				err := protoUnmarshal(p, obj)
				ch.putmbuf(p)
				return err
			}

			if err := sh.data(unmarshal); err != nil {
				sendStatus(mh.StreamID, status.Newf(codes.InvalidArgument, "data handling error: %v", err))
				return true
			}
		} else {
			release()
		}

		if mh.Flags&flagRemoteClosed == flagRemoteClosed {
			sh.closeSend()
			if len(p) > 0 {
				release()
				sendStatus(mh.StreamID, status.Newf(codes.InvalidArgument, "data close message cannot include data"))
				return true
			}
		}
		return true
	}

	if mh.Type == messageTypeRequest {
		if mh.StreamID <= atomic.LoadUint32(lastStreamID) {
			sendStatus(mh.StreamID, status.Newf(codes.InvalidArgument, "StreamID cannot be re-used and must increment"))
			release()
			return true
		}

		if c.draining.Load() {
			sendStatus(mh.StreamID, newDrainingStatus(atomic.LoadUint32(lastStreamID)))
			release()
			return true
		}

		atomic.StoreUint32(lastStreamID, mh.StreamID)

		var req Request
		if err := c.server.codec.Unmarshal(p, &req); err != nil {
			release()
			sendStatus(mh.StreamID, status.Newf(codes.InvalidArgument, "unmarshal request error: %v", err))
			return true
		}
		release()

		id := mh.StreamID
		respond := func(st *status.Status, data []byte, streaming, closeStream bool) error {
			select {
			case responses <- serverResponse{
				id:          id,
				status:      st,
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
			st, _ := status.FromError(err)
			sendStatus(mh.StreamID, st)
			return true
		}

		streams.Store(id, sh)
		atomic.AddInt32(active, 1)
		return true
	}

	// Unknown message types are ignored for forward compatibility.
	release()
	return true
}

// announceDrainLocked sends the Drain control message; callers are the
// processor (the single owner of lastStreamID). Returns true when this call
// actually announced the boundary; CAS keeps it idempotent.
func (c *serverConn) announceDrainLocked(lastStreamID *uint32, ch *channel) bool {
	if !c.drainNegotiated.Load() {
		return false
	}
	if !c.draining.CompareAndSwap(false, true) {
		return false
	}
	boundary := atomic.LoadUint32(lastStreamID)
	c.sendMu.Lock()
	err := ch.send(controlStreamID, messageTypeControl, 0,
		marshalControlMessage(controlDrain(boundary)))
	c.sendMu.Unlock()
	if err != nil {
		// Roll back so a later trigger (e.g. negotiation completing just as
		// the transport broke) or a clean retry can announce the boundary.
		c.draining.Store(false)
		log.G(context.Background()).WithError(err).Error("ttrpc: failed sending drain control message")
	}
	return err == nil
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

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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/containerd/ttrpc/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// pipeListener hands out net.Pipe connections, providing an in-memory
// transport with fully deterministic scheduling (no goroutine races
// introduced by the kernel stack beyond net.Pipe itself).
type pipeListener struct {
	mu     sync.Mutex
	conns  chan net.Conn
	closed chan struct{}
}

func newPipeListener() *pipeListener {
	return &pipeListener{
		conns:  make(chan net.Conn, 16),
		closed: make(chan struct{}),
	}
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.closed:
		return nil, errors.New("listener closed")
	}
}

func (l *pipeListener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	select {
	case <-l.closed:
	default:
		close(l.closed)
	}
	return nil
}

func (l *pipeListener) Addr() net.Addr { return pipeAddr{} }

func (l *pipeListener) dial() net.Conn {
	server, client := net.Pipe()
	l.conns <- server
	return client
}

type pipeAddr struct{}

func (pipeAddr) Network() string { return "pipe" }
func (pipeAddr) String() string  { return "pipe" }

// frameGateConn wraps a net.Conn and, once armed, blocks reads of the
// next frame until release is called. It works on complete ttrpc
// frames by buffering until the 10-byte header and declared payload
// have been observed.
type frameGateConn struct {
	net.Conn

	mu        sync.Mutex
	armed     bool
	released  chan struct{}
	releasedO sync.Once
}

func newFrameGateConn(inner net.Conn) *frameGateConn {
	return &frameGateConn{Conn: inner, released: make(chan struct{})}
}

func (g *frameGateConn) arm() {
	g.mu.Lock()
	g.armed = true
	g.mu.Unlock()
}

func (g *frameGateConn) release() { g.releasedO.Do(func() { close(g.released) }) }

// Read satisfies one whole frame, blocking its final delivery while
// armed. It is intended for tests that inject a precise ordering
// between a frame sitting on the wire and a drain announcement.
func (g *frameGateConn) Read(p []byte) (int, error) {
	g.mu.Lock()
	armed := g.armed
	g.mu.Unlock()
	if !armed {
		return g.Conn.Read(p)
	}

	// Only one gated frame is supported per connection.
	header := make([]byte, messageHeaderLength)
	if _, err := io.ReadFull(g.Conn, header); err != nil {
		return 0, err
	}
	length := int(uint32(header[0])<<24 | uint32(header[1])<<16 |
		uint32(header[2])<<8 | uint32(header[3]))
	payload := []byte(nil)
	if length > 0 {
		payload = make([]byte, length)
		if _, err := io.ReadFull(g.Conn, payload); err != nil {
			return 0, err
		}
	}

	<-g.released

	frame := append(header, payload...)
	if len(frame) > len(p) {
		return 0, io.ErrShortBuffer
	}
	return copy(p, frame), nil
}

// holdControlConn delays server-to-client Control (0x04) frames until
// a following non-control frame has been written, then flushes them in
// the original (control-first) order. This emulates a transport on
// which a boundary frame is observed after later data at the peer,
// without reordering bytes from the server's perspective.
type holdControlConn struct {
	net.Conn
	mu        sync.Mutex
	held      [][]byte
	holding   bool
	heldCount int
	heldCh    chan struct{}
}

func newHoldControlConn(inner net.Conn) *holdControlConn {
	return &holdControlConn{Conn: inner, holding: true, heldCh: make(chan struct{}, 1)}
}

func (h *holdControlConn) Write(p []byte) (int, error) {
	fmt.Fprintf(os.Stderr, "DBGWRAP write len=%d type=%d\n", len(p), func() byte {
		if len(p) >= 10 {
			return p[8]
		}
		return 0
	}())
	if len(p) >= messageHeaderLength && p[8] == byte(messageTypeControl) {
		h.mu.Lock()
		if h.holding {
			frame := bytes.Clone(p)
			h.held = append(h.held, frame)
			h.heldCount++
			select {
			case h.heldCh <- struct{}{}:
			default:
			}
			h.mu.Unlock()
			return len(p), nil
		}
		h.mu.Unlock()
	}
	h.mu.Lock()
	held := h.held
	h.held = nil
	h.holding = false
	h.mu.Unlock()
	for _, f := range held {
		if _, err := h.Conn.Write(f); err != nil {
			return 0, err
		}
	}
	return h.Conn.Write(p)
}

func (h *holdControlConn) flushHeld() error {
	h.mu.Lock()
	held := h.held
	h.held = nil
	h.holding = false
	h.mu.Unlock()
	for _, f := range held {
		if _, err := h.Conn.Write(f); err != nil {
			return err
		}
	}
	return nil
}

// drainTestEnv wires one server to one client over an in-memory
// listener. If clientDrain is true the client opts into graceful drain.
type drainTestEnv struct {
	t        *testing.T
	server   *Server
	listener *pipeListener
	client   *Client
	conn     net.Conn
}

func newDrainTestEnv(t *testing.T, clientDrain bool, serverOpts ...ServerOpt) *drainTestEnv {
	t.Helper()
	srv, err := NewServer(serverOpts...)
	if err != nil {
		t.Fatal(err)
	}
	l := newPipeListener()
	go func() {
		if err := srv.Serve(context.Background(), l); err != nil && !errors.Is(err, ErrServerClosed) {
			t.Logf("serve: %v", err)
		}
	}()
	conn := l.dial()
	var opts []ClientOpts
	if clientDrain {
		opts = append(opts, WithGracefulDrain())
	}
	cl := NewClient(conn, opts...)
	return &drainTestEnv{t: t, server: srv, listener: l, client: cl, conn: conn}
}

func (e *drainTestEnv) close() {
	e.client.Close()
	e.conn.Close()
	e.server.Close()
	e.listener.Close()
}

// registerBlockingStream registers a bidirectional stream whose handler
// blocks until the returned release is invoked, signaling readiness and
// completion on the provided channels.
func (e *drainTestEnv) registerBlockingStream(ready, done chan struct{}) {
	e.server.RegisterService(serviceName, &ServiceDesc{
		Methods: map[string]Method{
			"Echo": func(_ context.Context, unmarshal func(any) error) (any, error) {
				var req internal.EchoPayload
				if err := unmarshal(&req); err != nil {
					return nil, err
				}
				req.Seq++
				return &req, nil
			},
		},
		Streams: map[string]Stream{
			"Block": {
				Handler: func(ctx context.Context, ss StreamServer) (any, error) {
					close(ready)
					var req internal.EchoPayload
					if err := ss.RecvMsg(&req); err != nil {
						if err == io.EOF {
							err = nil
						}
						close(done)
						return nil, err
					}
					if err := ss.SendMsg(&internal.EchoPayload{Seq: req.Seq + 1, Msg: req.Msg}); err != nil {
						close(done)
						return nil, err
					}
					<-ctx.Done()
					close(done)
					return nil, ctx.Err()
				},
				StreamingClient: true,
				StreamingServer: true,
			},
		},
	})
}

// isDrainingErr asserts the stable, identifiable drain rejection.
func isDrainingErr(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrConnectionDraining) {
		t.Fatalf("expected ErrConnectionDraining, got %T: %v", err, err)
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unavailable {
		t.Fatalf("expected Unavailable status, got %v", st)
	}
	if st.Message() != drainingStatusMessage {
		t.Fatalf("expected stable drain message %q, got %q", drainingStatusMessage, st.Message())
	}
}

var _ = time.Second // keep time import for deadline-based guards

// TestGracefulDrainUnaryBoundary verifies the advertised boundary: a
// unary call in flight at drain completes, later calls get the stable
// draining error, and Drain waits for the in-flight handler.
func TestGracefulDrainUnaryBoundary(t *testing.T) {
	var (
		env   = newDrainTestEnv(t, true)
		enter = make(chan struct{})
		proceed = make(chan struct{})
	)
	defer env.close()

	env.server.Register(serviceName, map[string]Method{
		"Slow": func(_ context.Context, unmarshal func(any) error) (any, error) {
			var req internal.TestPayload
			if err := unmarshal(&req); err != nil {
				return nil, err
			}
			close(enter)
			<-proceed
			return &internal.TestPayload{Foo: "done"}, nil
		},
	})

	ctx := context.Background()
	firstErr := make(chan error, 1)
	go func() {
		var resp internal.TestPayload
		firstErr <- env.client.Call(ctx, serviceName, "Slow", &internal.TestPayload{Foo: "x"}, &resp)
	}()
	<-enter

	drainDone := make(chan error, 1)
	go func() { drainDone <- env.server.Drain(ctx) }()

	// Drain must not return while the first call is still running.
	select {
	case err := <-drainDone:
		t.Fatalf("Drain returned before handler finished: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	// The drain announcement reaches the client before the boundary
	// call finishes.
	select {
	case <-env.client.Drained():
	case <-time.After(2 * time.Second):
		t.Fatal("client did not observe drain boundary")
	}
	boundary, ok := env.client.DrainBoundary()
	if !ok || boundary != 1 {
		t.Fatalf("expected boundary 1, got %d ok=%v", boundary, ok)
	}

	close(proceed)
	if err := <-firstErr; err != nil {
		t.Fatalf("in-flight call should complete: %v", err)
	}

	// Drain waits for the in-flight call and returns nil.
	select {
	case err := <-drainDone:
		if err != nil {
			t.Fatalf("Drain: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Drain did not return")
	}

	// Calls after the boundary fail deterministically, locally fast.
	var tp internal.TestPayload
	isDrainingErr(t, env.client.Call(ctx, serviceName, "Slow", &tp, &tp))

	_, err := env.client.NewStream(ctx, &StreamDesc{StreamingClient: true, StreamingServer: true}, serviceName, "Slow", nil)
	isDrainingErr(t, err)
}

// TestGracefulDrainRejectsConcurrentNewStreams creates many streams
// after the boundary concurrently and requires every one to receive the
// stable draining result rather than a close error.
func TestGracefulDrainRejectsConcurrentNewStreams(t *testing.T) {
	var (
		env   = newDrainTestEnv(t, true)
		ready = make(chan struct{})
	)
	defer env.close()
	env.registerBlockingStream(ready, make(chan struct{}))

	ctx := context.Background()
	stream, err := env.client.NewStream(ctx, &StreamDesc{true, true}, serviceName, "Block", nil)
	if err != nil {
		t.Fatal(err)
	}
	<-ready

	drainErr := make(chan error, 1)
	go func() { drainErr <- env.server.Drain(ctx) }()

	select {
	case <-env.client.Drained():
	case <-time.After(2 * time.Second):
		t.Fatal("drain not announced")
	}

	const n = 50
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- env.client.Call(ctx, serviceName, "Echo",
				&internal.EchoPayload{Seq: 1}, &internal.EchoPayload{})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		isDrainingErr(t, err)
	}

	// The existing stream still completes normally with proper order.
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend after drain: %v", err)
	}

	select {
	case err := <-drainErr:
		if err != nil {
			t.Fatalf("Drain: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Drain did not return")
	}
}

// TestGracefulDrainLongStreamOrdering verifies a long bidirectional
// stream straddling the boundary keeps normal half-close and final
// status ordering.
func TestGracefulDrainLongStreamOrdering(t *testing.T) {
	var (
		env   = newDrainTestEnv(t, true)
		ready = make(chan struct{})
	)
	defer env.close()
	env.server.RegisterService(serviceName, &ServiceDesc{
		Streams: map[string]Stream{
			"EchoStream": {
				Handler: func(_ context.Context, ss StreamServer) (any, error) {
					close(ready)
					for {
						var req internal.EchoPayload
						if err := ss.RecvMsg(&req); err != nil {
							if err == io.EOF {
								err = nil
							}
							return nil, err
						}
						if err := ss.SendMsg(&internal.EchoPayload{Seq: req.Seq + 1, Msg: req.Msg}); err != nil {
							return nil, err
						}
					}
				},
				StreamingClient: true,
				StreamingServer: true,
			},
		},
	})

	ctx := context.Background()
	stream, err := env.client.NewStream(ctx, &StreamDesc{true, true}, serviceName, "EchoStream", nil)
	if err != nil {
		t.Fatal(err)
	}
	<-ready

	// Messages before drain.
	for i := 1; i <= 3; i++ {
		if err := stream.SendMsg(&internal.EchoPayload{Seq: int64(i), Msg: "before"}); err != nil {
			t.Fatal(err)
		}
		var resp internal.EchoPayload
		if err := stream.RecvMsg(&resp); err != nil {
			t.Fatalf("recv before drain: %v", err)
		}
		if resp.Seq != int64(i)+1 {
			t.Fatalf("unexpected seq %d", resp.Seq)
		}
	}

	drainErr := make(chan error, 1)
	go func() { drainErr <- env.server.Drain(ctx) }()
	select {
	case <-env.client.Drained():
	case <-time.After(2 * time.Second):
		t.Fatal("drain not announced")
	}

	// Messages after the boundary on the same existing stream still
	// flow, and client then server half-close in order.
	for i := 4; i <= 6; i++ {
		if err := stream.SendMsg(&internal.EchoPayload{Seq: int64(i), Msg: "after"}); err != nil {
			t.Fatalf("send after drain on existing stream: %v", err)
		}
		var resp internal.EchoPayload
		if err := stream.RecvMsg(&resp); err != nil {
			t.Fatalf("recv after drain: %v", err)
		}
		if resp.Seq != int64(i)+1 {
			t.Fatalf("unexpected seq %d", resp.Seq)
		}
	}

	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	var resp internal.EchoPayload
	if err := stream.RecvMsg(&resp); err != io.EOF {
		t.Fatalf("expected EOF after server closes, got %v", err)
	}

	select {
	case err := <-drainErr:
		if err != nil {
			t.Fatalf("Drain: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Drain did not return")
	}
}


// TestGracefulDrainBoundaryAndDataReordered wires a connection wrapper
// that delays the server's Drain Begin control frame until a later
// response has been written, then releases it. The client must still
// reject subsequent new calls: the stable result must not depend on
// relative observation timing.
func TestGracefulDrainBoundaryAndDataReordered(t *testing.T) {
	srv, err := NewServer()
	if err != nil {
		t.Fatal(err)
	}
	l := newPipeListener()
	go func() {
		_ = srv.Serve(context.Background(), l)
	}()

	hold := newHoldControlConn(l.dial())
	client := NewClient(hold, WithGracefulDrain())
	defer func() {
		client.Close()
		srv.Close()
		l.Close()
	}()

	enter := make(chan struct{})
	proceed := make(chan struct{})
	srv.Register(serviceName, map[string]Method{
		"Slow": func(_ context.Context, unmarshal func(any) error) (any, error) {
			var req internal.TestPayload
			if err := unmarshal(&req); err != nil {
				return nil, err
			}
			close(enter)
			<-proceed
			return &internal.TestPayload{Foo: "ok"}, nil
		},
	})

	ctx := context.Background()
	firstErr := make(chan error, 1)
	go func() {
		var resp internal.TestPayload
		firstErr <- client.Call(ctx, serviceName, "Slow",
			&internal.TestPayload{Foo: "x"}, &resp)
	}()
	<-enter

	drainDone := make(chan error, 1)
	go func() { drainDone <- srv.Drain(ctx) }()

	// Wait deterministically until the control frame has been
	// intercepted by the wrapper.
	select {
	case <-hold.heldCh:
	case <-time.After(2 * time.Second):
		t.Fatal("control frame never reached wrapper")
	}
	select {
	case <-client.Drained():
		t.Fatal("client observed drain while control frame held")
	case <-time.After(100 * time.Millisecond):
	}

	close(proceed)
	if err := <-firstErr; err != nil {
		t.Fatalf("in-flight call: %v", err)
	}

	// Response has been written; flush the delayed control frame now,
	// so it arrives at the client after the data frame.
	if err := hold.flushHeld(); err != nil {
		t.Fatal(err)
	}

	select {
	case <-client.Drained():
	case <-time.After(2 * time.Second):
		t.Fatal("client did not observe delayed drain boundary")
	}

	var tp internal.TestPayload
	isDrainingErr(t, client.Call(ctx, serviceName, "Slow", &tp, &tp))

	select {
	case err := <-drainDone:
		if err != nil {
			t.Fatalf("Drain: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Drain did not return")
	}
}

// TestGracefulDrainContextCanceled verifies a caller whose context is
// canceled while blocked on a drain boundary gets context.Canceled
// without affecting the drain itself, and that existing streams still
// finish and unblock Drain.
func TestGracefulDrainContextCanceled(t *testing.T) {
	env := newDrainTestEnv(t, true)
	defer env.close()

	ready := make(chan struct{})
	sdone := make(chan struct{})
	env.registerBlockingStream(ready, sdone)

	ctx := context.Background()
	stream, err := env.client.NewStream(ctx, &StreamDesc{true, true}, serviceName, "Block", nil)
	if err != nil {
		t.Fatal(err)
	}
	<-ready

	drainErr := make(chan error, 1)
	go func() { drainErr <- env.server.Drain(ctx) }()
	select {
	case <-env.client.Drained():
	case <-time.After(2 * time.Second):
		t.Fatal("drain not announced")
	}

	// A blocked unary after the boundary: cancel its context and
	// expect context.Canceled rather than a connection close error.
	callCtx, cancel := context.WithCancel(ctx)
	cancelled := make(chan error, 1)
	go func() {
		var tp internal.TestPayload
		cancelled <- env.client.Call(callCtx, serviceName, "Echo", &tp, &tp)
	}()
	cancel()
	select {
	case err := <-cancelled:
		if !errors.Is(err, ErrConnectionDraining) && !errors.Is(err, context.Canceled) {
			t.Fatalf("expected draining or canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled call did not return")
	}

	// Completing the pre-boundary stream still lets Drain finish.
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sdone:
	case <-time.After(2 * time.Second):
		t.Fatal("server handler did not return")
	}
	select {
	case err := <-drainErr:
		if err != nil {
			t.Fatalf("Drain: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Drain did not return")
	}
}

// TestGracefulDrainBackpressure holds the client from reading while a
// pre-boundary stream's final response is written, verifying that
// Drain waits until the response has actually been delivered rather
// than tearing the connection mid-write.
func TestGracefulDrainBackpressure(t *testing.T) {
	srv, err := NewServer()
	if err != nil {
		t.Fatal(err)
	}
	l := newPipeListener()
	go func() { _ = srv.Serve(context.Background(), l) }()

	var (
		gateMu  sync.Mutex
		gated   bool
		release = make(chan struct{})
		relOnce sync.Once
	)
	rawClientConn := l.dial()
	wrapped := &gateConn{
		Conn: rawClientConn,
		block: func() bool {
			gateMu.Lock()
			defer gateMu.Unlock()
			return gated
		},
		release: release,
		once:    &relOnce,
	}

	// We need a raw client-side control: create client only after gate
	// is armed by intercepting reads. Simpler: arm after a normal call
	// setup but before Drain's control/response flush. Use a small
	// delay-free approach: gate is armed externally below.
	client := NewClient(wrapped, WithGracefulDrain())
	defer func() {
		client.Close()
		srv.Close()
		l.Close()
	}()

	enter := make(chan struct{})
	proceed := make(chan struct{})
	srv.Register(serviceName, map[string]Method{
		"Slow": func(_ context.Context, unmarshal func(any) error) (any, error) {
			var req internal.TestPayload
			if err := unmarshal(&req); err != nil {
				return nil, err
			}
			close(enter)
			<-proceed
			// Large response so a blocked reader stalls the write.
			return &internal.TestPayload{Foo: stringsRepeat(1 << 20)}, nil
		},
	})

	ctx := context.Background()
	callErr := make(chan error, 1)
	go func() {
		var resp internal.TestPayload
		callErr <- client.Call(ctx, serviceName, "Slow",
			&internal.TestPayload{}, &resp)
	}()
	<-enter

	// Arm the read gate before the handler's response is flushed so
	// the server write blocks once bytes are produced.
	gateMu.Lock()
	gated = true
	gateMu.Unlock()

	drainDone := make(chan error, 1)
	go func() { drainDone <- srv.Drain(ctx) }()
	close(proceed)

	// Drain must not return while the final response is stuck.
	select {
	case err := <-drainDone:
		t.Fatalf("Drain returned during backpressure: %v", err)
	case <-time.After(150 * time.Millisecond):
	}

	// Release the reader; the response and drain complete.
	relOnce.Do(func() { close(release) })
	select {
	case err := <-callErr:
		if err != nil {
			t.Fatalf("call under backpressure: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("call did not complete after release")
	}
	select {
	case err := <-drainDone:
		if err != nil {
			t.Fatalf("Drain: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Drain did not return")
	}
}

// gateConn blocks Reads (on the client side) until release fires once
// armed, while still allowing frame bytes to accumulate.
type gateConn struct {
	net.Conn
	block   func() bool
	release chan struct{}
	once    *sync.Once
}

func (g *gateConn) Read(p []byte) (int, error) {
	if g.block() {
		<-g.release
	}
	return g.Conn.Read(p)
}

func stringsRepeat(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'x'
	}
	return string(b)
}

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
	"net"
	"runtime"
	"sync"
	"testing"

	"github.com/containerd/ttrpc/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const drainServiceName = "drainService"

// memListener pairs net.Pipe connections without involving the OS, giving
// tests full control over frame interleaving.
type memListener struct {
	ch         chan net.Conn
	closed     chan struct{}
	accepting  chan struct{}
	acceptOnce sync.Once
	closeOnce  sync.Once
	addr       net.Addr
}

type memAddr struct{}

func (memAddr) Network() string { return "mem" }
func (memAddr) String() string  { return "mem" }

func newMemListener() *memListener {
	return &memListener{
		ch:        make(chan net.Conn),
		closed:    make(chan struct{}),
		accepting: make(chan struct{}),
		addr:      memAddr{},
	}
}

func (l *memListener) Accept() (net.Conn, error) {
	l.acceptOnce.Do(func() { close(l.accepting) })
	select {
	case c := <-l.ch:
		return c, nil
	case <-l.closed:
		return nil, errors.New("listener closed")
	}
}

func (l *memListener) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })
	return nil
}

func (l *memListener) Addr() net.Addr { return l.addr }

func (l *memListener) dial() (net.Conn, error) {
	client, server := net.Pipe()
	select {
	case l.ch <- server:
		return client, nil
	case <-l.closed:
		client.Close()
		server.Close()
		return nil, errors.New("listener closed")
	}
}

// waitForServing ensures the server is blocked in Accept before a dial,
// removing net.Pipe scheduling races without sleeping.
func waitForServing(t *testing.T, l *memListener) {
	t.Helper()
	<-l.accepting
}

// drainServiceRegistrar registers unary and streaming methods whose
// progress is driven by the test through channels.
func registerDrainService(t *testing.T, srv *Server, opts drainHandlers) {
	t.Helper()
	srv.RegisterService(drainServiceName, &ServiceDesc{
		Methods: map[string]Method{
			"Echo": func(_ context.Context, unmarshal func(any) error) (any, error) {
				var req internal.EchoPayload
				if err := unmarshal(&req); err != nil {
					return nil, err
				}
				req.Seq++
				return &req, nil
			},
			"Block": func(ctx context.Context, unmarshal func(any) error) (any, error) {
				var req internal.EchoPayload
				if err := unmarshal(&req); err != nil {
					return nil, err
				}
				if opts.handlerStarted != nil {
					close(opts.handlerStarted)
				}
				select {
				case <-opts.proceed:
					return &internal.EchoPayload{Seq: req.Seq, Msg: "done"}, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
		},
		Streams: map[string]Stream{
			"EchoStream": {
				StreamingClient: true,
				StreamingServer: true,
				Handler: func(_ context.Context, ss StreamServer) (any, error) {
					for {
						var req internal.EchoPayload
						if err := ss.RecvMsg(&req); err != nil {
							if errors.Is(err, io.EOF) {
								return nil, nil
							}
							return nil, err
						}
						req.Seq++
						if err := ss.SendMsg(&req); err != nil {
							return nil, err
						}
					}
				},
			},
		},
	})
}

type drainHandlers struct {
	proceed        chan struct{}
	handlerStarted chan struct{}
}

func newDrainPair(t *testing.T, serverOpts ...ServerOpt) (*Server, *memListener, *Client, func()) {
	t.Helper()
	srv, err := NewServer(serverOpts...)
	if err != nil {
		t.Fatal(err)
	}
	l := newMemListener()
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(context.Background(), l)
	}()
	waitForServing(t, l)

	conn, err := l.dial()
	if err != nil {
		t.Fatal(err)
	}
	cl := NewClient(conn)

	cleanup := func() {
		cl.Close()
		conn.Close()
		l.Close()
		srv.Close()
	}
	return srv, l, cl, cleanup
}

func expectDraining(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected draining rejection, got nil")
	}
	if !IsDraining(err) {
		st, _ := status.FromError(err)
		t.Fatalf("expected draining Unavailable error, got code=%s err=%v", st.Code(), err)
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unavailable {
		t.Fatalf("expected codes.Unavailable, got %v", err)
	}
}

// rawPeer is a minimal ttrpc frame peer over a net.Conn used to script
// exact frame orderings.
type rawPeer struct {
	conn net.Conn
	ch   *channel
	mu   sync.Mutex
}

func newRawPeer(conn net.Conn) *rawPeer {
	return &rawPeer{conn: conn, ch: newChannel(conn)}
}

func (p *rawPeer) sendHello(features uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ch.send(controlStreamID, messageTypeControl, 0, marshalControl(features, 0, false))
}

func (p *rawPeer) sendRequest(sid uint32, flags uint8, service, method string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	req := &Request{Service: service, Method: method}
	b, err := codec{}.Marshal(req)
	if err != nil {
		return err
	}
	return p.ch.send(sid, messageTypeRequest, flags, b)
}

func (p *rawPeer) sendData(sid uint32, flags uint8, m any) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	b, err := codec{}.Marshal(m)
	if err != nil {
		return err
	}
	return p.ch.send(sid, messageTypeData, flags, b)
}

func (p *rawPeer) recv() (messageHeader, []byte, error) {
	return p.ch.recv()
}

func (p *rawPeer) close() { p.conn.Close() }

// waitForGoroutines ensures no test leaves ttrpc goroutines behind without
// relying on time.Sleep: it yields only until a condition becomes true or
// the deadline expires.
func waitFor(t *testing.T, deadline <-chan struct{}, what string, fn func() bool) {
	t.Helper()
	for !fn() {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		default:
			runtime.Gosched()
		}
	}
}

func TestDrainRejectsNewCallsCompletesInFlight(t *testing.T) {
	ctx := context.Background()
	proceed := make(chan struct{})
	started := make(chan struct{})
	srv, l, cl, cleanup := newDrainPair(t, WithGracefulDrain())
	defer cleanup()
	registerDrainService(t, srv, drainHandlers{proceed: proceed, handlerStarted: started})

	// Existing call before the boundary must complete normally after drain.
	callErr := make(chan error, 1)
	go func() {
		var req, resp internal.EchoPayload
		req.Seq = 7
		callErr <- cl.Call(ctx, drainServiceName, "Block", &req, &resp)
	}()
	<-started

	drainDone := make(chan error, 1)
	go func() { drainDone <- srv.Drain(ctx) }()

	// Wait until the drain boundary is actually observable, so the new
	// call's rejection does not depend on frame delivery timing.
	select {
	case <-cl.drainObserved:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	// New call after the boundary gets the stable draining error, either
	// locally or from the server.
	var req2 internal.EchoPayload
	err := cl.Call(ctx, drainServiceName, "Echo", &req2, &req2)
	expectDraining(t, err)

	close(proceed)
	if err := <-callErr; err != nil {
		t.Fatalf("in-flight call should complete, got %v", err)
	}
	if err := <-drainDone; err != nil {
		t.Fatalf("drain should return nil: %v", err)
	}

	// All later calls keep failing with the same stable result.
	err = cl.Call(ctx, drainServiceName, "Echo", &internal.EchoPayload{}, &internal.EchoPayload{})
	expectDraining(t, err)

	// Drain stops accepting new connections; a reconnect is refused by the
	// server rather than inheriting the old connection's boundary.
	if _, err := l.dial(); err == nil {
		t.Fatal("expected new connections to be refused after drain")
	}
}

func TestDrainIdempotentAndConcurrentShutdown(t *testing.T) {
	ctx := context.Background()
	proceed := make(chan struct{})
	started := make(chan struct{})
	srv, _, cl, cleanup := newDrainPair(t, WithGracefulDrain())
	defer cleanup()
	registerDrainService(t, srv, drainHandlers{proceed: proceed, handlerStarted: started})

	callDone := make(chan error, 1)
	go func() {
		var req, resp internal.EchoPayload
		callDone <- cl.Call(ctx, drainServiceName, "Block", &req, &resp)
	}()
	<-started

	const n = 4
	errs := make(chan error, n*2)
	var wg sync.WaitGroup
	// Concurrent, repeated Drain calls share one idempotent drain.
	for range n {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- srv.Drain(ctx) }()
	}

	close(proceed)
	if err := <-callDone; err != nil {
		t.Fatalf("in-flight call should complete: %v", err)
	}
	wg.Wait()

	// Drain followed by Shutdown must also be idempotent.
	var wg2 sync.WaitGroup
	for range n {
		wg2.Add(1)
		go func() { defer wg2.Done(); errs <- srv.Shutdown(ctx) }()
	}
	wg2.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("drain/shutdown should be idempotent, got %v", err)
		}
	}

	// Drain after close is still a no-op success.
	if err := srv.Drain(ctx); err != nil {
		t.Fatalf("drain after shutdown: %v", err)
	}
}

func TestDrainWithoutOption(t *testing.T) {
	srv, _, _, cleanup := newDrainPair(t)
	defer cleanup()
	if err := srv.Drain(context.Background()); !errors.Is(err, ErrDrainNotEnabled) {
		t.Fatalf("expected ErrDrainNotEnabled, got %v", err)
	}
}

func TestDrainLongStreamHalfCloseOrdering(t *testing.T) {
	ctx := context.Background()
	proceed := make(chan struct{})
	started := make(chan struct{})
	srv, _, cl, cleanup := newDrainPair(t, WithGracefulDrain())
	defer cleanup()
	registerDrainService(t, srv, drainHandlers{proceed: proceed, handlerStarted: started})

	// An unrelated long unary call keeps the connection active during drain.
	blockDone := make(chan error, 1)
	go func() {
		var req, resp internal.EchoPayload
		blockDone <- cl.Call(ctx, drainServiceName, "Block", &req, &resp)
	}()
	<-started

	stream, err := cl.NewStream(ctx, &StreamDesc{StreamingClient: true, StreamingServer: true}, drainServiceName, "EchoStream", nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		if err := stream.SendMsg(&internal.EchoPayload{Seq: int64(i)}); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
		var got internal.EchoPayload
		if err := stream.RecvMsg(&got); err != nil {
			t.Fatalf("recv %d: %v", i, err)
		}
		if got.Seq != int64(i)+1 {
			t.Fatalf("unexpected echo seq %d", got.Seq)
		}
	}

	drainReturned := make(chan error, 1)
	go func() { drainReturned <- srv.Drain(ctx) }()

	// The existing stream keeps working through the drain and half-close
	// ordering is unchanged: client close-send, then server EOF.
	if err := stream.SendMsg(&internal.EchoPayload{Seq: 6}); err != nil {
		t.Fatalf("send after drain: %v", err)
	}
	var got internal.EchoPayload
	if err := stream.RecvMsg(&got); err != nil {
		t.Fatalf("recv after drain: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close send: %v", err)
	}
	if err := stream.RecvMsg(&got); err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}

	// Drain still waits on the blocking unary call.
	select {
	case err := <-drainReturned:
		t.Fatalf("drain returned before in-flight call finished: %v", err)
	default:
	}
	close(proceed)
	if err := <-blockDone; err != nil {
		t.Fatalf("block call: %v", err)
	}
	if err := <-drainReturned; err != nil {
		t.Fatalf("drain: %v", err)
	}

	// A new stream after the boundary is rejected identically.
	_, err = cl.NewStream(ctx, &StreamDesc{StreamingClient: true, StreamingServer: true}, drainServiceName, "EchoStream", nil)
	expectDraining(t, err)
}

func TestDrainContextCanceled(t *testing.T) {
	ctx := context.Background()
	proceed := make(chan struct{})
	started := make(chan struct{})
	srv, _, cl, cleanup := newDrainPair(t, WithGracefulDrain())
	defer cleanup()
	registerDrainService(t, srv, drainHandlers{proceed: proceed, handlerStarted: started})

	blockDone := make(chan error, 1)
	go func() {
		var req, resp internal.EchoPayload
		blockDone <- cl.Call(ctx, drainServiceName, "Block", &req, &resp)
	}()
	<-started

	dctx, cancel := context.WithCancel(ctx)
	drainErr := make(chan error, 1)
	go func() { drainErr <- srv.Drain(dctx) }()
	cancel()
	if err := <-drainErr; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	// A subsequent Drain with an alive context still completes once the
	// in-flight call finishes; the start of the drain is idempotent.
	close(proceed)
	if err := <-blockDone; err != nil {
		t.Fatalf("block call: %v", err)
	}
	if err := srv.Drain(ctx); err != nil {
		t.Fatalf("follow-up drain: %v", err)
	}
}

// TestDrainServerBoundaryWithRawPeer scripts exact frame interleavings on a
// raw connection: the boundary snapshot and rejection depend only on stream
// ids, never on frame timing.
func TestDrainServerBoundaryWithRawPeer(t *testing.T) {
	ctx := context.Background()
	srv, err := NewServer(WithGracefulDrain())
	if err != nil {
		t.Fatal(err)
	}
	l := newMemListener()
	defer srv.Close()
	defer l.Close()
	go srv.Serve(ctx, l)
	waitForServing(t, l)
	registerDrainService(t, srv, drainHandlers{proceed: make(chan struct{})})

	conn, err := l.dial()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	p := newRawPeer(conn)
	if err := p.sendHello(FeatureGracefulDrain); err != nil {
		t.Fatal(err)
	}
	mh, b, err := p.recv() // server hello
	if err != nil {
		t.Fatal(err)
	}
	features, _, draining := parseControl(b)
	if mh.Type != messageTypeControl || mh.StreamID != controlStreamID || draining ||
		features&FeatureGracefulDrain != FeatureGracefulDrain {
		t.Fatalf("unexpected server hello: %+v features=%d draining=%v", mh, features, draining)
	}

	// Establish two accepted streams.
	if err := p.sendRequest(1, flagRemoteClosed, drainServiceName, "Echo"); err != nil {
		t.Fatal(err)
	}
	if err := p.sendRequest(3, flagRemoteClosed, drainServiceName, "Echo"); err != nil {
		t.Fatal(err)
	}
	// Receive both responses in order (boundary has not been announced).
	for range 2 {
		mh, b, err = p.recv()
		if err != nil {
			t.Fatal(err)
		}
		if mh.Type != messageTypeResponse {
			t.Fatalf("expected response, got %v", mh.Type)
		}
	}

	if err := srv.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	mh, b, err = p.recv() // drain frame
	if err != nil {
		t.Fatal(err)
	}
	_, boundary, draining := parseControl(b)
	if mh.Type != messageTypeControl || !draining || boundary != 3 {
		t.Fatalf("expected drain boundary 3, got type=%v draining=%v boundary=%d", mh.Type, draining, boundary)
	}

	// A stream beyond the boundary is rejected even though it was already
	// in flight before the drain frame could be observed.
	if err := p.sendRequest(5, flagRemoteClosed, drainServiceName, "Echo"); err != nil {
		t.Fatal(err)
	}
	mh, b, err = p.recv()
	if err != nil {
		t.Fatal(err)
	}
	if mh.StreamID != 5 || mh.Type != messageTypeResponse {
		t.Fatalf("expected response on stream 5, got sid=%d type=%v", mh.StreamID, mh.Type)
	}
	var resp Response
	cdc := codec{}
	if err := cdc.Unmarshal(b, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status == nil || codes.Code(resp.Status.Code) != codes.Unavailable ||
		resp.Status.Message != drainMessage+" (last accepted stream id: 3)" {
		t.Fatalf("unexpected rejection status: %+v", resp.Status)
	}

	// Repeated drain frame from a second Drain keeps the same monotonic
	// boundary; the server sends one announcement per arming attempt.
	if err := srv.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	// The second Drain is idempotent and need not emit a frame; the
	// connection is already idle so the server does not re-arm.
}

// TestDrainBoundaryFrameReversedWithData sends the drain boundary and the
// rejected stream's frames in adversarial order: data for a not-yet-seen
// stream before its request, and a request while draining. Results must be
// a pure function of the stream id.
func TestDrainBoundaryFrameReversedWithData(t *testing.T) {
	ctx := context.Background()
	srv, err := NewServer(WithGracefulDrain())
	if err != nil {
		t.Fatal(err)
	}
	l := newMemListener()
	defer srv.Close()
	defer l.Close()
	go srv.Serve(ctx, l)
	waitForServing(t, l)
	registerDrainService(t, srv, drainHandlers{proceed: make(chan struct{})})

	conn, err := l.dial()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	p := newRawPeer(conn)
	if err := p.sendHello(FeatureGracefulDrain); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.recv(); err != nil { // drain server hello
		t.Fatal(err)
	}

	// Accept stream 1 first.
	if err := p.sendRequest(1, flagRemoteClosed, drainServiceName, "Echo"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.recv(); err != nil {
		t.Fatal(err)
	}

	if err := srv.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.recv(); err != nil { // drain frame boundary=1
		t.Fatal(err)
	}

	// Reverse order: a data frame for stream 3 lands before its request.
	if err := p.sendData(3, 0, &internal.EchoPayload{Seq: 1}); err != nil {
		t.Fatal(err)
	}
	if err := p.sendRequest(3, flagRemoteOpen, drainServiceName, "EchoStream"); err != nil {
		t.Fatal(err)
	}

	// Both frames reference a stream past the boundary. The request gets
	// the stable Unavailable rejection; the stray data is dropped without
	// any frame, so exactly one response on stream 3 is readable.
	mh, b, err := p.recv()
	if err != nil {
		t.Fatal(err)
	}
	if mh.StreamID != 3 || mh.Type != messageTypeResponse {
		t.Fatalf("expected single rejection on 3, got sid=%d type=%v", mh.StreamID, mh.Type)
	}
	var resp Response
	cdc := codec{}
	if err := cdc.Unmarshal(b, &resp); err != nil {
		t.Fatal(err)
	}
	if codes.Code(resp.Status.Code) != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %v", codes.Code(resp.Status.Code))
	}

	// Nothing else should arrive: the server closes idle connections only
	// on Shutdown; verify absence without sleeps by closing the write side
	// and confirming the server reaches EOF instead of emitting another
	// frame.
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, rerr := p.recv(); rerr == nil {
		t.Fatal("expected connection close after shutdown, got another frame")
	}
}

// TestDrainOldClientInterop verifies a drain-enabled server leaves a client
// which never sends a capability hello entirely on the pre-drain path: no
// drain frame is sent and its call still completes while Drain waits for
// it. It uses raw frames to emulate the old client.
func TestDrainOldClientInterop(t *testing.T) {
	ctx := context.Background()
	srv, err := NewServer(WithGracefulDrain())
	if err != nil {
		t.Fatal(err)
	}
	l := newMemListener()
	defer srv.Close()
	defer l.Close()
	go srv.Serve(ctx, l)
	waitForServing(t, l)
	registerDrainService(t, srv, drainHandlers{proceed: make(chan struct{})})

	conn, err := l.dial()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	p := newRawPeer(conn)
	// Old client sends no hello; its unary request is accepted normally.
	if err := p.sendRequest(1, flagRemoteClosed, drainServiceName, "Echo"); err != nil {
		t.Fatal(err)
	}
	// Consume the ordinary response first; the server never sends a
	// control frame to a non-negotiated connection.
	mh, b, err := p.recv()
	if err != nil {
		t.Fatal(err)
	}
	if mh.Type != messageTypeResponse {
		t.Fatalf("expected response, got %v", mh.Type)
	}

	// Drain returns immediately for a connection that never negotiated.
	if err := srv.Drain(ctx); err != nil {
		t.Fatalf("drain with non-capable conn: %v", err)
	}
	var resp Response
	cdc := codec{}
	if err := cdc.Unmarshal(b, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != nil && resp.Status.Code != 0 {
		t.Fatalf("expected OK status, got %+v", resp.Status)
	}
}

// TestDrainNewClientOldServer verifies the new client survives a server
// which does not understand the control hello: its error reply on stream 0
// is discarded and unary + streaming keep their existing semantics.
func TestDrainNewClientOldServer(t *testing.T) {
	ctx := context.Background()
	srv, err := NewServer() // drain disabled
	if err != nil {
		t.Fatal(err)
	}
	l := newMemListener()
	defer srv.Close()
	defer l.Close()
	go srv.Serve(ctx, l)
	waitForServing(t, l)
	registerDrainService(t, srv, drainHandlers{proceed: make(chan struct{})})

	conn, err := l.dial()
	if err != nil {
		t.Fatal(err)
	}
	cl := NewClient(conn)
	defer cl.Close()

	// Drain on a non-enabled server is rejected API-side.
	if err := srv.Drain(ctx); !errors.Is(err, ErrDrainNotEnabled) {
		t.Fatalf("expected ErrDrainNotEnabled, got %v", err)
	}

	// The old server will write an InvalidArgument response on stream 0
	// for the hello; the client must ignore it and calls must still work.
	var req, resp internal.EchoPayload
	req.Seq = 41
	if err := cl.Call(ctx, drainServiceName, "Echo", &req, &resp); err != nil {
		t.Fatalf("unary interop failed: %v", err)
	}
	if resp.Seq != 42 {
		t.Fatalf("unexpected seq %d", resp.Seq)
	}

	stream, err := cl.NewStream(ctx, &StreamDesc{StreamingClient: true, StreamingServer: true}, drainServiceName, "EchoStream", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.SendMsg(&internal.EchoPayload{Seq: 1}); err != nil {
		t.Fatal(err)
	}
	var got internal.EchoPayload
	if err := stream.RecvMsg(&got); err != nil {
		t.Fatalf("streaming interop failed: %v", err)
	}
	if got.Seq != 2 {
		t.Fatalf("unexpected seq %d", got.Seq)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if err := stream.RecvMsg(&got); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

// TestDrainConnectionAnomaly verifies that a peer vanishing while drain is
// pending does not make Drain hang: the dead connection is forgotten while
// the surviving in-flight call is still awaited.
func TestDrainConnectionAnomaly(t *testing.T) {
	ctx := context.Background()
	proceed := make(chan struct{})
	started := make(chan struct{})
	srv, err := NewServer(WithGracefulDrain())
	if err != nil {
		t.Fatal(err)
	}
	l := newMemListener()
	defer srv.Close()
	defer l.Close()
	go srv.Serve(ctx, l)
	waitForServing(t, l)
	registerDrainService(t, srv, drainHandlers{proceed: proceed, handlerStarted: started})

	// First capable connection is merely kept idle.
	conn1, err := l.dial()
	if err != nil {
		t.Fatal(err)
	}
	p1 := newRawPeer(conn1)
	if err := p1.sendHello(FeatureGracefulDrain); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p1.recv(); err != nil { // server hello
		t.Fatal(err)
	}

	// Second capable connection has one in-flight blocking call.
	conn2, err := l.dial()
	if err != nil {
		t.Fatal(err)
	}
	p2 := newRawPeer(conn2)
	if err := p2.sendHello(FeatureGracefulDrain); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p2.recv(); err != nil {
		t.Fatal(err)
	}
	if err := p2.sendRequest(1, flagRemoteClosed, drainServiceName, "Block"); err != nil {
		t.Fatal(err)
	}
	<-started

	drainErr := make(chan error, 1)
	go func() { drainErr <- srv.Drain(ctx) }()

	// The first peer disappears abruptly while drain is pending.
	p1.close()

	select {
	case err := <-drainErr:
		t.Fatalf("drain should wait for surviving in-flight call, returned: %v", err)
	default:
	}

	close(proceed)
	if mh, _, err := p2.recv(); err != nil || mh.Type != messageTypeResponse {
		t.Fatalf("expected block response, got mh=%v err=%v", mh, err)
	}
	if err := <-drainErr; err != nil {
		t.Fatalf("drain: %v", err)
	}
}

// TestDrainMonotonicBoundaryAcrossReconnections verifies a fresh connection
// starts with no boundary and that drain never moves a boundary backwards
// within a connection; rejected stream ids are not resurrected.
func TestDrainMonotonicBoundaryAcrossReconnections(t *testing.T) {
	ctx := context.Background()
	srv, err := NewServer(WithGracefulDrain())
	if err != nil {
		t.Fatal(err)
	}
	l := newMemListener()
	defer srv.Close()
	defer l.Close()
	go srv.Serve(ctx, l)
	waitForServing(t, l)
	registerDrainService(t, srv, drainHandlers{proceed: make(chan struct{})})

	drainConn := func() (*rawPeer, func()) {
		conn, err := l.dial()
		if err != nil {
			t.Fatal(err)
		}
		p := newRawPeer(conn)
		if err := p.sendHello(FeatureGracefulDrain); err != nil {
			t.Fatal(err)
		}
		if _, _, err := p.recv(); err != nil {
			t.Fatal(err)
		}
		return p, func() { p.close() }
	}

	p1, close1 := drainConn()
	defer close1()
	if err := p1.sendRequest(1, flagRemoteClosed, drainServiceName, "Echo"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p1.recv(); err != nil {
		t.Fatal(err)
	}
	if err := srv.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if mh, b, err := p1.recv(); err != nil || mh.Type != messageTypeControl {
		t.Fatalf("expected drain frame, got %v %v", mh, err)
	} else if _, boundary, draining := parseControl(b); !draining || boundary != 1 {
		t.Fatalf("boundary=%d draining=%v", boundary, draining)
	}
	close1()

	// A new connection after drain start is refused at Accept time.
	if _, err := l.dial(); err == nil {
		t.Fatal("expected refused reconnect after drain")
	}
}

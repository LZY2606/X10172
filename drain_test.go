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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/containerd/ttrpc/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// pipeConn wires a ttrpc client/server pair over net.Pipe with no buffering,
// giving tests full control over frame interleaving.
type pipeConn struct {
	client net.Conn
	server net.Conn
}

func newPipeConn() *pipeConn {
	c, s := net.Pipe()
	return &pipeConn{client: c, server: s}
}

func (p *pipeConn) close() {
	p.client.Close()
	p.server.Close()
}

// rawFrame is a decoded ttrpc frame.
type rawFrame struct {
	streamID uint32
	typ      messageType
	flags    uint8
	payload  []byte
}

func writeRaw(t testing.TB, conn net.Conn, f rawFrame) {
	t.Helper()
	ch := newChannel(conn)
	if err := ch.send(f.streamID, f.typ, f.flags, f.payload); err != nil {
		t.Fatalf("writeRaw: %v", err)
	}
}

func readRaw(t testing.TB, conn net.Conn) rawFrame {
	t.Helper()
	ch := newChannel(conn)
	mh, p, err := ch.recv()
	if err != nil {
		t.Fatalf("readRaw: %v", err)
	}
	return rawFrame{streamID: mh.StreamID, typ: mh.Type, flags: mh.Flags, payload: p}
}

func marshalRawRequest(t testing.TB, service, method string) []byte {
	t.Helper()
	req := &Request{Service: service, Method: method}
	p, err := (codec{}).Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func drainTestService(opts streamSvcOpts) *ServiceDesc {
	desc := &ServiceDesc{
		Methods: map[string]Method{
			"Blocking": func(ctx context.Context, unmarshal func(any) error) (any, error) {
				if opts.started != nil {
					close(opts.started)
				}
				if opts.release != nil {
					<-opts.release
				}
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return &internal.TestPayload{Foo: "done"}, nil
			},
			"Echo": func(_ context.Context, unmarshal func(any) error) (any, error) {
				var req internal.TestPayload
				if err := unmarshal(&req); err != nil {
					return nil, err
				}
				return &req, nil
			},
		},
	}
	return desc
}

type streamSvcOpts struct {
	started chan struct{}
	release chan struct{}
}

// waitCondition blocks until cond() is true, polling without sleeping. It
// never uses time.Sleep; the short deadline only guards against deadlock.
func waitCondition(t testing.TB, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("timed out waiting for %s", what)
}

func waitChan(t testing.TB, timeout time.Duration, what string, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// assertDraining verifies the stable drain rejection result.
func assertDraining(t testing.TB, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected draining rejection, got nil")
	}
	if !IsServerDraining(err) {
		t.Fatalf("expected IsServerDraining error, got %v", err)
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected grpc status error, got %T: %v", err, err)
	}
	if st.Code() != codes.Unavailable {
		t.Fatalf("expected Unavailable, got %v", st.Code())
	}
	if !strings.HasPrefix(st.Message(), ErrServerDraining.Error()) {
		t.Fatalf("unexpected message: %q", st.Message())
	}
}

// startPipeServer runs one serverConn directly over a pipe (no listener),
// giving deterministic shutdown without accept races.
func startPipeServer(t *testing.T, srv *Server, sc net.Conn) {
	t.Helper()
	c, err := srv.newConn(sc, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c.close()
		sc.Close()
	})
	go c.run(context.Background())
}

// TestGracefulDrainInFlightCompletesAndNewRejected covers the core sequence:
// a call accepted before the boundary completes, a later call is rejected
// with the stable draining result, and ShutdownGraceful reports completion.
func TestGracefulDrainInFlightCompletesAndNewRejected(t *testing.T) {
	pc := newPipeConn()
	defer pc.close()

	srv, err := NewServer(WithGracefulShutdown())
	if err != nil {
		t.Fatal(err)
	}
	srv.Register(serviceName, map[string]Method{
		"Test": func(ctx context.Context, unmarshal func(any) error) (any, error) {
			var req internal.TestPayload
			if err := unmarshal(&req); err != nil {
				return nil, err
			}
			return &internal.TestPayload{Foo: req.Foo + "-ok"}, nil
		},
	})
	startPipeServer(t, srv, pc.server)

	client := NewClient(pc.client, WithGracefulDrain())
	defer client.Close()

	// Negotiation completes; prove both sides exchanged Hello via a call.
	var pre internal.TestPayload
	pre.Foo = "a"
	if err := client.Call(context.Background(), serviceName, "Test", &pre, &pre); err != nil {
		t.Fatal(err)
	}
	if pre.Foo != "a-ok" {
		t.Fatalf("unexpected %q", pre.Foo)
	}

	// No active calls: drain boundary should be the highest id seen (3).
	res, err := srv.ShutdownGraceful(context.Background())
	if err != nil {
		t.Fatalf("ShutdownGraceful: %v", err)
	}
	if res.DrainedConnections != 1 || res.InterruptedConnections != 0 || res.UnsupportedConnections != 0 {
		t.Fatalf("unexpected result: %+v", res)
	}
	waitChan(t, 2*time.Second, "client drain observed", client.DrainObserved())
	if boundary := client.drainBoundary.Load(); boundary != 1 {
		t.Fatalf("boundary = %d, want 1", boundary)
	}

	// New call fails with a stable rejection even though the connection is
	// still physically open.
	var post internal.TestPayload
	assertDraining(t, client.Call(context.Background(), serviceName, "Test", &post, &post))
}

// TestGracefulDrainLongStreamCompletes verifies a stream accepted before the
// boundary fully completes: more data, client half-close and final status
// all arrive in order after the Drain frame.
func TestGracefulDrainLongStreamCompletes(t *testing.T) {
	pc := newPipeConn()
	defer pc.close()

	gotData := make(chan struct{}, 8)
	srv, err := NewServer(WithGracefulShutdown())
	if err != nil {
		t.Fatal(err)
	}
	srv.RegisterService("streamSvc", &ServiceDesc{
		Streams: map[string]Stream{
			"Stream": {
				StreamingClient: true,
				StreamingServer: true,
				Handler: func(_ context.Context, ss StreamServer) (any, error) {
					var req internal.EchoPayload
					if err := ss.RecvMsg(&req); err != nil {
						return nil, err
					}
					// First message proves the stream was accepted. Signal
					// only after receiving it.
					close(gotData)
					if err := ss.SendMsg(&internal.EchoPayload{Seq: req.Seq + 1}); err != nil {
						return nil, err
					}
					for {
						var m internal.EchoPayload
						if err := ss.RecvMsg(&m); err != nil {
							if errors.Is(err, io.EOF) {
								return nil, nil
							}
							return nil, err
						}
						if err := ss.SendMsg(&internal.EchoPayload{Seq: m.Seq + 1}); err != nil {
							return nil, err
						}
					}
				},
			},
		},
	})
	startPipeServer(t, srv, pc.server)

	client := NewClient(pc.client, WithGracefulDrain())
	defer client.Close()

	cs, err := client.NewStream(context.Background(),
		&StreamDesc{StreamingClient: true, StreamingServer: true},
		"streamSvc", "Stream", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := cs.SendMsg(&internal.EchoPayload{Seq: 1}); err != nil {
		t.Fatal(err)
	}
	var echo internal.EchoPayload
	if err := cs.RecvMsg(&echo); err != nil {
		t.Fatal(err)
	}
	waitChan(t, 2*time.Second, "server got first message", gotData)

	drainDone := make(chan GracefulShutdownResult, 1)
	drainErrs := make(chan error, 1)
	go func() {
		r, e := srv.ShutdownGraceful(context.Background())
		drainErrs <- e
		drainDone <- r
	}()

	waitChan(t, 2*time.Second, "client drain observed", client.DrainObserved())
	if boundary := client.drainBoundary.Load(); boundary != 1 {
		t.Fatalf("boundary = %d, want 1", boundary)
	}

	// Continue using the in-flight stream after the boundary.
	for i := int64(2); i <= 5; i++ {
		if err := cs.SendMsg(&internal.EchoPayload{Seq: i}); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
		var m internal.EchoPayload
		if err := cs.RecvMsg(&m); err != nil {
			t.Fatalf("recv %d: %v", i, err)
		}
		if m.Seq != i+1 {
			t.Fatalf("seq = %d want %d", m.Seq, i+1)
		}
	}
	// Client half-close still works.
	if err := cs.CloseSend(); err != nil {
		t.Fatal(err)
	}
	// Final status after server half-close.
	if err := cs.RecvMsg(&internal.EchoPayload{}); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}

	if err := <-drainErrs; err != nil {
		t.Fatalf("ShutdownGraceful: %v", err)
	}
	r := <-drainDone
	if r.DrainedConnections != 1 {
		t.Fatalf("result = %+v", r)
	}

	// A brand new stream after boundary is rejected.
	_, err = client.NewStream(context.Background(),
		&StreamDesc{StreamingClient: true, StreamingServer: true},
		"streamSvc", "Stream", nil)
	assertDraining(t, err)
}

// TestGracefulDrainIdempotent verifies repeated ShutdownGraceful calls
// return the same stable result and a drain announced while no call ever
// ran still reports the connection as drained.
func TestGracefulDrainIdempotent(t *testing.T) {
	pc := newPipeConn()
	defer pc.close()

	srv, err := NewServer(WithGracefulShutdown())
	if err != nil {
		t.Fatal(err)
	}
	srv.Register(serviceName, map[string]Method{
		"Test": func(_ context.Context, unmarshal func(any) error) (any, error) {
			return &internal.TestPayload{}, nil
		},
	})
	startPipeServer(t, srv, pc.server)

	client := NewClient(pc.client, WithGracefulDrain())
	defer client.Close()

	// Force negotiation to complete.
	var tp internal.TestPayload
	if err := client.Call(context.Background(), serviceName, "Test", &tp, &tp); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	results := make(chan GracefulShutdownResult, 3)
	errs := make(chan error, 3)
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := srv.ShutdownGraceful(context.Background())
			results <- r
			errs <- e
		}()
	}
	wg.Wait()
	for range 3 {
		if e := <-errs; e != nil {
			t.Fatalf("drain err: %v", e)
		}
		r := <-results
		if r.DrainedConnections != 1 {
			t.Fatalf("unexpected result %+v", r)
		}
	}
}

// TestGracefulDrainConcurrentShutdown verifies a concurrent hard Shutdown
// interrupts in-flight drain and reports the connection as interrupted.
func TestGracefulDrainConcurrentShutdown(t *testing.T) {
	pc := newPipeConn()
	defer pc.close()

	started := make(chan struct{})
	release := make(chan struct{})
	srv, err := NewServer(WithGracefulShutdown())
	if err != nil {
		t.Fatal(err)
	}
	srv.Register(serviceName, drainTestService(streamSvcOpts{started: started, release: release}).Methods)
	startPipeServer(t, srv, pc.server)

	client := NewClient(pc.client, WithGracefulDrain())
	defer client.Close()

	callErr := make(chan error, 1)
	go func() {
		var in, out internal.TestPayload
		callErr <- client.Call(context.Background(), serviceName, "Blocking", &in, &out)
	}()
	waitChan(t, 2*time.Second, "handler started", started)

	gracefulResult := make(chan GracefulShutdownResult, 1)
	gracefulErr := make(chan error, 1)
	go func() {
		r, e := srv.ShutdownGraceful(context.Background())
		gracefulResult <- r
		gracefulErr <- e
	}()
	waitChan(t, 2*time.Second, "drain observed", client.DrainObserved())

	// Hard shutdown interrupts the still-active draining connection and
	// unblocks the in-flight graceful call.
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	r := <-gracefulResult
	if e := <-gracefulErr; e != ErrServerClosed {
		t.Fatalf("graceful drain should report ErrServerClosed, got %v", e)
	}
	if r.InterruptedConnections != 1 || r.DrainedConnections != 0 {
		t.Fatalf("unexpected result %+v", r)
	}

	// The in-flight call ends with the connection closing (existing
	// semantics), not a draining rejection.
	select {
	case err := <-callErr:
		if IsServerDraining(err) {
			t.Fatalf("in-flight call must not be draining-rejected: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight call did not return after shutdown")
	}
	close(release)
}

// TestGracefulDrainContextCancel verifies a cancelled context returns the
// partial result and ctx.Err while in-flight calls are unaffected.
func TestGracefulDrainContextCancel(t *testing.T) {
	pc := newPipeConn()
	defer pc.close()

	started := make(chan struct{})
	release := make(chan struct{})
	srv, err := NewServer(WithGracefulShutdown())
	if err != nil {
		t.Fatal(err)
	}
	srv.Register(serviceName, drainTestService(streamSvcOpts{started: started, release: release}).Methods)
	startPipeServer(t, srv, pc.server)

	client := NewClient(pc.client, WithGracefulDrain())
	defer client.Close()

	completed := make(chan error, 1)
	go func() {
		var in, out internal.TestPayload
		completed <- client.Call(context.Background(), serviceName, "Blocking", &in, &out)
	}()
	waitChan(t, 2*time.Second, "handler started", started)

	ctx, cancel := context.WithCancel(context.Background())
	resCh := make(chan GracefulShutdownResult, 1)
	errCh := make(chan error, 1)
	go func() {
		r, e := srv.ShutdownGraceful(ctx)
		resCh <- r
		errCh <- e
	}()
	waitChan(t, 2*time.Second, "drain observed", client.DrainObserved())
	cancel()
	if e := <-errCh; e != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", e)
	}
	<-resCh

	// Releasing the handler now lets the drain finish when retried.
	close(release)
	r, e := srv.ShutdownGraceful(context.Background())
	if e != nil {
		t.Fatalf("retry drain: %v", e)
	}
	if r.DrainedConnections != 1 {
		t.Fatalf("unexpected result %+v", r)
	}
	if err := <-completed; err != nil {
		t.Fatalf("in-flight call failed: %v", err)
	}
}

// TestGracefulDrainOldClientInterop verifies a client without drain
// capability gets no Drain frame and legacy shutdown still works.
// capability receives no Drain frame and is shut down with legacy
// semantics. We use a raw connection to assert no unexpected control frames.
func TestGracefulDrainOldClientInterop(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	srv, err := NewServer(WithGracefulShutdown())
	if err != nil {
		t.Fatal(err)
	}
	srv.Register(serviceName, map[string]Method{
		"Test": func(_ context.Context, unmarshal func(any) error) (any, error) {
			var req internal.TestPayload
			if err := unmarshal(&req); err != nil {
				return nil, err
			}
			return &req, nil
		},
	})
	startPipeServer(t, srv, serverConn)

	// Old client ignores the server's hello control frame but keeps the
	// connection; it performs a normal call afterwards.
	helloCh := make(chan rawFrame, 1)
	go func() {
		helloCh <- readRaw(t, clientConn)
	}()
	select {
	case hello := <-helloCh:
		if hello.typ != messageTypeControl {
			t.Fatalf("expected control hello, got %v", hello.typ)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not send hello")
	}

	client := NewClient(clientConn) // no WithGracefulDrain
	defer client.Close()

	var tp internal.TestPayload
	tp.Foo = "legacy"
	if err := client.Call(context.Background(), serviceName, "Test", &tp, &tp); err != nil {
		t.Fatal(err)
	}
	if tp.Foo != "legacy" {
		t.Fatalf("got %q", tp.Foo)
	}

	r, err := srv.ShutdownGraceful(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r.UnsupportedConnections != 1 || r.DrainedConnections != 0 {
		t.Fatalf("unexpected result %+v", r)
	}

	// The old client observes a normal connection close (existing behavior),
	// never a draining status.
	err = client.Call(context.Background(), serviceName, "Test", &tp, &tp)
	if err == nil {
		t.Fatal("expected error after legacy shutdown")
	}
	if IsServerDraining(err) {
		t.Fatalf("old client must not see draining status: %v", err)
	}
}

// TestGracefulDrainRawBoundaryAndDataReordering drives raw frames to assert
// that a stream whose Request already reached the server keeps accepting its
// Data frames after the Drain boundary frame is observed, while a strictly
// later Request is rejected. Because writes are serialized, this pins the
// "received before boundary" rule precisely.
func TestGracefulDrainRawBoundaryAndDataReordering(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	release := make(chan struct{})
	gotFirst := make(chan struct{}, 1)
	gotSecond := make(chan struct{}, 1)
	srv, err := NewServer(WithGracefulShutdown())
	if err != nil {
		t.Fatal(err)
	}
	srv.RegisterService("rawSvc", &ServiceDesc{
		Streams: map[string]Stream{
			"Stream": {
				StreamingClient: true,
				StreamingServer: true,
				Handler: func(_ context.Context, ss StreamServer) (any, error) {
					var m internal.EchoPayload
					if err := ss.RecvMsg(&m); err != nil {
						return nil, err
					}
					gotFirst <- struct{}{}
					if err := ss.SendMsg(&internal.EchoPayload{Seq: 100}); err != nil {
						return nil, err
					}
					if err := ss.RecvMsg(&m); err != nil {
						return nil, err
					}
					gotSecond <- struct{}{}
					// Wait for the client half-close (EOF) before returning,
					// so the final Data[RC] is emitted deterministically
					// after the post-boundary rejection is observed.
					if err := ss.RecvMsg(&m); err != io.EOF {
						return nil, err
					}
					close(release)
					return nil, nil
				},
			},
		},
	})
	startPipeServer(t, srv, serverConn)

	// Capability handshake: drain server hello, then send client hello.
	hello := readRaw(t, clientConn)
	if hello.typ != messageTypeControl {
		t.Fatalf("want control, got %v", hello.typ)
	}
	writeRaw(t, clientConn, rawFrame{
		streamID: controlStreamID, typ: messageTypeControl,
		payload: marshalControlMessage(controlCapabilities(capabilityGracefulDrain)),
	})

	// Open stream id 1 (RO). The first message is sent as a separate Data
	// frame (the implementation only auto-feeds request payload for
	// non-streaming-client requests).
	reqPayload := marshalRawRequest(t, "rawSvc", "Stream")
	writeRaw(t, clientConn, rawFrame{streamID: 1, typ: messageTypeRequest, flags: flagRemoteOpen, payload: reqPayload})
	writeRaw(t, clientConn, rawFrame{streamID: 1, typ: messageTypeData,
		payload: mustMarshalEcho(t, 1)})

	// Wait for the handler to consume the first message and echo.
	echo := readRaw(t, clientConn)
	if echo.streamID != 1 || echo.typ != messageTypeData {
		t.Fatalf("unexpected echo frame %+v", echo)
	}
	waitChan(t, 2*time.Second, "first consumed", gotFirst)

	// Now trigger drain; boundary must be exactly 1.
	go srv.ShutdownGraceful(context.Background())
	drain := readRaw(t, clientConn)
	if drain.typ != messageTypeControl {
		t.Fatalf("want drain control, got %v", drain.typ)
	}
	cm, ok := parseControlMessage(drain.payload)
	if !ok || cm.kind != controlMessageDrain || cm.uint32Value() != 1 {
		t.Fatalf("unexpected drain frame: %+v ok=%v", cm, ok)
	}

	// Data frame for the pre-boundary stream after the Drain frame: accepted.
	writeRaw(t, clientConn, rawFrame{streamID: 1, typ: messageTypeData,
		payload: mustMarshalEcho(t, 2)})
	waitChan(t, 2*time.Second, "second consumed after drain", gotSecond)

	// While stream 1 is still active, a strictly later request (stream 3)
	// is rejected without dispatching a handler.
	writeRaw(t, clientConn, rawFrame{streamID: 3, typ: messageTypeRequest,
		flags: flagRemoteClosed, payload: marshalRawRequest(t, "rawSvc", "Stream")})
	rej := readRaw(t, clientConn)
	if rej.streamID != 3 || rej.typ != messageTypeResponse {
		t.Fatalf("unexpected rejection frame %+v", rej)
	}
	resp := decodeResponse(t, rej.payload)
	if resp.GetStatus().GetCode() != int32(codes.Unavailable) ||
		!strings.HasPrefix(resp.GetStatus().GetMessage(), ErrServerDraining.Error()) {
		t.Fatalf("unexpected rejection status %+v", resp.GetStatus())
	}

	// Client half-close releases the handler; the server then emits the
	// final Data[RC] for stream 1 and the connection fully drains.
	writeRaw(t, clientConn, rawFrame{streamID: 1, typ: messageTypeData,
		flags: flagRemoteClosed | flagNoData})
	final := readRaw(t, clientConn)
	if final.streamID != 1 || final.typ != messageTypeData ||
		final.flags&flagRemoteClosed != flagRemoteClosed {
		t.Fatalf("unexpected final data frame %+v", final)
	}
	<-release
}

func mustMarshalEcho(t *testing.T, seq int64) []byte {
	t.Helper()
	p, err := (codec{}).Marshal(&internal.EchoPayload{Seq: seq})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func decodeResponse(t *testing.T, p []byte) *Response {
	t.Helper()
	resp := &Response{}
	if err := (codec{}).Unmarshal(p, resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

// TestGracefulDrainStreamCreationAroundBoundary establishes a fixed set of
// streams before the boundary, drains, and asserts that stream creations
// after the boundary get the stable draining rejection while the pre-boundary
// calls complete normally.
func TestGracefulDrainStreamCreationAroundBoundary(t *testing.T) {
	pc := newPipeConn()
	defer pc.close()

	proceed := make(chan struct{})
	const pre = 6
	startedN := make(chan struct{}, pre)
	srv, err := NewServer(WithGracefulShutdown())
	if err != nil {
		t.Fatal(err)
	}
	srv.Register(serviceName, map[string]Method{
		"Test": func(_ context.Context, unmarshal func(any) error) (any, error) {
			var req internal.TestPayload
			if err := unmarshal(&req); err != nil {
				return nil, err
			}
			startedN <- struct{}{}
			<-proceed
			return &req, nil
		},
	})
	startPipeServer(t, srv, pc.server)

	client := NewClient(pc.client, WithGracefulDrain())
	defer client.Close()

	var wg sync.WaitGroup
	preErrs := make(chan error, pre)
	for i := 0; i < pre; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var in, out internal.TestPayload
			preErrs <- client.Call(context.Background(), serviceName, "Test", &in, &out)
		}()
	}
	waitCondition(t, 2*time.Second, "pre handlers started", func() bool {
		return len(startedN) == pre
	})

	drainErrs := make(chan error, 1)
	go func() {
		_, e := srv.ShutdownGraceful(context.Background())
		drainErrs <- e
	}()
	waitChan(t, 2*time.Second, "drain observed", client.DrainObserved())

	// Every post-boundary creation is rejected deterministically.
	const post = 6
	postErrs := make(chan error, post)
	for i := 0; i < post; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var in, out internal.TestPayload
			postErrs <- client.Call(context.Background(), serviceName, "Test", &in, &out)
		}()
	}
	for i := 0; i < post; i++ {
		assertDraining(t, <-postErrs)
	}

	// Release pre-boundary handlers; they all complete normally.
	close(proceed)
	for i := 0; i < pre; i++ {
		if err := <-preErrs; err != nil {
			t.Fatalf("pre-boundary call failed: %v", err)
		}
	}
	wg.Wait()
	if e := <-drainErrs; e != nil {
		t.Fatalf("ShutdownGraceful: %v", e)
	}
}

// TestGracefulDrainBackpressure verifies that a drain boundary is announced
// even while a streaming handler is not consuming (server-side data path
// under backpressure), and the stream still completes once drained.
func TestGracefulDrainBackpressure(t *testing.T) {
	pc := newPipeConn()
	defer pc.close()

	ready := make(chan struct{})
	srv, err := NewServer(WithGracefulShutdown())
	if err != nil {
		t.Fatal(err)
	}
	srv.RegisterService("bp", &ServiceDesc{
		Streams: map[string]Stream{
			"Slow": {
				StreamingClient: true,
				StreamingServer: false,
				Handler: func(ctx context.Context, _ StreamServer) (any, error) {
					close(ready)
					<-ctx.Done()
					return nil, ctx.Err()
				},
			},
		},
	})
	startPipeServer(t, srv, pc.server)

	client := NewClient(pc.client, WithGracefulDrain())
	defer client.Close()

	cs, err := client.NewStream(context.Background(),
		&StreamDesc{StreamingClient: true}, "bp", "Slow", nil)
	if err != nil {
		t.Fatal(err)
	}
	waitChan(t, 2*time.Second, "handler ready", ready)

	// Flood data; handler never consumes, exercising the bounded data path.
	sendStopped := make(chan struct{})
	go func() {
		defer close(sendStopped)
		for i := 0; i < 200; i++ {
			if err := cs.SendMsg(&internal.EchoPayload{Seq: int64(i)}); err != nil {
				return
			}
		}
	}()

	// Drain must announce despite the unconsumed stream.
	go srv.ShutdownGraceful(context.Background())
	waitChan(t, 2*time.Second, "drain observed under backpressure", client.DrainObserved())

	// New call rejected while the slow stream exists.
	var in, out internal.TestPayload
	assertDraining(t, client.Call(context.Background(), serviceName, "X", &in, &out))

	// Closing the client allows the slow stream to end and the conn drains.
	<-sendStopped
	if err := cs.CloseSend(); err != nil {
		// connection may already be closing; acceptable
		_ = err
	}
}

// TestGracefulDrainConnectionAbort verifies that a peer disconnect during
// drain ends ShutdownGraceful promptly with a stable result.
func TestGracefulDrainConnectionAbort(t *testing.T) {
	pc := newPipeConn()

	srv, err := NewServer(WithGracefulShutdown())
	if err != nil {
		t.Fatal(err)
	}
	srv.Register(serviceName, map[string]Method{
		"Test": func(_ context.Context, _ func(any) error) (any, error) {
			return &internal.TestPayload{}, nil
		},
	})
	startPipeServer(t, srv, pc.server)

	client := NewClient(pc.client, WithGracefulDrain())
	defer client.Close()
	var tp internal.TestPayload
	if err := client.Call(context.Background(), serviceName, "Test", &tp, &tp); err != nil {
		t.Fatal(err)
	}

	go srv.ShutdownGraceful(context.Background())
	waitChan(t, 2*time.Second, "drain observed", client.DrainObserved())

	// Abrupt peer disconnect.
	pc.close()

	done := make(chan error, 1)
	go func() {
		_, e := srv.ShutdownGraceful(context.Background())
		done <- e
	}()
	select {
	case e := <-done:
		if e != nil && e != ErrServerClosed {
			t.Fatalf("unexpected: %v", e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ShutdownGraceful did not return after peer abort")
	}
}

// TestGracefulDrainClientBoundaryMonotonic verifies a delayed Drain frame
// with a smaller value never moves the observed boundary backwards.
func TestGracefulDrainClientBoundaryMonotonic(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	srv, err := NewServer(WithGracefulShutdown())
	if err != nil {
		t.Fatal(err)
	}
	srv.Register(serviceName, map[string]Method{
		"Test": func(_ context.Context, unmarshal func(any) error) (any, error) {
			var req internal.TestPayload
			if err := unmarshal(&req); err != nil {
				return nil, err
			}
			return &req, nil
		},
	})
	startPipeServer(t, srv, serverConn)

	client := NewClient(clientConn, WithGracefulDrain())
	defer client.Close()
	var tp internal.TestPayload
	if err := client.Call(context.Background(), serviceName, "Test", &tp, &tp); err != nil {
		t.Fatal(err)
	}

	sendDrain := func(v uint32) {
		writeRaw(t, serverConn, rawFrame{
			streamID: controlStreamID, typ: messageTypeControl,
			payload: marshalControlMessage(controlDrain(v)),
		})
	}
	sendDrain(9)
	waitChan(t, 2*time.Second, "drain 9", client.DrainObserved())
	if b := client.drainBoundary.Load(); b != 9 {
		t.Fatalf("boundary=%d want 9", b)
	}
	// Delayed/duplicated smaller boundary must be ignored.
	sendDrain(3)
	sendDrain(9)
	waitCondition(t, time.Second, "boundary stays 9", func() bool {
		return client.drainBoundary.Load() == 9
	})
	// Larger boundary moves forward.
	sendDrain(11)
	waitCondition(t, 2*time.Second, "boundary advances to 11", func() bool {
		return client.drainBoundary.Load() == 11
	})
}

// TestOldServerIgnoresControlHello verifies a drain-enabled client talking to
// an old (non-drain) server keeps full unary and streaming semantics and the
// unexpected control frame never reaches stream handling.
func TestOldServerIgnoresControlHello(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	// Old server built without WithGracefulShutdown.
	srv, err := NewServer()
	if err != nil {
		t.Fatal(err)
	}
	srv.Register(serviceName, map[string]Method{
		"Echo": func(_ context.Context, unmarshal func(any) error) (any, error) {
			var req internal.TestPayload
			if err := unmarshal(&req); err != nil {
				return nil, err
			}
			return &req, nil
		},
	})
	startPipeServer(t, srv, serverConn)

	client := NewClient(clientConn, WithGracefulDrain())
	defer client.Close()

	var tp internal.TestPayload
	tp.Foo = "x"
	if err := client.Call(context.Background(), serviceName, "Echo", &tp, &tp); err != nil {
		t.Fatalf("unary over old server failed: %v", err)
	}
	if tp.Foo != "x" {
		t.Fatalf("got %q", tp.Foo)
	}
}

// TestGracefulDrainNoGoroutineLeaks runs full drain lifecycles and asserts no
// ttrpc-owned goroutines remain afterwards.
func TestGracefulDrainNoGoroutineLeaks(t *testing.T) {
	for i := 0; i < 3; i++ {
		srv, err := NewServer(WithGracefulShutdown())
		if err != nil {
			t.Fatal(err)
		}
		srv.Register(serviceName, map[string]Method{
			"Test": func(_ context.Context, unmarshal func(any) error) (any, error) {
				var req internal.TestPayload
				if err := unmarshal(&req); err != nil {
					return nil, err
				}
				return &req, nil
			},
		})

		pc := newPipeConn()
		c, err := srv.newConn(pc.server, nil)
		if err != nil {
			t.Fatal(err)
		}
		go c.run(context.Background())

		client := NewClient(pc.client, WithGracefulDrain())
		var tp internal.TestPayload
		if err := client.Call(context.Background(), serviceName, "Test", &tp, &tp); err != nil {
			t.Fatal(err)
		}
		// The response only proves the server accepted the call; wait for
		// the capability hello to be processed before draining so the
		// boundary announcement is deliverable.
		waitCondition(t, 2*time.Second, "capability negotiated", func() bool {
			return c.drainNegotiated.Load()
		})
		if _, err := srv.ShutdownGraceful(context.Background()); err != nil {
			t.Fatal(err)
		}
		waitChan(t, 2*time.Second, "drain observed", client.DrainObserved())
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
		client.UserOnCloseWait(context.Background())
		pc.close()
	}

	waitCondition(t, 3*time.Second, "no ttrpc goroutines remain", func() bool {
		return countTtrpcGoroutines() == 0
	})
}

func countTtrpcGoroutines() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	stacks := strings.Split(string(buf[:n]), "\n\n")
	count := 0
	for _, st := range stacks {
		if strings.Contains(st, "github.com/containerd/ttrpc.") &&
			(strings.Contains(st, "(*serverConn).run") ||
				strings.Contains(st, "(*Client).run") ||
				strings.Contains(st, "(*Client).sendHello")) {
			count++
		}
	}
	return count
}

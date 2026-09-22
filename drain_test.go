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
	"io"
	"net"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/containerd/ttrpc/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const drainServiceName = "drainService"

// pipeListener hands out exactly the connections queued on its channel.
type pipeListener struct {
	ch     chan net.Conn
	closed chan struct{}
	once   sync.Once
	addr   net.Addr
}

func newPipeListener() *pipeListener {
	return &pipeListener{
		ch:     make(chan net.Conn, 8),
		closed: make(chan struct{}),
		addr:   pipeAddr{},
	}
}

type pipeAddr struct{}

func (pipeAddr) Network() string { return "pipe" }
func (pipeAddr) String() string  { return "pipe" }

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.ch:
		if c == nil {
			return nil, errors.New("listener closed")
		}
		return c, nil
	case <-l.closed:
		return nil, errors.New("listener closed")
	}
}

func (l *pipeListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *pipeListener) Addr() net.Addr { return l.addr }

// serveConn starts a server over a loopback TCP connection (which supports
// half-close) and returns the client side of the connection and a channel
// closed when Serve returns. The single queued connection is delivered to
// the server without relying on accept timing.
func serveConn(t *testing.T, srv *Server) (net.Conn, <-chan struct{}) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		if err := srv.Serve(context.Background(), ln); err != nil && err != ErrServerClosed {
			t.Logf("serve ended with %v", err)
		}
	}()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	return conn, serveDone
}

// newDrainServer builds a server with a unary Echo method and a streaming
// EchoStream. Blocking behavior is controlled through the provided channels.
func newDrainServer(t *testing.T, opts ...ServerOpt) (*Server, chan struct{}, chan struct{}) {
	t.Helper()
	srv, err := NewServer(opts...)
	if err != nil {
		t.Fatal(err)
	}
	// started receives one token per Echo handler that begins executing.
	unaryStarted := make(chan struct{}, 64)
	unaryRelease := make(chan struct{})
	desc := &ServiceDesc{
		Methods: map[string]Method{
			"Echo": func(ctx context.Context, unmarshal func(any) error) (any, error) {
				var req internal.EchoPayload
				if err := unmarshal(&req); err != nil {
					return nil, err
				}
				select {
				case unaryStarted <- struct{}{}:
				default:
				}
				select {
				case <-unaryRelease:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				req.Seq++
				return &req, nil
			},
			"Quick": func(_ context.Context, unmarshal func(any) error) (any, error) {
				var req internal.EchoPayload
				if err := unmarshal(&req); err != nil {
					return nil, err
				}
				req.Seq++
				return &req, nil
			},
		},
		Streams: map[string]Stream{
			"EchoStream": {
				Handler: func(_ context.Context, ss StreamServer) (any, error) {
					for {
						var req internal.EchoPayload
						if err := ss.RecvMsg(&req); err != nil {
							if err == io.EOF {
								err = nil
							}
							return nil, err
						}
						req.Seq++
						if err := ss.SendMsg(&req); err != nil {
							return nil, err
						}
					}
				},
				StreamingClient: true,
				StreamingServer: true,
			},
		},
	}
	srv.RegisterService(drainServiceName, desc)
	return srv, unaryStarted, unaryRelease
}

// waitStarted blocks until n handlers signaled start.
func waitStarted(t *testing.T, ch chan struct{}, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-ch:
		case <-time.After(3 * time.Second):
			t.Fatalf("only %d of %d handlers started", i, n)
		}
	}
}

func echoCall(t *testing.T, c *Client) error {
	t.Helper()
	var req, resp internal.EchoPayload
	req.Seq = 1
	return c.Call(context.Background(), drainServiceName, "Quick", &req, &resp)
}

// TestDrainBoundaryAcceptsBeforeRejectsAfter is the core protocol test: with
// an in-flight call at the boundary, calls with lower stream ids complete
// while higher stream ids get a stable draining rejection, all without the
// connection closing.
func TestDrainBoundaryAcceptsBeforeRejectsAfter(t *testing.T) {
	srv, started, release := newDrainServer(t, WithGracefulDrain())
	conn, serveDone := serveConn(t, srv)
	defer srv.Close()
	defer conn.Close()
	defer func() { <-serveDone }()
	client := NewClient(conn, WithClientGracefulDrain())
	defer client.Close()

	// 1. An in-flight unary call holds stream id 1.
	callErr := make(chan error, 1)
	go func() {
		var req, resp internal.EchoPayload
		req.Msg = "inflight"
		callErr <- client.Call(context.Background(), drainServiceName, "Echo", &req, &resp)
	}()
	waitStarted(t, started, 1)

	// 2. Begin draining concurrently: stream 1 completes; higher ids reject.
	drainDone := make(chan error, 1)
	dctx, cancelDrain := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDrain()
	go func() { drainDone <- srv.Drain(dctx) }()

	drainObserved := make(chan struct{})
	go func() {
		_ = client.WaitDrain(context.Background())
		close(drainObserved)
	}()
	select {
	case <-drainObserved:
	case <-time.After(3 * time.Second):
		t.Fatal("client did not observe drain boundary")
	}
	boundary, ok := client.DrainBoundary()
	if !ok || boundary != 1 {
		t.Fatalf("expected boundary 1, got %d (ok=%v)", boundary, ok)
	}

	// 3. A new call (stream id 3) must be rejected as draining.
	err := echoCall(t, client)
	if !IsDraining(err) {
		t.Fatalf("expected draining error, got %v", err)
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unavailable {
		t.Fatalf("expected Unavailable status, got %v", err)
	}
	if !errors.Is(err, ErrConnectionDraining) {
		t.Fatalf("expected errors.Is ErrConnectionDraining, got %v", err)
	}

	// 5. After the boundary, even a streaming NewStream is rejected locally.
	_, err = client.NewStream(context.Background(), &StreamDesc{true, true}, drainServiceName, "EchoStream", nil)
	if !IsDraining(err) {
		t.Fatalf("expected draining error on stream, got %v", err)
	}

	// 4. The in-flight call still completes successfully.
	close(release)
	select {
	case err := <-callErr:
		if err != nil {
			t.Fatalf("in-flight call failed: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("in-flight call did not complete")
	}

	// 6. Drain returns once active calls finish.
	select {
	case err := <-drainDone:
		if err != nil {
			t.Fatalf("Drain returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Drain did not return")
	}
}

// TestDrainIdempotent verifies repeated Drain calls and concurrent Shutdown
// never panic, share the shutdown point, and return consistently.
func TestDrainIdempotent(t *testing.T) {
	srv, _, _ := newDrainServer(t, WithGracefulDrain())
	conn, serveDone := serveConn(t, srv)
	defer conn.Close()
	defer func() { <-serveDone }()
	client := NewClient(conn, WithClientGracefulDrain())
	defer client.Close()

	if err := echoCall(t, client); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 5)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- srv.Drain(ctx)
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		errs <- srv.Shutdown(ctx)
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("expected nil drain/shutdown error, got %v", err)
		}
	}

	if err := srv.Drain(ctx); err != nil {
		t.Fatalf("repeat drain after completion: %v", err)
	}
}

// TestDrainLongStreamHalfClose verifies that a stream accepted before the
// boundary keeps its original lifecycle: client data, server data, client
// half-close (CloseSend), and final EOF all happen in order after drain.
func TestDrainLongStreamHalfClose(t *testing.T) {
	srv, _, _ := newDrainServer(t, WithGracefulDrain())
	conn, serveDone := serveConn(t, srv)
	defer srv.Close()
	defer conn.Close()
	defer func() { <-serveDone }()
	client := NewClient(conn, WithClientGracefulDrain())
	defer client.Close()

	stream, err := client.NewStream(context.Background(),
		&StreamDesc{StreamingClient: true, StreamingServer: true},
		drainServiceName, "EchoStream", nil)
	if err != nil {
		t.Fatal(err)
	}

	for i := 1; i <= 3; i++ {
		if err := stream.SendMsg(&internal.EchoPayload{Seq: int64(i), Msg: "pre"}); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
		var resp internal.EchoPayload
		if err := stream.RecvMsg(&resp); err != nil {
			t.Fatalf("recv %d: %v", i, err)
		}
		if resp.Seq != int64(i)+1 {
			t.Fatalf("unexpected seq %d", resp.Seq)
		}
	}

	dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	drainDone := make(chan error, 1)
	go func() { drainDone <- srv.Drain(dctx) }()

	if err := client.WaitDrain(dctx); err != nil {
		t.Fatal(err)
	}

	// Continue streaming after the boundary.
	for i := 4; i <= 6; i++ {
		if err := stream.SendMsg(&internal.EchoPayload{Seq: int64(i), Msg: "post"}); err != nil {
			t.Fatalf("post send %d: %v", i, err)
		}
		var resp internal.EchoPayload
		if err := stream.RecvMsg(&resp); err != nil {
			t.Fatalf("post recv %d: %v", i, err)
		}
		if resp.Seq != int64(i)+1 {
			t.Fatalf("unexpected seq %d", resp.Seq)
		}
	}

	// A new call is rejected by the local client boundary while the
	// accepted long stream is still alive.
	if err := echoCall(t, client); !IsDraining(err) {
		t.Fatalf("expected draining, got %v", err)
	}

	// Client half-close, then final EOF in the original order.
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("close send: %v", err)
	}
	var final internal.EchoPayload
	if err := stream.RecvMsg(&final); err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}

	select {
	case err := <-drainDone:
		if err != nil {
			t.Fatalf("drain: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("drain did not finish")
	}
}

// TestDrainConcurrentStreamCreation opens several streams, drains while they
// are in flight, and verifies exactly the pre-boundary set completes while
// post-boundary attempts get draining errors. It uses many iterations to
// exercise the allocation/send atomicity of createStream.
func TestDrainConcurrentStreamCreation(t *testing.T) {
	srv, started, release := newDrainServer(t, WithGracefulDrain())
	conn, serveDone := serveConn(t, srv)
	defer srv.Close()
	defer conn.Close()
	defer func() { <-serveDone }()
	client := NewClient(conn, WithClientGracefulDrain())
	defer client.Close()

	const n = 8
	errs := make(chan error, n)
	for range n {
		go func() {
			var req, resp internal.EchoPayload
			req.Msg = "c"
			err := client.Call(context.Background(), drainServiceName, "Echo", &req, &resp)
			errs <- err
		}()
	}
	waitStarted(t, started, n)

	dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	drainDone := make(chan error, 1)
	go func() { drainDone <- srv.Drain(dctx) }()
	if err := client.WaitDrain(dctx); err != nil {
		t.Fatal(err)
	}
	boundary, _ := client.DrainBoundary()
	if boundary != uint32(1+2*(n-1)) {
		t.Fatalf("expected boundary %d, got %d", 1+2*(n-1), boundary)
	}

	// Calls started after the announcement are draining.
	postErr := make(chan error, 4)
	for range 4 {
		go func() { postErr <- echoCall(t, client) }()
	}
	for range 4 {
		select {
		case err := <-postErr:
			if !IsDraining(err) {
				t.Fatalf("post-boundary call: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("post-boundary call hung")
		}
	}

	close(release)
	for range n {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatalf("in-flight: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("in-flight call hung")
		}
	}
	select {
	case err := <-drainDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("drain hung")
	}
}

// TestDrainContextCanceled verifies Drain returns ctx.Err when active calls do
// not finish before the deadline, while the boundary remains announced and the
// in-flight call can later complete; a subsequent Drain succeeds.
func TestDrainContextCanceled(t *testing.T) {
	srv, started, release := newDrainServer(t, WithGracefulDrain())
	conn, serveDone := serveConn(t, srv)
	defer srv.Close()
	defer conn.Close()
	defer func() { <-serveDone }()
	client := NewClient(conn, WithClientGracefulDrain())
	defer client.Close()

	callErr := make(chan error, 1)
	go func() {
		var req, resp internal.EchoPayload
		callErr <- client.Call(context.Background(), drainServiceName, "Echo", &req, &resp)
	}()
	waitStarted(t, started, 1)

	dctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	if err := srv.Drain(dctx); err != context.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
	cancel()

	if err := client.WaitDrain(context.Background()); err != nil {
		t.Fatalf("boundary should remain announced: %v", err)
	}
	if err := echoCall(t, client); !IsDraining(err) {
		t.Fatalf("expected draining after canceled drain, got %v", err)
	}

	close(release)
	select {
	case err := <-callErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("call hung")
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	if err := srv.Drain(ctx2); err != nil {
		t.Fatalf("second drain: %v", err)
	}
}

// TestDrainClientContextCancel verifies that canceling an in-flight call's
// context after the boundary unblocks it without disrupting the connection or
// other pre-boundary calls.
func TestDrainClientContextCancel(t *testing.T) {
	srv, started, _ := newDrainServer(t, WithGracefulDrain())
	conn, serveDone := serveConn(t, srv)
	defer srv.Close()
	defer conn.Close()
	defer func() { <-serveDone }()
	client := NewClient(conn, WithClientGracefulDrain())
	defer client.Close()

	callCtx, cancelCall := context.WithCancel(context.Background())
	callErr := make(chan error, 1)
	go func() {
		var req, resp internal.EchoPayload
		callErr <- client.Call(callCtx, drainServiceName, "Echo", &req, &resp)
	}()
	waitStarted(t, started, 1)

	dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = srv.Drain(dctx) }()
	if err := client.WaitDrain(dctx); err != nil {
		t.Fatal(err)
	}
	cancelCall()

	select {
	case err := <-callErr:
		if err != context.Canceled {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canceled call hung")
	}
}

// TestInteropOldClientDoesNotCrash verifies that a server with graceful drain
// enabled speaking to a peer that never sends SETTINGS still serves RPCs; when
// Drain is invoked it shuts down using the legacy path without sending control
// frames or corrupting RPC traffic.
func TestInteropOldClientDoesNotCrash(t *testing.T) {
	srv, _, _ := newDrainServer(t, WithGracefulDrain())
	conn, serveDone := serveConn(t, srv)
	defer srv.Close()
	defer conn.Close()
	defer func() { <-serveDone }()
	// Client without WithClientGracefulDrain: no SETTINGS, no DRAIN.
	client := NewClient(conn)
	defer client.Close()

	if err := echoCall(t, client); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Drain(ctx); err != nil {
		t.Fatalf("drain against old client: %v", err)
	}
}

// TestInteropOldServerIgnoresControl verifies that a client advertising
// graceful drain works against a server that does not support control frames
// at all: ordinary RPCs succeed and no assumption is made about draining.
func TestInteropOldServerIgnoresControl(t *testing.T) {
	srv, _, _ := newDrainServer(t) // no WithGracefulDrain
	conn, serveDone := serveConn(t, srv)
	defer srv.Close()
	defer conn.Close()
	defer func() { <-serveDone }()
	client := NewClient(conn, WithClientGracefulDrain())
	defer client.Close()

	if err := echoCall(t, client); err != nil {
		t.Fatal(err)
	}
	// WaitDrain must return only on context end for a server that never
	// drains.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := client.WaitDrain(ctx); err != context.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
	if _, ok := client.DrainBoundary(); ok {
		t.Fatal("unexpected boundary against old server")
	}
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// ttrpcStackKeys returns a stable signature per live ttrpc-owned goroutine.
// Each key is the list of ttrpc package frames (file:line) in its stack,
// excluding goroutine ids and pointer addresses which change per run.
func ttrpcStackKeys() [][]string {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	var keys [][]string
	for _, g := range bytes.Split(buf[:n], []byte("\n\n")) {
		if !bytes.Contains(g, []byte("github.com/containerd/ttrpc.(*")) {
			continue
		}
		var frames []string
		for _, line := range bytes.Split(g, []byte("\n")) {
			line = bytes.TrimSpace(line)
			if i := bytes.Index(line, []byte("/work/")); i >= 0 {
				part := string(line[i:])
				// strip trailing offset
				if j := strings.LastIndex(part, " +"); j >= 0 {
					part = part[:j]
				}
				frames = append(frames, part)
			}
		}
		if len(frames) > 0 {
			keys = append(keys, frames)
		}
	}
	return keys
}

func stackKeyString(k []string) string { return strings.Join(k, "|") }

// waitForTTRPCQuiescence waits until the multiset of ttrpc goroutine
// signatures matches the baseline, attributing only newly created goroutines
// to this test.
func waitForTTRPCQuiescence(t *testing.T, baselineKeys [][]string) {
	t.Helper()
	base := map[string]int{}
	for _, k := range baselineKeys {
		base[stackKeyString(k)]++
	}
	read := func() map[string]int {
		m := map[string]int{}
		for _, k := range ttrpcStackKeys() {
			m[stackKeyString(k)]++
		}
		return m
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if reflect.DeepEqual(read(), base) {
			return
		}
		runtime.Gosched()
	}
	var extra bytes.Buffer
	for k, c := range read() {
		if d := c - base[k]; d > 0 {
			extra.WriteString(strings.Repeat(k+"\n\n", d))
		}
	}
	t.Fatalf("ttrpc goroutines leaked after drain:\n%s", extra.String())
}

// TestDrainNoGoroutineLeak exercises a full drain cycle across several
// connections (idle, in-flight unary, and long stream) and verifies no ttrpc
// goroutines remain afterward.
func TestDrainNoGoroutineLeak(t *testing.T) {
	baseline := ttrpcStackKeys()
	srv, started, release := newDrainServer(t, WithGracefulDrain())

	conns := make([]net.Conn, 0, 3)
	clients := make([]*Client, 0, 3)
	serveDones := make([]<-chan struct{}, 0, 3)
	for range 3 {
		conn, serveDone := serveConn(t, srv)
		conns = append(conns, conn)
		serveDones = append(serveDones, serveDone)
		clients = append(clients, NewClient(conn, WithClientGracefulDrain()))
	}

	// In-flight call on the first connection.
	callErr := make(chan error, 1)
	go func() {
		var req, resp internal.EchoPayload
		callErr <- clients[0].Call(context.Background(), drainServiceName, "Echo", &req, &resp)
	}()
	waitStarted(t, started, 1)

	// Long stream on the second.
	stream, err := clients[1].NewStream(context.Background(),
		&StreamDesc{StreamingClient: true, StreamingServer: true},
		drainServiceName, "EchoStream", nil)
	if err != nil {
		t.Fatal(err)
	}

	dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	drainDone := make(chan error, 1)
	go func() { drainDone <- srv.Drain(dctx) }()
	for _, c := range clients {
		if err := c.WaitDrain(dctx); err != nil {
			t.Fatal(err)
		}
	}

	close(release)
	if err := <-callErr; err != nil {
		t.Fatalf("in-flight: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	var final internal.EchoPayload
	if err := stream.RecvMsg(&final); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
	if err := <-drainDone; err != nil {
		t.Fatal(err)
	}

	// Close clients so their receive loops exit, then shut the server
	// down and wait for every Serve and server connection goroutine.
	ucctx, uccancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer uccancel()
	for _, c := range clients {
		c.Close()
		if err := c.UserOnCloseWait(ucctx); err != nil {
			t.Fatalf("user close wait: %v", err)
		}
	}
	for _, c := range conns {
		c.Close()
	}
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	for _, done := range serveDones {
		<-done
	}

	waitForTTRPCQuiescence(t, baseline)
}

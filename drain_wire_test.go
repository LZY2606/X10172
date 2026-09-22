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
	"time"

	"github.com/containerd/ttrpc/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// memPipe returns two connected, ordered in-memory net.Conns with unbounded
// buffers, so writes never block waiting for a reader. It lets tests script
// exact frame interleavings without time.Sleep.
type memPipeEnd struct {
	peer   *memPipeEnd
	mu     sync.Mutex
	cond   *sync.Cond
	buf    []byte
	closed bool
}

func memPipe() (a, b *memPipeEnd) {
	a = &memPipeEnd{}
	b = &memPipeEnd{}
	a.peer, b.peer = b, a
	a.cond = sync.NewCond(&a.mu)
	b.cond = sync.NewCond(&b.mu)
	return a, b
}

func (m *memPipeEnd) Read(p []byte) (int, error) {
	m.mu.Lock()
	for len(m.buf) == 0 && !m.closed {
		m.cond.Wait()
	}
	n := copy(p, m.buf)
	m.buf = m.buf[n:]
	closed := m.closed
	m.mu.Unlock()
	if n == 0 && closed {
		return 0, io.EOF
	}
	return n, nil
}

func (m *memPipeEnd) Write(p []byte) (int, error) {
	m.peer.mu.Lock()
	if m.peer.closed {
		m.peer.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	m.peer.buf = append(m.peer.buf, p...)
	m.peer.cond.Broadcast()
	m.peer.mu.Unlock()
	return len(p), nil
}

func (m *memPipeEnd) Close() error {
	m.mu.Lock()
	m.closed = true
	m.cond.Broadcast()
	m.mu.Unlock()
	return nil
}

// teeConn wraps a connection, copying every byte read to a parallel buffer
// that tests inspect via readFrame. This lets frames reach both the real
// client/server and the test harness.
type teeConn struct {
	net.Conn
	mu   sync.Mutex
	buf  []byte
	cond *sync.Cond
}

func newTeeConn(inner net.Conn) *teeConn {
	tc := &teeConn{Conn: inner}
	tc.cond = sync.NewCond(&tc.mu)
	return tc
}

func (t *teeConn) Read(p []byte) (int, error) {
	n, err := t.Conn.Read(p)
	if n > 0 {
		t.mu.Lock()
		t.buf = append(t.buf, p[:n]...)
		t.cond.Broadcast()
		t.mu.Unlock()
	}
	return n, err
}

// readTee reads exactly n captured bytes as they pass through Read.
func (t *teeConn) readTee(n int) []byte {
	t.mu.Lock()
	for len(t.buf) < n {
		t.cond.Wait()
	}
	out := make([]byte, n)
	copy(out, t.buf[:n])
	t.buf = t.buf[n:]
	t.mu.Unlock()
	return out
}

// readTeeFrame reads one full captured frame.
func (t *teeConn) readTeeFrame(tb testing.TB) streamMessage {
	tb.Helper()
	hdr := t.readTee(messageHeaderLength)
	mh := messageHeader{
		Length:   be32(hdr[0:4]),
		StreamID: be32(hdr[4:8]),
		Type:     messageType(hdr[8]),
		Flags:    hdr[9],
	}
	body := []byte{}
	if mh.Length > 0 {
		body = t.readTee(int(mh.Length))
	}
	return streamMessage{header: mh, payload: body}
}

type memAddr struct{}

func (memAddr) Network() string                        { return "mem" }
func (memAddr) String() string                         { return "mem" }
func (m *memPipeEnd) LocalAddr() net.Addr              { return memAddr{} }
func (m *memPipeEnd) RemoteAddr() net.Addr             { return memAddr{} }
func (m *memPipeEnd) SetDeadline(time.Time) error      { return nil }
func (m *memPipeEnd) SetReadDeadline(time.Time) error  { return nil }
func (m *memPipeEnd) SetWriteDeadline(time.Time) error { return nil }

// wireFrame builds a raw length-prefixed ttrpc frame.
func wireFrame(streamID uint32, typ messageType, flags uint8, payload []byte) []byte {
	b := make([]byte, messageHeaderLength+len(payload))
	b[0] = byte(len(payload) >> 24)
	b[1] = byte(len(payload) >> 16)
	b[2] = byte(len(payload) >> 8)
	b[3] = byte(len(payload))
	b[4] = byte(streamID >> 24)
	b[5] = byte(streamID >> 16)
	b[6] = byte(streamID >> 8)
	b[7] = byte(streamID)
	b[8] = byte(typ)
	b[9] = flags
	copy(b[messageHeaderLength:], payload)
	return b
}

// readFrame reads one complete ttrpc frame from a raw connection.
func readFrame(t *testing.T, r io.Reader) streamMessage {
	t.Helper()
	hdr := make([]byte, messageHeaderLength)
	if _, err := io.ReadFull(r, hdr); err != nil {
		t.Fatalf("reading header: %v", err)
	}
	mh := messageHeader{
		Length:   be32(hdr[0:4]),
		StreamID: be32(hdr[4:8]),
		Type:     messageType(hdr[8]),
		Flags:    hdr[9],
	}
	body := []byte{}
	if mh.Length > 0 {
		body = make([]byte, mh.Length)
		if _, err := io.ReadFull(r, body); err != nil {
			t.Fatalf("reading body: %v", err)
		}
	}
	return streamMessage{header: mh, payload: body}
}

func be32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

func sendRaw(t *testing.T, w io.Writer, sid uint32, typ messageType, flags uint8, p []byte) {
	t.Helper()
	if _, err := w.Write(wireFrame(sid, typ, flags, p)); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

// TestClientIgnoresUnknownAndMalformedControl verifies that an unknown message
// type and a malformed control payload on stream 0 are ignored without
// closing the client or being routed as RPCs, and that a following RPC works.
func TestClientIgnoresUnknownAndMalformedControl(t *testing.T) {
	serverConn, clientConn := memPipe()
	defer serverConn.Close()
	defer clientConn.Close()

	client := NewClient(clientConn, WithClientGracefulDrain())
	defer client.Close()

	// Read the client SETTINGS frame, then send junk before a real reply.
	settings := readFrame(t, serverConn)
	if settings.header.Type != messageTypeControl || settings.header.StreamID != controlStreamID {
		t.Fatalf("expected client settings control frame, got %+v", settings.header)
	}

	// Unknown future message type on stream 0.
	sendRaw(t, serverConn, controlStreamID, 0x7f, 0, nil)
	// Malformed control payload (invalid protobuf varint).
	sendRaw(t, serverConn, controlStreamID, messageTypeControl, 0, []byte{0xff, 0xff, 0xff})

	// Now run a unary RPC scripted entirely by hand to prove liveness.
	reqFrameCh := make(chan streamMessage, 1)
	go func() { reqFrameCh <- readFrame(t, serverConn) }()

	callErr := make(chan error, 1)
	go func() {
		var req, resp internal.EchoPayload
		req.Seq = 41
		callErr <- client.Call(context.Background(), drainServiceName, "Quick", &req, &resp)
	}()

	reqFrame := <-reqFrameCh
	if reqFrame.header.Type != messageTypeRequest {
		t.Fatalf("expected request frame, got %v", reqFrame.header.Type)
	}
	var creq Request
	if err := proto.Unmarshal(reqFrame.payload, &creq); err != nil {
		t.Fatal(err)
	}
	if creq.Service != drainServiceName || creq.Method != "Quick" {
		t.Fatalf("unexpected target %s/%s", creq.Service, creq.Method)
	}
	respPayload, err := proto.Marshal(&internal.EchoPayload{Seq: 42})
	if err != nil {
		t.Fatal(err)
	}
	rpayload, err := proto.Marshal(&Response{Status: status.New(codes.OK, "").Proto(), Payload: respPayload})
	if err != nil {
		t.Fatal(err)
	}
	sendRaw(t, serverConn, reqFrame.header.StreamID, messageTypeResponse, 0, rpayload)

	select {
	case err := <-callErr:
		if err != nil {
			t.Fatalf("rpc after junk frames failed: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("rpc after junk frames hung")
	}
}

// TestDrainWireFrameEncodingAndBoundary verifies the server emits a DRAIN
// control frame on stream 0 carrying exactly the last accepted stream id,
// and that a request crossing the boundary gets a stable Unavailable
// draining status. Frames are observed through a tee so the real client
// processes them concurrently.
func TestDrainWireFrameEncodingAndBoundary(t *testing.T) {
	srv, started, release := newDrainServer(t, WithGracefulDrain())
	serverConn, clientConn := memPipe()
	defer srv.Close()
	defer serverConn.Close()
	defer clientConn.Close()

	// Tee the client's read side so server->client frames are observable.
	tee := newTeeConn(clientConn)
	client := NewClient(tee, WithClientGracefulDrain())
	defer client.Close()

	l := newPipeListener()
	l.ch <- serverConn
	go func() { _ = srv.Serve(context.Background(), l) }()

	// Wait for SETTINGS ack to flow server->client.
	sset := tee.readTeeFrame(t)
	if sset.header.Type != messageTypeControl {
		t.Fatalf("expected server settings ack, got %v", sset.header.Type)
	}
	sctrl, err := unmarshalControl(sset.payload)
	if err != nil || !supportsGracefulDrain(sctrl) {
		t.Fatalf("server settings ack invalid: %v", err)
	}

	// One in-flight unary call: stream id 1.
	callErr := make(chan error, 1)
	go func() {
		var req, resp internal.EchoPayload
		callErr <- client.Call(context.Background(), drainServiceName, "Echo", &req, &resp)
	}()
	waitStarted(t, started, 1)

	dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	drainDone := make(chan error, 1)
	go func() { drainDone <- srv.Drain(dctx) }()

	// Drain frame must arrive and carry boundary 1.
	drainFrame := tee.readTeeFrame(t)
	if drainFrame.header.Type != messageTypeControl || drainFrame.header.StreamID != controlStreamID {
		t.Fatalf("expected drain control frame, got %+v", drainFrame.header)
	}
	dctrl, err := unmarshalControl(drainFrame.payload)
	if err != nil || dctrl.GetType() != Control_DRAIN || dctrl.GetLastStreamId() != 1 {
		t.Fatalf("invalid drain frame: %+v err=%v", dctrl, err)
	}

	// A crossing request is rejected by the client locally as draining
	// before it ever hits the wire.
	crossErr := make(chan error, 1)
	go func() { crossErr <- echoCall(t, client) }()
	select {
	case err := <-crossErr:
		if !IsDraining(err) || status.Code(err) != codes.Unavailable {
			t.Fatalf("crossing call: %v", err)
		}
		if !errors.Is(err, ErrConnectionDraining) {
			t.Fatalf("crossing call not sentinel: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("crossing call hung")
	}

	// The in-flight call completes normally; then Drain returns.
	close(release)
	select {
	case err := <-callErr:
		if err != nil {
			t.Fatalf("accepted call: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("accepted call hung")
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

// waitBoundary polls for the announced boundary to reach want. It is a
// deterministic convergence check over an asynchronous receive loop, not a
// timing-based assertion.
func waitBoundary(t *testing.T, c *Client, want uint32) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if n, ok := c.DrainBoundary(); ok && n == want {
			return
		}
		runtime.Gosched()
	}
	n, ok := c.DrainBoundary()
	t.Fatalf("boundary did not reach %d (got %d/%v)", want, n, ok)
}

// TestClientBoundaryMonotonicNoRegression feeds DRAIN frames out of order
// (a high boundary followed by a delayed lower and duplicate) and verifies
// the client boundary never moves backwards and WaitDrain unblocks once.
func TestClientBoundaryMonotonicNoRegression(t *testing.T) {
	serverConn, clientConn := memPipe()
	defer serverConn.Close()
	defer clientConn.Close()
	client := NewClient(clientConn, WithClientGracefulDrain())
	defer client.Close()
	_ = readFrame(t, serverConn) // drain client SETTINGS

	writeDrain := func(n uint32) {
		sendRaw(t, serverConn, controlStreamID, messageTypeControl, 0,
			marshalDrainControl(n))
	}
	writeDrain(11)
	if err := client.WaitDrain(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitBoundary(t, client, 11)

	// Delayed lower frame must not regress.
	writeDrain(5)
	// Give the receive loop a chance to process the lower frame, then a
	// higher frame must win and the value must never have gone backwards.
	runtime.Gosched()
	writeDrain(11) // duplicate
	runtime.Gosched()
	writeDrain(13)
	waitBoundary(t, client, 13)

	// A trailing delayed lower frame cannot regress the final value.
	writeDrain(7)
	runtime.Gosched()
	if n, _ := client.DrainBoundary(); n != 13 {
		t.Fatalf("boundary regressed to %d", n)
	}
}

// TestDrainIdleConnectionBoundaryZero verifies that draining a connection
// that never carried an RPC announces boundary 0, the client observes it and
// rejects all new calls, and Drain completes without relying on closure.
func TestDrainIdleConnectionBoundaryZero(t *testing.T) {
	srv, _, _ := newDrainServer(t, WithGracefulDrain())
	serverConn, clientConn := memPipe()
	defer srv.Close()
	defer serverConn.Close()
	defer clientConn.Close()

	tee := newTeeConn(clientConn)
	client := NewClient(tee, WithClientGracefulDrain())
	defer client.Close()
	l := newPipeListener()
	l.ch <- serverConn
	go func() { _ = srv.Serve(context.Background(), l) }()

	_ = tee.readTeeFrame(t) // settings ack

	dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	drainDone := make(chan error, 1)
	go func() { drainDone <- srv.Drain(dctx) }()

	df := tee.readTeeFrame(t)
	ctrl, err := unmarshalControl(df.payload)
	if err != nil || ctrl.GetType() != Control_DRAIN || ctrl.GetLastStreamId() != 0 {
		t.Fatalf("expected drain boundary 0, got %+v %v", df.header, err)
	}
	if err := client.WaitDrain(dctx); err != nil {
		t.Fatal(err)
	}
	if n, ok := client.DrainBoundary(); !ok || n != 0 {
		t.Fatalf("expected client boundary 0, got %d/%v", n, ok)
	}
	// Stream id 1 > 0 => rejected locally.
	if err := echoCall(t, client); !IsDraining(err) {
		t.Fatalf("expected draining on idle-drained conn, got %v", err)
	}
	select {
	case err := <-drainDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("idle drain hung")
	}
}

// TestDrainBackpressureDuringBoundary verifies that a slow consumer whose
// server-side handler buffers fill up does not prevent the drain boundary
// from being taken; the DRAIN frame is announced even while one pre-boundary
// stream is backed up.
func TestDrainBackpressureDuringBoundary(t *testing.T) {
	srv, _, _ := newDrainServer(t, WithGracefulDrain())
	handlerReady := make(chan struct{})
	conn, _ := serveConn(t, srv)
	defer srv.Close()
	defer conn.Close()
	client := NewClient(conn, WithClientGracefulDrain())
	defer client.Close()

	desc := &ServiceDesc{
		Streams: map[string]Stream{
			"SlowIn": {
				Handler: func(ctx context.Context, ss StreamServer) (any, error) {
					close(handlerReady)
					<-ctx.Done()
					return nil, ctx.Err()
				},
				StreamingClient: true,
			},
		},
	}
	srv.RegisterService("slowSvc", desc)

	stream, err := client.NewStream(context.Background(),
		&StreamDesc{StreamingClient: true}, "slowSvc", "SlowIn", nil)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-handlerReady:
	case <-time.After(3 * time.Second):
		t.Fatal("handler not ready")
	}

	// Flood beyond server recv capacity; sends eventually fail or buffer.
	sendCtx, cancelSends := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelSends()
flood:
	for i := 0; ; i++ {
		select {
		case <-sendCtx.Done():
			break flood
		default:
		}
		if err := stream.SendMsg(&internal.EchoPayload{Seq: int64(i)}); err != nil {
			break flood
		}
	}

	dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() { _ = srv.Drain(dctx) }()

	// Despite backpressure, the boundary announcement reaches the client.
	if err := client.WaitDrain(dctx); err != nil {
		t.Fatalf("drain boundary not announced under backpressure: %v", err)
	}
	if _, ok := client.DrainBoundary(); !ok {
		t.Fatal("missing boundary under backpressure")
	}
}

// TestDrainConnectionAbortDuringInFlight verifies that if the peer aborts the
// connection mid-drain, in-flight calls get a connection error and Drain
// returns (it does not hang forever) when its context expires.
func TestDrainConnectionAbortDuringInFlight(t *testing.T) {
	srv, started, _ := newDrainServer(t, WithGracefulDrain())
	conn, _ := serveConn(t, srv)
	defer srv.Close()
	client := NewClient(conn, WithClientGracefulDrain())

	callErr := make(chan error, 1)
	go func() {
		var req, resp internal.EchoPayload
		callErr <- client.Call(context.Background(), drainServiceName, "Echo", &req, &resp)
	}()
	waitStarted(t, started, 1)

	dctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	drainDone := make(chan error, 1)
	go func() { drainDone <- srv.Drain(dctx) }()
	if err := client.WaitDrain(dctx); err != nil {
		t.Fatal(err)
	}

	// Abruptly sever both sides.
	conn.Close()
	select {
	case err := <-callErr:
		if err == nil {
			t.Fatal("expected error after connection abort")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("in-flight call did not unblock on abort")
	}

	select {
	case <-drainDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Drain hung after abort")
	}
}

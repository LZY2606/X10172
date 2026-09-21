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
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/containerd/ttrpc/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const drainServiceName = "drainService"

// drainHarness wires one ttrpc client to one server over net.Pipe, giving the
// tests deterministic control over the connection without sockets or sleeps.
type drainHarness struct {
	t       *testing.T
	server  *Server
	client  *Client
	sconn   *serverConn
	cconn   net.Conn
	srvConn net.Conn
	stop    context.CancelFunc
}

func newDrainHarness(t *testing.T, clientOpts []ClientOpts, serverOpts ...ServerOpt) *drainHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	srv, err := NewServer(serverOpts...)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	registerDrainService(srv)

	cc, sc := net.Pipe()

	sconn, err := srv.newConn(sc, nil)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	go sconn.run(ctx)

	// net.Pipe writes are synchronous, so the server must be reading before
	// the client sends its Hello frame in NewClient.
	client := NewClient(cc, clientOpts...)

	h := &drainHarness{
		t:       t,
		server:  srv,
		client:  client,
		sconn:   sconn,
		cconn:   cc,
		srvConn: sc,
		stop:    cancel,
	}
	t.Cleanup(h.cleanup)
	return h
}

func (h *drainHarness) cleanup() {
	h.client.Close()
	h.cconn.Close()
	h.srvConn.Close()
	h.sconn.close()
	h.stop()
	h.server.Close()
}

// waitConnGone blocks until the server has deregistered this connection.
func (h *drainHarness) waitConnGone(ctx context.Context) {
	h.t.Helper()
	for {
		if h.server.countConnection() == 0 {
			return
		}
		select {
		case <-h.server.connChanged:
		case <-ctx.Done():
			h.t.Fatalf("timeout waiting for connection teardown: %v", ctx.Err())
		}
	}
}

// waitDrainObserved blocks until the client has observed the drain boundary.
func (h *drainHarness) waitDrainObserved(ctx context.Context) uint32 {
	h.t.Helper()
	select {
	case <-h.client.drainObserved:
	case <-ctx.Done():
		h.t.Fatalf("timeout waiting for client drain boundary: %v", ctx.Err())
	}
	return h.client.drainBound
}

func drainTestCtx(t *testing.T) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 10*time.Second)
}

func registerDrainService(srv *Server) {
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
		},
		Streams: map[string]Stream{
			"EchoStream": {
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
				StreamingClient: true,
				StreamingServer: true,
			},
			"SlowSender": {
				Handler: func(_ context.Context, ss StreamServer) (any, error) {
					for i := int64(0); ; i++ {
						if err := ss.SendMsg(&internal.EchoPayload{Seq: i, Msg: "slow"}); err != nil {
							return nil, err
						}
					}
				},
				StreamingClient: false,
				StreamingServer: true,
			},
		},
	})
}

// echoCall performs one unary Echo.
func echoCall(ctx context.Context, client *Client, seq int64) (int64, error) {
	var resp internal.EchoPayload
	err := client.Call(ctx, drainServiceName, "Echo", &internal.EchoPayload{Seq: seq}, &resp)
	return resp.Seq, err
}

// TestDrainBasic verifies the core lifecycle: calls accepted before the
// boundary complete, calls after it get the stable drain rejection, and Drain
// blocks until the connection closes after in-flight work finishes.
func TestDrainBasic(t *testing.T) {
	ctx, cancel := drainTestCtx(t)
	defer cancel()

	handlerStarted := make(chan struct{})
	proceed := make(chan struct{})

	h := newDrainHarness(t, []ClientOpts{WithClientGracefulDrain()},
		WithServerGracefulDrain())
	h.server.Register("drainBlockService", map[string]Method{
		"Block": func(callCtx context.Context, unmarshal func(any) error) (any, error) {
			var req internal.EchoPayload
			if err := unmarshal(&req); err != nil {
				return nil, err
			}
			select {
			case <-callCtx.Done():
				return nil, callCtx.Err()
			default:
			}
			close(handlerStarted)
			<-proceed
			return &req, nil
		},
	})

	// Call 1: stream id 1, in flight when draining starts.
	call1Err := make(chan error, 1)
	go func() {
		err := h.client.Call(ctx, "drainBlockService", "Block", &internal.EchoPayload{Seq: 1}, &internal.EchoPayload{})
		call1Err <- err
	}()
	select {
	case <-handlerStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	// Only stream 1 has been accepted, so the boundary is 1; both the local
	// client and the server must reject the next stream id (3).
	drainDone := make(chan error, 1)
	go func() { drainDone <- h.server.Drain(ctx) }()

	boundary := h.waitDrainObserved(ctx)
	if boundary != 1 {
		t.Fatalf("boundary = %d, want 1", boundary)
	}

	// New calls after the boundary fail locally with the stable error.
	if _, err := echoCall(ctx, h.client, 2); !IsDrainingError(err) {
		t.Fatalf("post-boundary call err = %v, want drain rejection", err)
	}

	// NewStream must reject identically.
	if _, err := h.client.NewStream(ctx, &StreamDesc{true, true}, drainServiceName, "EchoStream", nil); !IsDrainingError(err) {
		t.Fatalf("post-boundary NewStream err = %v, want drain rejection", err)
	}

	// Drain must still be blocked on the in-flight call: the connection
	// stays active while stream 1 is outstanding.
	if h.server.countConnection() == 0 {
		t.Fatal("connection deregistered while in-flight call still running")
	}
	select {
	case err := <-drainDone:
		t.Fatalf("Drain returned before in-flight call completed: %v", err)
	default:
	}

	close(proceed)
	if err := <-call1Err; err != nil {
		t.Fatalf("pre-boundary call failed: %v", err)
	}

	select {
	case err := <-drainDone:
		if err != nil {
			t.Fatalf("Drain: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

// TestDrainIdempotent verifies repeated drain starts and concurrent
// Shutdown/Drain calls publish a single boundary and both waiters complete.
func TestDrainIdempotent(t *testing.T) {
	ctx, cancel := drainTestCtx(t)
	defer cancel()

	h := newRawDrainHarness(t)
	h.handshake(ctx)

	starts := sync.WaitGroup{}
	for range 5 {
		starts.Add(1)
		go func() {
			defer starts.Done()
			h.sconn.beginDrain()
		}()
	}
	starts.Wait()

	frames := h.readControlFrames(ctx, 1)
	kinds := map[byte]int{}
	// The Hello frame was already consumed by handshake.
	kinds[controlHelloKind]++
	for _, fr := range frames {
		kind, _, err := decodeControlPayload(fr.payload)
		if err != nil {
			t.Fatal(err)
		}
		kinds[kind]++
	}
	if kinds[controlHelloKind] != 1 || kinds[controlDrainKind] != 1 {
		t.Fatalf("expected exactly one hello and one drain frame, got %v", kinds)
	}

	// Concurrent Shutdown calls all return without error.
	var wg sync.WaitGroup
	errs := make(chan error, 3)
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- h.server.Shutdown(ctx)
		}()
	}
	wg.Wait()
	for range 3 {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent Shutdown: %v", err)
		}
	}
}

// TestDrainBoundaryMonotonic verifies delayed or duplicate drain frames cannot
// move the client boundary backwards, on a single connection and after the
// boundary has advanced.
func TestDrainBoundaryMonotonic(t *testing.T) {
	c := &Client{drainObserved: make(chan struct{})}
	c.setDrainBoundary(7)
	if b, ok := c.drainBoundary(); !ok || b != 7 {
		t.Fatalf("boundary = %d,%v", b, ok)
	}
	c.setDrainBoundary(7) // duplicate
	c.setDrainBoundary(3) // delayed older frame
	if b, _ := c.drainBoundary(); b != 7 {
		t.Fatalf("boundary regressed to %d", b)
	}
	c.setDrainBoundary(11)
	if b, _ := c.drainBoundary(); b != 11 {
		t.Fatalf("boundary = %d, want 11", b)
	}
}

// rawFrame captures one wire frame.
type rawFrame struct {
	header  messageHeader
	payload []byte
}

// rawDrainHarness is a server connection driven by a hand-rolled wire client,
// allowing exact ordering of hello, request and data frames.
type rawDrainHarness struct {
	t      *testing.T
	server *Server
	sconn  *serverConn
	conn   net.Conn
	stop   context.CancelFunc
}

func newRawDrainHarness(t *testing.T, serverOpts ...ServerOpt) *rawDrainHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	srv, err := NewServer(append(serverOpts, WithServerGracefulDrain())...)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	registerDrainService(srv)

	cconn, sconn := net.Pipe()
	sc, err := srv.newConn(sconn, nil)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	go sc.run(ctx)

	h := &rawDrainHarness{t: t, server: srv, sconn: sc, conn: cconn, stop: cancel}
	t.Cleanup(func() {
		cconn.Close()
		sconn.Close()
		cancel()
		srv.Close()
	})
	return h
}

func writeRawFrame(t *testing.T, conn net.Conn, sid uint32, mt messageType, flags uint8, payload []byte) {
	t.Helper()
	var hdr [messageHeaderLength]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(hdr[4:8], sid)
	hdr[8] = byte(mt)
	hdr[9] = flags
	if _, err := conn.Write(hdr[:]); err != nil {
		t.Fatalf("write header: %v", err)
	}
	if len(payload) > 0 {
		if _, err := conn.Write(payload); err != nil {
			t.Fatalf("write payload: %v", err)
		}
	}
}

func readRawFrame(t *testing.T, conn net.Conn) rawFrame {
	t.Helper()
	var hdr [messageHeaderLength]byte
	if _, err := io.ReadFull(conn, hdr[:]); err != nil {
		t.Fatalf("read header: %v", err)
	}
	mh := messageHeader{
		Length:   binary.BigEndian.Uint32(hdr[0:4]),
		StreamID: binary.BigEndian.Uint32(hdr[4:8]),
		Type:     messageType(hdr[8]),
		Flags:    hdr[9],
	}
	var p []byte
	if mh.Length > 0 {
		p = make([]byte, mh.Length)
		if _, err := io.ReadFull(conn, p); err != nil {
			t.Fatalf("read payload: %v", err)
		}
	}
	return rawFrame{header: mh, payload: p}
}

func (h *rawDrainHarness) writeHello() {
	writeRawFrame(h.t, h.conn, controlStreamID, messageTypeControl, 0,
		encodeControlHello(controlFeatureGracefulDrain))
}

func (h *rawDrainHarness) handshake(ctx context.Context) {
	h.writeHello()
	fr := h.readControlFrame(ctx)
	if fr.header.Type != messageTypeControl || fr.header.StreamID != controlStreamID {
		h.t.Fatalf("expected hello control frame, got type=%d sid=%d", fr.header.Type, fr.header.StreamID)
	}
	kind, _, err := decodeControlPayload(fr.payload)
	if err != nil || kind != controlHelloKind {
		h.t.Fatalf("bad hello: kind=%d err=%v", kind, err)
	}
}

func (h *rawDrainHarness) readControlFrame(ctx context.Context) rawFrame {
	frameCh := make(chan rawFrame, 1)
	go func() {
		frameCh <- readRawFrame(h.t, h.conn)
	}()
	select {
	case fr := <-frameCh:
		return fr
	case <-ctx.Done():
		h.t.Fatalf("timeout reading control frame: %v", ctx.Err())
	}
	return rawFrame{}
}

func (h *rawDrainHarness) readControlFrames(ctx context.Context, n int) []rawFrame {
	frames := make([]rawFrame, n)
	for i := range n {
		frames[i] = h.readControlFrame(ctx)
	}
	return frames
}

func marshalRequest(t *testing.T, service, method string, seq int64) []byte {
	t.Helper()
	p, err := (codec{}).Marshal(&Request{
		Service: service,
		Method:  method,
		Payload: mustMarshalEcho(t, &internal.EchoPayload{Seq: seq}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func mustMarshalEcho(t *testing.T, m proto.Message) []byte {
	t.Helper()
	p, err := (codec{}).Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func expectResponseStatus(t *testing.T, fr rawFrame, wantCode codes.Code) *Response {
	t.Helper()
	if fr.header.Type != messageTypeResponse {
		t.Fatalf("expected response frame, got type %d", fr.header.Type)
	}
	var resp Response
	if err := (codec{}).Unmarshal(fr.payload, &resp); err != nil {
		t.Fatal(err)
	}
	if got := status.FromProto(resp.Status).Code(); got != wantCode {
		t.Fatalf("status code = %s, want %s (msg=%q)", got, wantCode, resp.Status.Message)
	}
	return &resp
}

// TestDrainStreamHalfCloseOrder verifies that client half-close, server
// half-close and the final status of a pre-boundary streaming RPC still
// complete in their original order after a drain boundary is published.
func TestDrainStreamHalfCloseOrder(t *testing.T) {
	ctx, cancel := drainTestCtx(t)
	defer cancel()

	h := newDrainHarness(t, []ClientOpts{WithClientGracefulDrain()},
		WithServerGracefulDrain())

	stream, err := h.client.NewStream(ctx, &StreamDesc{
		StreamingClient: true,
		StreamingServer: true,
	}, drainServiceName, "EchoStream", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Exercise a normal request/response round before draining.
	for _, seq := range []int64{1, 2} {
		if err := stream.SendMsg(&internal.EchoPayload{Seq: seq, Msg: "pre-drain"}); err != nil {
			t.Fatalf("send %d: %v", seq, err)
		}
		var resp internal.EchoPayload
		if err := stream.RecvMsg(&resp); err != nil {
			t.Fatalf("recv %d: %v", seq, err)
		}
		if resp.Seq != seq+1 {
			t.Fatalf("seq = %d, want %d", resp.Seq, seq+1)
		}
	}

	drainDone := make(chan error, 1)
	go func() { drainDone <- h.server.Drain(ctx) }()
	if b := h.waitDrainObserved(ctx); b != 1 {
		t.Fatalf("boundary = %d, want 1 (stream 1 accepted)", b)
	}

	// Client half-close still works on the pre-boundary stream.
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend after drain: %v", err)
	}

	// Server half-close arrives as clean io.EOF, not an error and not the
	// drain rejection; the final status order is preserved.
	var last internal.EchoPayload
	err = stream.RecvMsg(&last)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected io.EOF after CloseSend during drain, got %v", err)
	}

	if err := <-drainDone; err != nil {
		t.Fatalf("Drain: %v", err)
	}
	h.waitConnGone(ctx)

	// After Drain completed, the connection has been closed; new calls fail
	// with the ordinary connection-closed error rather than racing it.
	if _, err := h.client.NewStream(ctx, &StreamDesc{
		StreamingClient: true,
		StreamingServer: true,
	}, drainServiceName, "EchoStream", nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-drain stream err = %v, want ErrClosed", err)
	}
}

// TestDrainContextCancel verifies Drain returns ctx.Err when pre-boundary RPCs
// never finish, while later calls still get the stable rejection.
func TestDrainContextCancel(t *testing.T) {
	ctx, cancel := drainTestCtx(t)
	defer cancel()

	handlerStarted := make(chan struct{})
	h := newDrainHarness(t, []ClientOpts{WithClientGracefulDrain()},
		WithServerGracefulDrain())
	h.server.Register("drainCancelService", map[string]Method{
		"Block": func(callCtx context.Context, unmarshal func(any) error) (any, error) {
			if err := unmarshal(&internal.EchoPayload{}); err != nil {
				return nil, err
			}
			close(handlerStarted)
			<-callCtx.Done()
			return nil, callCtx.Err()
		},
	})

	callErr := make(chan error, 1)
	go func() {
		callErr <- h.client.Call(ctx, "drainCancelService", "Block",
			&internal.EchoPayload{Seq: 1}, &internal.EchoPayload{})
	}()
	<-handlerStarted

	drainCtx, drainCancel := context.WithCancel(ctx)
	drainDone := make(chan error, 1)
	go func() { drainDone <- h.server.Drain(drainCtx) }()
	h.waitDrainObserved(ctx)

	if _, err := echoCall(ctx, h.client, 2); !IsDrainingError(err) {
		t.Fatalf("post-boundary call err = %v", err)
	}

	drainCancel()
	select {
	case err := <-drainDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Drain err = %v, want context.Canceled", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	// The still-active connection is force closed by Server.Close in
	// cleanup, ending the blocked handler call with a connection error.
}

// TestDrainConcurrentCallsBeforeAfter builds streams on both sides of the
// boundary concurrently and asserts that stream ids <= boundary finish while
// ids > boundary receive exactly the drain rejection.
func TestDrainConcurrentCallsBeforeAfter(t *testing.T) {
	ctx, cancel := drainTestCtx(t)
	defer cancel()

	const accepted = 4 // stream ids 1,3,5,7 accepted before the boundary

	h := newDrainHarness(t, []ClientOpts{WithClientGracefulDrain()},
		WithServerGracefulDrain())

	started := make(chan struct{}, accepted)
	proceed := make(chan struct{})
	h.server.Register("drainFanService", map[string]Method{
		"Block": func(callCtx context.Context, unmarshal func(any) error) (any, error) {
			var req internal.EchoPayload
			if err := unmarshal(&req); err != nil {
				return nil, err
			}
			started <- struct{}{}
			select {
			case <-proceed:
				return &req, nil
			case <-callCtx.Done():
				return nil, callCtx.Err()
			}
		},
	})

	okErrs := make(chan error, accepted)
	for range accepted {
		go func() {
			okErrs <- h.client.Call(ctx, "drainFanService", "Block",
				&internal.EchoPayload{Seq: 1}, &internal.EchoPayload{})
		}()
	}
	for range accepted {
		<-started
	}

	drainDone := make(chan error, 1)
	go func() { drainDone <- h.server.Drain(ctx) }()
	boundary := h.waitDrainObserved(ctx)
	if boundary != uint32(accepted*2-1) {
		t.Fatalf("boundary = %d, want %d", boundary, accepted*2-1)
	}

	rejectErrs := make(chan error, 4)
	for range 4 {
		go func() {
			_, err := echoCall(ctx, h.client, 9)
			rejectErrs <- err
		}()
	}
	for range 4 {
		if err := <-rejectErrs; !IsDrainingError(err) {
			t.Fatalf("post-boundary call err = %v", err)
		}
	}

	close(proceed)
	for range accepted {
		if err := <-okErrs; err != nil {
			t.Fatalf("pre-boundary call: %v", err)
		}
	}
	if err := <-drainDone; err != nil {
		t.Fatalf("Drain: %v", err)
	}
}

// TestDrainWireRejection drives a hand-rolled client to verify the server
// itself rejects post-boundary request frames on the wire with the stable
// codes.Unavailable status, independent of the local client fast-path.
func TestDrainWireRejection(t *testing.T) {
	ctx, cancel := drainTestCtx(t)
	defer cancel()

	h := newRawDrainHarness(t)
	h.handshake(ctx)

	// Stream id 1: accepted unary Echo, boundary becomes 1.
	req1 := marshalRequest(t, drainServiceName, "Echo", 10)
	writeRawFrame(t, h.conn, 1, messageTypeRequest, flagRemoteClosed, req1)
	fr := readRawFrame(t, h.conn)
	resp := expectResponseStatus(t, fr, codes.OK)
	if resp.Payload == nil {
		t.Fatal("expected echo payload")
	}

	h.sconn.beginDrain()
	ctrl := h.readControlFrame(ctx)
	if ctrl.header.Type != messageTypeControl {
		t.Fatalf("expected control frame, got %d", ctrl.header.Type)
	}
	kind, fields, err := decodeControlPayload(ctrl.payload)
	if err != nil || kind != controlDrainKind || fields[controlFieldU32] != 1 {
		t.Fatalf("bad drain frame: kind=%d fields=%v err=%v", kind, fields, err)
	}

	// Stream id 3: arrives after the boundary and must be rejected on wire.
	req3 := marshalRequest(t, drainServiceName, "Echo", 20)
	writeRawFrame(t, h.conn, 3, messageTypeRequest, flagRemoteClosed, req3)
	fr = readRawFrame(t, h.conn)
	expectResponseStatus(t, fr, codes.Unavailable)
	if resp := expectResponseStatus(t, fr, codes.Unavailable); resp.Status.Message != drainingMessage {
		t.Fatalf("rejection message = %q", resp.Status.Message)
	}
}

// TestDrainBoundaryVsFramesInterleave verifies precise interleaving between a
// drain signal and request frames: the second request frame is held by the
// controllable transport while drain captures boundary 1, then is released and
// must be rejected on the wire because its stream id 3 is past the boundary.
func TestDrainBoundaryVsFramesInterleave(t *testing.T) {
	ctx, cancel := drainTestCtx(t)
	defer cancel()

	frame3Queued := make(chan struct{})
	releaseFrame3 := make(chan struct{})

	srv, err := NewServer(WithServerGracefulDrain())
	if err != nil {
		t.Fatal(err)
	}
	registerDrainService(srv)

	clientPipe, serverPipe := memPipePair()
	// Hold frame 3 (the second request) until released; frames 1 and 2
	// (hello and request id 1) pass through immediately.
	serverPipe.deliver = func(frameNo int) <-chan struct{} {
		if frameNo == 3 {
			close(frame3Queued)
			return releaseFrame3
		}
		return nil
	}

	sconn, err := srv.newConn(serverPipe, nil)
	if err != nil {
		t.Fatal(err)
	}
	srvCtx, srvCancel := context.WithCancel(ctx)
	defer srvCancel()
	go sconn.run(srvCtx)

	go func() {
		writeRawFrame(t, clientPipe, controlStreamID, messageTypeControl, 0,
			encodeControlHello(controlFeatureGracefulDrain))
		writeRawFrame(t, clientPipe, 1, messageTypeRequest, flagRemoteClosed,
			marshalRequest(t, drainServiceName, "Echo", 10))
		writeRawFrame(t, clientPipe, 3, messageTypeRequest, flagRemoteClosed,
			marshalRequest(t, drainServiceName, "Echo", 20))
	}()

	// Frames from server: hello, then echo response for accepted stream 1.
	hello := readFrameCtx(ctx, t, clientPipe)
	if hello.header.Type != messageTypeControl {
		t.Fatalf("expected hello control, got %d", hello.header.Type)
	}
	echoResp := readFrameCtx(ctx, t, clientPipe)
	expectResponseStatus(t, echoResp, codes.OK)

	// Frame 3 is queued in the transport but not readable. Drain boundary is 1.
	<-frame3Queued
	drainDone := make(chan error, 1)
	go func() { drainDone <- srv.Drain(ctx) }()

	drainFrame := readFrameCtx(ctx, t, clientPipe)
	if drainFrame.header.Type != messageTypeControl {
		t.Fatalf("expected drain control, got %d", drainFrame.header.Type)
	}
	kind, fields, derr := decodeControlPayload(drainFrame.payload)
	if derr != nil || kind != controlDrainKind || fields[controlFieldU32] != 1 {
		t.Fatalf("bad drain frame: kind=%d fields=%v err=%v", kind, fields, derr)
	}

	// Release parked frame 3: stream id 3 > boundary 1, rejected on wire.
	close(releaseFrame3)
	reject := readFrameCtx(ctx, t, clientPipe)
	expectResponseStatus(t, reject, codes.Unavailable)

	select {
	case err := <-drainDone:
		if err != nil {
			t.Fatalf("Drain: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

// memConn is one endpoint of memPipePair: a deterministic, ordered,
// unbuffered-per-frame in-memory transport. A Read of a frame can be held by
// the deliver gate without the transport buffering ahead, which makes exact
// frame interleavings reproducible.
type memConn struct {
	name    string
	mu      sync.Mutex
	cond    *sync.Cond
	frames  [][]byte // written frames awaiting delivery
	closed  bool
	peer    *memConn
	deliver func(frameNo int) <-chan struct{}
	readNo  int
}

func memPipePair() (client *memConn, server *memConn) {
	a, b := &memConn{name: "client"}, &memConn{name: "server"}
	a.peer, b.peer = b, a
	a.cond = sync.NewCond(&a.mu)
	b.cond = sync.NewCond(&b.mu)
	return a, b
}

func (m *memConn) Write(p []byte) (int, error) {
	m.peer.mu.Lock()
	if m.peer.closed {
		m.peer.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	buf := append([]byte(nil), p...)
	m.peer.frames = append(m.peer.frames, buf)
	m.peer.cond.Broadcast()
	m.peer.mu.Unlock()
	return len(p), nil
}

func (m *memConn) Read(p []byte) (int, error) {
	m.mu.Lock()
	for {
		if len(m.frames) > 0 {
			frameNo := m.readNo + 1
			gate := m.deliver
			var wait <-chan struct{}
			if gate != nil {
				wait = gate(frameNo)
			}
			m.mu.Unlock()
			if wait != nil {
				<-wait
			}
			m.mu.Lock()
		}
		if len(m.frames) > 0 {
			n := copy(p, m.frames[0])
			if n == len(m.frames[0]) {
				m.frames[0] = nil
				m.frames = m.frames[1:]
				m.readNo++
			} else {
				m.frames[0] = m.frames[0][n:]
			}
			m.mu.Unlock()
			return n, nil
		}
		if m.closed {
			m.mu.Unlock()
			return 0, io.EOF
		}
		m.cond.Wait()
	}
}

func (m *memConn) Close() error {
	m.mu.Lock()
	m.closed = true
	m.cond.Broadcast()
	m.mu.Unlock()
	// Also wake the peer blocked writing.
	m.peer.mu.Lock()
	m.peer.cond.Broadcast()
	m.peer.mu.Unlock()
	return nil
}

func (m *memConn) LocalAddr() net.Addr              { return memAddr(m.name) }
func (m *memConn) RemoteAddr() net.Addr             { return memAddr(m.name) }
func (m *memConn) SetDeadline(time.Time) error      { return nil }
func (m *memConn) SetReadDeadline(time.Time) error  { return nil }
func (m *memConn) SetWriteDeadline(time.Time) error { return nil }

type memAddr string

func (a memAddr) Network() string { return "mem" }
func (a memAddr) String() string  { return string(a) }

func readFrameCtx(ctx context.Context, t *testing.T, conn net.Conn) rawFrame {
	t.Helper()
	ch := make(chan rawFrame, 1)
	go func() { ch <- readRawFrame(t, conn) }()
	select {
	case fr := <-ch:
		return fr
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	return rawFrame{}
}

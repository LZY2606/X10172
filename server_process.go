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
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// connResponse is a response produced by a handler and serialized by
// the connection's main loop.
type connResponse struct {
	id          uint32
	status      *status.Status
	data        []byte
	closeStream bool
	streaming   bool
	// final marks the terminal response of a dispatched call. The
	// main loop decrements the in-flight counter after writing it.
	final bool
}

// connFrame is one frame pulled off the wire. When recvErr is set the
// frame failed header/body validation; the error is reported back on
// header.StreamID like a per-stream failure.
type connFrame struct {
	header  messageHeader
	payload []byte
	recvErr error
}

// processFrame handles one inbound frame on behalf of a serverConn. It
// is only ever invoked by the connection's single processor goroutine,
// so lastStreamID and the drain boundary do not need their own locks.
func (c *serverConn) processFrame(
	ctx context.Context,
	frame connFrame,
	ch *channel,
	streams *sync.Map,
	active *int32,
	inFlight *int32,
	lastStreamID *uint32,
	drainBoundary *uint32,
	draining bool,
	sendStatus func(uint32, *status.Status) bool,
	responses chan<- connResponse,
) {
	mh := frame.header

	if frame.recvErr != nil {
		if st, ok := status.FromError(frame.recvErr); ok {
			sendStatus(mh.StreamID, st)
		} else {
			// Terminal framing errors are handled by the caller via
			// the read-error path; nothing to do here.
		}
		return
	}

	if mh.Type != messageTypeControl && mh.StreamID%2 != 1 {
		// enforce odd client initiated identifiers.
		if !sendStatus(mh.StreamID, status.Newf(codes.InvalidArgument, "StreamID must be odd for client initiated streams")) {
			return
		}
		return
	}

	if mh.Type == messageTypeControl {
		// Servers currently define no client-to-server control
		// messages. Discard valid control frames and their payload so
		// future extensions do not surface as RPCs.
		if frame.payload != nil {
			ch.putmbuf(frame.payload)
		}
		return
	}

	p := frame.payload

	if mh.Type == messageTypeData {
		i, ok := streams.Load(mh.StreamID)
		if !ok {
			if !sendStatus(mh.StreamID, status.Newf(codes.InvalidArgument, "StreamID is no longer active")) {
				return
			}
			return
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
				return
			}
		}

		if mh.Flags&flagRemoteClosed == flagRemoteClosed {
			sh.closeSend()
			if len(p) > 0 {
				if !sendStatus(mh.StreamID, status.Newf(codes.InvalidArgument, "data close message cannot include data")) {
					return
				}
				return
			}
		}
		return
	}

	if mh.Type == messageTypeRequest {
		if mh.StreamID <= *lastStreamID {
			// enforce monotonic, non-reused stream identifiers.
			if !sendStatus(mh.StreamID, status.Newf(codes.InvalidArgument, "StreamID cannot be re-used and must increment")) {
				return
			}
			return
		}

		if draining && mh.StreamID > *drainBoundary {
			// The stream id is past the announced boundary. It is
			// rejected with a stable status without being dispatched,
			// so the result does not depend on connection close
			// timing. The payload is discarded without unmarshalling.
			if p != nil {
				ch.putmbuf(p)
			}
			if !sendStatus(mh.StreamID, status.New(codes.Unavailable, drainingStatusMessage)) {
				return
			}
			return
		}

		*lastStreamID = mh.StreamID

		var req Request
		if err := c.server.codec.Unmarshal(p, &req); err != nil {
			ch.putmbuf(p)
			if !sendStatus(mh.StreamID, status.Newf(codes.InvalidArgument, "unmarshal request error: %v", err)) {
				return
			}
			return
		}
		ch.putmbuf(p)

		// Capabilities are connection scoped: once any request on the
		// connection advertises drain, the peer is drain capable.
		if peerSupportsDrain(&req) {
			c.peerDrain.Store(true)
		}

		id := mh.StreamID
		// Count every dispatched call, including unary, so a drained
		// connection never closes while a handler is still running.
		atomic.AddInt32(inFlight, 1)
		respond := func(st *status.Status, data []byte, streaming, closeStream bool) error {
			select {
			case responses <- connResponse{
				id:          id,
				status:      st,
				data:        data,
				closeStream: closeStream,
				streaming:   streaming,
			}:
			case <-c.shutdown:
				atomic.AddInt32(inFlight, -1)
				return ErrClosed
			}
			return nil
		}
		sh, err := c.server.services.handle(ctx, &req, respond)
		if err != nil {
			st, _ := status.FromError(err)
			if !sendStatus(mh.StreamID, st) {
				return
			}
			return
		}

		streams.Store(id, sh)
		atomic.AddInt32(active, 1)
		return
	}

	// Unknown message types are ignored for forward compatibility.
	if p != nil {
		ch.putmbuf(p)
	}
}

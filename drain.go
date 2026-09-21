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
	"encoding/binary"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// controlStreamID is the stream id used by all control frames. Control
// frames are connection scoped, so they never reference an rpc stream.
// Stream id 0 is even and is therefore never allocated to a client
// initiated stream.
const controlStreamID uint32 = 0

// Control frame item types. The control payload is a sequence of
// type-length-value items so future extensions can be added without
// changing the wire frame layout.
const (
	// controlItemDrainEnd announces the stream id boundary of a graceful
	// drain. The 4-byte big-endian value is the last stream id that the
	// server accepted on this connection.
	controlItemDrainEnd uint16 = 0x0001
)

// featureMetadataKey is the metadata key used on Request messages to
// negotiate optional connection features. The value is a comma
// separated list of feature names.
const featureMetadataKey = "ttrpc-features"

// featureDrain is the feature name indicating support for graceful
// drain control frames.
const featureDrain = "drain"

// encodeControlFrame builds the payload of a Control message from a
// sequence of control items. Each item is encoded as:
//
//	Item Type (16) | Item Length (16) | Item Value (*)
func encodeControlFrame(items ...controlItem) ([]byte, error) {
	n := 0
	for _, item := range items {
		n += 4 + len(item.value)
	}
	p := make([]byte, 0, n)
	for _, item := range items {
		if len(item.value) > 0xffff {
			return nil, fmt.Errorf("%w: control item %x too large", ErrProtocol, item.typ)
		}
		var hdr [4]byte
		binary.BigEndian.PutUint16(hdr[0:2], item.typ)
		binary.BigEndian.PutUint16(hdr[2:4], uint16(len(item.value)))
		p = append(p, hdr[:]...)
		p = append(p, item.value...)
	}
	return p, nil
}

type controlItem struct {
	typ   uint16
	value []byte
}

// controlFrame is the decoded view of a Control message payload.
type controlFrame struct {
	drainEnd uint32
	hasDrain bool
}

// decodeControlFrame parses a Control message payload. Unknown item
// types are skipped, so older implementations can safely coexist with
// newer ones on the same wire. A malformed payload is reported as a
// protocol error without touching any rpc stream.
func decodeControlFrame(p []byte) (controlFrame, error) {
	var cf controlFrame
	for len(p) > 0 {
		if len(p) < 4 {
			return cf, fmt.Errorf("%w: truncated control item header", ErrProtocol)
		}
		typ := binary.BigEndian.Uint16(p[0:2])
		n := int(binary.BigEndian.Uint16(p[2:4]))
		p = p[4:]
		if len(p) < n {
			return cf, fmt.Errorf("%w: truncated control item value", ErrProtocol)
		}
		value := p[:n]
		p = p[n:]
		switch typ {
		case controlItemDrainEnd:
			if n != 4 {
				return cf, fmt.Errorf("%w: drain boundary must be 4 bytes, got %d", ErrProtocol, n)
			}
			cf.drainEnd = binary.BigEndian.Uint32(value)
			cf.hasDrain = true
		}
	}
	return cf, nil
}

// drainingStatus is the stable grpc status used for calls rejected
// because they were created after a drain boundary. The code and
// message are part of the protocol and must not be changed.
func drainingStatus(boundary uint32) *status.Status {
	return status.Newf(codes.Unavailable, "ttrpc: server is draining; stream id is after the last accepted stream id %d", boundary)
}

// drainingError is the client-side error returned for calls rejected
// by a drain boundary. It carries a grpc Status with code
// codes.Unavailable so callers can recognize it through the usual
// status helpers, and supports errors.Is(err, ErrServerDraining) even
// when the error arrived over the wire from a different process.
type drainingError struct {
	boundary uint32
	st       *status.Status
}

func (e *drainingError) Error() string {
	return e.st.Message()
}

// GRPCStatus implements the interface used by status.Convert.
func (e *drainingError) GRPCStatus() *status.Status {
	return e.st
}

// Is supports errors.Is comparisons, including against drain errors
// reconstructed from wire statuses.
func (e *drainingError) Is(target error) bool {
	_, ok := target.(*drainingError)
	return ok
}

// newDrainingError builds the drain rejection error for a boundary.
func newDrainingError(boundary uint32) error {
	st := drainingStatus(boundary)
	return &drainingError{boundary: boundary, st: st}
}

// drainingSentinel is the stable target used by errors.Is for every
// drain rejection regardless of the announced boundary.
var drainingSentinel = &drainingError{boundary: 0, st: drainingStatus(0)}

// ErrServerDraining is returned by client calls that are rejected
// because the server started graceful draining before the call's
// stream was accepted.
//
// The error always carries a gRPC status with code Unavailable. Use
// errors.Is(err, ErrServerDraining) or IsServerDraining(err) to detect
// it without depending on the error text.
var ErrServerDraining error = drainingSentinel

// IsServerDraining reports whether err is a drain rejection, whether
// produced locally or received from a remote drain-aware peer.
func IsServerDraining(err error) bool {
	if errors.Is(err, ErrServerDraining) {
		return true
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Unavailable && isDrainingMessage(st.Message())
}

func isDrainingMessage(msg string) bool {
	// The drain status message prefix is part of the wire contract, so a
	// client can recognize drain rejections even when the concrete error
	// type was lost during serialization.
	const prefix = "ttrpc: server is draining;"
	return len(msg) >= len(prefix) && msg[:len(prefix)] == prefix
}

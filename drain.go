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
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrServerDraining is the sentinel wrapped by the gRPC status
// (codes.Unavailable) returned for calls that arrive after a server has
// announced a graceful drain boundary for the connection.
var ErrServerDraining = errors.New("ttrpc: server is draining the connection")

// IsServerDraining reports whether err is the stable rejection result for a
// call refused because its stream id is greater than the server's last
// accepted stream id. It matches both the locally generated fast-fail and
// the status received over the wire.
func IsServerDraining(err error) bool {
	if errors.Is(err, ErrServerDraining) {
		return true
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Unavailable &&
		strings.HasPrefix(st.Message(), ErrServerDraining.Error())
}

func newDrainingStatus(lastStreamID uint32) *status.Status {
	return status.Newf(codes.Unavailable,
		"%s: stream id is greater than the last accepted stream id %d",
		ErrServerDraining.Error(), lastStreamID)
}

// Control frames (messageTypeControl) are connection scoped. They always use
// stream id 0, carry no flags and have a fixed 5 byte payload:
//
//	+------------------+-------------------------------------------+
//	| Control Msg (8)  |                Value (32)                 |
//	+------------------+-------------------------------------------+
//
// The value is a bitmask of capabilities for Hello messages and the last
// accepted client initiated stream id for Drain messages.
const (
	controlStreamID     uint32 = 0
	controlPayloadLen          = 5
	controlMessageHello uint8  = 0x1
	controlMessageDrain uint8  = 0x2

	// capabilityGracefulDrain is advertised by both peers in Hello when they
	// understand the graceful drain extension.
	capabilityGracefulDrain uint32 = 0x1
)

type controlMessage struct {
	kind  uint8
	value [4]byte
}

func marshalControlMessage(cm controlMessage) []byte {
	p := make([]byte, controlPayloadLen)
	p[0] = cm.kind
	copy(p[1:], cm.value[:])
	return p
}

// parseControlMessage decodes a control frame payload. Malformed payloads
// are reported as false and must be ignored by the receiver.
func parseControlMessage(payload []byte) (controlMessage, bool) {
	if len(payload) != controlPayloadLen {
		return controlMessage{}, false
	}
	var cm controlMessage
	cm.kind = payload[0]
	copy(cm.value[:], payload[1:])
	return cm, true
}

func controlCapabilities(caps uint32) controlMessage {
	var cm controlMessage
	cm.kind = controlMessageHello
	binary.BigEndian.PutUint32(cm.value[:], caps)
	return cm
}

func controlDrain(lastStreamID uint32) controlMessage {
	var cm controlMessage
	cm.kind = controlMessageDrain
	binary.BigEndian.PutUint32(cm.value[:], lastStreamID)
	return cm
}

func (cm controlMessage) uint32Value() uint32 {
	return binary.BigEndian.Uint32(cm.value[:])
}

// WithGracefulDrain opts a client into the graceful drain connection
// protocol. When disabled (the default) the client is wire compatible with
// older servers and never sends control frames.
func WithGracefulDrain() ClientOpts {
	return func(c *Client) {
		c.drainEnabled = true
	}
}

// WithGracefulShutdown opts a server into the graceful drain connection
// protocol. When disabled (the default) the server is wire compatible with
// older clients and never sends control frames.
func WithGracefulShutdown() ServerOpt {
	return func(c *serverConfig) error {
		c.gracefulShutdown = true
		return nil
	}
}

// GracefulShutdownResult summarizes a ShutdownGraceful call.
type GracefulShutdownResult struct {
	// DrainedConnections is the number of connections that negotiated the
	// graceful drain protocol, announced a last accepted stream id and
	// reached zero in-flight calls.
	DrainedConnections int

	// InterruptedConnections is the number of connections that were forcibly
	// closed by a concurrent Shutdown or Close before draining completed.
	InterruptedConnections int

	// UnsupportedConnections is the number of connections whose peer did not
	// negotiate the graceful drain protocol. These were drained using the
	// legacy Shutdown semantics: idle connections are closed and active ones
	// are closed once their in-flight calls complete.
	UnsupportedConnections int
}

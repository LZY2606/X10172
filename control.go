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

import "encoding/binary"

// controlStreamID is the reserved stream identifier used by control frames.
// It is never a valid stream: client initiated streams are odd and server
// initiated streams are not supported, so zero is unambiguous and peers
// which do not understand control frames safely drop them as messages on an
// inactive stream.
const controlStreamID = 0

// controlType identifies the kind of a control frame payload.
type controlType uint8

const (
	// controlTypeHello advertises connection level capabilities from the
	// client to the server. The payload is a fixed 8-byte big-endian
	// capability bitmask.
	controlTypeHello controlType = 0x1
	// controlTypeHelloAck advertises the server capabilities in response
	// to a hello. Same payload layout as controlTypeHello.
	controlTypeHelloAck controlType = 0x2
	// controlTypeDrain announces the last stream ID the server will accept
	// on this connection. The payload is a fixed 4-byte big-endian stream
	// identifier.
	controlTypeDrain controlType = 0x3
)

const (
	// capabilityDrain indicates support for the graceful connection drain
	// protocol.
	capabilityDrain uint64 = 1 << 0
)

const (
	controlHelloLength = 1 + 8 // type + capability bitmask
	controlDrainLength = 1 + 4 // type + stream id
)

// marshalControlHello encodes a hello or hello-ack control frame payload.
func marshalControlHello(t controlType, capabilities uint64) []byte {
	p := make([]byte, controlHelloLength)
	p[0] = byte(t)
	binary.BigEndian.PutUint64(p[1:], capabilities)
	return p
}

// marshalControlDrain encodes a drain control frame payload carrying the
// last stream ID accepted by the server.
func marshalControlDrain(lastStreamID uint32) []byte {
	p := make([]byte, controlDrainLength)
	p[0] = byte(controlTypeDrain)
	binary.BigEndian.PutUint32(p[1:], lastStreamID)
	return p
}

// parseControl splits a control frame payload into its type and body. The
// returned body aliases the input and is only valid until the input is
// released.
func parseControl(p []byte) (controlType, []byte, bool) {
	if len(p) < 1 {
		return 0, nil, false
	}
	return controlType(p[0]), p[1:], true
}

// parseControlCapabilities decodes the body of a hello or hello-ack payload.
func parseControlCapabilities(body []byte) (uint64, bool) {
	if len(body) < 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(body), true
}

// parseControlDrain decodes the body of a drain payload.
func parseControlDrain(body []byte) (uint32, bool) {
	if len(body) < 4 {
		return 0, false
	}
	return binary.BigEndian.Uint32(body), true
}

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
	"fmt"

	"google.golang.org/grpc/codes"
)

// drainStatusMessage is the status message used by the server to reject
// requests that arrive after the drain boundary. Clients which negotiated
// drain support map this status to ErrDraining.
const drainStatusMessage = "ttrpc: server is draining"

// controlKind identifies the kind of a control frame payload.
type controlKind uint8

const (
	// controlKindCapability announces protocol capabilities of the sender.
	// The body is a single byte bitmask of capability bits.
	controlKindCapability controlKind = 0x1
	// controlKindDrain announces that the sender is draining the
	// connection. The body is the 4-byte big-endian stream id of the last
	// stream the sender accepted before draining.
	controlKindDrain controlKind = 0x2
)

// capabilityDrain indicates support for the connection drain protocol.
const capabilityDrain uint8 = 0x1

func encodeControlCapability(caps uint8) []byte {
	return []byte{byte(controlKindCapability), caps}
}

func encodeControlDrain(lastStreamID uint32) []byte {
	p := make([]byte, 5)
	p[0] = byte(controlKindDrain)
	binary.BigEndian.PutUint32(p[1:], lastStreamID)
	return p
}

// decodeControl splits a control frame payload into its kind and body.
// Unknown kinds are not an error; callers must ignore kinds they do not
// understand for forward compatibility.
func decodeControl(p []byte) (controlKind, []byte, error) {
	if len(p) < 1 {
		return 0, nil, fmt.Errorf("%w: empty control frame", ErrProtocol)
	}
	return controlKind(p[0]), p[1:], nil
}

// isDrainStatus reports whether the given response status is the stable
// rejection produced by a draining server for requests past the drain
// boundary.
func isDrainStatus(code int32, message string) bool {
	return code == int32(codes.Unavailable) && message == drainStatusMessage
}

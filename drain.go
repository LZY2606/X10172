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

// controlStreamID is the reserved stream identifier used by connection-level
// control frames. The value is odd so that peers which do not understand
// control frames silently ignore them instead of rejecting the frame as a
// mis-initiated stream or routing it to an RPC stream.
const controlStreamID uint32 = 0xFFFFFFFF

// Control frame opcodes, carried in the flags byte of a control frame.
const (
	// controlOpCapability announces the capabilities of the sender. The
	// payload is a 4-byte big-endian bitmask of capability bits.
	controlOpCapability uint8 = 0x1
	// controlOpDrain announces the last stream id accepted by the sender.
	// The payload is a 4-byte big-endian stream id.
	controlOpDrain uint8 = 0x2
)

// Capability bits announced with controlOpCapability frames.
const (
	// capabilityDrain indicates the peer understands drain control frames.
	capabilityDrain uint32 = 0x1
)

func controlUint32Payload(v uint32) []byte {
	var p [4]byte
	binary.BigEndian.PutUint32(p[:], v)
	return p[:]
}

func controlParseUint32(p []byte) uint32 {
	return binary.BigEndian.Uint32(p)
}

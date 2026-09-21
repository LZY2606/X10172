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

// controlStreamID is the reserved stream identifier for connection-level
// control frames. Control frames are never part of an RPC stream.
const controlStreamID = 0

// Control frame types, carried as the first byte of a control frame payload.
const (
	controlTypeCapabilities uint8 = 0x1
	controlTypeDrain        uint8 = 0x2
)

// Capability bits carried by a capabilities control frame.
const (
	capabilityDrain uint64 = 0x1
)

// controlFrameLength is the fixed length of a control frame payload:
// a 1 byte control type followed by an 8 byte big-endian value.
const controlFrameLength = 9

// marshalControl encodes a control frame payload.
func marshalControl(ct uint8, value uint64) []byte {
	p := make([]byte, controlFrameLength)
	p[0] = ct
	binary.BigEndian.PutUint64(p[1:], value)
	return p
}

// parseControl decodes a control frame payload. Unknown or malformed
// frames report ok == false and must be ignored by the receiver.
func parseControl(p []byte) (ct uint8, value uint64, ok bool) {
	if len(p) != controlFrameLength {
		return 0, 0, false
	}
	return p[0], binary.BigEndian.Uint64(p[1:]), true
}

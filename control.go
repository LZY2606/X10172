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
)

// Control frames are connection-scoped protocol messages that are not part of
// any RPC stream. They always use stream ID 0 and message type
// messageTypeControl (0x04). A peer that does not recognize the message type
// MUST ignore the frame; control frames are therefore a backward and forward
// compatible extension point.
const (
	// controlStreamID is the stream ID carried by every control frame.
	// Client initiated streams use odd stream IDs, so zero never collides
	// with an active RPC.
	controlStreamID uint32 = 0

	controlKindLength = 1
	controlHelloKind  = 0x01
	controlDrainKind  = 0x02

	// controlFieldU32 is the one-byte tag for 4-byte unsigned integer
	// fields in the control payload TLV encoding.
	controlFieldU32 = 0x01
	controlU32Len   = 4
)

// controlFeature is a bit in the 32-bit capability mask advertised in a Hello
// control frame.
type controlFeature uint32

const (
	// controlFeatureGracefulDrain (bit 0) advertises support for the
	// graceful drain protocol: the sender understands Drain control
	// frames carrying a last accepted stream ID boundary.
	controlFeatureGracefulDrain controlFeature = 1 << 0
)

// encodeControlHello builds the payload of a Hello control frame:
//
//	kind (1) = 0x01
//	TLV: tag (1) = 0x01, length (1) = 4, value (4, BE) = feature mask
func encodeControlHello(features controlFeature) []byte {
	p := make([]byte, 0, controlKindLength+3+controlU32Len)
	p = append(p, controlHelloKind)
	p = append(p, controlFieldU32, controlU32Len)
	p = binary.BigEndian.AppendUint32(p, uint32(features))
	return p
}

// encodeControlDrain builds the payload of a Drain control frame:
//
//	kind (1) = 0x02
//	TLV: tag (1) = 0x01, length (1) = 4, value (4, BE) = last accepted stream ID
//
// The last accepted stream ID is the highest client-initiated stream the server
// will service on this connection. New calls with greater stream IDs are
// rejected. The boundary is connection scoped and only increases.
func encodeControlDrain(lastStreamID uint32) []byte {
	p := make([]byte, 0, controlKindLength+3+controlU32Len)
	p = append(p, controlDrainKind)
	p = append(p, controlFieldU32, controlU32Len)
	p = binary.BigEndian.AppendUint32(p, lastStreamID)
	return p
}

// decodeControlPayload parses a control frame payload. Unknown kinds, unknown
// fields and malformed payloads return an error so callers can safely ignore
// the frame, preserving forward compatibility.
func decodeControlPayload(p []byte) (kind byte, u32s map[byte]uint32, err error) {
	if len(p) < controlKindLength {
		return 0, nil, fmt.Errorf("%w: control payload too short", ErrProtocol)
	}
	kind = p[0]
	switch kind {
	case controlHelloKind, controlDrainKind:
	default:
		return kind, nil, fmt.Errorf("%w: unknown control kind %d", ErrProtocol, kind)
	}

	u32s = make(map[byte]uint32)
	b := p[controlKindLength:]
	for len(b) > 0 {
		if len(b) < 2 {
			return kind, nil, fmt.Errorf("%w: truncated control TLV header", ErrProtocol)
		}
		tag, fieldLen := b[0], int(b[1])
		b = b[2:]
		if fieldLen < 0 || len(b) < fieldLen {
			return kind, nil, fmt.Errorf("%w: malformed control TLV value", ErrProtocol)
		}
		switch tag {
		case controlFieldU32:
			if fieldLen != controlU32Len {
				return kind, nil, fmt.Errorf("%w: malformed u32 control TLV value", ErrProtocol)
			}
			u32s[tag] = binary.BigEndian.Uint32(b[:controlU32Len])
		}
		// Unknown tags are skipped so additional fields can be added without
		// breaking older peers.
		b = b[fieldLen:]
	}
	return kind, u32s, nil
}

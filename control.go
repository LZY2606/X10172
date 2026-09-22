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

// Control frames carry connection-level, stream-independent protocol
// extensions. They reuse the 10-byte message header with
// messageTypeControl (0x04) and stream ID 0. A peer that does not
// understand a control frame simply discards it; a control frame is
// never routed as an RPC.
const (
	// controlStreamID is the stream ID used by every control frame.
	// Stream ID 0 is neither a valid client (odd) nor server (even)
	// initiated stream, which makes control frames unambiguous to both
	// old and new peers.
	controlStreamID uint32 = 0

	// controlV1Length is the fixed payload length of a version 1
	// control message.
	controlV1Length = 12
)

// Magic and payload version bytes for control frames.
const (
	controlMagic0   byte = 'T'
	controlMagic1   byte = 'R'
	controlMagic2   byte = 'P'
	controlMagic3   byte = 'C'
	controlVersion1 byte = 1
)

// controlMessageType enumerates the control messages defined for
// payload version 1.
type controlMessageType uint32

const (
	// controlMessageDrainBegin (0x01) is sent from server to client to
	// announce that the server is draining the connection. Its payload
	// carries the last client-initiated stream ID accepted before the
	// drain boundary.
	controlMessageDrainBegin controlMessageType = 0x01
)

// controlV1 holds the 12-byte version 1 control header:
//
//	 0       3 4       7 8       11
//	+---------+---------+-----------+
//	| magic   | version | type (32) |
//	+---------+---------+-----------+
//
// Immediately followed by the type-specific body: drain begin carries
// one big-endian uint32 (the drain boundary stream ID), making its full
// payload 16 bytes.
type controlV1 struct {
	typ controlMessageType
	// lastStreamID is the type-specific payload. For drain begin it is
	// the highest client stream ID accepted before draining.
	lastStreamID uint32
}

// marshalControl serializes a version 1 control message.
func marshalControl(c controlV1) []byte {
	p := make([]byte, controlV1Length+4)
	copy(p[0:4], []byte{controlMagic0, controlMagic1, controlMagic2, controlMagic3})
	p[4] = controlVersion1
	// bytes 5,6,7 are reserved and remain zero.
	binary.BigEndian.PutUint32(p[8:12], uint32(c.typ))
	binary.BigEndian.PutUint32(p[12:16], c.lastStreamID)
	return p
}

// unmarshalControl parses a control frame payload. It returns the
// parsed message and true only for a well-formed version 1 payload
// carrying a recognized message type. Any other payload is ignored by
// returning false: future versions or unknown types must not disrupt
// the connection.
func unmarshalControl(p []byte) (controlV1, bool) {
	if len(p) < controlV1Length {
		return controlV1{}, false
	}
	if p[0] != controlMagic0 || p[1] != controlMagic1 ||
		p[2] != controlMagic2 || p[3] != controlMagic3 {
		return controlV1{}, false
	}
	if p[4] != controlVersion1 {
		return controlV1{}, false
	}

	c := controlV1{
		typ: controlMessageType(binary.BigEndian.Uint32(p[8:12])),
	}
	switch c.typ {
	case controlMessageDrainBegin:
		if len(p) < controlV1Length+4 {
			return controlV1{}, false
		}
		c.lastStreamID = binary.BigEndian.Uint32(p[12:16])
		return c, true
	default:
		return controlV1{}, false
	}
}

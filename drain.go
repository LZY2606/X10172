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
)

// Control message kinds, carried in the Flags byte of a control frame.
// Control frames use message type 0x04 and the reserved stream identifier
// zero; they apply to the whole connection rather than to a single stream.
// Receivers must ignore control frames with kinds they do not recognize so
// that newer peers can extend the set without breaking older ones.
const (
	controlKindHello uint8 = 0x1
	controlKindDrain uint8 = 0x2
)

// Capability bits exchanged in control hello frames.
const (
	// capabilityDrain indicates the sender understands connection drain
	// control frames and correctly handles rejection of streams opened
	// past a published drain boundary.
	capabilityDrain uint32 = 0x1
)

// controlHelloPayload encodes the payload of a control hello frame as a
// big-endian 32-bit capability bitmask.
func controlHelloPayload(caps uint32) []byte {
	var p [4]byte
	binary.BigEndian.PutUint32(p[:], caps)
	return p[:]
}

// controlDrainPayload encodes the payload of a control drain frame as the
// big-endian 32-bit identifier of the last stream the sender will accept.
func controlDrainPayload(boundary uint32) []byte {
	var p [4]byte
	binary.BigEndian.PutUint32(p[:], boundary)
	return p[:]
}

// controlPayloadUint32 decodes the 32-bit big-endian value carried by a
// control frame. The second return value is false if the payload is
// malformed, in which case the frame must be ignored.
func controlPayloadUint32(p []byte) (uint32, bool) {
	if len(p) < 4 {
		return 0, false
	}
	return binary.BigEndian.Uint32(p), true
}

// handleControl processes a connection-level control frame received by the
// client. Unknown kinds and malformed payloads are ignored for forward
// compatibility.
func (c *Client) handleControl(msg *streamMessage) {
	if msg.payload != nil {
		defer c.channel.putmbuf(msg.payload)
	}

	v, ok := controlPayloadUint32(msg.payload)
	if !ok {
		return
	}

	switch msg.header.Flags {
	case controlKindHello:
		c.peerCaps.Store(v)
	case controlKindDrain:
		if !c.drainEnabled {
			return
		}
		// The boundary is monotonic: a delayed or duplicated frame must
		// never move it backwards.
		for {
			cur := c.drainBoundary.Load()
			if v <= cur {
				break
			}
			if c.drainBoundary.CompareAndSwap(cur, v) {
				break
			}
		}
		c.draining.Store(true)
		c.drainOnce.Do(func() {
			close(c.drainCh)
		})
	}
}

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
	"google.golang.org/protobuf/encoding/protowire"
)

// Control messages are connection-level messages exchanged on stream ID 0.
// They are never routed to any RPC stream.
//
// A Control message is encoded with the proto3 wire format (the same format
// used by Request and Response payloads) so that it remains compatible with
// standard proto tooling, even though this implementation encodes and
// decodes it without a generated type:
//
//	message Control {
//	  ControlType type = 1;
//	  uint32 last_stream_id = 2;
//	  fixed64 features = 3;
//	}
type controlMessage struct {
	// typ selects the meaning of the control message.
	typ controlType
	// lastStreamID is the advertised stream id boundary of a drain. It is
	// only set on drain announcements.
	lastStreamID uint32
	// features is the bitmask of connection-level features understood or
	// enabled by the sender. It is only set on capabilities messages.
	features uint64
}

type controlType uint32

const (
	// controlTypeCapabilities announces the connection-level features the
	// sender is able to speak on this connection.
	controlTypeCapabilities controlType = 1
	// controlTypeDrain announces that the server is draining the
	// connection. New calls with a stream id greater than lastStreamID are
	// rejected; calls at or below the boundary complete normally.
	controlTypeDrain controlType = 2
)

// FeatureGracefulDrain is the feature bit negotiated by both peers to enable
// the connection-level graceful drain protocol.
const FeatureGracefulDrain uint64 = 1

const (
	controlFieldType         = 1
	controlFieldLastStreamID = 2
	controlFieldFeatures     = 3
)

// marshalControl encodes a controlMessage using the proto3 wire format.
func marshalControl(msg *controlMessage) []byte {
	b := make([]byte, 0, 16)
	b = protowire.AppendTag(b, controlFieldType, protowire.VarintType)
	b = protowire.AppendVarint(b, uint64(msg.typ))
	if msg.lastStreamID != 0 {
		b = protowire.AppendTag(b, controlFieldLastStreamID, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(msg.lastStreamID))
	}
	if msg.features != 0 {
		b = protowire.AppendTag(b, controlFieldFeatures, protowire.VarintType)
		b = protowire.AppendVarint(b, msg.features)
	}
	return b
}

// unmarshalControl decodes a controlMessage. Unknown fields are skipped so
// future control message extensions stay forward compatible. A malformed
// message yields ok == false and the caller must ignore it.
func unmarshalControl(p []byte) (controlMessage, bool) {
	var (
		msg controlMessage

		typ           uint64
		typSeen       bool
		lastStreamID  uint64
		features      uint64
	)
	for len(p) > 0 {
		num, typWire, n := protowire.ConsumeTag(p)
		if n < 0 {
			return controlMessage{}, false
		}
		p = p[n:]
		v, n := protowire.ConsumeVarint(p)
		if n < 0 {
			return controlMessage{}, false
		}
		p = p[n:]
		switch num {
		case controlFieldType:
			if typWire != protowire.VarintType {
				return controlMessage{}, false
			}
			typ, typSeen = v, true
		case controlFieldLastStreamID:
			lastStreamID = v
		case controlFieldFeatures:
			features = v
		}
	}
	if !typSeen || lastStreamID > uint64(^uint32(0)) {
		return controlMessage{}, false
	}
	msg.typ = controlType(typ)
	msg.lastStreamID = uint32(lastStreamID)
	msg.features = features
	return msg, true
}

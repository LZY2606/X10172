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

// Control messages are connection-level frames. They always use stream ID 0
// and never create or address an RPC stream.
const controlStreamID uint32 = 0

// FeatureGracefulDrain is the capability bit which advertises support for
// the graceful drain protocol described in PROTOCOL.md.
const FeatureGracefulDrain uint64 = 0x1

// controlPayload field numbers. The payload is a tiny protobuf message so
// that the wire format stays compatible with future extensions; fields are
// read and written directly with protowire to avoid a code generation step.
const (
	controlFieldFeatures protowire.Number = 1 // varint: bitmask of Feature* values
	controlFieldLastSID  protowire.Number = 2 // varint: last accepted stream id when draining
	controlFieldDraining protowire.Number = 3 // varint (bool): present and non-zero on drain announcements
)

func marshalControl(features uint64, lastStreamID uint32, draining bool) []byte {
	var b []byte
	if features != 0 {
		b = protowire.AppendTag(b, controlFieldFeatures, protowire.VarintType)
		b = protowire.AppendVarint(b, features)
	}
	if lastStreamID != 0 {
		b = protowire.AppendTag(b, controlFieldLastSID, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(lastStreamID))
	}
	if draining {
		b = protowire.AppendTag(b, controlFieldDraining, protowire.VarintType)
		b = protowire.AppendVarint(b, 1)
	}
	return b
}

func parseControl(b []byte) (features uint64, lastStreamID uint32, draining bool) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return
		}
		b = b[n:]
		switch typ {
		case protowire.VarintType:
			v, m := protowire.ConsumeVarint(b)
			if m < 0 {
				return
			}
			b = b[m:]
			switch num {
			case controlFieldFeatures:
				features = v
			case controlFieldLastSID:
				lastStreamID = uint32(v)
			case controlFieldDraining:
				draining = v != 0
			}
		default:
			m := protowire.ConsumeFieldValue(num, typ, b)
			if m < 0 {
				return
			}
			b = b[m:]
		}
	}
	return
}

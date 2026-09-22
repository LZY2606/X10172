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
	"errors"
	"fmt"
	"sync"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	// controlStreamID is the connection-scoped stream id used by Control
	// message frames. Client initiated streams use odd ids, so stream 0
	// never collides with RPC traffic.
	controlStreamID uint32 = 0

	// featureGracefulDrain is the SETTINGS feature name negotiated between
	// peers before any DRAIN control frame may be sent. The value is "1"
	// when the sender supports the feature.
	featureGracefulDrain = "graceful-drain"
	featureEnabledValue  = "1"

	// drainErrorDomain and drainErrorReason identify draining rejections in
	// the google.rpc.ErrorInfo status detail. They let clients of any
	// implementation recognize the condition without parsing message text.
	drainErrorDomain = "ttrpc"
	drainErrorReason = "CONNECTION_DRAINING"
)

// drainingError is the stable error returned when an RPC is rejected because
// the connection it was sent on is draining. It is both a sentinel (usable
// with errors.Is) and a gRPC status with code codes.Unavailable.
type drainingError struct {
	lastStreamID uint32
}

func (e *drainingError) Error() string {
	return fmt.Sprintf("ttrpc: connection is draining; stream ids greater than %d are rejected on this connection", e.lastStreamID)
}

// GRPCStatus maps the draining condition to codes.Unavailable, the standard
// code for transiently-unavailable services where callers may retry.
func (e *drainingError) GRPCStatus() *status.Status {
	st := mustDrainingStatus(e.lastStreamID)
	return st
}

func mustDrainingStatus(lastStreamID uint32) *status.Status {
	st, err := drainingStatus(lastStreamID)
	if err != nil {
		// The only possible error is a failure attaching the detail, which
		// cannot happen for an ErrorInfo constructed here.
		panic(err)
	}
	return st
}

func drainingStatus(lastStreamID uint32) (*status.Status, error) {
	return status.New(codes.Unavailable,
		fmt.Sprintf("ttrpc: connection is draining; stream ids greater than %d are rejected on this connection", lastStreamID)).
		WithDetails(&errdetails.ErrorInfo{
			Reason:   drainErrorReason,
			Domain:   drainErrorDomain,
			Metadata: map[string]string{"last_stream_id": fmt.Sprint(lastStreamID)},
		})
}

// ErrConnectionDraining is the stable, identifiable error returned when an RPC
// cannot be started, or an in-flight RPC is rejected, because the server is
// gracefully draining the connection. Its gRPC status code is Unavailable and
// it carries an ErrorInfo detail with domain "ttrpc" and reason
// "CONNECTION_DRAINING".
var ErrConnectionDraining error = &drainingError{}

// IsDraining reports whether err indicates a rejected call due to a server
// gracefully draining the connection. It matches both a locally produced
// ErrConnectionDraining and a draining status received from any peer.
func IsDraining(err error) bool {
	if errors.Is(err, ErrConnectionDraining) {
		return true
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unavailable {
		return false
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok &&
			info.Domain == drainErrorDomain && info.Reason == drainErrorReason {
			return true
		}
	}
	return false
}

// drainBoundaryFromStatus extracts the last accepted stream id announced with a
// draining status detail, if present.
func drainBoundaryFromStatus(err error) (uint32, bool) {
	st, ok := status.FromError(err)
	if !ok {
		return 0, false
	}
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok &&
			info.Domain == drainErrorDomain && info.Reason == drainErrorReason {
			var sid uint32
			if v, ok := info.Metadata["last_stream_id"]; ok {
				var parsed uint64
				for i := 0; i < len(v); i++ {
					if v[i] < '0' || v[i] > '9' {
						return 0, false
					}
					parsed = parsed*10 + uint64(v[i]-'0')
					if parsed > uint64(^uint32(0)) {
						return 0, false
					}
				}
				sid = uint32(parsed)
			}
			return sid, true
		}
	}
	return 0, false
}

// drainBoundary is a monotonic connection drain boundary. A boundary of 0 is
// valid: it means the server had not accepted any client stream when draining
// began. Once announced, the value may only move forward; a delayed or
// duplicate frame with a smaller value is ignored.
type drainBoundary struct {
	mu        sync.Mutex
	v         uint32
	announced bool
}

func (b *drainBoundary) get() (uint32, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.v, b.announced
}

// advance records the boundary the first time and afterwards only when n is
// greater than the current value. It reports whether the boundary changed.
func (b *drainBoundary) advance(n uint32) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.announced && n <= b.v {
		return false
	}
	b.announced = true
	b.v = n
	return true
}

// marshalSettingsControl encodes a SETTINGS control frame advertising the
// given feature names.
func marshalSettingsControl(features ...string) []byte {
	ctrl := &Control{Type: Control_SETTINGS}
	for _, f := range features {
		ctrl.Settings = append(ctrl.Settings, &Control_SettingsEntry{
			Name:  f,
			Value: featureEnabledValue,
		})
	}
	p, err := proto.Marshal(ctrl)
	if err != nil {
		panic(err)
	}
	return p
}

// marshalDrainControl encodes a DRAIN control frame carrying the last accepted
// stream id.
func marshalDrainControl(lastStreamID uint32) []byte {
	p, err := proto.Marshal(&Control{
		Type:         Control_DRAIN,
		LastStreamId: lastStreamID,
	})
	if err != nil {
		panic(err)
	}
	return p
}

// supportsGracefulDrain reports whether a SETTINGS control frame advertises the
// graceful drain feature.
func supportsGracefulDrain(ctrl *Control) bool {
	for _, e := range ctrl.GetSettings() {
		if e.GetName() == featureGracefulDrain && e.GetValue() == featureEnabledValue {
			return true
		}
	}
	return false
}

// unmarshalControl parses a control frame payload. Malformed frames report an
// error that the caller may log and ignore without poisoning the connection.
func unmarshalControl(p []byte) (*Control, error) {
	ctrl := &Control{}
	if err := proto.Unmarshal(p, ctrl); err != nil {
		return nil, err
	}
	return ctrl, nil
}

// clientResponseError converts a wire response status into a client error,
// mapping an identifiable draining status to the stable ErrConnectionDraining
// sentinel.
func clientResponseError(st *rpcstatus.Status) error {
	if st == nil {
		return nil
	}
	err := status.ErrorProto(st)
	if IsDraining(err) {
		return ErrConnectionDraining
	}
	return err
}

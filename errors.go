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

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	// ErrProtocol is a general error in the handling the protocol.
	ErrProtocol = errors.New("protocol error")

	// ErrClosed is returned by client methods when the underlying connection is
	// closed.
	ErrClosed = errors.New("ttrpc: closed")

	// ErrServerClosed is returned when the Server has closed its connection.
	ErrServerClosed = errors.New("ttrpc: server closed")

	// ErrStreamClosed is when the streaming connection is closed.
	ErrStreamClosed = errors.New("ttrpc: stream closed")

	// ErrStreamFull is returned when a stream's receive buffer is full
	// and the message cannot be delivered without blocking the
	// connection's receive loop. This prevents a single unconsumed
	// stream from deadlocking all other streams on the same connection.
	ErrStreamFull = errors.New("ttrpc: stream buffer full")

	// ErrDrainNotEnabled is returned from Server.Drain when the server was
	// created without the WithGracefulDrain option.
	ErrDrainNotEnabled = errors.New("ttrpc: graceful drain not enabled")
)

// drainMessage is the stable status message used for every rejection caused
// by a connection's drain boundary. Keeping the message identical allows
// local rejections and server-side responses to be recognized the same way.
const drainMessage = "ttrpc: connection is draining"

// drainingError is returned for calls rejected because they start after the
// drain boundary of the underlying connection. It maps to the gRPC
// codes.Unavailable status and is recognizable with IsDraining.
type drainingError struct {
	lastStreamID uint32
}

func (e *drainingError) Error() string {
	return fmt.Sprintf("%s (last accepted stream id: %d)", drainMessage, e.lastStreamID)
}

// GRPCStatus implements the gRPC status interface.
func (e *drainingError) GRPCStatus() *status.Status {
	return status.New(codes.Unavailable, e.Error())
}

// Is allows errors.Is(err, ErrConnectionDraining) style checks against
// locally generated drain rejections.
func (e *drainingError) Is(target error) bool {
	return target == ErrConnectionDraining
}

// ErrConnectionDraining is returned for calls rejected by the local client
// once the server announced that the connection is draining.
var ErrConnectionDraining = errors.New(drainMessage)

// IsDraining reports whether err is a rejection caused by the drain
// boundary. This matches both local rejections (ErrConnectionDraining) and
// the stable Unavailable status received from a draining server, even
// across package boundaries where the concrete error type is unavailable.
func IsDraining(err error) bool {
	if errors.Is(err, ErrConnectionDraining) {
		return true
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Unavailable && st.Message() == drainMessage
}

// OversizedMessageErr is used to indicate refusal to send an oversized message.
// It wraps a ResourceExhausted grpc Status together with the offending message
// length.
type OversizedMessageErr struct {
	messageLength int
	err           error
}

// OversizedMessageError returns an OversizedMessageErr error for the given message
// length if it exceeds the allowed maximum. Otherwise a nil error is returned.
func OversizedMessageError(messageLength int) error {
	if messageLength <= messageLengthMax {
		return nil
	}

	return &OversizedMessageErr{
		messageLength: messageLength,
		err:           status.Errorf(codes.ResourceExhausted, "message length %v exceed maximum message size of %v", messageLength, messageLengthMax),
	}
}

// Error returns the error message for the corresponding grpc Status for the error.
func (e *OversizedMessageErr) Error() string {
	return e.err.Error()
}

// Unwrap returns the corresponding error with our grpc status code.
func (e *OversizedMessageErr) Unwrap() error {
	return e.err
}

// RejectedLength retrieves the rejected message length which triggered the error.
func (e *OversizedMessageErr) RejectedLength() int {
	return e.messageLength
}

// MaximumLength retrieves the maximum allowed message length that triggered the error.
func (*OversizedMessageErr) MaximumLength() int {
	return messageLengthMax
}

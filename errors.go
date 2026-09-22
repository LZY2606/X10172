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

	// ErrDraining is returned for calls rejected by a connection that is
	// gracefully draining. It is stable and recognizable: it is detectable
	// with errors.Is and, on the wire, maps to a gRPC status with code
	// codes.Unavailable. Callers should retry such a call on a new
	// connection rather than relying on connection-close timing.
	ErrDraining = errors.New("ttrpc: connection draining")
)

// isDrainingStatus reports whether st is the stable drain-rejection status a
// server sends for calls past the drain boundary.
func isDrainingStatus(st *status.Status) bool {
	return st != nil && st.Code() == codes.Unavailable && st.Message() == drainStatusMessage
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

// drainStatusMessage is the stable message carried by the gRPC status that a
// server uses to reject calls that arrive after its graceful drain boundary.
// Clients recognize this exact message together with codes.Unavailable and map
// it onto ErrDraining, so callers can distinguish a deliberate drain from a
// lost connection.
const drainStatusMessage = "ttrpc: server is draining this connection"

// drainingError is the error returned to clients for calls rejected by a
// server graceful drain, and for new streams a client refuses to allocate
// locally after it has observed the drain boundary. It remains inspectable as
// both a gRPC status (codes.Unavailable) and the sentinel ErrDraining.
type drainingError struct {
	st *status.Status
}

func newDrainingError() error {
	return &drainingError{
		st: status.New(codes.Unavailable, drainStatusMessage),
	}
}

func (e *drainingError) Error() string { return e.st.Message() }

// GRPCStatus exposes the stable wire status for the rejection.
func (e *drainingError) GRPCStatus() *status.Status { return e.st }

// Unwrap makes the error match the ErrDraining sentinel via errors.Is.
func (e *drainingError) Unwrap() error { return ErrDraining }

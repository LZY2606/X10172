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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/containerd/log"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Control frame operations, carried in the flags byte of control frames.
// Unknown operations must be ignored for forward compatibility.
const (
	// controlFlagCapabilities advertises the capabilities of the sender.
	// The payload is a 4-byte big-endian capability bitmask.
	controlFlagCapabilities uint8 = 0x1
	// controlFlagDrain announces a graceful connection drain. The payload
	// is the 4-byte big-endian last stream identifier the server accepts.
	controlFlagDrain uint8 = 0x2
)

// Capability bits advertised in controlFlagCapabilities frames.
const (
	// capabilityDrain indicates the sender understands drain control frames.
	capabilityDrain uint32 = 0x1
)

// ErrServerDraining is returned to calls attempted after the server has
// announced a graceful drain of the connection. It is returned directly when
// the client already knows the drain boundary, and wrapped together with the
// wire status (codes.Unavailable) when the server rejects a call that raced
// with the drain announcement, so both errors.Is(err, ErrServerDraining) and
// status.Code(err) == codes.Unavailable hold in that case.
var ErrServerDraining = errors.New("ttrpc: server is draining")

// isDrainStatus reports whether a response status is a drain rejection.
func isDrainStatus(st *spb.Status) bool {
	return st != nil && st.Code == int32(codes.Unavailable) && st.Message == ErrServerDraining.Error()
}

// asDrainError wraps a drain rejection status so that callers can match on
// either ErrServerDraining or the grpc status code.
func asDrainError(st *spb.Status) error {
	return fmt.Errorf("%w: %w", ErrServerDraining, status.ErrorProto(st))
}

// WithDrainSupport enables graceful drain support on the client. The client
// advertises the drain capability to the server during connection setup,
// allowing the server to publish a drain boundary that the client observes
// through Drained and DrainBoundary. Clients without this option never
// receive drain control frames and keep the previous wire behavior.
func WithDrainSupport() ClientOpts {
	return func(c *Client) {
		c.drainSupport = true
	}
}

// Drained returns a channel that is closed once the server announces a
// graceful drain of this connection. It never closes if the server does not
// drain the connection.
func (c *Client) Drained() <-chan struct{} {
	return c.drainedCh
}

// DrainBoundary returns the last stream identifier the server accepts on
// this connection and whether a drain has been announced. Streams created
// before the boundary complete normally; new streams are rejected with
// ErrServerDraining.
func (c *Client) DrainBoundary() (uint32, bool) {
	c.drainMu.Lock()
	defer c.drainMu.Unlock()
	return uint32(c.drainLast), c.draining
}

// handleControl processes a connection-scoped control frame. Unknown
// operations are ignored for forward compatibility.
func (c *Client) handleControl(mh messageHeader, p []byte) {
	if len(p) > 0 {
		defer c.channel.putmbuf(p)
	}
	switch mh.Flags {
	case controlFlagCapabilities:
		// No server capabilities are currently defined.
	case controlFlagDrain:
		if len(p) < 4 {
			log.G(c.ctx).Error("ttrpc: received malformed drain control frame")
			return
		}
		boundary := streamID(binary.BigEndian.Uint32(p))
		c.drainMu.Lock()
		defer c.drainMu.Unlock()
		if c.draining && boundary <= c.drainLast {
			// The boundary must be monotonic: delayed or duplicated
			// frames must not move it backwards.
			return
		}
		c.drainLast = boundary
		if !c.draining {
			c.draining = true
			close(c.drainedCh)
		}
	}
}

// Drain gracefully drains all current server connections. Each connection
// publishes the last stream identifier it received; streams at or below the
// boundary run to completion while new streams are rejected with
// ErrServerDraining (codes.Unavailable on the wire). Drain blocks until
// every drained connection has finished its in-flight streams or terminated,
// and returns nil. It returns the context error if the context is done
// first. Drain is idempotent and safe to call concurrently with Shutdown or
// Close; it does not close listeners or prevent new connections.
func (s *Server) Drain(ctx context.Context) error {
	s.mu.Lock()
	conns := make([]*serverConn, 0, len(s.connections))
	for c := range s.connections {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	for _, c := range conns {
		c.startDrain()
	}
	for _, c := range conns {
		select {
		case <-c.drainDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// startDrain asks the connection run loop to publish the drain boundary.
// It is idempotent and never blocks.
func (c *serverConn) startDrain() {
	c.drainOnce.Do(func() {
		c.drainReq <- struct{}{}
	})
}

// finishDrain unblocks Drain waiters. It is called when the drained
// connection becomes idle and when the connection run loop exits.
func (c *serverConn) finishDrain() {
	c.drainDoneOnce.Do(func() {
		close(c.drainDone)
	})
}

// handleControl processes a control frame received from the client.
func (c *serverConn) handleControl(mh messageHeader, p []byte) {
	if mh.Flags == controlFlagCapabilities && len(p) >= 4 {
		atomic.StoreUint32(&c.peerCaps, binary.BigEndian.Uint32(p))
	}
}

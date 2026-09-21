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
	"time"

	"github.com/containerd/log"
	"google.golang.org/grpc/status"
)

// This file implements the optional graceful connection drain extension.
//
// Capability negotiation reuses the existing request metadata channel: a
// client created with WithClientDrainSupport attaches the reserved metadata
// key drainCapabilityKey to every request. The server strips the key before
// dispatching so handlers never observe it, and remembers per connection
// that the peer understands control frames.
//
// Once negotiated, Server.Drain publishes a control frame of kind
// controlKindDrain carrying the last stream id the server accepted on the
// connection. Streams received up to and including the boundary complete
// normally; any request with a higher stream id is rejected with
// ErrConnectionDraining (codes.Unavailable). Peers that do not understand
// control frames never receive them, and unknown control kinds must be
// ignored by receivers for forward compatibility.

const (
	// controlKindDrain is the control frame kind announcing a graceful
	// connection drain. The payload is a 4-byte big-endian uint32 holding
	// the last stream id the server will accept on this connection.
	controlKindDrain uint8 = 0x01

	// controlFramePayloadLength is the payload length of a drain control
	// frame: a single big-endian uint32 stream id boundary.
	controlFramePayloadLength = 4

	// drainCapabilityKey is the reserved request metadata key a client
	// uses to announce that it understands drain control frames. The
	// server strips this key before the request is dispatched.
	drainCapabilityKey = "ttrpc-drain-capable"
)

// WithClientDrainSupport enables the graceful drain extension on the
// client. The client announces support to the server on every request and
// processes drain control frames: once a drain boundary is received, new
// calls and streams fail fast with ErrConnectionDraining and the channel
// returned by Drained is closed. Without this option the client behaves
// exactly as before and ignores control frames.
func WithClientDrainSupport() ClientOpts {
	return func(c *Client) {
		c.drainSupport = true
	}
}

// Drained returns a channel that is closed once the server has announced a
// graceful drain of this connection. It is only meaningful for clients
// created with WithClientDrainSupport; for other clients the channel never
// closes. After the channel is closed, new calls and streams on this
// client fail with ErrConnectionDraining while in-flight streams received
// before the drain boundary complete normally.
func (c *Client) Drained() <-chan struct{} {
	return c.drainedCh
}

// setDrainCapability attaches the drain capability announcement to an
// outgoing request. It is a no-op unless the client opted in with
// WithClientDrainSupport.
func (c *Client) setDrainCapability(req *Request) {
	if !c.drainSupport {
		return
	}
	req.Metadata = append(req.Metadata, &KeyValue{Key: drainCapabilityKey, Value: "true"})
}

// handleControl processes a control frame received on the connection.
// Unknown control kinds are ignored so that peers may introduce new kinds
// without breaking this implementation.
func (c *Client) handleControl(header messageHeader, payload []byte) {
	if payload != nil {
		defer c.channel.putmbuf(payload)
	}

	switch header.Flags {
	case controlKindDrain:
		if len(payload) != controlFramePayloadLength {
			log.G(c.ctx).WithField("length", len(payload)).Error("ttrpc: ignoring malformed drain control frame")
			return
		}
		c.setDrainBoundary(streamID(binary.BigEndian.Uint32(payload)))
	default:
		log.G(c.ctx).WithField("kind", header.Flags).Debug("ttrpc: ignoring unknown control frame")
	}
}

// setDrainBoundary records the drain boundary announced by the server. The
// boundary is monotonic: a delayed or duplicated frame carrying a smaller
// boundary never regresses the recorded value.
func (c *Client) setDrainBoundary(boundary streamID) {
	for {
		current := c.drainBoundary.Load()
		if uint32(boundary) <= current {
			break
		}
		if c.drainBoundary.CompareAndSwap(current, uint32(boundary)) {
			break
		}
	}
	c.drained.Store(true)
	c.drainOnce.Do(func() {
		close(c.drainedCh)
	})
}

// stripDrainCapability removes the reserved drain capability key from an
// incoming request, reporting whether it was present. Stripping keeps the
// negotiation key invisible to service handlers and interceptors.
func stripDrainCapability(req *Request) bool {
	var (
		md    = req.Metadata
		found bool
		kept  int
	)
	for _, kv := range md {
		if kv.Key == drainCapabilityKey {
			found = true
			continue
		}
		md[kept] = kv
		kept++
	}
	req.Metadata = md[:kept]
	return found
}

// startDrain initiates a graceful drain of the connection. It is idempotent:
// only the first call takes effect and the drain boundary computed then is
// kept for the lifetime of the connection.
func (c *serverConn) startDrain() {
	c.drainOnce.Do(func() {
		close(c.drainCh)
	})
}

// setDrainCapable records that the peer understands drain control frames.
// If the connection is already draining, an announcement is requested so a
// peer that connected to an already draining server still learns the
// boundary.
func (c *serverConn) setDrainCapable() {
	if c.drainCapable.CompareAndSwap(false, true) && c.drained.Load() {
		c.requestDrainAnnounce()
	}
}

// requestDrainAnnounce asks the connection run loop to send the drain
// control frame. The frame is sent at most once and the request never
// blocks, even if the run loop has already exited.
func (c *serverConn) requestDrainAnnounce() {
	c.drainAnnounceOnce.Do(func() {
		c.drainAnnounceCh <- struct{}{}
	})
}

// isDrained reports whether the connection finished draining, either
// because it has no active streams left or because it is closed.
func (c *serverConn) isDrained() bool {
	st, ok := c.getState()
	return !ok || st != connStateActive
}

// Drain gracefully drains all current and future connections of the server
// and blocks until every connection present at the time of the call has
// finished its in-flight streams, the context expires, or the server is
// shut down.
//
// For each connection that negotiated drain support (see
// WithClientDrainSupport), Drain publishes the last accepted stream id as a
// control frame. Streams received up to that boundary complete normally;
// any later request on the connection is rejected with
// ErrConnectionDraining. Connections whose peer did not negotiate drain
// support receive no control frame but are held to the same boundary.
//
// Drain is idempotent: concurrent or repeated calls observe the same
// per-connection boundary, which never regresses. Drain does not close
// listeners or connections; call Shutdown afterwards to release idle
// connections. A nil return means all connections drained; a non-nil
// return is either the context error or ErrServerClosed.
func (s *Server) Drain(ctx context.Context) error {
	s.mu.Lock()
	select {
	case <-s.done:
		s.mu.Unlock()
		return ErrServerClosed
	default:
	}
	s.draining = true
	conns := make([]*serverConn, 0, len(s.connections))
	for c := range s.connections {
		c.startDrain()
		conns = append(conns, c)
	}
	s.mu.Unlock()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		drained := true
		for _, c := range conns {
			if !c.isDrained() {
				drained = false
				break
			}
		}
		if drained {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.done:
			return ErrServerClosed
		case <-ticker.C:
		}
	}
}

// drainingStatus returns the status used to reject requests that arrive
// after the drain boundary.
func drainingStatus() *status.Status {
	st, _ := status.FromError(ErrConnectionDraining)
	return st
}

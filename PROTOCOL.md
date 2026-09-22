# Protocol Specification

The ttrpc protocol is client/server protocol to support multiple request streams
over a single connection with lightweight framing. The client represents the
process which initiated the underlying connection and the server is the process
which accepted the connection. The protocol is currently defined as
asymmetrical, with clients sending requests and servers sending responses. Both
clients and servers are able to send stream data. The roles are also used in
determining the stream identifiers, with client initiated streams using odd
number identifiers and server initiated using even number. The protocol may be
extended in the future to support server initiated streams, that is not
supported in the latest version.

## Purpose

The ttrpc protocol is designed to be lightweight and optimized for low latency
and reliable connections between processes on the same host. The protocol does
not include features for handling unreliable connections such as handshakes,
resets, pings, or flow control. The protocol is designed to make low-overhead
implementations as simple as possible. It is not intended as a suitable
replacement for HTTP2/3 over the network.

## Message Frame

Each Message Frame consists of a 10-byte message header followed
by message data. The data length and stream ID are both big-endian
4-byte unsigned integers. The message type is an unsigned 1-byte
integer. The flags are also an unsigned 1-byte integer and
use is defined by the message type.

    +---------------------------------------------------------------+
    |                       Data Length (32)                        |
    +---------------------------------------------------------------+
    |                        Stream ID (32)                         |
    +---------------+-----------------------------------------------+
    | Msg Type (8)  |
    +---------------+
    |   Flags (8)   |
    +---------------+-----------------------------------------------+
    |                           Data (*)                            |
    +---------------------------------------------------------------+

The Data Length field represents the number of bytes in the Data field. The
total frame size will always be Data Length + 10 bytes. The maximum data length
is 4MB and any larger size should be rejected. Due to the maximum data size
being less than 16MB, the first frame byte should always be zero. This first
byte should be considered reserved for future use.

The Stream ID must be odd for client initiated streams and even for server
initiated streams. Server initiated streams are not currently supported.

## Mesage Types

| Message Type | Name     | Description                      |
|--------------|----------|----------------------------------|
| 0x01         | Request  | Initiates stream                 |
| 0x02         | Response | Final stream data and terminates |
| 0x03         | Data     | Stream data                      |
| 0x04         | Control  | Connection-level control message |

Message types that an implementation does not recognize must be
ignored: the message payload is consumed according to the header data
length and protocol processing continues. Control messages must never
be treated as RPC traffic.

### Request

The request message is used to initiate stream and send along request data for
properly routing and handling the stream. The stream may indicate unary without
any inbound or outbound stream data with only a response is expected on the
stream. The request may also indicate the stream is still open for more data and
no response is expected until data is finished. If the remote indicates the
stream is closed, the request may be considered non-unary but without anymore
stream data sent. In the case of `remote closed`, the remote still expects to
receive a response or stream data. For compatibility with non streaming clients,
a request with empty flags indicates a unary request.

#### Request Flags

| Flag | Name            | Description                                      |
|------|-----------------|--------------------------------------------------|
| 0x01 | `remote closed` | Non-unary, but no more data expected from remote |
| 0x02 | `remote open`   | Non-unary, remote is still sending data          |

### Response

The response message is used to end a stream with data, an empty response, or
an error. A response message is the only expected message after a unary request.
A non-unary request does not require a response message if the server is sending
back stream data. A non-unary stream may return a single response message but no
other stream data may follow.

#### Response Flags

No response flags are defined at this time, flags should be empty.

### Data

The data message is used to send data on an already initialized stream. Either
client or server may send data. A data message is not allowed on a unary stream.
A data message should not be sent after indicating `remote closed` to the peer.
The last data message on a stream must set the `remote closed` flag.

The `no data` flag is used to indicate that the data message does not include
any data. This is normally used with the `remote closed` flag to indicate the
stream is now closed without transmitting any data. Since ttrpc normally
transmits a single object per message, a zero length data message may be
interpreted as an empty object. For example, transmitting the number zero as a
protobuf message ends up with a data length of zero, but the message is still
considered data and should be processed.

#### Data Flags

| Flag | Name            | Description                       |
|------|-----------------|-----------------------------------|
| 0x01 | `remote closed` | No more data expected from remote |
| 0x04 | `no data`       | This message does not have data   |

### Control

Control messages are connection scoped rather than belonging to any
RPC stream. A control frame always uses Stream ID `0`, which is
neither a valid client (odd) nor a server (even) initiated stream. The
Stream ID parity checks that apply to requests and data do not apply to
Stream ID `0`. A peer which does not implement control messages simply
discards them; under no circumstances may a control frame be delivered
to a stream handler or reported as an unknown stream error.

The control payload starts with a fixed 12-byte little-agnostic header,
with all multi-byte integers encoded big-endian:

    +---------------------------------------------------------------+
    |                    Magic "TRPC" (32 bits)                     |
    +---------------------------------------------------------------+
    | Payload Version (8) |        Reserved (24 bits, zero)         |
    +---------------------------------------------------------------+
    |                    Control Type (32 bits)                     |
    +---------------------------------------------------------------+

The 4-byte magic is the ASCII bytes `T`, `R`, `P`, `C`. Payload Version
`1` is currently defined. A peer must ignore a control frame whose
magic does not match, whose payload version is not supported, or whose
payload is shorter than the header, without disturbing the connection.

| Control Type | Name        | Direction | Description                     |
|--------------|-------------|-----------|---------------------------------|
| 0x01         | Drain Begin | S -> C    | Announce a drain stream boundary|

Unknown control types for a supported payload version are ignored.

#### Drain Begin

The Drain Begin control type (`0x01`) is sent by the server to announce
that the connection is being gracefully drained. Its 12-byte header is
immediately followed by one big-endian uint32, for a total payload of
16 bytes:

    +---------------------------------------------------------------+
    |              Last Accepted Stream ID (32 bits)                |
    +---------------------------------------------------------------+

`Last Accepted Stream ID` is the highest client-initiated stream ID
that the server had accepted before the boundary. Every stream with an
ID less than or equal to the boundary that was received before the
boundary is allowed to run to completion with the usual client
half-close, server half-close and final status ordering. Requests with
stream IDs strictly greater than the boundary are rejected by the
server without dispatching them to a handler.

The boundary is monotonic for the lifetime of a connection. A repeated
or delayed Drain Begin frame with a smaller stream ID must not move the
boundary backwards. Boundaries do not carry across reconnects: a fresh
connection starts with no boundary.

## Capability Negotiation

Graceful drain is optional and is negotiated on the existing metadata
channel, so no new handshake round trip is required and peers that
predate the feature keep working unchanged.

A drain-capable client includes a reserved metadata entry on every
Request it sends:

| Key                 | Value |
|---------------------|-------|
| `ttrpc-capabilities`| drain |

The value may be a comma-separated list of capability tokens, allowing
future extensions to share the key. The key is reserved for protocol
negotiation and is stripped before request metadata is exposed to a
server handler; user metadata never sees it.

Once the server observes the `drain` token on any request of a
connection, the connection is considered drain capable and the server
may send Control frames on it. A server must not send Control frames on
a connection whose client never advertised the capability.

Capability advertisement is one way per connection role and purely
additive. A client that does not advertise `drain` receives no control
frames; a server that does not implement control frames ignores the
metadata entry.

## Graceful Drain

When a server enters maintenance it initiates a graceful drain:

1. The server stops accepting new connections.
2. On each drain-capable connection, the receive path first drains any
   frame already pulled off the wire, snapshots the highest accepted
   client stream ID and sends one Drain Begin control frame. A frame
   already received is therefore always classified before the boundary,
   even if a control frame and a data frame become observable out of
   order at the transport.
3. Streams at or below the boundary complete normally.
4. New requests strictly above the boundary receive a final Response on
   their own stream ID carrying gRPC status code `Unavailable` (14) with
   the stable message `ttrpc: connection is draining`, instead of
   relying on the connection being torn down.
5. Once every in-flight stream has produced its final response and the
   boundary frame has been written, the server closes the connection.

On a drain-aware client, calls past the boundary fail with a stable,
identifiable error (`codes.Unavailable`, message
`ttrpc: connection is draining`) regardless of whether the client
locally fast-fails after observing the control frame or the server
rejects the request. This maps to the client API error
`ErrConnectionDraining`. Existing streams keep their original ordering:
client half-close, server half-close and the final status occur exactly
as without draining.

Connections whose clients did not negotiate the capability retain the
legacy shutdown behavior: they are not sent control frames, active
streams are left to finish, and idle connections are closed.

## Streaming

All ttrpc requests use streams to transfer data. Unary streams will only have
two messages sent per stream, a request from a client and a response from the
server. Non-unary streams, however, may send any numbers of messages from the
client and the server. This makes stream management more complicated than unary
streams since both client and server need to track additional state. To keep
this management as simple as possible, ttrpc minimizes the number of states and
uses two flags instead of control frames. Each stream has two states while a
stream is still alive: `local closed` and `remote closed`. Each peer considers
local and remote from their own perspective and sets flags from the other peer's
perspective. For example, if a client sends a data frame with the
`remote closed` flag, that is indicating that the client is now `local closed`
and the server will be `remote closed`. A unary operation does not need to send
these flags since each received message always indicates `remote closed`. Once a
peer is both `local closed` and `remote closed`, the stream is considered
finished and may be cleaned up.

Due to the asymmetric nature of the current protocol, a client should
always be in the `local closed` state before `remote closed` and a server should
always be in the `remote closed` state before `local closed`. This happens
because the client is always initiating requests and a client always expects a
final response back from a server to indicate the initiated request has been
fulfilled. This may mean server sends a final empty response to finish a stream
even after it has already completed sending data before the client.

### Unary State Diagram

         +--------+                                    +--------+
         | Client |                                    | Server |
         +---+----+                                    +----+---+
             |               +---------+                    |
      local  >---------------+ Request +--------------------> remote
      closed |               +---------+                    | closed
             |                                              |
             |              +----------+                    |
    finished <--------------+ Response +--------------------< finished
             |              +----------+                    |
             |                                              |

### Non-Unary State Diagrams

RC: `remote closed` flag
RO: `remote open` flag

         +--------+                                    +--------+
         | Client |                                    | Server |
         +---+----+                                    +----+---+
             |             +--------------+                 |
             >-------------+ Request [RO] +----------------->
             |             +--------------+                 |
             |                                              |
             |                 +------+                     |
             >-----------------+ Data +--------------------->
             |                 +------+                     |
             |                                              |
             |               +-----------+                  |
      local  >---------------+ Data [RC] +------------------> remote
      closed |               +-----------+                  | closed
             |                                              |
             |              +----------+                    |
    finished <--------------+ Response +--------------------< finished
             |              +----------+                    |
             |                                              |

         +--------+                                    +--------+
         | Client |                                    | Server |
         +---+----+                                    +----+---+
             |             +--------------+                 |
      local  >-------------+ Request [RC] +-----------------> remote
      closed |             +--------------+                 | closed
             |                                              |
             |                 +------+                     |
             <-----------------+ Data +---------------------<
             |                 +------+                     |
             |                                              |
             |               +-----------+                  |
    finished <---------------+ Data [RC] +------------------< finished
             |               +-----------+                  |
             |                                              |

         +--------+                                    +--------+
         | Client |                                    | Server |
         +---+----+                                    +----+---+
             |             +--------------+                 |
             >-------------+ Request [RO] +----------------->
             |             +--------------+                 |
             |                                              |
             |                 +------+                     |
             >-----------------+ Data +--------------------->
             |                 +------+                     |
             |                                              |
             |                 +------+                     |
             <-----------------+ Data +---------------------<
             |                 +------+                     |
             |                                              |
             |                 +------+                     |
             >-----------------+ Data +--------------------->
             |                 +------+                     |
             |                                              |
             |               +-----------+                  |
      local  >---------------+ Data [RC] +------------------> remote
      closed |               +-----------+                  | closed
             |                                              |
             |                 +------+                     |
             <-----------------+ Data +---------------------<
             |                 +------+                     |
             |                                              |
             |               +-----------+                  |
    finished <---------------+ Data [RC] +------------------< finished
             |               +-----------+                  |
             |                                              |

## RPC

While this protocol is defined primarily to support Remote Procedure Calls, the
protocol does not define the request and response types beyond the messages
defined in the protocol. The implementation provides a default protobuf
definition of request and response which may be used for cross language rpc.
All implementations should at least define a request type which support
routing by procedure name and a response type which supports call status.

## Version History

| Version | Features            |
|---------|---------------------|
| 1.0     | Unary requests only |
| 1.2     | Streaming support   |
| 1.3     | Control frames and optional graceful drain |

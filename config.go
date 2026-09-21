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
	"errors"
)

type serverConfig struct {
	handshaker  Handshaker
	interceptor UnaryServerInterceptor
	gracefulDrain bool
}

// WithServerGracefulDrain enables the optional graceful drain
// protocol. Drain-aware clients negotiate the feature through request
// metadata; once the server starts draining (see Server.Drain), each
// negotiated connection is told the last stream id it will accept,
// streams at or before that boundary complete normally, and later calls
// fail with a stable Unavailable drain rejection instead of racing a
// connection close.
//
// Servers created without this option keep the historical behavior and
// wire format exactly, even when speaking to drain-aware clients.
func WithServerGracefulDrain() ServerOpt {
	return func(c *serverConfig) error {
		c.gracefulDrain = true
		return nil
	}
}

// ServerOpt for configuring a ttrpc server
type ServerOpt func(*serverConfig) error

// WithServerHandshaker can be passed to NewServer to ensure that the
// handshaker is called before every connection attempt.
//
// Only one handshaker is allowed per server.
func WithServerHandshaker(handshaker Handshaker) ServerOpt {
	return func(c *serverConfig) error {
		if c.handshaker != nil {
			return errors.New("only one handshaker allowed per server")
		}
		c.handshaker = handshaker
		return nil
	}
}

// WithUnaryServerInterceptor sets the provided interceptor on the server
func WithUnaryServerInterceptor(i UnaryServerInterceptor) ServerOpt {
	return func(c *serverConfig) error {
		if c.interceptor != nil {
			return errors.New("only one unchained interceptor allowed per server")
		}
		c.interceptor = i
		return nil
	}
}

// WithChainUnaryServerInterceptor sets the provided chain of server interceptors
func WithChainUnaryServerInterceptor(interceptors ...UnaryServerInterceptor) ServerOpt {
	return func(c *serverConfig) error {
		if len(interceptors) == 0 {
			return nil
		}
		if c.interceptor != nil {
			interceptors = append([]UnaryServerInterceptor{c.interceptor}, interceptors...)
		}
		c.interceptor = func(
			ctx context.Context,
			unmarshal Unmarshaler,
			info *UnaryServerInfo,
			method Method) (any, error) {
			return interceptors[0](ctx, unmarshal, info,
				chainUnaryServerInterceptors(info, method, interceptors[1:]))
		}
		return nil
	}
}

func chainUnaryServerInterceptors(info *UnaryServerInfo, method Method, interceptors []UnaryServerInterceptor) Method {
	if len(interceptors) == 0 {
		return method
	}
	return func(ctx context.Context, unmarshal func(any) error) (any, error) {
		return interceptors[0](ctx, unmarshal, info,
			chainUnaryServerInterceptors(info, method, interceptors[1:]))
	}
}

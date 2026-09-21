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
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestControlFrameCodec(t *testing.T) {
	features := controlFeatureGracefulDrain
	hello := encodeControlHello(features)
	kind, fields, err := decodeControlPayload(hello)
	if err != nil {
		t.Fatalf("hello decode: %v", err)
	}
	if kind != controlHelloKind {
		t.Fatalf("hello kind = %d, want %d", kind, controlHelloKind)
	}
	if controlFeature(fields[controlFieldU32]) != features {
		t.Fatalf("hello features = %#x, want %#x", fields[controlFieldU32], features)
	}

	const boundary uint32 = 43
	drain := encodeControlDrain(boundary)
	kind, fields, err = decodeControlPayload(drain)
	if err != nil {
		t.Fatalf("drain decode: %v", err)
	}
	if kind != controlDrainKind {
		t.Fatalf("drain kind = %d, want %d", kind, controlDrainKind)
	}
	if fields[controlFieldU32] != boundary {
		t.Fatalf("drain boundary = %d, want %d", fields[controlFieldU32], boundary)
	}
}

func TestControlFrameCodecRejectsGarbage(t *testing.T) {
	for name, p := range map[string][]byte{
		"empty":             nil,
		"unknown kind":      {0x7f},
		"truncated header":  {controlHelloKind, controlFieldU32},
		"truncated value":   {controlDrainKind, controlFieldU32, 4, 0, 0},
		"bad field length":  {controlHelloKind, controlFieldU32, 2, 0, 0},
		"kind only unknown": {0x09, controlFieldU32, 4, 0, 0, 0, 0},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := decodeControlPayload(p); err == nil {
				t.Fatalf("expected error decoding % x", p)
			}
		})
	}
}

func TestControlFrameIgnoresUnknownFields(t *testing.T) {
	p := []byte{
		controlHelloKind,
		0x02, 8, 0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe, 0xba, 0xbe,
		controlFieldU32, 4, 0, 0, 0, 1,
	}
	kind, fields, err := decodeControlPayload(p)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if kind != controlHelloKind {
		t.Fatalf("kind = %d", kind)
	}
	if got := controlFeature(fields[controlFieldU32]); got != controlFeatureGracefulDrain {
		t.Fatalf("features = %#x", got)
	}
}

func TestIsDrainingError(t *testing.T) {
	if !IsDrainingError(ErrConnectionDraining) {
		t.Fatal("ErrConnectionDraining must be recognized")
	}
	wireErr := status.Error(codes.Unavailable, drainingMessage)
	if !IsDrainingError(wireErr) {
		t.Fatalf("wire draining status must be recognized: %v", wireErr)
	}
	if IsDrainingError(status.Error(codes.Unavailable, "something else")) {
		t.Fatal("generic Unavailable error must not match drain")
	}
	if IsDrainingError(status.Error(codes.Internal, drainingMessage)) {
		t.Fatal("non-Unavailable status must not match drain")
	}
}

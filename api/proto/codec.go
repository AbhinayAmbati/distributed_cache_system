package proto

import (
	"encoding/json"
	"google.golang.org/grpc/encoding"
)

// JSONCodec implements the gRPC encoding.Codec interface.
// It serializes/deserializes Go structs using JSON.
type JSONCodec struct{}

// Marshal serializes the given value into JSON bytes.
func (JSONCodec) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// Unmarshal deserializes the given JSON bytes into the value.
func (JSONCodec) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// Name returns the name of the codec. We register it under "json"
// to allow serializing arbitrary Go structs.
func (JSONCodec) Name() string {
	return "json"
}

func init() {
	// Register our JSON-based custom codec under the "proto" name.
	// This overrides gRPC's default protobuf marshaller, allowing us to use
	// standard Go structs directly without protoc-generated code.
	encoding.RegisterCodec(JSONCodec{})
}

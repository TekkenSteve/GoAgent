package grpcbackend

import (
	"encoding/json"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

const jsonCodecName = "json"

var _ = func() bool {
	encoding.RegisterCodec(jsonCodec{})

	return true
}()

// JSONServerCodecOption configures a gRPC server to use the AgentOS JSON codec.
func JSONServerCodecOption() grpc.ServerOption {
	return grpc.ForceServerCodec(jsonCodec{})
}

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (jsonCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

func (jsonCodec) Name() string {
	return jsonCodecName
}

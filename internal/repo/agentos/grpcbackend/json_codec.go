package grpcbackend

import (
	"encoding/json"

	"google.golang.org/grpc/encoding"
)

const jsonCodecName = "json"

func init() {
	encoding.RegisterCodec(jsonCodec{})
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

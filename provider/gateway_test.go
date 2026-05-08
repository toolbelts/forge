package provider

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/types/known/sourcecontextpb"
	"google.golang.org/protobuf/types/known/typepb"
)

func TestGatewayJsonPbUsesProtoNames(t *testing.T) {
	body, err := gatewayJsonPb().Marshal(&typepb.Type{
		SourceContext: &sourcecontextpb.SourceContext{FileName: "test.proto"},
	})
	if err != nil {
		t.Fatalf("marshal gateway json: %v", err)
	}

	if !bytes.Contains(body, []byte(`"source_context"`)) {
		t.Fatalf("gateway json should use proto field names: %s", body)
	}
	if bytes.Contains(body, []byte(`"sourceContext"`)) {
		t.Fatalf("gateway json should not use lower camel field names: %s", body)
	}
	if !bytes.Contains(body, []byte(`"file_name"`)) {
		t.Fatalf("nested gateway json should use proto field names: %s", body)
	}
}

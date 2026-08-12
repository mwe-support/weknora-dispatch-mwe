package datasource

import (
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
)

func TestFeishuMetadataDoesNotAdvertiseWebhook(t *testing.T) {
	meta := ConnectorMetadataRegistry[types.ConnectorTypeFeishu]

	for _, capability := range meta.Capabilities {
		if capability == "webhook" {
			t.Fatalf("Feishu connector should not advertise webhook until webhook sync is implemented")
		}
	}
}

func TestTencentDocsMetadataMatchesImplementedSyncPolicies(t *testing.T) {
	meta, ok := ConnectorMetadataRegistry[types.ConnectorTypeTencentDocs]
	if !ok {
		t.Fatal("Tencent Docs connector metadata is not registered")
	}
	if meta.AuthType != "token" {
		t.Fatalf("AuthType = %q, want token", meta.AuthType)
	}
	for _, capability := range meta.Capabilities {
		if capability == "deletion_sync" {
			t.Fatal("Tencent Docs must not advertise deletion_sync while WeKnora only counts deletion markers")
		}
	}
}

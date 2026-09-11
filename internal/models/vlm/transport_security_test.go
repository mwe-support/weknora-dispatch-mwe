package vlm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	secutils "github.com/Tencent/WeKnora/internal/utils"
)

func withVLMSSRFWhitelist(t *testing.T, raw string) {
	t.Helper()
	t.Setenv("SSRF_WHITELIST", raw)
	secutils.ResetSSRFWhitelistForTest()
	t.Cleanup(secutils.ResetSSRFWhitelistForTest)
}

func TestRemoteVLMThinkingWire(t *testing.T) {
	withVLMSSRFWhitelist(t, "127.0.0.1")
	var sent map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sent = nil
		if err := json.NewDecoder(r.Body).Decode(&sent); err != nil {
			t.Error(err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"visible text"},"finish_reason":"stop"}]}`))
	}))
	defer server.Close()
	for _, tc := range []struct {
		provider, control string
		expected          bool
	}{{"generic", "", true}, {"openai", "", false}, {"generic", "none", false}, {"openai", "chat_template_kwargs", true}} {
		v, err := NewRemoteAPIVLM(&Config{BaseURL: server.URL + "/v1", ModelName: "test", Provider: tc.provider, Extra: map[string]any{"thinking_control": tc.control}})
		if err != nil {
			t.Fatal(err)
		}
		if out, err := v.Predict(context.Background(), [][]byte{[]byte("image")}, "read image"); err != nil || out != "visible text" {
			t.Fatalf("predict: %q %v", out, err)
		}
		value, exists := sent["chat_template_kwargs"]
		if exists != tc.expected {
			t.Fatalf("provider=%s control=%s unexpected wire field: %v", tc.provider, tc.control, value)
		}
		if exists && value.(map[string]any)["enable_thinking"] != false {
			t.Fatal("thinking not disabled")
		}
	}
}

func TestRemoteAPIVLMRejectsInternalBaseURL(t *testing.T) {
	withVLMSSRFWhitelist(t, "")

	_, err := NewRemoteAPIVLM(&Config{
		BaseURL:   "http://169.254.169.254/latest/meta-data/",
		ModelName: "vlm-test",
	})
	if err == nil {
		t.Fatalf("NewRemoteAPIVLM returned nil error for blocked internal BaseURL")
	}
}

package chat

import (
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
)

// TestConfigFromModel 验证 ConfigFromModel 能把 types.Model 完整映射到 ChatConfig，
// 避免后续新加字段时只在生产路径或测试路径其中一侧更新导致功能漏配。
func TestConfigFromModel(t *testing.T) {
	m := &types.Model{
		ID:     "model-123",
		Name:   "gpt-4o-mini",
		Source: types.ModelSourceRemote,
		Parameters: types.ModelParameters{
			BaseURL:  "https://api.example.com/v1",
			APIKey:   "sk-xxx",
			Provider: "openai",
			ExtraConfig: map[string]string{
				"deployment": "prod",
			},
			CustomHeaders: map[string]string{
				"X-Gateway-Token": "abc",
				"X-Trace-ID":      "t-1",
			},
		},
	}

	cfg := ConfigFromModel(m, "app-id", "app-secret")
	if cfg == nil {
		t.Fatal("ConfigFromModel returned nil")
	}
	if cfg.ModelID != "model-123" || cfg.ModelName != "gpt-4o-mini" {
		t.Errorf("model identity not propagated: %+v", cfg)
	}
	if cfg.BaseURL != "https://api.example.com/v1" || cfg.APIKey != "sk-xxx" {
		t.Errorf("connection fields not propagated: %+v", cfg)
	}
	if cfg.Provider != "openai" {
		t.Errorf("provider not propagated: %q", cfg.Provider)
	}
	if cfg.ExtraConfig["deployment"] != "prod" {
		t.Errorf("ExtraConfig not propagated: %+v", cfg.ExtraConfig)
	}
	if cfg.CustomHeaders["X-Gateway-Token"] != "abc" ||
		cfg.CustomHeaders["X-Trace-ID"] != "t-1" {
		t.Errorf("CustomHeaders not propagated: %+v", cfg.CustomHeaders)
	}
	if cfg.AppID != "app-id" || cfg.AppSecret != "app-secret" {
		t.Errorf("cloud credentials not propagated: %+v", cfg)
	}
}

func TestConfigFromModel_Nil(t *testing.T) {
	if got := ConfigFromModel(nil, "", ""); got != nil {
		t.Fatalf("expected nil for nil model, got %+v", got)
	}
}

func TestBuiltinThinkingCompatibilityPreservesStoredConfiguration(t *testing.T) {
	m := &types.Model{ID: "builtin-local-qwythos-q4-mm", IsBuiltin: true, Parameters: types.ModelParameters{Provider: "openai", ExtraConfig: map[string]string{"other": "kept"}}}
	if cfg := ConfigFromModel(m, "", ""); EffectiveThinkingControl(cfg) != "chat_template_kwargs" || cfg.ExtraConfig["other"] != "kept" {
		t.Fatal("legacy built-in does not send the configured thinking choice")
	}
	if _, ok := m.Parameters.ExtraConfig[ExtraConfigThinkingControl]; ok {
		t.Fatal("persisted model parameters mutated")
	}
	m.Parameters.ExtraConfig[ExtraConfigThinkingControl] = "none"
	if EffectiveThinkingControl(ConfigFromModel(m, "", "")) != "none" {
		t.Fatal("explicit override lost")
	}
	delete(m.Parameters.ExtraConfig, ExtraConfigThinkingControl)
	m.IsBuiltin = false
	if EffectiveThinkingControl(ConfigFromModel(m, "", "")) != "none" {
		t.Fatal("custom model affected")
	}
}

package service

import (
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestLoadDefaultVLMSettings(t *testing.T) {
	env := map[string]string{
		defaultMultimodalEnabledEnv:      "true",
		defaultVLMModelIDEnv:             " preferred-vlm ",
		defaultVLMDescriptionLanguageEnv: " Chinese ",
		defaultVLMCustomInstructionsEnv:  " describe charts ",
	}

	settings := loadDefaultVLMSettings(func(key string) string { return env[key] })

	require.True(t, settings.Enabled)
	require.Equal(t, "preferred-vlm", settings.ModelID)
	require.Equal(t, "Chinese", settings.DescriptionLanguage)
	require.Equal(t, "describe charts", settings.CustomInstructions)
}

func TestLoadDefaultVLMSettingsInvalidBooleanIsDisabled(t *testing.T) {
	settings := loadDefaultVLMSettings(func(string) string { return "not-a-bool" })
	require.False(t, settings.Enabled)
}

func TestSelectDefaultVLMModel(t *testing.T) {
	models := []*types.Model{
		{ID: "chat", Type: types.ModelTypeKnowledgeQA, Status: types.ModelStatusActive, IsDefault: true},
		{ID: "inactive-vlm", Type: types.ModelTypeVLLM, Status: types.ModelStatusDownloadFailed, IsDefault: true},
		{ID: "default-vlm", Type: types.ModelTypeVLLM, Status: types.ModelStatusActive, IsDefault: true},
		{ID: "preferred-vlm", Type: types.ModelTypeVLLM, Status: types.ModelStatusActive},
	}

	require.Equal(t, "default-vlm", selectDefaultVLMModel(models, "").ID)
	require.Equal(t, "preferred-vlm", selectDefaultVLMModel(models, "preferred-vlm").ID)
	require.Nil(t, selectDefaultVLMModel(models, "missing-vlm"))
}

func TestApplyResolvedDefaultVLMConfig(t *testing.T) {
	kb := &types.KnowledgeBase{}
	resolved := ResolvedDefaultVLMConfig{
		Enabled:             true,
		ModelID:             "default-vlm",
		DescriptionLanguage: "Chinese",
		CustomInstructions:  "describe charts",
	}

	applyResolvedDefaultVLMConfig(kb, resolved)

	require.Equal(t, types.VLMConfig{
		Enabled:             true,
		ModelID:             "default-vlm",
		DescriptionLanguage: "Chinese",
		CustomInstructions:  "describe charts",
	}, kb.VLMConfig)
}

func TestApplyResolvedDefaultVLMConfigDoesNotOverwriteExplicitConfig(t *testing.T) {
	kb := &types.KnowledgeBase{VLMConfig: types.VLMConfig{
		Enabled: true,
		ModelID: "explicit-vlm",
	}}
	applyResolvedDefaultVLMConfig(kb, ResolvedDefaultVLMConfig{Enabled: true, ModelID: "default-vlm"})

	require.Equal(t, "explicit-vlm", kb.VLMConfig.ModelID)
}

func TestApplyResolvedDefaultVLMConfigPreservesExplicitDisabledConfig(t *testing.T) {
	kb := &types.KnowledgeBase{VLMConfigProvided: true}

	applyResolvedDefaultVLMConfig(kb, ResolvedDefaultVLMConfig{Enabled: true, ModelID: "default-vlm"})

	require.False(t, kb.VLMConfig.Enabled)
	require.Empty(t, kb.VLMConfig.ModelID)
}

func TestResolveDefaultVLMConfigRequiresEnabledSettingAndUsableModel(t *testing.T) {
	models := []*types.Model{{
		ID: "default-vlm", Type: types.ModelTypeVLLM, Status: types.ModelStatusActive, IsDefault: true,
	}}

	disabled := resolveDefaultVLMConfig(models, defaultVLMSettings{Enabled: false})
	require.False(t, disabled.Enabled)

	enabled := resolveDefaultVLMConfig(models, defaultVLMSettings{Enabled: true, DescriptionLanguage: "Chinese"})
	require.True(t, enabled.Enabled)
	require.Equal(t, "default-vlm", enabled.ModelID)
	require.Equal(t, "Chinese", enabled.DescriptionLanguage)
}

package service

import (
	"context"
	"os"
	"strconv"
	"strings"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
)

const (
	defaultMultimodalEnabledEnv      = "WEKNORA_DEFAULT_MULTIMODAL_ENABLED"
	defaultVLMModelIDEnv             = "WEKNORA_DEFAULT_VLM_MODEL_ID"
	defaultVLMDescriptionLanguageEnv = "WEKNORA_DEFAULT_VLM_DESCRIPTION_LANGUAGE"
	defaultVLMCustomInstructionsEnv  = "WEKNORA_DEFAULT_VLM_CUSTOM_INSTRUCTIONS"
)

type defaultVLMSettings struct {
	Enabled             bool
	ModelID             string
	DescriptionLanguage string
	CustomInstructions  string
}

// ResolvedDefaultVLMConfig is the single runtime representation consumed by
// both the create service and the frontend. Enabled is true only when the
// deployment switch is on and the tenant can see the selected active VLLM.
type ResolvedDefaultVLMConfig struct {
	Enabled             bool   `json:"enabled"`
	ModelID             string `json:"model_id"`
	DescriptionLanguage string `json:"description_language"`
	CustomInstructions  string `json:"custom_instructions"`
}

func loadDefaultVLMSettings(getenv func(string) string) defaultVLMSettings {
	enabled, err := strconv.ParseBool(strings.TrimSpace(getenv(defaultMultimodalEnabledEnv)))
	if err != nil {
		enabled = false
	}
	return defaultVLMSettings{
		Enabled:             enabled,
		ModelID:             strings.TrimSpace(getenv(defaultVLMModelIDEnv)),
		DescriptionLanguage: strings.TrimSpace(getenv(defaultVLMDescriptionLanguageEnv)),
		CustomInstructions:  strings.TrimSpace(getenv(defaultVLMCustomInstructionsEnv)),
	}
}

func hasExplicitVLMConfig(kb *types.KnowledgeBase) bool {
	if kb == nil {
		return false
	}
	config := kb.VLMConfig
	return kb.VLMConfigProvided || config.Enabled || config.ModelID != "" || config.ModelName != "" ||
		config.BaseURL != "" || config.APIKey != "" || config.InterfaceType != "" ||
		config.DescriptionLanguage != "" || config.CustomInstructions != ""
}

func isUsableVLMModel(model *types.Model) bool {
	return model != nil && model.Type == types.ModelTypeVLLM && model.Status == types.ModelStatusActive
}

func selectDefaultVLMModel(models []*types.Model, preferredID string) *types.Model {
	if preferredID != "" {
		for _, model := range models {
			if model.ID == preferredID && isUsableVLMModel(model) {
				return model
			}
		}
		return nil
	}
	for _, model := range models {
		if model.IsDefault && isUsableVLMModel(model) {
			return model
		}
	}
	return nil
}

func resolveDefaultVLMConfig(models []*types.Model, settings defaultVLMSettings) ResolvedDefaultVLMConfig {
	resolved := ResolvedDefaultVLMConfig{
		DescriptionLanguage: settings.DescriptionLanguage,
		CustomInstructions:  settings.CustomInstructions,
	}
	if !settings.Enabled {
		return resolved
	}
	model := selectDefaultVLMModel(models, settings.ModelID)
	if model == nil {
		return resolved
	}
	resolved.Enabled = true
	resolved.ModelID = model.ID
	return resolved
}

// ResolveDefaultVLMConfig exposes the deployment policy after validating it
// against the caller's tenant-visible models.
func ResolveDefaultVLMConfig(models []*types.Model) ResolvedDefaultVLMConfig {
	return resolveDefaultVLMConfig(models, loadDefaultVLMSettings(os.Getenv))
}

func applyResolvedDefaultVLMConfig(kb *types.KnowledgeBase, resolved ResolvedDefaultVLMConfig) {
	if kb == nil || !resolved.Enabled || resolved.ModelID == "" || hasExplicitVLMConfig(kb) {
		return
	}
	kb.VLMConfig = types.VLMConfig{
		Enabled:             true,
		ModelID:             resolved.ModelID,
		DescriptionLanguage: resolved.DescriptionLanguage,
		CustomInstructions:  resolved.CustomInstructions,
	}
}

// applyDefaultVLMConfig makes the deployment-level multimodal default work
// for every creation path (UI, API, MCP). The selected model still belongs to
// the caller's visible model set, so tenant isolation remains enforced.
func (s *knowledgeBaseService) applyDefaultVLMConfig(ctx context.Context, kb *types.KnowledgeBase) {
	settings := loadDefaultVLMSettings(os.Getenv)
	if !settings.Enabled || hasExplicitVLMConfig(kb) {
		return
	}
	if s.modelService == nil {
		logger.Warnf(ctx, "[kb.create] default multimodal enabled but model service is unavailable")
		return
	}

	models, err := s.modelService.ListModels(ctx)
	if err != nil {
		logger.Warnf(ctx, "[kb.create] list models for default VLM failed: %v", err)
		return
	}
	resolved := resolveDefaultVLMConfig(models, settings)
	if !resolved.Enabled {
		logger.Warnf(ctx, "[kb.create] default multimodal enabled but no matching active VLLM was found")
		return
	}

	applyResolvedDefaultVLMConfig(kb, resolved)
	logger.Infof(ctx, "[kb.create] applied default VLM model_id=%s language=%s",
		resolved.ModelID, resolved.DescriptionLanguage)
}

package service

import (
	"os"
	"strings"

	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
)

const (
	defaultEmbeddingModelIDEnv = types.DefaultEmbeddingModelIDEnv
	defaultRerankModelIDEnv    = "WEKNORA_DEFAULT_RERANK_MODEL_ID"
	defaultLLMModelIDEnv       = types.DefaultLLMModelIDEnv
)

type deploymentModelDefaults struct {
	EmbeddingModelID string
	RerankModelID    string
	LLMModelID       string
	VLMModelID       string
}

func loadDeploymentModelDefaults() deploymentModelDefaults {
	return deploymentModelDefaults{
		EmbeddingModelID: strings.TrimSpace(os.Getenv(defaultEmbeddingModelIDEnv)),
		RerankModelID:    strings.TrimSpace(os.Getenv(defaultRerankModelIDEnv)),
		LLMModelID:       strings.TrimSpace(os.Getenv(defaultLLMModelIDEnv)),
		VLMModelID:       strings.TrimSpace(os.Getenv(defaultVLMModelIDEnv)),
	}
}

func (d deploymentModelDefaults) enabled() bool {
	return d.EmbeddingModelID != "" || d.RerankModelID != "" || d.LLMModelID != "" || d.VLMModelID != ""
}

func applyDeploymentKnowledgeBaseModelDefaults(kb *types.KnowledgeBase) {
	if kb == nil {
		return
	}
	kb.ApplyDeploymentModelDefaults()
}

// validateDeploymentKnowledgeBaseModels closes the API/MCP omission path once
// an operator enables the deployment model policy. An all-empty environment
// deliberately keeps upstream behaviour unchanged; a partially configured
// production policy fails closed instead of persisting a KB that can never be
// indexed or summarized.
func validateDeploymentKnowledgeBaseModels(kb *types.KnowledgeBase) error {
	if kb == nil {
		return nil
	}
	defaults := loadDeploymentModelDefaults()
	if !defaults.enabled() {
		return nil
	}
	if kb.NeedsEmbeddingModel() && strings.TrimSpace(kb.EmbeddingModelID) == "" {
		return apperrors.NewBadRequestError("Deployment default embedding model is not configured")
	}
	if strings.TrimSpace(kb.SummaryModelID) == "" {
		return apperrors.NewBadRequestError("Deployment default chat model is not configured")
	}
	if kb.VLMConfig.Enabled && strings.TrimSpace(kb.VLMConfig.ModelID) == "" {
		return apperrors.NewBadRequestError("Deployment default multimodal model is not configured")
	}
	return nil
}

func applyDeploymentAgentModelDefaults(agent *types.CustomAgent) {
	if agent == nil {
		return
	}
	defaults := loadDeploymentModelDefaults()
	if strings.TrimSpace(agent.Config.ModelID) == "" {
		agent.Config.ModelID = defaults.LLMModelID
	}
	if strings.TrimSpace(agent.Config.RerankModelID) == "" {
		agent.Config.RerankModelID = defaults.RerankModelID
	}
	if strings.TrimSpace(agent.Config.VLMModelID) == "" {
		agent.Config.VLMModelID = defaults.VLMModelID
	}
}

func validateDeploymentAgentModels(agent *types.CustomAgent) error {
	if agent == nil {
		return nil
	}
	defaults := loadDeploymentModelDefaults()
	if !defaults.enabled() {
		return nil
	}
	if strings.TrimSpace(agent.Config.ModelID) == "" {
		return apperrors.NewBadRequestError("Deployment default chat model is not configured")
	}
	if agentRequiresRerankModel(agent) && strings.TrimSpace(agent.Config.RerankModelID) == "" {
		return apperrors.NewBadRequestError("Deployment default rerank model is not configured")
	}
	if agent.Config.ImageUploadEnabled && strings.TrimSpace(agent.Config.VLMModelID) == "" {
		return apperrors.NewBadRequestError("Deployment default multimodal model is not configured")
	}
	return nil
}

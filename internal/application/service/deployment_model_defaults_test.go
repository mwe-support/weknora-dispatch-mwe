package service

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

type modelDefaultsAgentRepo struct {
	agent *types.CustomAgent
}

func (r *modelDefaultsAgentRepo) CreateAgent(_ context.Context, agent *types.CustomAgent) error {
	r.agent = agent
	return nil
}

func (r *modelDefaultsAgentRepo) GetAgentByID(context.Context, string, uint64) (*types.CustomAgent, error) {
	return r.agent, nil
}

func (r *modelDefaultsAgentRepo) ListAgentsByTenantID(context.Context, uint64) ([]*types.CustomAgent, error) {
	return nil, nil
}

func (r *modelDefaultsAgentRepo) UpdateAgent(_ context.Context, agent *types.CustomAgent) error {
	r.agent = agent
	return nil
}

func (r *modelDefaultsAgentRepo) DeleteAgent(context.Context, string, uint64) error { return nil }

func (r *modelDefaultsAgentRepo) CountByModelID(context.Context, uint64, string) (int64, error) {
	return 0, nil
}

func setDeploymentModelDefaultsForTest(t *testing.T) {
	t.Helper()
	t.Setenv("WEKNORA_DEFAULT_EMBEDDING_MODEL_ID", "deployment-embedding")
	t.Setenv("WEKNORA_DEFAULT_RERANK_MODEL_ID", "deployment-rerank")
	t.Setenv("WEKNORA_DEFAULT_LLM_MODEL_ID", "deployment-llm")
	t.Setenv("WEKNORA_DEFAULT_VLM_MODEL_ID", "deployment-vlm")
}

func clearDeploymentModelDefaultsForTest(t *testing.T) {
	t.Helper()
	t.Setenv("WEKNORA_DEFAULT_EMBEDDING_MODEL_ID", "")
	t.Setenv("WEKNORA_DEFAULT_RERANK_MODEL_ID", "")
	t.Setenv("WEKNORA_DEFAULT_LLM_MODEL_ID", "")
	t.Setenv("WEKNORA_DEFAULT_VLM_MODEL_ID", "")
}

func TestCreateKnowledgeBaseUsesDeploymentModelDefaultsWhenAPIFieldsAreOmitted(t *testing.T) {
	setDeploymentModelDefaultsForTest(t)
	repo := newFakeKBRepo()
	svc := newPR3KBService(repo, &fakeRegistry{registered: map[string]struct{}{}}, &fakeOwnership{})

	kb, err := svc.CreateKnowledgeBase(ctxWithTenant(1), &types.KnowledgeBase{Name: "api-created"})
	require.NoError(t, err)
	require.Equal(t, "deployment-embedding", kb.EmbeddingModelID)
	require.Equal(t, "deployment-llm", kb.SummaryModelID)
}

func TestCreateKnowledgeBasePreservesExplicitModelBindings(t *testing.T) {
	setDeploymentModelDefaultsForTest(t)
	repo := newFakeKBRepo()
	svc := newPR3KBService(repo, &fakeRegistry{registered: map[string]struct{}{}}, &fakeOwnership{})

	kb, err := svc.CreateKnowledgeBase(ctxWithTenant(1), &types.KnowledgeBase{
		Name:             "explicit",
		EmbeddingModelID: "explicit-embedding",
		SummaryModelID:   "explicit-llm",
	})
	require.NoError(t, err)
	require.Equal(t, "explicit-embedding", kb.EmbeddingModelID)
	require.Equal(t, "explicit-llm", kb.SummaryModelID)
}

func TestCreateAgentUsesDeploymentModelDefaultsWhenAPIFieldsAreOmitted(t *testing.T) {
	setDeploymentModelDefaultsForTest(t)
	svc := &customAgentService{repo: &stubAgentRepoForModelDelete{}}

	agent, err := svc.CreateAgent(ctxWithTenant(1), &types.CustomAgent{
		Name: "api-created",
		Config: types.CustomAgentConfig{
			ImageUploadEnabled: true,
		},
	})
	require.NoError(t, err)
	require.Equal(t, "deployment-llm", agent.Config.ModelID)
	require.Equal(t, "deployment-rerank", agent.Config.RerankModelID)
	require.Equal(t, "deployment-vlm", agent.Config.VLMModelID)
}

func TestCreateAgentPreservesExplicitModelBindings(t *testing.T) {
	setDeploymentModelDefaultsForTest(t)
	svc := &customAgentService{repo: &stubAgentRepoForModelDelete{}}

	agent, err := svc.CreateAgent(ctxWithTenant(1), &types.CustomAgent{
		Name: "explicit",
		Config: types.CustomAgentConfig{
			ModelID:            "explicit-llm",
			RerankModelID:      "explicit-rerank",
			ImageUploadEnabled: true,
			VLMModelID:         "explicit-vlm",
		},
	})
	require.NoError(t, err)
	require.Equal(t, "explicit-llm", agent.Config.ModelID)
	require.Equal(t, "explicit-rerank", agent.Config.RerankModelID)
	require.Equal(t, "explicit-vlm", agent.Config.VLMModelID)
}

func TestUpdateAgentUsesDeploymentModelDefaultsWhenReplacementConfigOmitsModels(t *testing.T) {
	setDeploymentModelDefaultsForTest(t)
	repo := &modelDefaultsAgentRepo{agent: &types.CustomAgent{
		ID:       "agent-1",
		Name:     "before",
		TenantID: 1,
	}}
	svc := &customAgentService{repo: repo}

	agent, err := svc.UpdateAgent(ctxWithTenant(1), &types.CustomAgent{
		ID:   "agent-1",
		Name: "after",
		Config: types.CustomAgentConfig{
			ImageUploadEnabled: true,
		},
	})
	require.NoError(t, err)
	require.Equal(t, "deployment-llm", agent.Config.ModelID)
	require.Equal(t, "deployment-rerank", agent.Config.RerankModelID)
	require.Equal(t, "deployment-vlm", agent.Config.VLMModelID)
}

func TestCreateKnowledgeBaseRejectsIncompleteDeploymentModelPolicy(t *testing.T) {
	t.Setenv("WEKNORA_DEFAULT_EMBEDDING_MODEL_ID", "")
	t.Setenv("WEKNORA_DEFAULT_RERANK_MODEL_ID", "deployment-rerank")
	t.Setenv("WEKNORA_DEFAULT_LLM_MODEL_ID", "deployment-llm")
	t.Setenv("WEKNORA_DEFAULT_VLM_MODEL_ID", "deployment-vlm")
	repo := newFakeKBRepo()
	svc := newPR3KBService(repo, &fakeRegistry{registered: map[string]struct{}{}}, &fakeOwnership{})

	_, err := svc.CreateKnowledgeBase(ctxWithTenant(1), &types.KnowledgeBase{Name: "invalid"})
	require.ErrorContains(t, err, "embedding model")
	require.Empty(t, repo.rows)
}

func TestCreateAgentRejectsIncompleteDeploymentModelPolicy(t *testing.T) {
	t.Setenv("WEKNORA_DEFAULT_EMBEDDING_MODEL_ID", "deployment-embedding")
	t.Setenv("WEKNORA_DEFAULT_RERANK_MODEL_ID", "deployment-rerank")
	t.Setenv("WEKNORA_DEFAULT_LLM_MODEL_ID", "")
	t.Setenv("WEKNORA_DEFAULT_VLM_MODEL_ID", "deployment-vlm")
	svc := &customAgentService{repo: &stubAgentRepoForModelDelete{}}

	_, err := svc.CreateAgent(ctxWithTenant(1), &types.CustomAgent{Name: "invalid"})
	require.ErrorContains(t, err, "chat model")
}

func TestDeploymentModelPolicyDisabledPreservesUpstreamCreateBehavior(t *testing.T) {
	clearDeploymentModelDefaultsForTest(t)
	repo := newFakeKBRepo()
	svc := newPR3KBService(repo, &fakeRegistry{registered: map[string]struct{}{}}, &fakeOwnership{})

	kb, err := svc.CreateKnowledgeBase(ctxWithTenant(1), &types.KnowledgeBase{Name: "upstream"})
	require.NoError(t, err)
	require.Empty(t, kb.EmbeddingModelID)
	require.Empty(t, kb.SummaryModelID)
}

func TestExistingKnowledgeBaseGetsEffectiveDeploymentDefaultsOnRead(t *testing.T) {
	setDeploymentModelDefaultsForTest(t)
	repo := newFakeKBRepo()
	repo.rows["kb-1"] = &types.KnowledgeBase{ID: "kb-1", Name: "legacy", TenantID: 1}
	svc := newPR3KBService(repo, &fakeRegistry{registered: map[string]struct{}{}}, &fakeOwnership{})

	kb, err := svc.GetKnowledgeBaseByID(ctxWithTenant(1), "kb-1")
	require.NoError(t, err)
	require.Equal(t, "deployment-embedding", kb.EmbeddingModelID)
	require.Equal(t, "deployment-llm", kb.SummaryModelID)
}

func TestExistingAgentGetsEffectiveDeploymentDefaultsOnRead(t *testing.T) {
	setDeploymentModelDefaultsForTest(t)
	repo := &modelDefaultsAgentRepo{agent: &types.CustomAgent{ID: "agent-1", Name: "legacy", TenantID: 1}}
	svc := &customAgentService{repo: repo}

	agent, err := svc.GetAgentByID(ctxWithTenant(1), "agent-1")
	require.NoError(t, err)
	require.Equal(t, "deployment-llm", agent.Config.ModelID)
	require.Equal(t, "deployment-rerank", agent.Config.RerankModelID)
	require.Equal(t, "deployment-vlm", agent.Config.VLMModelID)
}

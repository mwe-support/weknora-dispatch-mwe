package service

import (
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestCreateKnowledgeBaseAppliesDeploymentParserDefaults(t *testing.T) {
	t.Setenv(types.DefaultMinerUEndpointEnv, "http://mineru-api:8000")
	repo := newFakeKBRepo()
	svc := newPR3KBService(repo, &fakeRegistry{registered: map[string]struct{}{}}, &fakeOwnership{})

	kb, err := svc.CreateKnowledgeBase(ctxWithTenant(1), &types.KnowledgeBase{Name: "kb"})
	require.NoError(t, err)

	for _, fileType := range []string{"pdf", "jpg", "jpeg", "png", "bmp", "tiff", "pptx"} {
		require.Equal(t, "mineru", kb.ChunkingConfig.ResolveParserEngine(fileType), fileType)
	}
	require.Equal(t, "markitdown", kb.ChunkingConfig.ResolveParserEngine("ppt"))
	for _, fileType := range []string{"xls", "xlsx"} {
		require.Equal(t, "builtin", kb.ChunkingConfig.ResolveParserEngine(fileType), fileType)
	}
}

func TestCreateKnowledgeBasePreservesExplicitParserRules(t *testing.T) {
	t.Setenv(types.DefaultMinerUEndpointEnv, "http://mineru-api:8000")
	repo := newFakeKBRepo()
	svc := newPR3KBService(repo, &fakeRegistry{registered: map[string]struct{}{}}, &fakeOwnership{})
	kb := &types.KnowledgeBase{
		Name: "kb",
		ChunkingConfig: types.ChunkingConfig{ParserEngineRules: []types.ParserEngineRule{{
			FileTypes: []string{"pdf"},
			Engine:    "builtin",
		}}},
	}

	created, err := svc.CreateKnowledgeBase(ctxWithTenant(1), kb)
	require.NoError(t, err)
	require.Equal(t, "builtin", created.ChunkingConfig.ResolveParserEngine("pdf"))
	require.Len(t, created.ChunkingConfig.ParserEngineRules, 1)
}

func TestCreateKnowledgeBasePreservesExplicitEmptyParserRules(t *testing.T) {
	t.Setenv(types.DefaultMinerUEndpointEnv, "http://mineru-api:8000")
	repo := newFakeKBRepo()
	svc := newPR3KBService(repo, &fakeRegistry{registered: map[string]struct{}{}}, &fakeOwnership{})
	kb := &types.KnowledgeBase{Name: "kb", ParserEngineRulesProvided: true}

	created, err := svc.CreateKnowledgeBase(ctxWithTenant(1), kb)
	require.NoError(t, err)
	require.Empty(t, created.ChunkingConfig.ParserEngineRules)
}

func TestCreateKnowledgeBaseWithoutDeploymentPolicyKeepsUpstreamDefaults(t *testing.T) {
	t.Setenv(types.DefaultMinerUEndpointEnv, "")
	repo := newFakeKBRepo()
	svc := newPR3KBService(repo, &fakeRegistry{registered: map[string]struct{}{}}, &fakeOwnership{})

	created, err := svc.CreateKnowledgeBase(ctxWithTenant(1), &types.KnowledgeBase{Name: "kb"})
	require.NoError(t, err)
	require.Empty(t, created.ChunkingConfig.ParserEngineRules)
}

package service

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/service/retriever"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestProcessingDestinationRejectsChangedPhysicalRoute(t *testing.T) {
	t.Setenv("DB_HOST", "original-index-host")
	t.Setenv("DB_NAME", "original-index-db")
	registry := retriever.NewRetrieveEngineRegistry(nil, nil)
	index := &processingPipelineIndex{}
	require.NoError(t, registry.Register(retriever.NewKVHybridRetrieveEngine(index, types.PostgresRetrieverEngineType)))
	engines := []types.RetrieverEngineParams{{RetrieverEngineType: types.PostgresRetrieverEngineType, RetrieverType: types.VectorRetrieverType}}
	ctx := context.WithValue(context.Background(), types.TenantInfoContextKey, &types.Tenant{ID: 1, RetrieverEngines: types.RetrieverEngines{Engines: engines}})
	s := &knowledgeService{retrieveEngine: registry}
	e := &processingDocumentExecution{s: s, kb: &types.KnowledgeBase{TenantID: 1, Type: types.KnowledgeBaseTypeDocument}, lease: types.ProcessingLease{Job: types.ProcessingJob{TenantID: 1}}}
	e.kb.IndexingStrategy = types.IndexingStrategy{VectorEnabled: true}
	destination, err := e.indexDestination(ctx, 2)
	require.NoError(t, err)
	_, err = processingIndexEngine(ctx, s, 1, destination)
	require.NoError(t, err)
	// An existing process keeps its original connection even when the next
	// process starts with changed settings. Compare captured engine routes.
	t.Setenv("DB_NAME", "different-index-db")
	_, err = processingIndexEngine(ctx, s, 1, destination)
	require.NoError(t, err)
	nextRegistry := retriever.NewRetrieveEngineRegistry(nil, nil)
	require.NoError(t, nextRegistry.Register(retriever.NewKVHybridRetrieveEngine(index, types.PostgresRetrieverEngineType)))
	s.retrieveEngine = nextRegistry
	_, err = processingIndexEngine(ctx, s, 1, destination)
	require.ErrorContains(t, err, "INDEX_DESTINATION_CHANGED")
	require.Empty(t, index.writes)
}

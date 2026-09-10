package service

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	graphrepo "github.com/Tencent/WeKnora/internal/application/repository/retriever/neo4j"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
	"github.com/stretchr/testify/require"
)

func TestProcessingGraphNeo4jContributionVisibilityAndExactDeletion(t *testing.T) {
	uri := os.Getenv("PROCESSING_TEST_NEO4J")
	if uri == "" {
		t.Skip("requires isolated PROCESSING_TEST_NEO4J")
	}
	require.Equal(t, "bolt://lifecycle-neo4j:7687", uri)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	driver, err := neo4j.NewDriver(uri, neo4j.NoAuth())
	require.NoError(t, err)
	defer driver.Close(ctx)
	require.NoError(t, driver.VerifyConnectivity(ctx))
	repo := graphrepo.NewNeo4jRepository(driver)
	jobID := uuid.NewString()
	ns := types.NameSpace{KnowledgeBase: "kb", Knowledge: types.ProcessingKnowledgeID(jobID)}
	first, second, hidden := ns, ns, ns
	first.ProcessingContribution, second.ProcessingContribution, hidden.ProcessingContribution = uuid.NewString(), uuid.NewString(), uuid.NewString()
	defer repo.DelGraph(context.Background(), []types.NameSpace{first, second, hidden, ns})
	data := func(chunk string) []*types.GraphData {
		return []*types.GraphData{{Node: []*types.GraphNode{{Name: "Lifecycle", Chunks: []string{chunk}, Attributes: []string{"shared", chunk}}, {Name: "Receipt", Chunks: []string{chunk}}}, Relation: []*types.GraphRelation{{Node1: "Lifecycle", Node2: "Receipt", Type: "records"}}}}
	}
	require.NoError(t, repo.AddGraph(ctx, first, data("first-chunk")))
	require.NoError(t, repo.AddGraph(ctx, second, data("second-chunk")))
	require.NoError(t, repo.AddGraph(ctx, hidden, data("unconfirmed-chunk")))
	require.NoError(t, repo.AddGraph(ctx, ns, data("legacy-chunk")))
	search := ns
	search.ConfirmedContributions = []string{first.ProcessingContribution, second.ProcessingContribution}
	search.VisibleLegacyKnowledge = []string{ns.Knowledge}
	graph, err := repo.SearchNode(ctx, search, []string{"Lifecycle"})
	require.NoError(t, err)
	require.Len(t, graph.Node, 2)
	for _, node := range graph.Node {
		require.ElementsMatch(t, []string{"first-chunk", "second-chunk", "legacy-chunk"}, node.Chunks)
	}
	require.NoError(t, repo.AddGraph(ctx, first, []*types.GraphData{{Node: []*types.GraphNode{{Name: "Isolated", Chunks: []string{"first-chunk"}}}}}))
	single, err := repo.SearchNode(ctx, search, []string{"Isolated"})
	require.NoError(t, err)
	require.Len(t, single.Node, 1, "an entity without a relationship is still a searchable contribution")
	require.NoError(t, repo.DelGraph(ctx, []types.NameSpace{first}))
	require.NoError(t, repo.DelGraph(ctx, []types.NameSpace{first}))
	graph, err = repo.SearchNode(ctx, search, []string{"Lifecycle"})
	require.NoError(t, err)
	for _, node := range graph.Node {
		require.ElementsMatch(t, []string{"second-chunk", "legacy-chunk"}, node.Chunks)
	}
	search.ConfirmedContributions, search.VisibleLegacyKnowledge = nil, nil
	graph, err = repo.SearchNode(ctx, search, []string{"Lifecycle"})
	require.NoError(t, err)
	require.Empty(t, graph.Node, "an empty publication set must fail closed")

	// A provider may finish an old write after retirement's successful delete.
	// Exercise the real maintenance entry point against that late contribution.
	db := processingServiceTestDatabase(t)
	old := time.Now().Add(-8 * 24 * time.Hour)
	require.NoError(t, db.Create(&types.ProcessingJob{ID: jobID, Kind: types.ProcessingJobDocument, TenantID: 1,
		KnowledgeBaseID: ns.KnowledgeBase, DataSourceID: "source", ExternalID: jobID, Generation: 1,
		KnowledgeID: ns.Knowledge, RetirementState: "deleted", CreatedAt: old, UpdatedAt: old}).Error)
	write := types.ProcessingGraphWrite{ID: first.ProcessingContribution, TenantID: 1, JobID: jobID,
		KnowledgeBaseID: ns.KnowledgeBase, KnowledgeID: ns.Knowledge, StepID: uuid.NewString(), Attempt: 1,
		DestinationDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", State: "deleted"}
	require.NoError(t, db.Create(&write).Error)
	require.NoError(t, repo.AddGraph(ctx, first, data("late-after-delete")))
	s := &knowledgeService{graphEngine: &ProcessingGraphRepository{RetrieveGraphRepository: repo, destination: write.DestinationDigest}}
	ledger := repository.NewProcessingRepository(db)
	_, err = s.collectRetiredIndexGarbage(ctx, ledger, "", 20)
	require.NoError(t, err)
	search.ConfirmedContributions = []string{first.ProcessingContribution}
	graph, err = repo.SearchNode(ctx, search, []string{"Lifecycle"})
	require.NoError(t, err)
	require.Empty(t, graph.Node, "retired graph-only jobs must physically remove late writes")
	search.ConfirmedContributions = []string{second.ProcessingContribution}
	graph, err = repo.SearchNode(ctx, search, []string{"Lifecycle"})
	require.NoError(t, err)
	require.NotEmpty(t, graph.Node, "exact cleanup preserves a different contribution")
	require.NoError(t, db.Model(&types.ProcessingJob{}).Where("id = ?", jobID).Update("updated_at", old).Error)
	s.graphEngine.(*ProcessingGraphRepository).destination = "changed"
	_, err = s.collectRetiredIndexGarbage(ctx, ledger, "", 20)
	require.ErrorContains(t, err, "GRAPH_DESTINATION_CHANGED")
}

package repository

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestProcessingPublicationKeepsOldVersionAndRejectsUnconfirmedIndexes(t *testing.T) {
	r := processingTestStore(t)
	require.NoError(t, r.db.AutoMigrate(&types.Knowledge{}))
	require.NoError(t, r.db.Create(&types.Knowledge{ID: "legacy", TenantID: 1, KnowledgeBaseID: "kb", CustomMetadata: types.JSON(`{}`)}).Error)
	ctx := context.Background()
	scope := []types.KnowledgeSearchScope{{TenantID: 1, KBID: "kb"}}
	newJob := func(revision string) *types.ProcessingJob {
		t.Helper()
		job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", SourceRevision: revision, PipelineFingerprint: "p1"})
		require.NoError(t, err)
		require.NoError(t, r.PlanSteps(ctx, 1, job.ID, []types.ProcessingStepSpec{
			{Stage: "index", UnitKey: "body", Phase: types.ProcessingPhasePrepare, InputFingerprint: "index", RequiredForReady: true, RequiredForCompletion: true},
			{Stage: "publish", UnitKey: "version", Phase: types.ProcessingPhasePublish, InputFingerprint: "publish", DependsOn: []string{"index/body"}, RequiredForCompletion: true},
			{Stage: "summary", UnitKey: "body", Phase: types.ProcessingPhaseProjection, InputFingerprint: "summary", DependsOn: []string{"publish/version"}, RequiredForCompletion: true},
		}))
		return job
	}
	claim := func(job *types.ProcessingJob, stage string) *types.ProcessingLease {
		t.Helper()
		steps, err := r.ListSteps(ctx, 1, job.ID)
		require.NoError(t, err)
		for _, step := range steps {
			if step.Stage == stage {
				lease, err := r.ClaimStep(ctx, 1, processingStepRef(job, &step), time.Minute)
				require.NoError(t, err)
				return lease
			}
		}
		t.Fatal("planned stage missing")
		return nil
	}
	ready := func(job *types.ProcessingJob) *types.ProcessingLease {
		lease := claim(job, "index")
		require.NoError(t, r.FinishStep(ctx, 1, *lease, types.ProcessingOutcome{Status: types.ProcessingSucceeded,
			OutputManifestRef: "synthetic/index", OutputDigest: "verified", Completeness: "complete", Candidate: &types.Knowledge{Title: "synthetic"}}))
		return lease
	}
	publish := func(job *types.ProcessingJob) {
		lease := claim(job, "publish")
		outcome := types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "synthetic/publication", OutputDigest: "verified"}
		require.NoError(t, r.FinishStep(ctx, 1, *lease, outcome))
		require.NoError(t, r.FinishStep(ctx, 1, *lease, outcome)) // Lost ACK.
	}
	first := newJob("v1")
	firstIndex := ready(first)
	first, _ = r.GetJob(ctx, 1, first.ID)
	sourceID, err := types.ProcessingIndexSourceID(firstIndex.Step.ID, firstIndex.Ref.Attempt, uuid.NewString())
	require.NoError(t, err)
	orphan, err := types.ProcessingIndexSourceID(firstIndex.Step.ID, 99, uuid.NewString())
	require.NoError(t, err)
	hits := []*types.IndexWithScore{{KnowledgeID: "legacy", KnowledgeBaseID: "kb", SourceID: "legacy-index"},
		{KnowledgeID: first.KnowledgeID, KnowledgeBaseID: "kb", SourceID: sourceID}, {KnowledgeID: first.KnowledgeID, KnowledgeBaseID: "kb", SourceID: orphan}}
	filtered, err := r.FilterIndexes(ctx, scope, hits)
	require.NoError(t, err)
	require.Len(t, filtered, 1)
	publish(first)
	first, _ = r.GetJob(ctx, 1, first.ID)
	require.True(t, first.IsPublished)
	require.EqualValues(t, 1, first.PublicationEpoch)
	require.NotEqual(t, types.ProcessingSucceeded, first.Status) // Summary is still outstanding.
	lateSummary := claim(first, "summary")
	filtered, err = r.FilterIndexes(ctx, scope, hits)
	require.NoError(t, err)
	require.Len(t, filtered, 2)
	second := newJob("v2")
	filtered, err = r.FilterIndexes(ctx, scope, hits)
	require.NoError(t, err)
	require.Len(t, filtered, 2) // A new desired generation does not hide the published one.
	ready(second)
	publish(second)
	second, _ = r.GetJob(ctx, 1, second.ID)
	require.EqualValues(t, 2, second.PublicationEpoch)
	require.ErrorIs(t, r.FinishStep(ctx, 1, *lateSummary, types.ProcessingOutcome{Status: types.ProcessingSucceeded,
		OutputManifestRef: "late", OutputDigest: "late"}), ErrProcessingConflict)
	stopped, err := r.GetStep(ctx, 1, first.ID, lateSummary.Step.ID)
	require.NoError(t, err)
	require.Equal(t, types.ProcessingSuperseded, stopped.Status)
	require.Empty(t, stopped.LeaseToken)
	filtered, err = r.FilterIndexes(ctx, scope, hits)
	require.NoError(t, err)
	require.Len(t, filtered, 1)
	var manifest map[string]types.ProcessingArtifactVersion
	require.NoError(t, json.Unmarshal([]byte(second.ActiveIndexManifest), &manifest))
	require.NotEmpty(t, manifest)
	otherScope, err := r.FilterIndexes(ctx, []types.KnowledgeSearchScope{{TenantID: 2, KBID: "kb"}}, hits)
	require.NoError(t, err)
	require.Empty(t, otherScope)
	first, _ = r.GetJob(ctx, 1, first.ID)
	require.NoError(t, r.SetRollbackPin(ctx, 1, first.ID, first.Revision, true, "pin-v1", "operator"))
	first, _ = r.GetJob(ctx, 1, first.ID)
	require.NoError(t, r.RollbackVersion(ctx, 1, first.ID, first.Revision, first.ActiveIndexManifest, "rollback-v1", "operator"))
	require.NoError(t, r.RollbackVersion(ctx, 1, first.ID, first.Revision, first.ActiveIndexManifest, "rollback-v1", "operator"))
	first, _ = r.GetJob(ctx, 1, first.ID)
	require.True(t, first.IsPublished)
	require.True(t, first.IsCurrent)
	require.True(t, first.RollbackPin)
	require.EqualValues(t, 3, first.PublicationEpoch)
	filtered, err = r.FilterIndexes(ctx, scope, hits)
	require.NoError(t, err)
	require.Len(t, filtered, 2)
	require.ErrorIs(t, r.Heartbeat(ctx, 1, *lateSummary, time.Minute, "old-epoch"), ErrProcessingConflict)
	freshSummary := claim(first, "summary")
	require.Greater(t, freshSummary.Ref.Attempt, lateSummary.Ref.Attempt)
	require.EqualValues(t, 3, freshSummary.Step.ExpectedPublicationEpoch)
	require.NoError(t, r.FinishStep(ctx, 1, *freshSummary, types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: "new-summary", OutputDigest: "verified-summary"}))
	second, _ = r.GetJob(ctx, 1, second.ID)
	require.False(t, second.IsPublished)
	require.False(t, second.IsCurrent)
}

func TestProcessingFAQManifestRejectsUnconfirmedAndStaleIndexes(t *testing.T) {
	r := processingTestStore(t)
	require.NoError(t, r.db.AutoMigrate(&types.Knowledge{}, &types.Chunk{}))
	require.NoError(t, r.db.Create(&types.Knowledge{ID: "faq", TenantID: 1, KnowledgeBaseID: "kb", Type: types.KnowledgeTypeFAQ, EnableStatus: "enabled"}).Error)
	c := &types.Chunk{ID: uuid.NewString(), TenantID: 1, KnowledgeBaseID: "kb", KnowledgeID: "faq", ChunkType: types.ChunkTypeFAQ, IsEnabled: true, Status: int(types.ChunkStatusIndexed)}
	require.NoError(t, c.SetFAQMetadata(&types.FAQChunkMetadata{StandardQuestion: "Question", Answers: []string{"old answer"}}))
	ctx := context.Background()
	repo := NewChunkRepository(r.db)
	require.NoError(t, repo.CreateChunks(ctx, []*types.Chunk{c}))
	indexes := []*types.IndexInfo{{SourceID: c.ID, ChunkID: c.ID, KnowledgeID: "faq", KnowledgeBaseID: "kb"}}
	manifest, err := types.VersionFAQIndexes("attempt-one", c, indexes)
	require.NoError(t, err)
	scope := []types.KnowledgeSearchScope{{TenantID: 1, KBID: "kb"}}
	legacy := &types.IndexWithScore{SourceID: c.ID, ChunkID: c.ID, KnowledgeID: "faq", KnowledgeBaseID: "kb"}
	staged := &types.IndexWithScore{SourceID: indexes[0].SourceID, ChunkID: c.ID, KnowledgeID: "faq", KnowledgeBaseID: "kb"}
	hits, err := r.FilterIndexes(ctx, scope, []*types.IndexWithScore{legacy, staged})
	require.NoError(t, err)
	require.Equal(t, []*types.IndexWithScore{legacy}, hits)
	c.FAQIndexManifest = manifest
	confirmFAQTestManifest(t, repo, c)
	require.NoError(t, repo.UpdateChunk(ctx, c))
	hits, err = r.FilterIndexes(ctx, scope, []*types.IndexWithScore{legacy, staged})
	require.NoError(t, err)
	require.Equal(t, []*types.IndexWithScore{staged}, hits)
	old := *c
	require.NoError(t, c.SetFAQMetadata(&types.FAQChunkMetadata{StandardQuestion: "Question", Answers: []string{"new answer"}}))
	require.NoError(t, repo.UpdateChunk(ctx, c))
	hits, err = r.FilterIndexes(ctx, scope, []*types.IndexWithScore{legacy, staged})
	require.NoError(t, err)
	require.Empty(t, hits, "old ranking cannot serve an unconfirmed new answer")
	require.ErrorIs(t, repo.UpdateChunk(ctx, &old), ErrChunkRevisionConflict)
	indexes[0].SourceID = c.ID
	c.FAQIndexManifest, err = types.VersionFAQIndexes("attempt-two", c, indexes)
	require.NoError(t, err)
	confirmFAQTestManifest(t, repo, c)
	require.NoError(t, repo.UpdateChunk(ctx, c))
	current := &types.IndexWithScore{SourceID: indexes[0].SourceID, ChunkID: c.ID, KnowledgeID: "faq", KnowledgeBaseID: "kb"}
	hits, err = r.FilterIndexes(ctx, scope, []*types.IndexWithScore{legacy, staged, current})
	require.NoError(t, err)
	require.Equal(t, []*types.IndexWithScore{current}, hits, "canonical FAQ visibility does not depend on source-job retention")
	require.NoError(t, r.db.Model(&types.Knowledge{}).Where("id = ?", "faq").Update("enable_status", "disabled").Error)
	hits, err = r.FilterIndexes(ctx, scope, []*types.IndexWithScore{current})
	require.NoError(t, err)
	require.Empty(t, hits, "disabled canonical containers must remain invisible")
	require.NoError(t, r.db.Model(&types.Knowledge{}).Where("id = ?", "faq").Update("enable_status", "enabled").Error)
	foreign := *current
	foreign.ChunkID = uuid.NewString()
	hits, err = r.FilterIndexes(ctx, scope, []*types.IndexWithScore{&foreign})
	require.NoError(t, err)
	require.Empty(t, hits)
	require.NoError(t, repo.DeleteChunk(ctx, 1, c.ID))
	hits, err = r.FilterIndexes(ctx, scope, []*types.IndexWithScore{current})
	require.NoError(t, err)
	require.Empty(t, hits)
}

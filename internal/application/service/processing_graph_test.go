package service

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type processingTestGraph struct {
	writes      map[string]types.NameSpace
	calls       int
	afterWrite  func(context.Context, types.NameSpace)
	afterSearch func()
}

func (g *processingTestGraph) AddGraph(ctx context.Context, ns types.NameSpace, graphs []*types.GraphData) error {
	g.calls++
	g.writes[ns.GraphID()] = ns
	if g.afterWrite != nil {
		g.afterWrite(ctx, ns)
	}
	if g.calls == 1 {
		return context.DeadlineExceeded
	}
	return nil
}

func (g *processingTestGraph) DelGraph(_ context.Context, namespaces []types.NameSpace) error {
	for _, ns := range namespaces {
		delete(g.writes, ns.GraphID())
	}
	return nil
}

func (g *processingTestGraph) SearchNode(_ context.Context, ns types.NameSpace, _ []string) (*types.GraphData, error) {
	result := &types.GraphData{}
	for id, write := range g.writes {
		if write.KnowledgeBase != ns.KnowledgeBase || (ns.Knowledge != "" && ns.Knowledge != write.Knowledge) {
			continue
		}
		if slices.Contains(ns.ConfirmedContributions, id) || (write.ProcessingContribution == "" && slices.Contains(ns.VisibleLegacyKnowledge, id)) {
			result.Node = append(result.Node, &types.GraphNode{Name: id})
		}
	}
	if g.afterSearch != nil {
		g.afterSearch()
	}
	return result, nil
}

func verifyProcessingGraphLifecycle(t *testing.T, ctx context.Context, db *gorm.DB, r *repository.ProcessingRepository, s *knowledgeService, execute ProcessingExecutor, job *types.ProcessingJob, graph *processingTestGraph, model *processingPipelineChat) {
	t.Helper()
	search := func() *types.GraphData {
		t.Helper()
		data, err := s.graphEngine.SearchNode(ctx, types.NameSpace{KnowledgeBase: "kb"}, []string{"Lifecycle"})
		require.NoError(t, err)
		return data
	}
	require.Positive(t, model.graphCalls)
	units := model.graphCalls
	require.Equal(t, units+1, graph.calls, "only the lost-ACK graph apply retries")
	require.Len(t, search().Node, units, "lost-ACK orphan contributions stay hidden")
	var writes []types.ProcessingGraphWrite
	require.NoError(t, db.Where("job_id = ?", job.ID).Find(&writes).Error)
	require.Len(t, writes, units+1)
	// Editing one source chunk retracts that exact contribution immediately.
	for _, write := range writes {
		if write.State != "confirmed" {
			continue
		}
		require.NoError(t, db.Model(&types.Chunk{}).Where("id = ?", write.ChunkID).Update("content_revision", write.ContentRevision+1).Error)
		require.Len(t, search().Node, units-1)
		require.NoError(t, db.Model(&types.Chunk{}).Where("id = ?", write.ChunkID).Update("content_revision", write.ContentRevision).Error)
		require.NoError(t, db.Model(&types.Chunk{}).Where("id = ?", write.ChunkID).Update("is_enabled", false).Error)
		require.Len(t, search().Node, units-1, "disabled chunks must not contribute to graph answers")
		require.NoError(t, db.Model(&types.Chunk{}).Where("id = ?", write.ChunkID).Update("is_enabled", true).Error)
		break
	}
	require.NoError(t, db.AutoMigrate(&types.SyncRunItem{}, &types.SyncLog{}))
	// An unrelated legacy source sharing the same graph remains visible.
	require.NoError(t, db.Create(&types.Knowledge{ID: "legacy-other", TenantID: 1, KnowledgeBaseID: "kb", EnableStatus: "enabled", Title: "synthetic"}).Error)
	graph.writes["legacy-other"] = types.NameSpace{KnowledgeBase: "kb", Knowledge: "legacy-other"}
	graph.afterSearch = func() {
		graph.afterSearch = nil
		processingControlCandidate(t, db, "file", true, "replacement")
	}
	_, err := s.graphEngine.SearchNode(ctx, types.NameSpace{KnowledgeBase: "kb"}, []string{"Lifecycle"})
	require.ErrorIs(t, err, repository.ErrProcessingConflict, "a publication change during graph I/O must not leak the old graph")
	require.Len(t, search().Node, 1)
	job, err = r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	require.NoError(t, RollbackProcessingVersion(ctx, s, r, 1, job.ID, job.Revision, "graph-rollback", "operator"))
	require.Len(t, search().Node, 1, "rollback must reapply graph contributions under its new publication epoch")
	run := func(done func(*types.ProcessingJob) bool) {
		t.Helper()
		for round := 0; round < 15; round++ {
			current, err := r.GetJob(ctx, 1, job.ID)
			require.NoError(t, err)
			if done(current) {
				return
			}
			require.NoError(t, db.Model(&types.ProcessingStep{}).Where("job_id = ? AND next_run_at IS NOT NULL", job.ID).Update("next_run_at", time.Now().Add(-time.Minute)).Error)
			require.NoError(t, r.ReconcileJob(ctx, 1, job.ID))
			steps, err := r.ListSteps(ctx, 1, job.ID)
			require.NoError(t, err)
			for _, step := range steps {
				if step.Status != types.ProcessingQueued && step.Status != types.ProcessingEnqueuePending {
					continue
				}
				lease, err := r.ClaimStep(ctx, 1, types.ProcessingRef{Protocol: 2, JobID: job.ID, Generation: job.Generation, StepID: step.ID, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq, InputFingerprint: step.InputFingerprint}, time.Minute)
				require.NoError(t, err)
				outcome, err := execute(ctx, *lease)
				require.NoError(t, err)
				require.NotEqual(t, types.ProcessingBlocked, outcome.Status, "%s: %+v", step.Stage, outcome)
				require.NoError(t, r.FinishStep(ctx, 1, *lease, outcome))
			}
		}
		t.Fatal("graph lifecycle stalled")
	}
	run(func(job *types.ProcessingJob) bool { return job.Status == types.ProcessingSucceeded })
	require.Equal(t, units, model.graphCalls, "rollback reuses confirmed extraction outputs")
	require.Len(t, search().Node, units+1)
	processingControlCandidate(t, db, "file", true, "replacement-after-rollback")
	job, err = r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	require.NoError(t, r.SetRollbackPin(ctx, 1, job.ID, job.Revision, false, "graph-unpin", "operator"))
	job, err = r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	require.NoError(t, r.PlanRetirement(ctx, 1, job.ID, job.Revision, "graph-retire", "operator"))
	run(func(job *types.ProcessingJob) bool { return job.RetirementState == "deleted" })
	require.Len(t, graph.writes, 1)
	require.Contains(t, graph.writes, "legacy-other", "retirement cannot delete another source's graph")
	require.Len(t, search().Node, 1)
}

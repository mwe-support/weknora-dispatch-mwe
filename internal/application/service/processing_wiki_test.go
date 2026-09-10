package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/models/chat"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func (m *processingPipelineChat) wikiResponse(purpose string, messages []chat.Message) (*types.ChatResponse, error) {
	if m.wikiCalls == nil {
		m.wikiCalls = map[string]int{}
	}
	m.wikiCalls[purpose]++
	content := ""
	switch purpose {
	case "wiki_candidate_slug":
		content = `{"entities":[{"name":"Lifecycle","slug":"entity/lifecycle","description":"Durable stages"}],"concepts":[]}`
	case "wiki_deduplication":
		content = `{"merges":{}}`
	case "wiki_chunk_citation":
		content = `{"citations":{"entity/lifecycle":["c000"]},"new_slugs":[]}`
	case "wiki_summary":
		if m.wikiCalls[purpose] == 1 {
			return nil, context.DeadlineExceeded
		}
		content = "SUMMARY: Lifecycle summary\nDocument Lifecycle [[entity/missing]] WIKI-SUMMARY-7391."
	case "wiki_taxonomy_plan":
		if m.wikiCalls[purpose] == 1 {
			content = `{"assignments":[]}`
		} else {
			content = `{"assignments":[{"slug":"entity/lifecycle","path":["Engineering","Lifecycle"]}]}`
		}
	case "wiki_index_intro":
		if m.wikiCalls[purpose] == 1 {
			return nil, context.DeadlineExceeded
		}
		content = "# Lifecycle\nWIKI-INDEX-7391"
		for _, message := range messages {
			if strings.Contains(message.Content, "MANUAL-INDEX-7391") {
				content += " MANUAL-INDEX-7391"
				break
			}
		}
	case "wiki_page_modify":
		if m.wikiPageHook != nil {
			m.wikiPageHook()
			m.wikiPageHook = nil
		}
		content = "SUMMARY: Lifecycle\nWIKI-PAGE-7391."
		for _, message := range messages {
			if strings.Contains(message.Content, "MANUAL-CAS-7391") {
				content += " MANUAL-CAS-7391."
			}
		}
	default:
		return nil, context.Canceled
	}
	return &types.ChatResponse{Content: content}, nil
}

func setupProcessingWiki(t *testing.T, db *gorm.DB, s *knowledgeService, model *processingPipelineChat, untracked bool) {
	t.Helper()
	if db.Dialector.Name() == "postgres" {
		require.NoError(t, db.Exec("CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public").Error)
		var schema string
		require.NoError(t, db.Raw("SELECT current_schema()").Scan(&schema).Error)
		require.True(t, strings.HasPrefix(schema, "processing_service_"))
		require.NoError(t, db.Exec("SET search_path TO "+schema+", public").Error)
	}
	require.NoError(t, db.AutoMigrate(&types.WikiPage{}, &types.WikiPageRevision{}, &types.WikiFolder{}))
	s.wikiRepo = repository.NewWikiPageRepository(db)
	s.wikiService = NewWikiPageService(s.wikiRepo, s.chunkRepo, s.kbService, nil, nil)
	if untracked {
		_, err := s.wikiService.CreatePage(context.Background(), &types.WikiPage{TenantID: 1, KnowledgeBaseID: "kb", Slug: "index", Title: "Manual index", PageType: types.WikiPageTypeIndex, Content: "MANUAL-INDEX-7391"})
		require.NoError(t, err)
		for i := 0; i < 62; i++ {
			_, _, err := s.wikiService.FindOrCreateFolderPath(context.Background(), "kb", 1, []string{"Existing", fmt.Sprintf("Topic-%02d", i)})
			require.NoError(t, err)
		}
	}
	refs := types.StringArray{"legacy-other"}
	if untracked {
		refs = nil
	}
	_, err := s.wikiService.CreatePage(context.Background(), &types.WikiPage{TenantID: 1, KnowledgeBaseID: "kb", Slug: "entity/lifecycle",
		Title: "Lifecycle", Content: "Other contributor", PageType: types.WikiPageTypeEntity, SourceRefs: refs})
	require.NoError(t, err)
	model.wikiPageHook = func() {
		page, err := s.wikiRepo.GetBySlug(context.Background(), "kb", "entity/lifecycle")
		require.NoError(t, err)
		page.Content = "MANUAL-CAS-7391"
		_, err = s.wikiService.UpdatePage(types.WithWikiEditSource(context.Background(), types.WikiEditSourceUser), page)
		require.NoError(t, err)
		if untracked {
			folder, _, err := s.wikiService.FindOrCreateFolderPath(context.Background(), "kb", 1, []string{"Manual"})
			require.NoError(t, err)
			_, err = s.wikiService.MovePage(context.Background(), "kb", page.Slug, folder)
			require.NoError(t, err)
			_, err = s.wikiService.MovePage(context.Background(), "kb", page.Slug, "")
			require.NoError(t, err)
		}
	}
}

func verifyProcessingWikiLifecycle(t *testing.T, ctx context.Context, db *gorm.DB, r *repository.ProcessingRepository, s *knowledgeService, execute ProcessingExecutor, job *types.ProcessingJob, model *processingPipelineChat, untracked bool) {
	t.Helper()
	page, err := s.wikiRepo.GetBySlug(ctx, "kb", "entity/lifecycle")
	require.NoError(t, err)
	require.Contains(t, page.Content, "WIKI-PAGE-7391")
	require.Contains(t, page.Content, "MANUAL-CAS-7391")
	refs := types.StringArray{"legacy-other"}
	if untracked {
		refs = nil
	}
	require.ElementsMatch(t, append(append(types.StringArray{}, refs...), job.KnowledgeID), page.SourceRefs)
	require.Equal(t, 1, model.wikiCalls["wiki_candidate_slug"], "page conflicts must not re-extract the document")
	require.Equal(t, 1, model.wikiCalls["wiki_chunk_citation"])
	require.Equal(t, 2, model.wikiCalls["wiki_summary"], "the failed summary alone retries")
	require.Equal(t, 2, model.wikiCalls["wiki_page_modify"], "only the conflicting page remerges")
	var writes []types.ProcessingWikiWrite
	require.NoError(t, db.Where("job_id = ? AND state = ?", job.ID, "active").Find(&writes).Error)
	require.Len(t, writes, 3)
	if untracked {
		require.Empty(t, page.FolderID, "an explicitly chosen root folder remains at root")
		require.Empty(t, page.CategoryPath)
	} else {
		require.NotEmpty(t, page.FolderID)
		require.Equal(t, types.StringArray{"Engineering", "Lifecycle"}, page.CategoryPath)
	}
	require.Equal(t, 2, model.wikiCalls["wiki_taxonomy_plan"], "only the malformed taxonomy call retries")
	require.Equal(t, 2, model.wikiCalls["wiki_index_intro"], "only the failed index call retries")
	var vectorSteps []types.ProcessingStep
	require.NoError(t, db.Where("job_id = ? AND stage = ?", job.ID, "wiki_taxonomy_vectors").Find(&vectorSteps).Error)
	if untracked {
		require.Len(t, vectorSteps, 2)
		for _, step := range vectorSteps {
			require.Equal(t, types.ProcessingSucceeded, step.Status)
			require.Equal(t, 1, step.Attempt)
		}
	} else {
		require.Empty(t, vectorSteps)
	}
	initialSummary, err := s.wikiRepo.GetBySlug(ctx, "kb", "summary/"+slugify(job.KnowledgeID))
	require.NoError(t, err)
	require.Contains(t, initialSummary.OutLinks, "entity/lifecycle")
	require.NotContains(t, initialSummary.Content, "[[entity/missing]]")
	require.Contains(t, initialSummary.Content, "CONCURRENT-LINKS-7391")
	require.Equal(t, 1, initialSummary.Version, "automatic link maintenance must not bump the visible version")
	index, err := s.wikiRepo.GetBySlug(ctx, "kb", "index")
	require.NoError(t, err)
	require.Contains(t, index.Content, "WIKI-INDEX-7391")
	var errorsCount int64
	require.NoError(t, db.Model(&types.ProcessingEvent{}).Where("job_id = ? AND error_code = ?", job.ID, "WIKI_PAGE_CHANGED").Count(&errorsCount).Error)
	require.EqualValues(t, 2, errorsCount)
	require.NoError(t, db.AutoMigrate(&types.SyncRunItem{}, &types.SyncLog{}))
	processingControlCandidate(t, db, "file", true, "wiki-replacement")
	job, err = r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	require.NoError(t, RollbackProcessingVersion(ctx, s, r, 1, job.ID, job.Revision, "wiki-rollback", "operator"))
	lostCommit := false
	drain := func(retiring bool) {
		t.Helper()
		for round := 0; round < 20; round++ {
			current, err := r.GetJob(ctx, 1, job.ID)
			require.NoError(t, err)
			if (retiring && current.RetirementState == "deleted") || (!retiring && current.Status == types.ProcessingSucceeded) {
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
				if !retiring && step.Stage == "wiki_page" && !lostCommit {
					lostCommit = true
					before, err := s.wikiRepo.GetByID(ctx, outcome.WikiPage.Page.ID)
					require.NoError(t, err)
					require.NoError(t, db.Callback().Create().Before("gorm:create").Register("wiki_commit_failure", func(tx *gorm.DB) {
						if tx.Statement.Table == "processing_events" {
							tx.AddError(errors.New("synthetic event failure"))
						}
					}))
					require.Error(t, r.FinishStep(ctx, 1, *lease, outcome))
					require.NoError(t, db.Callback().Create().Remove("wiki_commit_failure"))
					after, err := s.wikiRepo.GetByID(ctx, before.ID)
					require.NoError(t, err)
					require.Equal(t, before.MutationRevision, after.MutationRevision, "failed ledger commit rolls back the page")
					require.NoError(t, db.Where("id = ?", step.ID).Take(&lease.Step).Error)
					calls := model.wikiCalls["wiki_page_modify"]
					outcome, err = execute(ctx, *lease)
					require.NoError(t, err)
					require.Equal(t, calls, model.wikiCalls["wiki_page_modify"], "checkpoint avoids a second LLM call after the DB failure")
				}
				require.NoError(t, r.FinishStep(ctx, 1, *lease, outcome))
				require.NoError(t, r.FinishStep(ctx, 1, *lease, outcome))
			}
		}
		t.Fatal("wiki lifecycle stalled")
	}
	drain(false)
	require.True(t, lostCommit)
	require.Equal(t, 1, model.wikiCalls["wiki_candidate_slug"])
	require.Equal(t, 1, model.wikiCalls["wiki_chunk_citation"])
	require.Equal(t, 2, model.wikiCalls["wiki_summary"])
	require.Equal(t, 2, model.wikiCalls["wiki_taxonomy_plan"], "rollback reuses taxonomy results")
	processingControlCandidate(t, db, "file", true, "wiki-retirement-replacement")
	job, err = r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	require.NoError(t, r.SetRollbackPin(ctx, 1, job.ID, job.Revision, false, "wiki-unpin", "operator"))
	job, err = r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	require.NoError(t, r.PlanRetirement(ctx, 1, job.ID, job.Revision, "wiki-retire", "operator"))
	// A rollback fences a retraction already generated under the old epoch.
	steps, err := r.ListSteps(ctx, 1, job.ID)
	require.NoError(t, err)
	var late *types.ProcessingLease
	for _, step := range steps {
		var input map[string]string
		if step.Stage != "wiki_retire_page" || json.Unmarshal(step.Input, &input) != nil || input["page_id"] != page.ID {
			continue
		}
		late, err = r.ClaimStep(ctx, 1, types.ProcessingRef{Protocol: 2, JobID: job.ID, Generation: job.Generation, StepID: step.ID, Attempt: step.Attempt, DispatchSeq: step.DispatchSeq, InputFingerprint: step.InputFingerprint}, time.Minute)
		require.NoError(t, err)
		break
	}
	require.NotNil(t, late)
	lateOutcome, err := execute(ctx, *late)
	require.NoError(t, err)
	require.Equal(t, types.ProcessingSucceeded, lateOutcome.Status)
	job, err = r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	require.Equal(t, "retained", job.RetirementState)
	require.NoError(t, RollbackProcessingVersion(ctx, s, r, 1, job.ID, job.Revision, "wiki-late-rollback", "operator"))
	require.ErrorIs(t, r.FinishStep(ctx, 1, *late, lateOutcome), repository.ErrProcessingConflict)
	page, err = s.wikiRepo.GetByID(ctx, page.ID)
	require.NoError(t, err)
	require.Contains(t, page.SourceRefs, job.KnowledgeID)
	drain(false)
	processingControlCandidate(t, db, "file", true, "wiki-final-replacement")
	job, err = r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	require.NoError(t, r.SetRollbackPin(ctx, 1, job.ID, job.Revision, false, "wiki-final-unpin", "operator"))
	job, err = r.GetJob(ctx, 1, job.ID)
	require.NoError(t, err)
	require.NoError(t, r.PlanRetirement(ctx, 1, job.ID, job.Revision, "wiki-final-retire", "operator"))
	if untracked {
		old, err := s.wikiRepo.GetBySlug(ctx, "kb", "summary/"+slugify(job.KnowledgeID))
		require.NoError(t, err)
		// Simulate removal by an independent owner followed by same-slug creation.
		require.NoError(t, db.Unscoped().Where("id = ?", old.ID).Delete(&types.WikiPage{}).Error)
		_, err = s.wikiService.CreatePage(types.WithWikiEditSource(ctx, types.WikiEditSourceUser), &types.WikiPage{TenantID: 1, KnowledgeBaseID: "kb", Slug: old.Slug,
			Title: "Independent replacement", PageType: types.WikiPageTypeSummary, Content: "INDEPENDENT-PAGE-7391"})
		require.NoError(t, err)
	}
	drain(true)
	page, err = s.wikiRepo.GetBySlug(ctx, "kb", "entity/lifecycle")
	require.NoError(t, err)
	require.ElementsMatch(t, refs, page.SourceRefs)
	require.NotEqual(t, types.WikiPageStatusArchived, page.Status)
	require.Contains(t, page.Content, "MANUAL-CAS-7391")
	var active int64
	require.NoError(t, db.Model(&types.ProcessingWikiWrite{}).Where("job_id = ? AND state = ?", job.ID, "active").Count(&active).Error)
	require.Zero(t, active)
	summary, err := s.wikiRepo.GetBySlug(ctx, "kb", "summary/"+slugify(job.KnowledgeID))
	require.NoError(t, err)
	if untracked {
		require.Equal(t, "INDEPENDENT-PAGE-7391", summary.Content)
		require.NotEqual(t, types.WikiPageStatusArchived, summary.Status)
	} else {
		require.Equal(t, types.WikiPageStatusArchived, summary.Status)
	}
	index, err = s.wikiRepo.GetBySlug(ctx, "kb", "index")
	require.NoError(t, err)
	if untracked {
		require.Contains(t, index.Content, "MANUAL-INDEX-7391")
		require.NotEqual(t, types.WikiPageStatusArchived, index.Status)
	} else {
		require.Equal(t, types.WikiPageStatusArchived, index.Status)
	}
}

func TestProcessingWikiTaxonomyRequiresEveryKnownSlug(t *testing.T) {
	items := []wikiTaxonomyItem{{slug: "entity/a"}}
	for _, raw := range []string{`{"assignments":[]}`, `{"assignments":[{"slug":"entity/other","path":[]}]}`, `{"assignments":[{"slug":"entity/a","path":null}]}`} {
		_, err := parseProcessingWikiTaxonomy(raw, items)
		require.Error(t, err)
	}
	paths, err := parseProcessingWikiTaxonomy(`{"assignments":[{"slug":"entity/a","path":[]}]}`, items)
	require.NoError(t, err)
	require.Contains(t, paths, "entity/a")
	require.Empty(t, paths["entity/a"])
}

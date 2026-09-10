package repository

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func WikiRevisionFromPage(p *types.WikiPage) *types.WikiPageRevision {
	return &types.WikiPageRevision{ID: uuid.NewString(), TenantID: p.TenantID, KnowledgeBaseID: p.KnowledgeBaseID,
		PageID: p.ID, Slug: p.Slug, Version: p.Version, Title: p.Title, PageType: p.PageType, Status: p.Status, Content: p.Content,
		Summary: p.Summary, Aliases: append(types.StringArray(nil), p.Aliases...), EditSource: types.NormalizeWikiEditSource(p.LastEditSource),
		EditorID: p.LastEditorID, EditedAt: p.UpdatedAt, CreatedAt: time.Now()}
}

func processingWikiOwnedQuery(tx *gorm.DB, job *types.ProcessingJob, slug string) *gorm.DB {
	q := tx.Table("processing_wiki_writes AS w").Select("w.*").
		Joins("JOIN processing_jobs j ON j.id = w.job_id AND j.tenant_id = w.tenant_id").
		Where("w.tenant_id = ? AND w.knowledge_base_id = ? AND j.datasource_id = ? AND j.external_id = ? AND j.kind = ? AND w.state = ?",
			job.TenantID, job.KnowledgeBaseID, job.DataSourceID, job.ExternalID, job.Kind, "active")
	if slug != "" {
		q = q.Where("w.slug = ?", slug)
	}
	return q.Order("w.id")
}

func (r *ProcessingRepository) WikiContributions(ctx context.Context, tenant uint64, lease types.ProcessingLease, slug string) (writes []types.ProcessingWikiWrite, err error) {
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingLease(tx, tenant, lease)
		if err != nil {
			return err
		}
		q := processingWikiOwnedQuery(tx, job, slug)
		if lease.Step.Stage == "wiki_retire_page" {
			q = q.Where("w.job_id = ?", job.ID)
		}
		return q.Find(&writes).Error
	})
	return
}

func (r *ProcessingRepository) WikiRetirementInput(ctx context.Context, tenant uint64, lease types.ProcessingLease) (kb *types.KnowledgeBase, history *types.ProcessingWikiWrite, err error) {
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingLease(tx, tenant, lease)
		if err != nil {
			return err
		}
		var input map[string]string
		if lease.Step.Stage != "wiki_retire_page" || json.Unmarshal(lease.Step.Input, &input) != nil || input["page_id"] == "" {
			return ErrProcessingScope
		}
		var row types.ProcessingWikiWrite
		if err := tx.Where("job_id = ? AND tenant_id = ? AND page_id = ?", job.ID, tenant, input["page_id"]).Order("created_at DESC, id").Take(&row).Error; err != nil {
			return err
		}
		history = &row
		var base types.KnowledgeBase
		if err := tx.Unscoped().Where("id = ? AND tenant_id = ?", job.KnowledgeBaseID, tenant).Take(&base).Error; err != nil {
			return err
		}
		kb = &base
		return nil
	})
	return
}

func processingWikiRetired(tx *gorm.DB, job *types.ProcessingJob) error {
	var count int64
	if err := tx.Model(&types.ProcessingWikiWrite{}).Where("job_id = ? AND state = ?", job.ID, "active").Count(&count).Error; err != nil {
		return err
	}
	if count != 0 {
		return errors.New("WIKI_RETIREMENT_PENDING")
	}
	return nil
}

func ProcessingWikiPageIdentity(page *types.WikiPage) string {
	return fmt.Sprintf("%s/%d", page.ID, page.MutationRevision)
}

func (r *ProcessingRepository) WikiLinkTargets(ctx context.Context, tenant uint64, lease types.ProcessingLease, slugs []string) (pages []types.WikiPage, err error) {
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockProcessingLease(tx, tenant, lease)
		if err != nil {
			return err
		}
		return tx.Select("id", "slug", "title", "aliases", "page_type", "status", "mutation_revision").
			Where("knowledge_base_id = ? AND tenant_id = ? AND slug IN ?", job.KnowledgeBaseID, tenant, slugs).Order("slug").Find(&pages).Error
	})
	return
}

func processingWikiFolder(tx *gorm.DB, job *types.ProcessingJob, page *types.WikiPage, path []string, now time.Time) error {
	if len(path) == 0 {
		return nil
	}
	if page.FolderID != "" || !slices.Equal(types.CleanWikiCategoryPath(path), path) {
		return ErrProcessingScope
	}
	parentID, parentPath := "", ""
	for depth, name := range path {
		if len(name) > 255 {
			return ErrProcessingScope
		}
		var folder types.WikiFolder
		lookup := func() error {
			return tx.Where("knowledge_base_id = ? AND tenant_id = ? AND parent_id = ? AND name = ?", job.KnowledgeBaseID, job.TenantID, parentID, name).Take(&folder).Error
		}
		err := lookup()
		if errors.Is(err, gorm.ErrRecordNotFound) {
			fullPath := name
			if parentPath != "" {
				fullPath = parentPath + "/" + name
			}
			folder = types.WikiFolder{ID: uuid.NewString(), TenantID: job.TenantID, KnowledgeBaseID: job.KnowledgeBaseID, ParentID: parentID,
				Name: name, Path: fullPath, Depth: depth + 1, CreatedAt: now, UpdatedAt: now}
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&folder).Error; err != nil {
				return err
			}
			folder = types.WikiFolder{}
			err = lookup()
		}
		if err != nil {
			return err
		}
		// Hold the folder against rename/delete until the page's derived path is committed.
		if tx.Dialector.Name() == "postgres" {
			if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).Where("id = ?", folder.ID).Take(&folder).Error; err != nil {
				return err
			}
		}
		parentID, parentPath = folder.ID, folder.Path
	}
	page.FolderID = parentID
	page.CategoryPath = types.CleanWikiCategoryPath(strings.Split(parentPath, "/"))
	page.Depth = len(page.CategoryPath)
	page.WikiPath = strings.Join(append(append([]string{page.PageType}, page.CategoryPath...), page.Title), "/")
	return nil
}

// Called inside FinishStep's source/job/lease transaction. A page conflict is
// committed as a retry of this page unit, preserving all extraction outputs.
func processingWikiChunks(tx *gorm.DB, job *types.ProcessingJob, revisions map[string]int) error {
	if len(revisions) == 0 {
		return ErrProcessingConflict
	}
	ids := make([]string, 0, len(revisions))
	for id := range revisions {
		ids = append(ids, id)
	}
	var chunks []types.Chunk
	q := tx.Select("id", "content_revision", "is_enabled").Where("id IN ? AND tenant_id = ? AND knowledge_base_id = ? AND knowledge_id = ?", ids, job.TenantID, job.KnowledgeBaseID, job.KnowledgeID).Order("id")
	if tx.Dialector.Name() == "postgres" {
		q = q.Clauses(clause.Locking{Strength: "SHARE"})
	}
	if err := q.Find(&chunks).Error; err != nil {
		return err
	}
	if len(chunks) != len(ids) {
		return ErrProcessingConflict
	}
	for _, chunk := range chunks {
		if !chunk.IsEnabled || chunk.ContentRevision != revisions[chunk.ID] {
			return ErrProcessingConflict
		}
	}
	return nil
}

func commitProcessingWiki(tx *gorm.DB, job *types.ProcessingJob, step *types.ProcessingStep, mutation *types.ProcessingWikiMutation, outcome types.ProcessingOutcome, now time.Time) error {
	if mutation == nil || mutation.Page == nil || step.ExpectedPublicationEpoch != job.PublicationEpoch || mutation.PublicationEpoch != job.PublicationEpoch {
		return ErrProcessingConflict
	}
	retire := mutation.RetireOnly
	maintenance := mutation.Maintenance
	if (maintenance && (retire || step.Stage != "wiki_links")) || (!maintenance && !retire && step.Stage != "wiki_page") ||
		(!retire && (!job.IsPublished || step.Phase != types.ProcessingPhaseProjection)) ||
		(retire && (step.Stage != "wiki_retire_page" || job.IsPublished || job.IsCurrent || job.RollbackPin || step.Phase != types.ProcessingPhaseRetire)) {
		return ErrProcessingConflict
	}
	page := *mutation.Page
	if !retire && step.UnitKey != fmt.Sprintf("%x", sha256.Sum256([]byte(page.Slug))) {
		return ErrProcessingScope
	}
	if retire {
		var input map[string]string
		if json.Unmarshal(step.Input, &input) != nil || input["page_id"] != page.ID {
			return ErrProcessingScope
		}
	}
	if page.ID == "" || page.TenantID != job.TenantID || page.KnowledgeBaseID != job.KnowledgeBaseID || page.Slug == "" ||
		!types.IsValidWikiPageType(page.PageType) || !types.IsValidWikiPageStatus(page.Status) || len(page.Content) > 1<<20 || len(page.Title) > 512 {
		return ErrProcessingScope
	}
	if outcome.OutputManifestRef == "" || len(outcome.OutputDigest) != 64 {
		return ErrProcessingConflict
	}
	// Only our own source revisions may be retired by this replacement.
	var previous []types.ProcessingWikiWrite
	previousQuery := processingWikiOwnedQuery(tx, job, page.Slug)
	if retire {
		previousQuery = previousQuery.Where("w.job_id = ?", job.ID)
	}
	if err := previousQuery.Where("? = false", maintenance).Find(&previous).Error; err != nil {
		return err
	}
	ids := make([]string, 0, len(previous))
	for _, write := range previous {
		ids = append(ids, write.ID)
	}
	wanted := append([]string{}, mutation.RetireIDs...)
	slices.Sort(wanted)
	if !slices.Equal(ids, wanted) {
		return ErrWikiPageConflict
	}
	if retire && len(ids) == 0 {
		return nil
	}
	if !retire {
		if err := processingWikiChunks(tx, job, mutation.ChunkRevisions); err != nil {
			return err
		}
	}
	// Lock the target and linked pages in a single stable order before updating
	// either the page or backlinks. There is no external I/O in this transaction.
	var current types.WikiPage
	err := tx.Where("knowledge_base_id = ? AND slug = ?", job.KnowledgeBaseID, page.Slug).Take(&current).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}
	oldLinks := current.OutLinks
	slugs := append(append([]string{page.Slug}, oldLinks...), page.OutLinks...)
	for slug := range mutation.LinkedRevisions {
		slugs = append(slugs, slug)
	}
	slices.Sort(slugs)
	slugs = slices.Compact(slugs)
	var locked []types.WikiPage
	q := tx.Where("knowledge_base_id = ? AND slug IN ?", job.KnowledgeBaseID, slugs).Order("id")
	if tx.Dialector.Name() == "postgres" {
		q = q.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	if err := q.Find(&locked).Error; err != nil {
		return err
	}
	found := false
	for _, value := range locked {
		if value.Slug == page.Slug {
			current, found = value, true
			break
		}
	}
	if retire && (!found || current.ID != page.ID) {
		return tx.Model(&types.ProcessingWikiWrite{}).Where("id IN ?", ids).Updates(map[string]any{"state": "retracted", "updated_at": now}).Error
	}
	if (mutation.ExpectedRevision == 0 && found) || (mutation.ExpectedRevision != 0 && (!found || current.ID != page.ID || current.MutationRevision != mutation.ExpectedRevision)) {
		return ErrWikiPageConflict
	}
	for slug, expected := range mutation.LinkedRevisions {
		actual := ""
		for i := range locked {
			if locked[i].Slug == slug {
				actual = ProcessingWikiPageIdentity(&locked[i])
				break
			}
		}
		if actual != expected {
			return ErrWikiPageConflict
		}
	}
	// Provenance for other contributors is copied unchanged; model output can
	// never choose source IDs. The service supplies only its newly owned ref.
	removedKnowledge := map[string]bool{}
	for _, old := range previous {
		removedKnowledge[old.KnowledgeID] = true
	}
	expectedRefs := types.StringArray{}
	for _, ref := range current.SourceRefs {
		kid, _, _ := strings.Cut(ref, "|")
		if !removedKnowledge[kid] {
			expectedRefs = append(expectedRefs, ref)
		}
	}
	if !retire && !maintenance && !slices.Contains(expectedRefs, job.KnowledgeID) {
		expectedRefs = append(expectedRefs, job.KnowledgeID)
	}
	if !slices.Equal(expectedRefs, page.SourceRefs) {
		return ErrProcessingScope
	}
	page.CreatedAt, page.UpdatedAt = now, now
	page.LastEditSource, page.LastEditorID = types.WikiEditSourcePipeline, ""
	if maintenance {
		if !found || len(mutation.PlannedPath) != 0 || len(mutation.RetireIDs) != 0 {
			return ErrProcessingScope
		}
		page.Version, page.MutationRevision = current.Version, current.MutationRevision
		if page.Content != current.Content || !slices.Equal(page.OutLinks, current.OutLinks) {
			if err := (&wikiPageRepository{db: tx}).UpdateAutoLinkedContent(tx.Statement.Context, &page); err != nil {
				return err
			}
		}
	} else if !found {
		if err := processingWikiFolder(tx, job, &page, mutation.PlannedPath, now); err != nil {
			return err
		}
		page.Version, page.MutationRevision = 1, 1
		created := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&page)
		if created.Error != nil {
			return created.Error
		}
		if created.RowsAffected != 1 {
			return ErrWikiPageConflict
		}
	} else {
		if len(mutation.PlannedPath) > 0 {
			if current.FolderID != "" || len(current.CategoryPath) != 0 || current.HasManualFolder() {
				return ErrWikiPageConflict
			}
			if err := processingWikiFolder(tx, job, &page, mutation.PlannedPath, now); err != nil {
				return err
			}
		}
		page.Version, page.MutationRevision, page.CreatedAt, page.InLinks = current.Version, current.MutationRevision, current.CreatedAt, current.InLinks
		r := &wikiPageRepository{db: tx}
		changed := page.Content != current.Content || page.Title != current.Title || page.Summary != current.Summary || page.PageType != current.PageType || page.Status != current.Status || !slices.Equal(page.Aliases, current.Aliases)
		if changed {
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(WikiRevisionFromPage(&current)).Error; err != nil {
				return err
			}
			if err := updateWikiPageRow(tx, &page); err != nil {
				return err
			}
		} else if err := r.UpdateMeta(tx.Statement.Context, &page); err != nil {
			return err
		}
	}
	for i := range locked {
		target := &locked[i]
		if target.Slug == page.Slug {
			continue
		}
		before := append(types.StringArray{}, target.InLinks...)
		if slices.Contains(page.OutLinks, target.Slug) {
			if !slices.Contains(target.InLinks, page.Slug) {
				target.InLinks = append(target.InLinks, page.Slug)
			}
		} else {
			target.InLinks = slices.DeleteFunc(target.InLinks, func(slug string) bool { return slug == page.Slug })
		}
		if !slices.Equal(before, target.InLinks) {
			if err := updateWikiPageCAS(tx, target, map[string]interface{}{"in_links": target.InLinks, "updated_at": now}); err != nil {
				return err
			}
		}
	}
	if len(ids) > 0 {
		if err := tx.Model(&types.ProcessingWikiWrite{}).Where("id IN ?", ids).Updates(map[string]any{"state": "retracted", "updated_at": now}).Error; err != nil {
			return err
		}
	}
	if retire || maintenance {
		return nil
	}
	write := types.ProcessingWikiWrite{ID: uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("wiki/%s/%d/%d", step.ID, step.Attempt, job.PublicationEpoch))).String(),
		TenantID: job.TenantID, JobID: job.ID, StepID: step.ID, Attempt: step.Attempt, PublicationEpoch: job.PublicationEpoch,
		KnowledgeBaseID: job.KnowledgeBaseID, KnowledgeID: job.KnowledgeID, PageID: page.ID, Slug: page.Slug, State: "active",
		ArtifactRef: outcome.OutputManifestRef, ArtifactDigest: outcome.OutputDigest, InputFingerprint: step.InputFingerprint}
	return tx.Create(&write).Error
}

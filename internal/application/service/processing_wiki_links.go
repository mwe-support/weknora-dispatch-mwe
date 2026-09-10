package service

import (
	"context"
	"errors"
	"slices"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
)

func (e *processingDocumentExecution) wikiLinks(ctx context.Context) (types.ProcessingOutcome, error) {
	var document processingWikiDocument
	if err := e.dependency(ctx, "wiki_prepare", "body", "wiki_prepare", &document); err != nil {
		return types.ProcessingOutcome{}, err
	}
	var slug string
	var slugs []string
	for _, update := range document.Updates {
		slugs = append(slugs, update.Slug)
		if processingWikiUnit(update.Slug) == e.lease.Step.UnitKey {
			slug = update.Slug
		}
	}
	if slug == "" || slug == "index" {
		return types.ProcessingOutcome{}, errors.New("WIKI_LINK_INPUT_INVALID")
	}
	page, err := e.s.wikiRepo.GetBySlug(ctx, e.kb.ID, slug)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	slugs = append(slugs, page.OutLinks...)
	slices.Sort(slugs)
	slugs = slices.Compact(slugs)
	targets, err := e.repo.WikiLinkTargets(ctx, e.lease.Job.TenantID, e.lease, slugs)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	live, dead := map[string]struct{}{}, map[string]struct{}{}
	titleToSlug, revisions := map[string]string{}, map[string]string{}
	for _, candidate := range slugs {
		dead[candidate] = struct{}{}
		revisions[candidate] = ""
	}
	var pages []*types.WikiPage
	for i := range targets {
		target := &targets[i]
		revisions[target.Slug] = repository.ProcessingWikiPageIdentity(target)
		if target.Status == types.WikiPageStatusArchived {
			continue
		}
		delete(dead, target.Slug)
		live[target.Slug] = struct{}{}
		if target.Title != "" {
			titleToSlug[target.Title] = target.Slug
		}
		pages = append(pages, target)
	}
	delete(revisions, page.Slug) // the full page has its own CAS below
	if page.Status != types.WikiPageStatusArchived {
		page.Content, _ = stripDeadWikiLinks(page.Content, dead, live, titleToSlug)
		page.Content, _ = linkifyContent(page.Content, collectLinkRefs(pages), page.Slug)
	}
	page.OutLinks = (&wikiPageService{}).parseOutLinks(page.Content)
	mutation := types.ProcessingWikiMutation{Maintenance: true, Page: page, ExpectedRevision: page.MutationRevision,
		PublicationEpoch: e.lease.Job.PublicationEpoch, LinkedRevisions: revisions, ChunkRevisions: document.ChunkRevisions}
	out, err := e.wikiSuccess(ctx, mutation)
	out.WikiPage = &mutation
	return out, err
}

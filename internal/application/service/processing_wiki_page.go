package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/Tencent/WeKnora/internal/agent"
	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
)

func (e *processingDocumentExecution) wikiPages(ctx context.Context) (types.ProcessingOutcome, error) {
	if e.lease.Step.PlanSealed {
		return e.success(ctx, "wiki_pages", map[string]bool{"confirmed": true})
	}
	var document processingWikiDocument
	if err := e.dependency(ctx, "wiki_prepare", "body", "wiki_prepare", &document); err != nil {
		return types.ProcessingOutcome{}, err
	}
	var specs []types.ProcessingStepSpec
	var pageDeps []string
	for _, update := range document.Updates {
		if update.Type == types.WikiPageTypeIndex {
			continue
		}
		specs = append(specs, e.wikiStep("wiki_page", processingWikiUnit(update.Slug)))
		pageDeps = append(pageDeps, "wiki_page/"+processingWikiUnit(update.Slug))
	}
	specs = append(specs, e.wikiStep("wiki_page", processingWikiUnit("index"), pageDeps...))
	pageDeps = append(pageDeps, "wiki_page/"+processingWikiUnit("index"))
	for _, update := range document.Updates {
		if update.Type != types.WikiPageTypeIndex {
			specs = append(specs, e.wikiStep("wiki_links", processingWikiUnit(update.Slug), pageDeps...))
		}
	}
	return processingWikiWait(specs), nil
}

func (e *processingDocumentExecution) wikiPage(ctx context.Context) (types.ProcessingOutcome, error) {
	var document processingWikiDocument
	if err := e.dependency(ctx, "wiki_prepare", "body", "wiki_prepare", &document); err != nil {
		return types.ProcessingOutcome{}, err
	}
	_, revisions, err := e.wikiSource(ctx)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	encoded, _ := json.Marshal(revisions)
	expected, _ := json.Marshal(document.ChunkRevisions)
	if string(encoded) != string(expected) {
		return types.ProcessingOutcome{}, errors.New("WIKI_SOURCE_CHANGED")
	}
	var update *SlugUpdate
	for i := range document.Updates {
		if processingWikiUnit(document.Updates[i].Slug) == e.lease.Step.UnitKey {
			update = &document.Updates[i]
			break
		}
	}
	if update == nil {
		return types.ProcessingOutcome{}, errors.New("WIKI_PAGE_INPUT_INVALID")
	}
	current, err := e.s.wikiRepo.GetBySlug(ctx, e.kb.ID, update.Slug)
	if err != nil && !errors.Is(err, repository.ErrWikiPageNotFound) {
		return types.ProcessingOutcome{}, err
	}
	if current == nil {
		current = &types.WikiPage{ID: uuid.NewSHA1(uuid.NameSpaceOID, []byte("wiki/"+e.kb.ID+"/"+update.Slug)).String(), TenantID: e.lease.Job.TenantID,
			KnowledgeBaseID: e.kb.ID, Slug: update.Slug, Title: update.Item.Name, PageType: update.Type, Status: types.WikiPageStatusPublished}
	}
	previous, err := e.repo.WikiContributions(ctx, e.lease.Job.TenantID, e.lease, update.Slug)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	var retire []string
	for _, write := range previous {
		retire = append(retire, write.ID)
	}
	var mutation types.ProcessingWikiMutation
	out, cached, err := e.wikiCheckpoint(ctx, &mutation)
	if err != nil {
		return out, err
	}
	if cached && mutation.Page != nil && mutation.Page.ID == current.ID && mutation.ExpectedRevision == current.MutationRevision &&
		mutation.PublicationEpoch == e.lease.Job.PublicationEpoch && slices.Equal(mutation.RetireIDs, retire) {
		out.WikiPage = &mutation
		return out, nil
	}
	page := *current
	if page.Status == types.WikiPageStatusArchived && len(page.SourceRefs) == 0 && !page.HasUntrackedContent() && types.NormalizeWikiEditSource(page.LastEditSource) == types.WikiEditSourcePipeline {
		page.Status = types.WikiPageStatusPublished
	}
	mutation = types.ProcessingWikiMutation{Page: &page, ExpectedRevision: current.MutationRevision, PublicationEpoch: e.lease.Job.PublicationEpoch,
		RetireIDs: retire, ChunkRevisions: revisions, ContributionContent: update.DocSummary, ContributionTitle: update.DocTitle, PreserveUntracked: current.HasUntrackedContent()}
	if update.Type == types.WikiPageTypeSummary {
		mutation.ContributionContent = update.SummaryBody
	}
	removeKnowledge, removeChunks := map[string]bool{}, map[string]bool{}
	var deleted strings.Builder
	for _, write := range previous {
		prior, err := e.s.readProcessingWikiContribution(ctx, e.repo, write)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		removeKnowledge[write.KnowledgeID] = true
		mutation.PreserveUntracked = mutation.PreserveUntracked || prior.PreserveUntracked
		for id := range prior.ChunkRevisions {
			removeChunks[id] = true
		}
		fmt.Fprintf(&deleted, "<document><title>%s</title><content>%s</content></document>\n", prior.ContributionTitle, prior.ContributionContent)
	}
	page.SourceRefs = nil
	for _, ref := range current.SourceRefs {
		kid, _, _ := strings.Cut(ref, "|")
		if !removeKnowledge[kid] {
			page.SourceRefs = append(page.SourceRefs, ref)
		}
	}
	if !slices.Contains(page.SourceRefs, e.lease.Job.KnowledgeID) {
		page.SourceRefs = append(page.SourceRefs, e.lease.Job.KnowledgeID)
	}
	page.ChunkRefs = nil
	for _, id := range current.ChunkRefs {
		if !removeChunks[id] {
			page.ChunkRefs = append(page.ChunkRefs, id)
		}
	}
	for _, id := range update.SourceChunks {
		if _, ok := revisions[id]; !ok {
			return types.ProcessingOutcome{}, errors.New("WIKI_CITATION_UNKNOWN_CHUNK")
		}
		if !slices.Contains(page.ChunkRefs, id) {
			page.ChunkRefs = append(page.ChunkRefs, id)
		}
	}
	if update.Type == types.WikiPageTypeSummary && !mutation.PreserveUntracked {
		page.Title, page.Content, page.Summary = update.DocTitle+" - Summary", update.SummaryBody, update.SummaryLine
		page.ChunkRefs = nil
		mutation.ContributionContent = update.SummaryBody
	} else if update.Type == types.WikiPageTypeIndex {
		if err := e.wikiIndex(ctx, &page, update, deleted.String()); err != nil {
			return types.ProcessingOutcome{}, err
		}
	} else {
		model, err := e.s.modelService.GetChatModel(ctx, e.kb.SummaryModelID)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		var facts strings.Builder
		for _, id := range update.SourceChunks {
			chunk, err := e.s.chunkRepo.GetChunkByID(ctx, e.lease.Job.TenantID, id)
			if err != nil {
				return types.ProcessingOutcome{}, err
			}
			fmt.Fprintln(&facts, chunk.Content)
		}
		if facts.Len() == 0 {
			fmt.Fprintln(&facts, update.Item.Details, update.SummaryBody)
		}
		page.Aliases = append(append(types.StringArray{}, page.Aliases...), update.Item.Aliases...)
		slices.Sort(page.Aliases)
		page.Aliases = slices.Compact(page.Aliases)
		if page.Title == "" {
			page.Title = update.Item.Name
		}
		contentInstructions := ""
		if e.kb.WikiConfig != nil {
			contentInstructions = e.kb.WikiConfig.ContentInstructions
		}
		hasRetractions := ""
		if len(retire) > 0 {
			hasRetractions = "1"
		}
		available := []string{update.Slug}
		for _, other := range document.Updates {
			available = append(available, other.Slug)
		}
		available = append(available, page.OutLinks...)
		slices.Sort(available)
		available = slices.Compact(available)
		handles := newWikiSlugHandles()
		known := map[string]struct{}{}
		var links strings.Builder
		for _, slug := range available {
			known[slug] = struct{}{}
			fmt.Fprintln(&links, handles.handle(slug))
		}
		content := handles.encodeContent(stripWikiInlineChunkCitations(current.Content), known)
		if content == "" {
			content = "(New page)"
		}
		raw, err := e.wikiIngest().generateWithTemplate(ctx, model, agent.WikiPageModifyUserPrompt, map[string]string{
			"HasAdditions": "1", "HasRetractions": hasRetractions, "PageSlug": update.Slug, "PageTitle": page.Title, "PageType": page.PageType,
			"PageAliases": strings.Join(page.Aliases, ", "), "ExistingContent": content, "SharedSourceContexts": update.DocSummary,
			"NewContent": facts.String(), "DeletedContent": deleted.String(), "RemainingSourcesContent": "Preserve all other sources and manual edits in the existing page.",
			"AvailableSlugs": links.String(), "Language": update.Language, "CustomInstructions": contentInstructions, "InstructionScope": "wiki_content"})
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		if strings.TrimSpace(raw) == "" || len(raw) > 1<<20 {
			return types.ProcessingOutcome{}, errors.New("WIKI_PAGE_OUTPUT_INVALID")
		}
		line, body := splitSummaryLine(handles.decodeContent(raw))
		if body == "" {
			body = handles.decodeContent(raw)
		}
		page.Content = body
		if line != "" {
			page.Summary = line
		}
	}
	if mutation.PreserveUntracked {
		page.PreserveUntrackedContent()
	}
	if page.FolderID == "" && len(page.CategoryPath) == 0 && !page.HasManualFolder() && (page.PageType == types.WikiPageTypeEntity || page.PageType == types.WikiPageTypeConcept) {
		var paths map[string][]string
		if err := e.dependency(ctx, "wiki_taxonomy", "body", "wiki_taxonomy", &paths); err != nil {
			return types.ProcessingOutcome{}, err
		}
		path, ok := paths[page.Slug]
		if !ok {
			return types.ProcessingOutcome{}, errors.New("WIKI_TAXONOMY_MISSING")
		}
		mutation.PlannedPath = path
		page.CategoryPath = path
	}
	stripWikiPageInlineChunkCitations(&page)
	page.OutLinks = (&wikiPageService{}).parseOutLinks(page.Content)
	normalizeWikiHierarchy(&page)
	out, err = e.wikiSuccess(ctx, mutation)
	out.WikiPage = &mutation
	return out, err
}

func (e *processingDocumentExecution) wikiIndex(ctx context.Context, page *types.WikiPage, update *SlugUpdate, deleted string) error {
	model, err := e.s.modelService.GetChatModel(ctx, e.kb.SummaryModelID)
	if err != nil {
		return err
	}
	intro := strings.TrimSpace(page.Content)
	if intro == "" {
		intro = strings.TrimSpace(page.Summary)
	}
	if i := strings.Index(intro, "\n## "); i >= 0 {
		intro = strings.TrimSpace(intro[:i])
	}
	prompt := agent.WikiIndexIntroUpdatePrompt
	inputs := map[string]string{"ExistingIntro": intro, "ChangeDescription": "Added or replaced source: " + update.DocTitle + "\n" + update.DocSummary + "\nRetired contributions:\n" + deleted,
		"DocumentSummaries": "", "Language": update.Language, "InstructionScope": "wiki_content"}
	if e.kb.WikiConfig != nil {
		inputs["CustomInstructions"] = e.kb.WikiConfig.ContentInstructions
	}
	if intro == "" || intro == "Wiki index - table of contents" {
		prompt = agent.WikiIndexIntroPrompt
		pages, err := e.s.wikiService.ListByTypeRecent(ctx, e.kb.ID, types.WikiPageTypeSummary, indexIntroSummaryCap)
		if err != nil {
			return err
		}
		counts, err := e.s.wikiService.CountByType(ctx, e.kb.ID)
		if err != nil {
			return err
		}
		var summaries strings.Builder
		fmt.Fprintf(&summaries, "Showing %d most recent of %d documents.\n", len(pages), counts[types.WikiPageTypeSummary])
		for _, summary := range pages {
			fmt.Fprintf(&summaries, "<document><title>%s</title><summary>%s</summary></document>\n", summary.Title, summary.Summary)
		}
		inputs["DocumentSummaries"] = summaries.String()
	}
	raw, err := e.wikiIngest().generateWithTemplate(ctx, model, prompt, inputs)
	if err != nil {
		return err
	}
	if i := strings.Index(raw, "\n## "); i >= 0 {
		raw = raw[:i]
	}
	raw = strings.TrimSpace(raw)
	if raw == "" || len(raw) > 1<<20 {
		return errors.New("WIKI_INDEX_INVALID")
	}
	if page.Title == "" {
		page.Title = "Wiki Index"
	}
	page.Content, page.Summary = raw, raw
	return nil
}

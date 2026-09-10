package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/Tencent/WeKnora/internal/agent"
	"github.com/Tencent/WeKnora/internal/types"
)

type processingWikiDocument struct {
	Updates        []SlugUpdate   `json:"updates"`
	ChunkRevisions map[string]int `json:"chunk_revisions"`
}

func (e *processingDocumentExecution) wikiSource(ctx context.Context) ([]chunkBatch, map[string]int, error) {
	chunks, err := e.graphChunks(ctx)
	if err != nil {
		return nil, nil, err
	}
	revisions := make(map[string]int, len(chunks))
	rows, err := e.s.chunkRepo.ListChunksByKnowledgeID(ctx, e.lease.Job.TenantID, e.lease.Job.KnowledgeID)
	if err != nil {
		return nil, nil, err
	}
	stored := make(map[string]*types.Chunk, len(rows))
	for _, row := range rows {
		stored[row.ID] = row
	}
	text := make([]*types.Chunk, 0, len(chunks))
	for i, chunk := range chunks {
		current := stored[chunk.ID]
		if current == nil || !current.IsEnabled || current.ContentRevision != chunk.ContentRevision || current.Content != chunk.Content {
			return nil, nil, errors.New("WIKI_SOURCE_CHANGED")
		}
		revisions[chunk.ID] = chunk.ContentRevision
		copy := *chunk
		copy.ChunkType, copy.ChunkIndex = types.ChunkTypeText, i
		text = append(text, &copy)
	}
	batches := splitChunksIntoCitationBatches(text)
	if len(batches) == 0 {
		return nil, nil, errors.New("WIKI_SOURCE_EMPTY")
	}
	return batches, revisions, nil
}

func processingWikiUnit(slug string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(slug))) }

func (e *processingDocumentExecution) wikiStep(stage, unit string, deps ...string) types.ProcessingStepSpec {
	return types.ProcessingStepSpec{Stage: stage, UnitKey: unit, Phase: e.lease.Step.Phase,
		InputFingerprint: processingWikiUnit(e.lease.Step.InputFingerprint + "/" + stage + "/" + unit), DependsOn: deps}
}

func processingWikiWait(specs []types.ProcessingStepSpec) types.ProcessingOutcome {
	next := time.Now().UTC().Add(time.Second)
	return types.ProcessingOutcome{Status: types.ProcessingWaitingExternal, NextRunAt: &next, SealPlan: true, ChildSteps: specs}
}

func (e *processingDocumentExecution) wikiBarrier(ctx context.Context) (types.ProcessingOutcome, error) {
	if e.s.wikiService == nil || e.s.wikiRepo == nil || !e.kb.IsWikiEnabled() || e.kb.SummaryModelID == "" {
		return types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "configuration", ErrorCode: "WIKI_CONFIGURATION_INVALID"}, nil
	}
	if e.lease.Step.PlanSealed {
		return e.success(ctx, "wiki", map[string]bool{"confirmed": true})
	}
	batches, _, err := e.wikiSource(ctx)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	var specs []types.ProcessingStepSpec
	var extracted, prepared, summaries []string
	for i := range batches {
		unit := strconv.Itoa(i)
		specs = append(specs, e.wikiStep("wiki_extract", unit))
		extracted = append(extracted, "wiki_extract/"+unit)
		specs = append(specs, e.wikiStep("wiki_cite", unit, "wiki_dedup/body"), e.wikiStep("wiki_summary_part", unit, "wiki_dedup/body"))
		prepared = append(prepared, "wiki_cite/"+unit)
		summaries = append(summaries, "wiki_summary_part/"+unit)
	}
	specs = append(specs, e.wikiStep("wiki_dedup", "body", extracted...), e.wikiStep("wiki_summary", "body", summaries...))
	prepared = append(prepared, "wiki_summary/body")
	specs = append(specs, e.wikiStep("wiki_prepare", "body", prepared...))
	specs = append(specs, e.wikiStep("wiki_taxonomy_input", "body", "wiki_prepare/body"))
	taxonomy := e.wikiStep("wiki_taxonomy", "body", "wiki_taxonomy_input/body")
	taxonomy.Kind = "barrier"
	specs = append(specs, taxonomy)
	pages := e.wikiStep("wiki_pages", "body", "wiki_taxonomy/body")
	pages.Kind = "barrier"
	specs = append(specs, pages)
	return processingWikiWait(specs), nil
}

// A persisted checkpoint survives an ACK loss or a database outage after the
// LLM call. Dynamic page merges additionally compare the saved page revision.
func (e *processingDocumentExecution) wikiSuccess(ctx context.Context, value any) (types.ProcessingOutcome, error) {
	out, err := e.success(ctx, e.lease.Step.Stage, value)
	if err != nil {
		return out, err
	}
	ref, _ := json.Marshal(processingArtifactRef{Path: out.OutputManifestRef, Digest: out.OutputDigest})
	if err := e.repo.Heartbeat(ctx, e.lease.Job.TenantID, e.lease, 2*time.Minute, string(ref)); err != nil {
		return types.ProcessingOutcome{}, err
	}
	out.CheckpointRef = string(ref)
	return out, nil
}

func (e *processingDocumentExecution) wikiCheckpoint(ctx context.Context, out any) (types.ProcessingOutcome, bool, error) {
	if e.lease.Step.CheckpointRef == "" {
		return types.ProcessingOutcome{}, false, nil
	}
	var ref processingArtifactRef
	if json.Unmarshal([]byte(e.lease.Step.CheckpointRef), &ref) != nil || ref.Path == "" || ref.Digest == "" {
		return types.ProcessingOutcome{}, false, errors.New("WIKI_CHECKPOINT_INVALID")
	}
	data, err := e.artifacts.Read(ctx, e.lease.Job, e.lease.Step, e.lease.Step.Stage, ref.Path, ref.Digest)
	if err != nil {
		return types.ProcessingOutcome{}, false, err
	}
	if !json.Valid(data) {
		return types.ProcessingOutcome{}, false, errors.New("WIKI_CHECKPOINT_INVALID")
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return types.ProcessingOutcome{}, false, err
		}
	}
	return types.ProcessingOutcome{Status: types.ProcessingSucceeded, OutputManifestRef: ref.Path, OutputDigest: ref.Digest}, true, nil
}

func wikiBatchContent(batch chunkBatch) string {
	var out strings.Builder
	for _, chunk := range batch.chunks {
		out.WriteString(chunk.Content)
		out.WriteString("\n\n")
	}
	return out.String()
}

func validProcessingWikiItem(item extractedItem, kind string) bool {
	if strings.TrimSpace(item.Name) == "" || len(item.Name) > 512 || len(item.Slug) > 255 || !strings.HasPrefix(item.Slug, kind+"/") || len(item.Aliases) > 64 {
		return false
	}
	base := strings.TrimPrefix(item.Slug, kind+"/")
	if base == "" {
		return false
	}
	for _, c := range base {
		if !unicode.IsLetter(c) && !unicode.IsNumber(c) && !unicode.IsMark(c) && c != '-' && c != '_' {
			return false
		}
	}
	for _, alias := range item.Aliases {
		if len(alias) > 512 {
			return false
		}
	}
	return len(item.Description) <= 16<<10 && len(item.Details) <= 64<<10
}

func validateProcessingWikiExtraction(value combinedExtraction) error {
	if value.Entities == nil || value.Concepts == nil || len(value.Entities)+len(value.Concepts) > 512 {
		return errors.New("WIKI_EXTRACTION_INVALID")
	}
	for _, group := range []struct {
		kind  string
		items []extractedItem
	}{{"entity", value.Entities}, {"concept", value.Concepts}} {
		for _, item := range group.items {
			if !validProcessingWikiItem(item, group.kind) {
				return errors.New("WIKI_EXTRACTION_INVALID")
			}
		}
	}
	return nil
}

func mergeProcessingWikiItems(groups ...[]extractedItem) []extractedItem {
	items := map[string]extractedItem{}
	for _, group := range groups {
		for _, item := range group {
			if old, ok := items[item.Slug]; ok {
				if !strings.Contains(old.Description, item.Description) {
					old.Description += "\n" + item.Description
				}
				if !strings.Contains(old.Details, item.Details) {
					old.Details += "\n" + item.Details
				}
				old.Aliases = append(old.Aliases, item.Aliases...)
				old.SourceChunks = append(old.SourceChunks, item.SourceChunks...)
				item = old
			}
			slices.Sort(item.Aliases)
			item.Aliases = slices.Compact(item.Aliases)
			slices.Sort(item.SourceChunks)
			item.SourceChunks = slices.Compact(item.SourceChunks)
			items[item.Slug] = item
		}
	}
	out := make([]extractedItem, 0, len(items))
	for _, item := range items {
		out = append(out, item)
	}
	slices.SortFunc(out, func(a, b extractedItem) int { return strings.Compare(a.Slug, b.Slug) })
	return out
}

func (e *processingDocumentExecution) wikiIngest() *wikiIngestService {
	return &wikiIngestService{wikiService: e.s.wikiService, kbService: e.s.kbService, knowledgeSvc: e.s,
		knowledgeRepo: e.s.repo, chunkRepo: e.s.chunkRepo, modelService: e.s.modelService, redisClient: e.s.redisClient}
}

func (e *processingDocumentExecution) wikiStage(ctx context.Context) (types.ProcessingOutcome, error) {
	if strings.HasPrefix(e.lease.Step.Stage, "wiki_taxonomy") {
		return e.wikiTaxonomy(ctx)
	}
	if e.lease.Step.Stage == "wiki" {
		return e.wikiBarrier(ctx)
	}
	if e.lease.Step.Stage == "wiki_pages" {
		return e.wikiPages(ctx)
	}
	if e.lease.Step.Stage == "wiki_page" {
		return e.wikiPage(ctx)
	}
	if e.lease.Step.Stage == "wiki_links" {
		return e.wikiLinks(ctx)
	}
	batches, revisions, err := e.wikiSource(ctx)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	if out, cached, err := e.wikiCheckpoint(ctx, nil); err != nil || cached {
		return out, err
	}
	model, err := e.s.modelService.GetChatModel(ctx, e.kb.SummaryModelID)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	ingest := e.wikiIngest()
	lang := types.ResolveLanguageName(ctx, e.document.Language)
	granularity := types.WikiExtractionStandard
	contentInstructions, extractionInstructions := "", ""
	if e.kb.WikiConfig != nil {
		granularity = e.kb.WikiConfig.ExtractionGranularity.Normalize()
		contentInstructions, extractionInstructions = e.kb.WikiConfig.ContentInstructions, e.kb.WikiConfig.ExtractionInstructions
	}
	generate := func(prompt string, inputs map[string]string) (string, error) {
		text, err := ingest.generateWithTemplate(ctx, model, prompt, inputs)
		if err == nil && (strings.TrimSpace(text) == "" || len(text) > 1<<20) {
			err = errors.New("WIKI_OUTPUT_INVALID")
		}
		return text, err
	}
	batch := chunkBatch{}
	if e.lease.Step.UnitKey != "body" {
		i, err := strconv.Atoi(e.lease.Step.UnitKey)
		if err != nil || i < 0 || i >= len(batches) {
			return types.ProcessingOutcome{}, errors.New("WIKI_BATCH_INVALID")
		}
		batch = batches[i]
	}
	var candidates combinedExtraction
	if e.lease.Step.Stage != "wiki_extract" && e.lease.Step.Stage != "wiki_dedup" {
		if err := e.dependency(ctx, "wiki_dedup", "body", "wiki_dedup", &candidates); err != nil {
			return types.ProcessingOutcome{}, err
		}
	}
	switch e.lease.Step.Stage {
	case "wiki_extract":
		raw, err := generate(agent.WikiCandidateSlugPrompt, map[string]string{"Content": wikiBatchContent(batch), "Language": lang,
			"PreviousSlugs": "(none)", "Granularity": string(granularity), "GranularityGuidance": agent.WikiGranularityGuidance(string(granularity)),
			"CustomInstructions": extractionInstructions, "InstructionScope": "wiki_extraction"})
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		var result combinedExtraction
		if json.Unmarshal([]byte(cleanLLMJSON(raw)), &result) != nil {
			return types.ProcessingOutcome{}, errors.New("WIKI_EXTRACTION_INVALID")
		}
		if err := validateProcessingWikiExtraction(result); err != nil {
			return types.ProcessingOutcome{}, err
		}
		return e.wikiSuccess(ctx, result)
	case "wiki_dedup":
		for i := range batches {
			var part combinedExtraction
			if err := e.dependency(ctx, "wiki_extract", strconv.Itoa(i), "wiki_extract", &part); err != nil {
				return types.ProcessingOutcome{}, err
			}
			candidates.Entities = mergeProcessingWikiItems(candidates.Entities, part.Entities)
			candidates.Concepts = mergeProcessingWikiItems(candidates.Concepts, part.Concepts)
		}
		candidates.Entities, candidates.Concepts, err = ingest.deduplicateExtractedBatchResult(ctx, model, e.kb.ID, candidates.Entities, candidates.Concepts, true)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		candidates.Entities = mergeProcessingWikiItems(candidates.Entities)
		candidates.Concepts = mergeProcessingWikiItems(candidates.Concepts)
		if err := validateProcessingWikiExtraction(candidates); err != nil {
			return types.ProcessingOutcome{}, err
		}
		return e.wikiSuccess(ctx, candidates)
	case "wiki_cite":
		raw, err := generate(agent.WikiChunkCitationPrompt, map[string]string{"CandidateSlugs": renderCandidateSlugsXML(candidates.Entities, candidates.Concepts), "ChunksXML": renderChunksXML(batch), "Language": lang})
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		result, err := parseProcessingWikiCitations(raw, batch, candidates)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		return e.wikiSuccess(ctx, result)
	case "wiki_summary_part", "wiki_summary":
		content := wikiBatchContent(batch)
		if e.lease.Step.Stage == "wiki_summary" {
			var parts []string
			for i := range batches {
				var part string
				if err := e.dependency(ctx, "wiki_summary_part", strconv.Itoa(i), "wiki_summary_part", &part); err != nil {
					return types.ProcessingOutcome{}, err
				}
				parts = append(parts, part)
			}
			if len(parts) == 1 {
				return e.wikiSuccess(ctx, parts[0])
			}
			content = strings.Join(parts, "\n\n")
		}
		raw, err := generate(agent.WikiSummaryPrompt, map[string]string{"Content": content, "Language": lang,
			"ExtractedSlugs": renderCandidateSlugsXML(candidates.Entities, candidates.Concepts), "CustomInstructions": contentInstructions, "InstructionScope": "wiki_content"})
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		return e.wikiSuccess(ctx, raw)
	case "wiki_prepare":
		citations := map[string][]string{}
		var newSlugs []newSlugFromCitation
		for i := range batches {
			var part citationBatchResult
			if err := e.dependency(ctx, "wiki_cite", strconv.Itoa(i), "wiki_cite", &part); err != nil {
				return types.ProcessingOutcome{}, err
			}
			for slug, ids := range part.Citations {
				citations[slug] = append(citations[slug], ids...)
			}
			newSlugs = append(newSlugs, part.NewSlugs...)
		}
		candidates.Entities, candidates.Concepts, _ = mergeCitationsIntoItems(candidates.Entities, candidates.Concepts, citations, newSlugs)
		var summary string
		if err := e.dependency(ctx, "wiki_summary", "body", "wiki_summary", &summary); err != nil {
			return types.ProcessingOutcome{}, err
		}
		line, body := splitSummaryLine(summary)
		if body == "" {
			body = summary
		}
		if line == "" {
			line = e.document.Title
		}
		output := processingWikiDocument{ChunkRevisions: revisions}
		kid := e.lease.Job.KnowledgeID
		output.Updates = append(output.Updates, SlugUpdate{Slug: "summary/" + slugify(kid), Type: types.WikiPageTypeSummary, DocTitle: e.document.Title,
			KnowledgeID: kid, SourceRef: kid, Language: lang, SummaryBody: body, SummaryLine: line})
		output.Updates = append(output.Updates, SlugUpdate{Slug: "index", Type: types.WikiPageTypeIndex, DocTitle: e.document.Title,
			KnowledgeID: kid, SourceRef: kid, Language: lang, DocSummary: body})
		for _, group := range []struct {
			kind  string
			items []extractedItem
		}{{"entity", candidates.Entities}, {"concept", candidates.Concepts}} {
			for _, item := range mergeProcessingWikiItems(group.items) {
				output.Updates = append(output.Updates, SlugUpdate{Slug: item.Slug, Type: group.kind, Item: item, KnowledgeID: kid, SourceRef: kid,
					DocTitle: e.document.Title, Language: lang, SourceChunks: item.SourceChunks, DocSummary: body})
			}
		}
		return e.wikiSuccess(ctx, output)
	default:
		return types.ProcessingOutcome{}, errors.New("WIKI_STAGE_INVALID")
	}
}

func parseProcessingWikiCitations(raw string, batch chunkBatch, candidates combinedExtraction) (citationBatchResult, error) {
	var result citationBatchResult
	if json.Unmarshal([]byte(cleanLLMJSON(raw)), &result) != nil || result.Citations == nil || result.NewSlugs == nil || len(result.NewSlugs) > 512 {
		return result, errors.New("WIKI_CITATIONS_INVALID")
	}
	allowed := map[string]bool{}
	for _, item := range append(append([]extractedItem{}, candidates.Entities...), candidates.Concepts...) {
		allowed[item.Slug] = true
	}
	resolve := func(handles []string) ([]string, error) {
		var ids []string
		for _, handle := range handles {
			id, ok := batch.handles.Resolve(handle)
			if !ok {
				return nil, errors.New("WIKI_CITATION_UNKNOWN_CHUNK")
			}
			ids = append(ids, id)
		}
		slices.Sort(ids)
		return slices.Compact(ids), nil
	}
	for i := range result.NewSlugs {
		item := &result.NewSlugs[i]
		if (item.Type != "entity" && item.Type != "concept") || !validProcessingWikiItem(extractedItem{Name: item.Name, Slug: item.Slug, Aliases: item.Aliases, Description: item.Description, Details: item.Details}, item.Type) || allowed[item.Slug] {
			return result, errors.New("WIKI_CITATION_NEW_SLUG_INVALID")
		}
		var err error
		item.SourceChunks, err = resolve(item.SourceChunks)
		if err != nil || len(item.SourceChunks) == 0 {
			return result, errors.New("WIKI_CITATION_UNKNOWN_CHUNK")
		}
		allowed[item.Slug] = true
	}
	for slug, handles := range result.Citations {
		if !allowed[slug] {
			return result, errors.New("WIKI_CITATION_UNKNOWN_SLUG")
		}
		ids, err := resolve(handles)
		if err != nil {
			return result, err
		}
		result.Citations[slug] = ids
	}
	return result, nil
}

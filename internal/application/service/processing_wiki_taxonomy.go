package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/Tencent/WeKnora/internal/agent"
	"github.com/Tencent/WeKnora/internal/types"
)

const processingWikiVectorBatch = 64

func processingWikiTaxonomyItems(document processingWikiDocument) []wikiTaxonomyItem {
	updates := map[string][]SlugUpdate{}
	for _, update := range document.Updates {
		updates[update.Slug] = append(updates[update.Slug], update)
	}
	return collectTaxonomyItems(updates)
}

func processingWikiTaxonomyTexts(pool [][]string, items []wikiTaxonomyItem) (folders, descriptions []string) {
	for _, path := range pool {
		if len(path) >= 2 {
			folders = append(folders, strings.Join(path, " / "))
		}
	}
	for _, item := range items {
		descriptions = append(descriptions, strings.TrimSpace(item.title+" "+previewText(item.about, 120)))
	}
	return
}

func (e *processingDocumentExecution) wikiTaxonomy(ctx context.Context) (types.ProcessingOutcome, error) {
	if e.lease.Step.Stage != "wiki_taxonomy" {
		if out, cached, err := e.wikiCheckpoint(ctx, nil); err != nil || cached {
			return out, err
		}
	}
	if e.lease.Step.Stage == "wiki_taxonomy_input" {
		paths, err := e.s.wikiService.ListDistinctCategoryPaths(ctx, e.kb.ID, wikiTaxonomyFolderPoolMax)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		return e.wikiSuccess(ctx, paths)
	}
	var document processingWikiDocument
	if err := e.dependency(ctx, "wiki_prepare", "body", "wiki_prepare", &document); err != nil {
		return types.ProcessingOutcome{}, err
	}
	var pool [][]string
	if err := e.dependency(ctx, "wiki_taxonomy_input", "body", "wiki_taxonomy_input", &pool); err != nil {
		return types.ProcessingOutcome{}, err
	}
	items := processingWikiTaxonomyItems(document)
	folders, descriptions := processingWikiTaxonomyTexts(pool, items)
	useVectors := len(pool) > wikiTaxonomyFeedAllMaxFolders && len(folders) > 0 && strings.TrimSpace(e.kb.EmbeddingModelID) != ""
	vectorGroups := []struct {
		key   string
		texts []string
	}{{"f", folders}, {"i", descriptions}}
	if e.lease.Step.Stage == "wiki_taxonomy" {
		if e.lease.Step.PlanSealed {
			paths := map[string][]string{}
			for start := 0; start < len(items); start += wikiTaxonomyPlanChunkSize {
				var part map[string][]string
				if err := e.dependency(ctx, "wiki_taxonomy_plan", strconv.Itoa(start/wikiTaxonomyPlanChunkSize), "wiki_taxonomy_plan", &part); err != nil {
					return types.ProcessingOutcome{}, err
				}
				for slug, path := range part {
					paths[slug] = path
				}
			}
			return e.wikiSuccess(ctx, paths)
		}
		var specs []types.ProcessingStepSpec
		var deps []string
		if useVectors && len(items) > 0 {
			for _, group := range vectorGroups {
				for start := 0; start < len(group.texts); start += processingWikiVectorBatch {
					unit := group.key + "-" + strconv.Itoa(start/processingWikiVectorBatch)
					specs = append(specs, e.wikiStep("wiki_taxonomy_vectors", unit))
					deps = append(deps, "wiki_taxonomy_vectors/"+unit)
				}
			}
		}
		for start := 0; start < len(items); start += wikiTaxonomyPlanChunkSize {
			unit := strconv.Itoa(start / wikiTaxonomyPlanChunkSize)
			specs = append(specs, e.wikiStep("wiki_taxonomy_plan", unit, deps...))
			// Later batches reuse the earlier batch's new folders.
			deps = append(deps, "wiki_taxonomy_plan/"+unit)
		}
		if len(specs) == 0 {
			return e.wikiSuccess(ctx, map[string][]string{})
		}
		return processingWikiWait(specs), nil
	}
	if e.lease.Step.Stage == "wiki_taxonomy_vectors" {
		key, number, ok := strings.Cut(e.lease.Step.UnitKey, "-")
		index, err := strconv.Atoi(number)
		if !ok || err != nil || index < 0 || !useVectors {
			return types.ProcessingOutcome{}, errors.New("WIKI_TAXONOMY_BATCH_INVALID")
		}
		texts := folders
		if key == "i" {
			texts = descriptions
		} else if key != "f" {
			return types.ProcessingOutcome{}, errors.New("WIKI_TAXONOMY_BATCH_INVALID")
		}
		start := index * processingWikiVectorBatch
		if start < 0 || start >= len(texts) {
			return types.ProcessingOutcome{}, errors.New("WIKI_TAXONOMY_BATCH_INVALID")
		}
		texts = texts[start:min(start+processingWikiVectorBatch, len(texts))]
		model, err := e.s.modelService.GetEmbeddingModel(ctx, e.kb.EmbeddingModelID)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		vectors, err := model.BatchEmbed(ctx, texts)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		if len(vectors) != len(texts) {
			return types.ProcessingOutcome{}, errors.New("EMBEDDING_COUNT_MISMATCH")
		}
		for _, vector := range vectors {
			if len(vector) == 0 || len(vector) != model.GetDimensions() {
				return types.ProcessingOutcome{}, errors.New("EMBEDDING_DIMENSION_MISMATCH")
			}
			for _, value := range vector {
				if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
					return types.ProcessingOutcome{}, errors.New("EMBEDDING_VALUE_INVALID")
				}
			}
		}
		return e.wikiSuccess(ctx, vectors)
	}
	index, err := strconv.Atoi(e.lease.Step.UnitKey)
	start := index * wikiTaxonomyPlanChunkSize
	if e.lease.Step.Stage != "wiki_taxonomy_plan" || err != nil || index < 0 || start < 0 || start >= len(items) {
		return types.ProcessingOutcome{}, errors.New("WIKI_TAXONOMY_BATCH_INVALID")
	}
	existing := pool
	if useVectors {
		vectors := map[string][][]float32{}
		for _, group := range vectorGroups {
			for offset := 0; offset < len(group.texts); offset += processingWikiVectorBatch {
				var part [][]float32
				if err := e.dependency(ctx, "wiki_taxonomy_vectors", group.key+"-"+strconv.Itoa(offset/processingWikiVectorBatch), "wiki_taxonomy_vectors", &part); err != nil {
					return types.ProcessingOutcome{}, err
				}
				vectors[group.key] = append(vectors[group.key], part...)
			}
		}
		var deeper [][]string
		existing = nil
		seen := map[string]bool{}
		for _, path := range pool {
			if len(path) == 0 {
				continue
			}
			if !seen[path[0]] {
				existing = append(existing, []string{path[0]})
				seen[path[0]] = true
			}
			if len(path) >= 2 {
				deeper = append(deeper, path)
			}
		}
		existing = append(existing, selectFoldersByVectors(deeper, vectors["f"], vectors["i"], wikiTaxonomyRelevantTopK)...)
	}
	existing = capFolders(existing, wikiTaxonomyPromptMaxPaths)
	for i := 0; i < index; i++ {
		var prior map[string][]string
		if err := e.dependency(ctx, "wiki_taxonomy_plan", strconv.Itoa(i), "wiki_taxonomy_plan", &prior); err != nil {
			return types.ProcessingOutcome{}, err
		}
		keys := make([]string, 0, len(prior))
		for slug := range prior {
			keys = append(keys, slug)
		}
		slices.Sort(keys)
		for _, slug := range keys {
			if len(prior[slug]) > 0 {
				existing = append(existing, prior[slug])
			}
		}
	}
	chunk := items[start:min(start+wikiTaxonomyPlanChunkSize, len(items))]
	var block strings.Builder
	for _, item := range chunk {
		fmt.Fprintf(&block, "- slug: %s | title: %s | type: %s | about: %s\n", item.slug, item.title, item.pageType, previewText(item.about, 120))
	}
	tree := formatExistingTaxonomyForPrompt(existing)
	if strings.TrimSpace(tree) == "" {
		tree = wikiTaxonomyEmptyTreeHint
	}
	model, err := e.s.modelService.GetChatModel(ctx, e.kb.SummaryModelID)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	raw, err := e.wikiIngest().generateWithTemplate(ctx, model, agent.WikiTaxonomyPlanPrompt, map[string]string{"ExistingTaxonomy": tree, "Items": block.String(), "Language": types.ResolveLanguageName(ctx, e.document.Language)})
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	paths, err := parseProcessingWikiTaxonomy(raw, chunk)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	return e.wikiSuccess(ctx, paths)
}

func parseProcessingWikiTaxonomy(raw string, items []wikiTaxonomyItem) (map[string][]string, error) {
	var parsed struct {
		Assignments []struct {
			Slug string   `json:"slug"`
			Path []string `json:"path"`
		} `json:"assignments"`
	}
	if len(raw) > 1<<20 || json.Unmarshal([]byte(cleanLLMJSON(raw)), &parsed) != nil || len(parsed.Assignments) != len(items) {
		return nil, errors.New("WIKI_TAXONOMY_INVALID")
	}
	allowed := map[string]bool{}
	for _, item := range items {
		allowed[item.slug] = true
	}
	out := map[string][]string{}
	for _, assignment := range parsed.Assignments {
		if !allowed[assignment.Slug] || assignment.Path == nil || len(assignment.Path) > types.WikiCategoryMaxDepth {
			return nil, errors.New("WIKI_TAXONOMY_INVALID")
		}
		delete(allowed, assignment.Slug)
		for _, name := range assignment.Path {
			if strings.TrimSpace(name) == "" || len(name) > 255 {
				return nil, errors.New("WIKI_TAXONOMY_INVALID")
			}
		}
		out[assignment.Slug] = types.CleanWikiCategoryPath(assignment.Path)
	}
	return out, nil
}

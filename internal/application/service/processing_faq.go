package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
)

type processingFAQInput struct {
	FingerprintBase string `json:"fingerprint_base,omitempty"`
	Row             int    `json:"row"`
	Start           int    `json:"start,omitempty"`
	Count           int    `json:"count,omitempty"`
}

func processingFAQInvalid(row int) types.ProcessingOutcome {
	message := "The FAQ source table has an invalid schema or entry"
	if row >= 0 {
		message = fmt.Sprintf("FAQ entry %d is invalid or conflicts with another question", row+1)
	}
	return types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "format", ErrorCode: "FAQ_SOURCE_INVALID", Message: message}
}

func validateProcessingFAQMeta(meta *types.FAQChunkMetadata) error {
	if meta == nil || meta.StandardQuestion == "" || len(meta.Answers) == 0 {
		return errors.New("FAQ_SOURCE_INVALID")
	}
	positives := append([]string{meta.StandardQuestion}, meta.SimilarQuestions...)
	for _, negative := range meta.NegativeQuestions {
		if slices.Contains(positives, negative) {
			return errors.New("FAQ_NEGATIVE_CONFLICT")
		}
	}
	return nil
}

func (e *processingDocumentExecution) faqStage(ctx context.Context) (types.ProcessingOutcome, error) {
	if e.kb.Type != types.KnowledgeBaseTypeFAQ {
		return types.ProcessingOutcome{}, repository.ErrProcessingScope
	}
	switch e.lease.Step.Stage {
	case "faq_prepare":
		var parsed processingParsed
		if err := e.dependency(ctx, "assets", "body", "assets", &parsed); err != nil {
			return types.ProcessingOutcome{}, err
		}
		metadata := map[string]string{}
		if e.document.Kind == "sheet" {
			metadata["sheet_export_mode"] = "cell_ranges"
		}
		entries, err := parseFAQFetchedItem(&types.FetchedItem{FileName: "source.md", Content: []byte(parsed.MarkdownContent), Metadata: metadata})
		if err != nil {
			return processingFAQInvalid(-1), nil
		}
		seen := map[string]bool{}
		for i := range entries {
			meta, err := sanitizeFAQEntryPayload(&entries[i])
			if err != nil || validateProcessingFAQMeta(meta) != nil {
				return processingFAQInvalid(i), nil
			}
			for _, q := range append([]string{meta.StandardQuestion}, meta.SimilarQuestions...) {
				if seen[q] {
					return processingFAQInvalid(i), nil
				}
				seen[q] = true
			}
		}
		return e.success(ctx, "faq_prepare", entries)
	case "faq_index":
		var entries []types.FAQEntryPayload
		if err := e.dependency(ctx, "faq_prepare", "body", "faq_prepare", &entries); err != nil {
			return types.ProcessingOutcome{}, err
		}
		if len(entries) == 0 {
			return processingFAQInvalid(0), nil
		}
		if !e.lease.Step.PlanSealed {
			var specs []types.ProcessingStepSpec
			for i := range entries {
				input, _ := json.Marshal(processingFAQInput{Row: i})
				specs = append(specs, types.ProcessingStepSpec{Stage: "faq_entry", UnitKey: fmt.Sprintf("%06d", i), Kind: "barrier", Phase: e.lease.Step.Phase, Input: input, InputFingerprint: processingFingerprint(e.lease.Step.InputFingerprint, i, entries[i])})
			}
			next := time.Now().UTC().Add(time.Second)
			return types.ProcessingOutcome{Status: types.ProcessingWaitingExternal, NextRunAt: &next, SealPlan: true, ChildSteps: specs}, nil
		}
		mutations := make([]types.ProcessingFAQMutation, 0, len(entries))
		for i := range entries {
			var mutation types.ProcessingFAQMutation
			if err := e.dependency(ctx, "faq_entry", fmt.Sprintf("%06d", i), "faq_entry", &mutation); err != nil {
				return types.ProcessingOutcome{}, err
			}
			mutations = append(mutations, mutation)
		}
		return e.success(ctx, "faq_index", mutations)
	case "faq_entry":
		return e.faqEntry(ctx)
	case "faq_embedding":
		return e.faqEmbedding(ctx)
	case "faq_write":
		return e.faqWrite(ctx)
	}
	return types.ProcessingOutcome{}, errors.New("FAQ_STAGE_INVALID")
}

func (e *processingDocumentExecution) faqProposal(ctx context.Context, step types.ProcessingStep) (types.ProcessingFAQMutation, error) {
	var proposal types.ProcessingFAQMutation
	var result struct {
		Digest string `json:"proposal_digest"`
	}
	if json.Unmarshal(step.Result, &result) != nil {
		return proposal, errors.New("FAQ_PROPOSAL_INVALID")
	}
	data, err := e.artifacts.Read(ctx, e.lease.Job, step, "faq_proposal", step.CheckpointRef, result.Digest)
	if err != nil {
		return proposal, err
	}
	if json.Unmarshal(data, &proposal) != nil || proposal.Chunk == nil || proposal.EntryStepID != step.ID || proposal.Attempt != step.Attempt ||
		proposal.Chunk.TenantID != e.lease.Job.TenantID || proposal.Chunk.KnowledgeBaseID != e.kb.ID || proposal.Chunk.ChunkType != types.ChunkTypeFAQ {
		return proposal, errors.New("FAQ_PROPOSAL_SCOPE_INVALID")
	}
	return proposal, nil
}

func (e *processingDocumentExecution) faqEntry(ctx context.Context) (types.ProcessingOutcome, error) {
	if !e.kb.IsVectorEnabled() && !e.kb.IsKeywordEnabled() {
		return types.ProcessingOutcome{Status: types.ProcessingBlocked, ErrorClass: "configuration", ErrorCode: "FAQ_INDEX_DISABLED", Message: "FAQ requires a vector or keyword index"}, nil
	}
	if !e.lease.Step.PlanSealed {
		var input processingFAQInput
		var entries []types.FAQEntryPayload
		if json.Unmarshal(e.lease.Step.Input, &input) != nil {
			return types.ProcessingOutcome{}, errors.New("FAQ_INPUT_INVALID")
		}
		if err := e.dependency(ctx, "faq_prepare", "body", "faq_prepare", &entries); err != nil {
			return types.ProcessingOutcome{}, err
		}
		if input.Row < 0 || input.Row >= len(entries) {
			return types.ProcessingOutcome{}, errors.New("FAQ_INPUT_INVALID")
		}
		payload := entries[input.Row]
		meta, err := sanitizeFAQEntryPayload(&payload)
		if err != nil {
			return processingFAQInvalid(input.Row), nil
		}
		existing, err := e.s.chunkRepo.FindFAQChunkWithDuplicateQuestion(ctx, e.lease.Job.TenantID, e.kb.ID, "", []string{meta.StandardQuestion})
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		proposal := types.ProcessingFAQMutation{EntryStepID: e.lease.Step.ID, Attempt: e.lease.Step.Attempt, NewEntry: existing == nil}
		if existing == nil {
			canonical, err := e.s.ensureFAQKnowledge(ctx, e.lease.Job.TenantID, e.kb)
			if err != nil {
				return types.ProcessingOutcome{}, err
			}
			tag, err := e.s.resolveTagID(ctx, e.kb.ID, &payload)
			if err != nil {
				return types.ProcessingOutcome{}, err
			}
			proposal.Chunk = &types.Chunk{ID: uuid.NewString(), TenantID: e.lease.Job.TenantID, KnowledgeBaseID: e.kb.ID, KnowledgeID: canonical.ID, ChunkType: types.ChunkTypeFAQ, IsEnabled: true, TagID: tag}
		} else {
			prior, err := existing.FAQMetadata()
			if err != nil || prior == nil || prior.StandardQuestion != meta.StandardQuestion {
				return processingFAQInvalid(input.Row), nil
			}
			meta.SimilarQuestions = unionStrings(prior.SimilarQuestions, meta.SimilarQuestions)
			meta.NegativeQuestions = unionStrings(prior.NegativeQuestions, meta.NegativeQuestions)
			meta.Version, meta.Source = prior.Version+1, prior.Source
			proposal.Chunk = existing
		}
		if validateProcessingFAQMeta(meta) != nil {
			return processingFAQInvalid(input.Row), nil
		}
		duplicate, err := e.s.chunkRepo.FindFAQChunkWithDuplicateQuestion(ctx, e.lease.Job.TenantID, e.kb.ID, proposal.Chunk.ID, append([]string{meta.StandardQuestion}, meta.SimilarQuestions...))
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		if duplicate != nil {
			return processingFAQInvalid(input.Row), nil
		}
		if payload.IsEnabled != nil {
			proposal.Chunk.IsEnabled = *payload.IsEnabled
		}
		if payload.IsRecommended != nil {
			if *payload.IsRecommended {
				proposal.Chunk.Flags = proposal.Chunk.Flags.SetFlag(types.ChunkFlagRecommended)
			} else {
				proposal.Chunk.Flags = proposal.Chunk.Flags.ClearFlag(types.ChunkFlagRecommended)
			}
		}
		if err := proposal.Chunk.SetFAQMetadata(meta); err != nil {
			return types.ProcessingOutcome{}, err
		}
		mode := types.FAQIndexModeQuestionAnswer
		if e.kb.FAQConfig != nil && e.kb.FAQConfig.IndexMode != "" {
			mode = e.kb.FAQConfig.IndexMode
		}
		proposal.Chunk.Content = buildFAQChunkContent(meta, mode)
		proposal.Chunk.Status, proposal.Chunk.IndexStatus = int(types.ChunkStatusIndexed), "indexed"
		var parsed processingParsed
		if err := e.dependency(ctx, "assets", "body", "assets", &parsed); err != nil {
			return types.ProcessingOutcome{}, err
		}
		for _, asset := range parsed.Assets {
			for _, answer := range meta.Answers {
				if strings.Contains(answer, asset.StoredURL) {
					proposal.ResourceRefs = append(proposal.ResourceRefs, asset.StoredURL)
					break
				}
			}
		}
		items, err := e.s.buildFAQIndexInfoList(ctx, e.kb, proposal.Chunk)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		data, err := json.Marshal(proposal)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		path, digest, err := e.artifacts.Save(ctx, e.lease.Job, e.lease.Step, "faq_proposal", data)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		var specs []types.ProcessingStepSpec
		for start := 0; start < len(items); start += processingEmbeddingBatchSize {
			count := min(processingEmbeddingBatchSize, len(items)-start)
			input.Start, input.Count = start, count
			input.FingerprintBase = e.lease.Step.InputFingerprint
			encoded, _ := json.Marshal(input)
			fingerprint := processingFAQBatchFingerprint(items[start:start+count], input.FingerprintBase)
			unit := fmt.Sprintf("%s/%d/%06d", e.lease.Step.UnitKey, e.lease.Step.Attempt, start/processingEmbeddingBatchSize)
			specs = append(specs, types.ProcessingStepSpec{Stage: "faq_embedding", UnitKey: unit, Phase: e.lease.Step.Phase, Input: encoded, InputFingerprint: fingerprint},
				types.ProcessingStepSpec{Stage: "faq_write", UnitKey: unit, Phase: e.lease.Step.Phase, Input: encoded, InputFingerprint: fingerprint, DependsOn: []string{"faq_embedding/" + unit}})
		}
		next := time.Now().UTC().Add(time.Second)
		result, _ := json.Marshal(map[string]string{"proposal_digest": digest})
		return types.ProcessingOutcome{Status: types.ProcessingWaitingExternal, NextRunAt: &next, CheckpointRef: path, Result: result, SealPlan: true, ChildSteps: specs}, nil
	}
	proposal, err := e.faqProposal(ctx, e.lease.Step)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	items, err := e.s.buildFAQIndexInfoList(ctx, e.kb, proposal.Chunk)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	digest, err := proposal.Chunk.FAQIndexContentDigest()
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	manifest := types.FAQIndexManifest{ContentDigest: digest}
	covered := make([]bool, len(items))
	for _, step := range e.steps {
		if step.ParentStepID != e.lease.Step.ID || step.Stage != "faq_write" {
			continue
		}
		var receipt types.FAQIndexManifest
		if err := e.dependency(ctx, step.Stage, step.UnitKey, "faq_write", &receipt); err != nil {
			return types.ProcessingOutcome{}, err
		}
		var batch processingFAQInput
		if json.Unmarshal(step.Input, &batch) != nil || batch.Start < 0 || batch.Count < 1 || batch.Start+batch.Count > len(items) ||
			receipt.ContentDigest != digest || len(receipt.SourceIDs) != batch.Count || len(receipt.WriteIDs) != 1 {
			return types.ProcessingOutcome{}, errors.New("FAQ_INDEX_MANIFEST_INVALID")
		}
		// Rebuild this attempt's exact expected keys; an arbitrary receipt cannot
		// claim coverage for another question, chunk, batch or worker attempt.
		expectedItems := make([]*types.IndexInfo, batch.Count)
		for i, item := range items[batch.Start : batch.Start+batch.Count] {
			copy := *item
			expectedItems[i] = &copy
		}
		expected, err := types.VersionFAQIndexes(fmt.Sprintf("%s/%d", step.ID, step.Attempt), proposal.Chunk, expectedItems)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		var wanted types.FAQIndexManifest
		_ = json.Unmarshal([]byte(expected), &wanted)
		if !slices.Equal(receipt.SourceIDs, wanted.SourceIDs) || !slices.Equal(receipt.WriteIDs, wanted.WriteIDs) {
			return types.ProcessingOutcome{}, errors.New("FAQ_INDEX_MANIFEST_INVALID")
		}
		for i := batch.Start; i < batch.Start+batch.Count; i++ {
			if covered[i] {
				return types.ProcessingOutcome{}, errors.New("FAQ_INDEX_COVERAGE_DUPLICATE")
			}
			covered[i] = true
		}
		manifest.SourceIDs = append(manifest.SourceIDs, receipt.SourceIDs...)
		manifest.WriteIDs = append(manifest.WriteIDs, receipt.WriteIDs...)
	}
	if slices.Contains(covered, false) {
		return types.ProcessingOutcome{}, errors.New("FAQ_INDEX_COVERAGE_INCOMPLETE")
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	proposal.Manifest = string(encoded)
	return e.success(ctx, "faq_entry", proposal)
}

// Only the exact provider inputs determine embedding reuse. Operational flags
// and a concurrently changed question in another batch do not invalidate it.
func processingFAQBatchDigest(items []*types.IndexInfo) string {
	values := make([][2]string, len(items))
	for i, item := range items {
		if item == nil {
			return ""
		}
		values[i] = [2]string{item.SourceID, item.Content}
	}
	data, _ := json.Marshal(values)
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

func processingFAQBatchFingerprint(items []*types.IndexInfo, base string) string {
	if base == "" {
		return processingFAQBatchDigest(items)
	}
	return processingFingerprint(base, processingFAQBatchDigest(items))
}

func (e *processingDocumentExecution) faqBatchFingerprint(items []*types.IndexInfo) string {
	var input processingFAQInput
	if json.Unmarshal(e.lease.Step.Input, &input) != nil {
		return ""
	}
	return processingFAQBatchFingerprint(items, input.FingerprintBase)
}

func (e *processingDocumentExecution) faqBatch(ctx context.Context) (types.ProcessingFAQMutation, []*types.IndexInfo, error) {
	var proposal types.ProcessingFAQMutation
	var parent *types.ProcessingStep
	for i := range e.steps {
		if e.steps[i].ID == e.lease.Step.ParentStepID && e.steps[i].Stage == "faq_entry" {
			parent = &e.steps[i]
			break
		}
	}
	if parent == nil || !parent.PlanSealed {
		return proposal, nil, errors.New("FAQ_PARENT_INVALID")
	}
	proposal, err := e.faqProposal(ctx, *parent)
	if err != nil {
		return proposal, nil, err
	}
	var batch processingFAQInput
	if json.Unmarshal(e.lease.Step.Input, &batch) != nil || batch.Start < 0 || batch.Count < 1 || batch.Count > processingEmbeddingBatchSize {
		return proposal, nil, errors.New("FAQ_BATCH_INVALID")
	}
	items, err := e.s.buildFAQIndexInfoList(ctx, e.kb, proposal.Chunk)
	if err != nil {
		return proposal, nil, err
	}
	if batch.Start+batch.Count > len(items) {
		return proposal, nil, errors.New("FAQ_BATCH_INVALID")
	}
	items = items[batch.Start : batch.Start+batch.Count]
	if processingFAQBatchFingerprint(items, batch.FingerprintBase) != e.lease.Step.InputFingerprint {
		return proposal, nil, errors.New("FAQ_BATCH_INPUT_CHANGED")
	}
	return proposal, items, nil
}

func (e *processingDocumentExecution) faqEmbedding(ctx context.Context) (types.ProcessingOutcome, error) {
	_, items, err := e.faqBatch(ctx)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	for _, prior := range e.steps {
		if prior.ID == e.lease.Step.ID || prior.Stage != "faq_embedding" || prior.InputFingerprint != e.lease.Step.InputFingerprint ||
			(prior.Status != types.ProcessingSucceeded && prior.Status != types.ProcessingSuperseded) || prior.OutputDigest == "" {
			continue
		}
		data, err := e.artifacts.Read(ctx, e.lease.Job, prior, "faq_embedding", prior.OutputManifestRef, prior.OutputDigest)
		if err != nil {
			continue
		} // An unavailable optional cache is recomputed.
		var saved []*types.IndexInfo
		if json.Unmarshal(data, &saved) != nil || len(saved) != len(items) || e.faqBatchFingerprint(saved) != e.lease.Step.InputFingerprint {
			continue
		}
		valid := true
		for i := range items {
			if saved[i] == nil || (e.kb.IsVectorEnabled() && len(saved[i].PreparedEmbedding) == 0) {
				valid = false
				break
			}
			items[i].PreparedEmbedding = saved[i].PreparedEmbedding
		}
		if valid {
			return e.success(ctx, "faq_embedding", items)
		}
	}
	if reused, err := e.reuseEmbeddings(ctx, items); err != nil {
		return types.ProcessingOutcome{}, err
	} else if reused {
		out, err := e.success(ctx, "faq_embedding", items)
		out.CopiedArtifact = true
		return out, err
	}
	if err := e.prepareEmbeddings(ctx, items); err != nil {
		return types.ProcessingOutcome{}, err
	}
	return e.success(ctx, "faq_embedding", items)
}

func (e *processingDocumentExecution) faqWrite(ctx context.Context) (outcome types.ProcessingOutcome, runErr error) {
	proposal, expected, err := e.faqBatch(ctx)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	var items []*types.IndexInfo
	if err := e.dependency(ctx, "faq_embedding", e.lease.Step.UnitKey, "faq_embedding", &items); err != nil {
		return types.ProcessingOutcome{}, err
	}
	if len(items) != len(expected) || e.faqBatchFingerprint(items) != e.lease.Step.InputFingerprint {
		return types.ProcessingOutcome{}, errors.New("FAQ_EMBEDDING_INVALID")
	}
	for _, item := range items {
		if item == nil || item.ChunkID != proposal.Chunk.ID || item.KnowledgeID != proposal.Chunk.KnowledgeID || item.KnowledgeBaseID != e.kb.ID || (e.kb.IsVectorEnabled() && len(item.PreparedEmbedding) == 0) {
			return types.ProcessingOutcome{}, errors.New("FAQ_EMBEDDING_SCOPE_INVALID")
		}
	}
	encoded, err := types.VersionFAQIndexes(fmt.Sprintf("%s/%d", e.lease.Step.ID, e.lease.Step.Attempt), proposal.Chunk, items)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	var manifest types.FAQIndexManifest
	_ = json.Unmarshal([]byte(encoded), &manifest)
	destination, err := e.indexDestination(ctx, len(items[0].PreparedEmbedding))
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	if err := e.repo.RecordIndexDestination(ctx, e.lease.Job.TenantID, e.lease, destination); err != nil {
		return types.ProcessingOutcome{}, err
	}
	e.lease.Job.IndexDestination, _ = json.Marshal(destination)
	engine, err := e.indexEngine(ctx)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	keys, _ := json.Marshal(manifest.SourceIDs)
	write := types.FAQIndexWrite{ID: manifest.WriteIDs[0], TenantID: e.lease.Job.TenantID, KnowledgeBaseID: e.kb.ID, KnowledgeID: proposal.Chunk.KnowledgeID, ChunkID: proposal.Chunk.ID,
		BaseRevision: proposal.Chunk.ContentRevision, NewEntry: proposal.NewEntry, ContentDigest: manifest.ContentDigest, SourceIDs: keys, Destination: e.lease.Job.IndexDestination,
		EstimatedBytes: engine.EstimateStorageSize(ctx, nil, items)}
	if err := e.s.chunkRepo.RegisterFAQIndexWrites(ctx, []types.FAQIndexWrite{write}); err != nil {
		if errors.Is(err, repository.ErrChunkRevisionConflict) {
			return types.ProcessingOutcome{Status: types.ProcessingFailed, ErrorClass: "conflict", ErrorCode: "FAQ_ENTRY_CHANGED", Retryable: true, FAQConflicts: []string{proposal.EntryStepID}}, nil
		}
		return types.ProcessingOutcome{}, err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		if err := e.repo.ValidateLease(cleanup, e.lease.Job.TenantID, e.lease); err != nil {
			outcome = types.ProcessingOutcome{}
			runErr = errors.Join(err, engine.DeleteBySourceIDList(cleanup, manifest.SourceIDs, destination.Dimension, destination.KnowledgeType))
		}
	}()
	if err := engine.BatchIndex(ctx, nil, items); err != nil {
		return types.ProcessingOutcome{}, err
	}
	if err := e.s.chunkRepo.ConfirmFAQIndexWrites(ctx, e.lease.Job.TenantID, manifest.WriteIDs); err != nil {
		return types.ProcessingOutcome{}, err
	}
	return e.success(ctx, "faq_write", manifest)
}

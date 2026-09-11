package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/Tencent/WeKnora/internal/application/service/retriever"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
)

// Each provider batch has its own durable embedding output. A failed index
// write reads that output and never calls the embedding provider again.
const processingEmbeddingBatchSize = 32

type processingIndexBatch struct {
	Stage string `json:"stage"`
	// The persisted name also reads earlier text/image batches. Questions use
	// their own UUID as the unit while ChunkID still points to the answer text.
	UnitIDs []string `json:"chunk_ids"`
}

func (e *processingDocumentExecution) indexChunks(ctx context.Context, stage string) ([]*types.Chunk, error) {
	if stage != "chunk" && stage != "summary" && stage != "images" {
		return nil, errors.New("INDEX_SOURCE_INVALID")
	}
	var chunks []*types.Chunk
	legacyIndexed := map[string]bool{}
	if e.document.LegacyEvidenceID != "" && (stage == "chunk" || stage == "images") {
		var legacy processingLegacyManifest
		if err := e.dependency(ctx, "legacy_snapshot", "body", "legacy_snapshot", &legacy); err != nil {
			return nil, err
		}
		chunks = legacy.Chunks
		for _, item := range legacy.Indexes {
			legacyIndexed[item.ChunkID] = true
		}
	} else {
		if err := e.dependency(ctx, stage, "body", stage, &chunks); err != nil {
			return nil, err
		}
	}
	result := make([]*types.Chunk, 0, len(chunks))
	for _, chunk := range chunks {
		if chunk == nil || chunk.TenantID != e.lease.Job.TenantID || chunk.KnowledgeBaseID != e.kb.ID || chunk.KnowledgeID != e.lease.Job.KnowledgeID {
			return nil, errors.New("INDEX_CHUNK_SCOPE_INVALID")
		}
		if (stage == "chunk" && chunk.ChunkType == types.ChunkTypeText) || (stage == "summary" && chunk.ChunkType == types.ChunkTypeSummary) || (stage == "images" && (chunk.ChunkType == types.ChunkTypeImageOCR || chunk.ChunkType == types.ChunkTypeImageCaption)) {
			if e.document.LegacyEvidenceID != "" && stage == "chunk" && !legacyIndexed[chunk.ID] {
				continue
			}
			result = append(result, chunk)
		}
	}
	if len(result) == 0 && stage != "images" {
		return nil, errors.New("INDEX_COVERAGE_EMPTY")
	}
	return result, nil
}

func (e *processingDocumentExecution) indexBarrier(ctx context.Context) (types.ProcessingOutcome, error) {
	stage := "chunk"
	if e.lease.Step.Stage == "legacy_indexes" {
		stage = "legacy"
		var legacy processingLegacyManifest
		if err := e.dependency(ctx, "legacy_snapshot", "body", "legacy_snapshot", &legacy); err != nil {
			return types.ProcessingOutcome{}, err
		}
		if legacy.Reembed {
			stage = "legacy_reembed"
		}
	}
	if e.lease.Step.Stage == "summary_index" {
		stage = "summary"
	}
	if e.lease.Step.Stage == "image_index" {
		stage = "images"
	}
	if e.lease.Step.Stage == "question_index" {
		stage = "questions"
	}
	items, err := e.indexInputs(ctx, stage)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	if len(items) == 0 || (!e.kb.IsVectorEnabled() && !e.kb.IsKeywordEnabled()) {
		outcome, err := e.success(ctx, "index_coverage", map[string]any{"disabled_by_configuration": true, "entries": len(items)})
		outcome.SealPlan = !e.lease.Step.PlanSealed
		return outcome, err
	}
	if !e.lease.Step.PlanSealed {
		var specs []types.ProcessingStepSpec
		for start := 0; start < len(items); start += processingEmbeddingBatchSize {
			batch := processingIndexBatch{Stage: stage}
			for _, item := range items[start:min(start+processingEmbeddingBatchSize, len(items))] {
				batch.UnitIDs = append(batch.UnitIDs, item.SourceID)
			}
			input, err := json.Marshal(batch)
			if err != nil {
				return types.ProcessingOutcome{}, err
			}
			unit := fmt.Sprintf("%s/%06d", stage, start/processingEmbeddingBatchSize)
			var contents []string
			for _, item := range items[start:min(start+processingEmbeddingBatchSize, len(items))] {
				contents = append(contents, item.Content)
			}
			fingerprint := processingFingerprint(e.lease.Step.InputFingerprint, stage, contents)
			index := types.ProcessingStepSpec{Stage: "index", UnitKey: unit, Phase: e.lease.Step.Phase, Input: input, InputFingerprint: fingerprint}
			if stage != "legacy" {
				specs = append(specs, types.ProcessingStepSpec{Stage: "embedding", UnitKey: unit, Phase: e.lease.Step.Phase, Input: input, InputFingerprint: fingerprint})
				index.DependsOn = []string{"embedding/" + unit}
			}
			specs = append(specs, index)
		}
		next := time.Now().UTC().Add(time.Second)
		return types.ProcessingOutcome{Status: types.ProcessingWaitingExternal, NextRunAt: &next, SealPlan: true, ChildSteps: specs}, nil
	}
	expected := map[string]bool{}
	parents := map[string]string{}
	for _, item := range items {
		if _, exists := expected[item.SourceID]; exists {
			return types.ProcessingOutcome{}, errors.New("INDEX_DUPLICATE_CHUNK")
		}
		expected[item.SourceID], parents[item.SourceID] = false, item.ChunkID
	}
	for _, step := range e.steps {
		if step.ParentStepID != e.lease.Step.ID || step.Stage != "index" {
			continue
		}
		var confirmed []*types.IndexInfo
		if err := e.dependency(ctx, "index", step.UnitKey, "index", &confirmed); err != nil {
			return types.ProcessingOutcome{}, err
		}
		var batch processingIndexBatch
		if json.Unmarshal(step.Input, &batch) != nil || batch.Stage != stage || len(batch.UnitIDs) != len(confirmed) {
			return types.ProcessingOutcome{}, errors.New("INDEX_MANIFEST_INVALID")
		}
		for i, item := range confirmed {
			if item == nil {
				return types.ProcessingOutcome{}, errors.New("INDEX_MANIFEST_INVALID")
			}
			unit := batch.UnitIDs[i]
			seen, exists := expected[unit]
			stamp, err := types.ProcessingIndexSourceID(step.ID, step.Attempt, unit)
			if !exists || seen || err != nil || stamp != item.SourceID || item.ChunkID != parents[unit] || item.KnowledgeID != e.lease.Job.KnowledgeID || item.KnowledgeBaseID != e.kb.ID {
				return types.ProcessingOutcome{}, errors.New("INDEX_MANIFEST_COVERAGE_INVALID")
			}
			expected[unit] = true
		}
	}
	for _, present := range expected {
		if !present {
			return types.ProcessingOutcome{}, errors.New("INDEX_COVERAGE_INCOMPLETE")
		}
	}
	uniqueChunks := map[string]bool{}
	for _, id := range parents {
		uniqueChunks[id] = true
	}
	return e.success(ctx, "index_coverage", map[string]int{"confirmed_entries": len(expected), "confirmed_chunks": len(uniqueChunks)})
}

func (e *processingDocumentExecution) indexInputs(ctx context.Context, stage string) ([]*types.IndexInfo, error) {
	if stage == "legacy" || stage == "legacy_reembed" {
		var legacy processingLegacyManifest
		if e.document.LegacyEvidenceID == "" {
			return nil, errors.New("LEGACY_EVIDENCE_REQUIRED")
		}
		if err := e.dependency(ctx, "legacy_snapshot", "body", "legacy_snapshot", &legacy); err != nil {
			return nil, err
		}
		if (stage == "legacy_reembed") != legacy.Reembed {
			return nil, errors.New("LEGACY_INDEX_MODEL_PROOF_INVALID")
		}
		enabled := map[string]bool{}
		for _, chunk := range legacy.Chunks {
			enabled[chunk.ID] = chunk.IsEnabled
		}
		for _, item := range legacy.Indexes {
			item.IsEnabled = enabled[item.ChunkID]
		}
		return legacy.Indexes, nil
	}
	knowledge, err := e.s.repo.GetKnowledgeByID(ctx, e.lease.Job.TenantID, e.lease.Job.KnowledgeID)
	if err != nil {
		return nil, err
	}
	var items []*types.IndexInfo
	add := func(id, chunkID, content string) {
		items = append(items, &types.IndexInfo{SourceID: id, Content: buildKnowledgeIndexContent(knowledge, content), SourceType: types.ChunkSourceType,
			ChunkID: chunkID, KnowledgeID: knowledge.ID, KnowledgeBaseID: knowledge.KnowledgeBaseID, IsEnabled: true})
	}
	if stage == "questions" {
		var groups []types.ProcessingChunkQuestions
		if err := e.dependency(ctx, "questions", "body", "questions", &groups); err != nil {
			return nil, err
		}
		chunks, err := e.indexChunks(ctx, "chunk")
		if err != nil {
			return nil, err
		}
		parents := map[string]int{}
		for _, chunk := range chunks {
			parents[chunk.ID] = chunk.ContentRevision
		}
		for _, group := range groups {
			revision, exists := parents[group.ChunkID]
			if !exists || revision != group.ContentRevision || len(group.Questions) == 0 {
				return nil, errors.New("QUESTION_INDEX_SCOPE_INVALID")
			}
			delete(parents, group.ChunkID)
			for _, question := range group.Questions {
				add(question.ID, group.ChunkID, question.Question)
			}
		}
		if len(parents) != 0 {
			return nil, errors.New("QUESTION_INDEX_COVERAGE_INCOMPLETE")
		}
	} else {
		chunks, err := e.indexChunks(ctx, stage)
		if err != nil {
			return nil, err
		}
		for _, chunk := range chunks {
			add(chunk.ID, chunk.ID, chunk.EmbeddingContent())
		}
	}
	return items, nil
}

func (e *processingDocumentExecution) embedBatch(ctx context.Context) (types.ProcessingOutcome, error) {
	var batch processingIndexBatch
	if err := json.Unmarshal(e.lease.Step.Input, &batch); err != nil || len(batch.UnitIDs) == 0 || len(batch.UnitIDs) > processingEmbeddingBatchSize {
		return types.ProcessingOutcome{}, errors.New("EMBEDDING_BATCH_INVALID")
	}
	entries, err := e.indexInputs(ctx, batch.Stage)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	byID := map[string]*types.IndexInfo{}
	for _, entry := range entries {
		byID[entry.SourceID] = entry
	}
	var items []*types.IndexInfo
	for _, id := range batch.UnitIDs {
		item := byID[id]
		if item == nil {
			return types.ProcessingOutcome{}, errors.New("EMBEDDING_CHUNK_MISSING")
		}
		delete(byID, id)
		items = append(items, item)
	}
	if reused, err := e.reuseEmbeddings(ctx, items); err != nil {
		return types.ProcessingOutcome{}, err
	} else if reused {
		out, err := e.success(ctx, "embedding", items)
		out.CopiedArtifact = true
		return out, err
	}
	if err := e.prepareEmbeddings(ctx, items); err != nil {
		return types.ProcessingOutcome{}, err
	}
	return e.success(ctx, "embedding", items)
}

func (e *processingDocumentExecution) prepareEmbeddings(ctx context.Context, items []*types.IndexInfo) error {
	if len(items) == 0 || len(items) > processingEmbeddingBatchSize {
		return errors.New("EMBEDDING_BATCH_INVALID")
	}
	var inputs []string
	for _, item := range items {
		if item == nil {
			return errors.New("EMBEDDING_ITEM_INVALID")
		}
		input, err := retriever.PrepareProcessingEmbeddingInput(ctx, item.Content)
		if err != nil {
			return err
		}
		inputs = append(inputs, input)
	}
	if e.kb.IsVectorEnabled() {
		model, err := e.s.modelService.GetEmbeddingModel(ctx, e.kb.EmbeddingModelID)
		if err != nil {
			return err
		}
		vectors, err := model.BatchEmbed(ctx, inputs)
		if err != nil {
			return err
		}
		if len(vectors) != len(items) {
			return errors.New("EMBEDDING_COUNT_MISMATCH")
		}
		for i, vector := range vectors {
			if len(vector) == 0 || len(vector) != model.GetDimensions() {
				return errors.New("EMBEDDING_DIMENSION_MISMATCH")
			}
			for _, value := range vector {
				if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
					return errors.New("EMBEDDING_VALUE_INVALID")
				}
			}
			items[i].PreparedEmbedding = vector
		}
	}
	return nil
}

func (e *processingDocumentExecution) indexBatch(ctx context.Context) (outcome types.ProcessingOutcome, runErr error) {
	var items []*types.IndexInfo
	var batch processingIndexBatch
	if err := json.Unmarshal(e.lease.Step.Input, &batch); err != nil || len(batch.UnitIDs) == 0 || len(batch.UnitIDs) > processingEmbeddingBatchSize {
		return types.ProcessingOutcome{}, errors.New("INDEX_BATCH_INVALID")
	}
	if batch.Stage == "legacy" {
		entries, err := e.indexInputs(ctx, "legacy")
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		byID := map[string]*types.IndexInfo{}
		for _, item := range entries {
			byID[item.SourceID] = item
		}
		for _, id := range batch.UnitIDs {
			item := byID[id]
			if item == nil {
				return types.ProcessingOutcome{}, errors.New("LEGACY_INDEX_BATCH_INVALID")
			}
			delete(byID, id)
			items = append(items, item)
		}
	} else if err := e.dependency(ctx, "embedding", e.lease.Step.UnitKey, "embedding", &items); err != nil {
		return types.ProcessingOutcome{}, err
	}
	if len(items) == 0 || len(items) != len(batch.UnitIDs) {
		return types.ProcessingOutcome{}, errors.New("INDEX_BATCH_INVALID")
	}
	for i, item := range items {
		if item == nil || item.KnowledgeID != e.lease.Job.KnowledgeID || item.KnowledgeBaseID != e.kb.ID ||
			(batch.Stage != "questions" && item.ChunkID != batch.UnitIDs[i]) || (batch.Stage == "questions" && (item.SourceID != batch.UnitIDs[i] || item.ChunkID == "")) {
			return types.ProcessingOutcome{}, errors.New("INDEX_BATCH_SCOPE_INVALID")
		}
		var err error
		item.SourceID, err = types.ProcessingIndexSourceID(e.lease.Step.ID, e.lease.Ref.Attempt, batch.UnitIDs[i])
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		if e.kb.IsVectorEnabled() && len(item.PreparedEmbedding) == 0 {
			return types.ProcessingOutcome{}, errors.New("INDEX_EMBEDDING_MISSING")
		}
	}
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
	if err := e.repo.ReserveIndexStorage(ctx, e.lease.Job.TenantID, e.lease, engine.EstimateStorageSize(ctx, nil, items)); err != nil {
		return types.ProcessingOutcome{}, err
	}
	// PreparedEmbedding is mandatory above for vector writes; a nil embedder
	// ensures an index retry cannot accidentally make a fresh provider call.
	defer func() {
		// A provider can finish after supersession or a cancelled HTTP request.
		// Compensate only this uncommitted attempt, using the captured engine.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		if err := e.repo.ValidateLease(cleanupCtx, e.lease.Job.TenantID, e.lease); err != nil {
			ids := make([]string, 0, len(items))
			for _, item := range items {
				ids = append(ids, item.SourceID)
			}
			outcome = types.ProcessingOutcome{}
			runErr = errors.Join(err, engine.DeleteBySourceIDList(cleanupCtx, ids, destination.Dimension, destination.KnowledgeType))
		}
	}()
	if err := engine.BatchIndex(ctx, nil, items); err != nil {
		return types.ProcessingOutcome{}, err
	}
	outcome, err = e.success(ctx, "index", items)
	seen := map[string]bool{}
	for _, item := range items {
		if !seen[item.ChunkID] {
			outcome.IndexedChunkIDs = append(outcome.IndexedChunkIDs, item.ChunkID)
			seen[item.ChunkID] = true
		}
	}
	return outcome, err
}

func (e *processingDocumentExecution) indexEngine(ctx context.Context) (*retriever.CompositeRetrieveEngine, error) {
	if len(e.lease.Job.IndexDestination) > 0 {
		var destination types.ProcessingIndexDestination
		if err := json.Unmarshal(e.lease.Job.IndexDestination, &destination); err != nil {
			return nil, errors.New("INDEX_DESTINATION_INVALID")
		}
		return processingIndexEngine(ctx, e.s, e.lease.Job.TenantID, destination)
	}
	return nil, errors.New("INDEX_DESTINATION_UNVERIFIED")
}

func (e *processingDocumentExecution) indexDestination(ctx context.Context, dimension int) (types.ProcessingIndexDestination, error) {
	var destination types.ProcessingIndexDestination
	if len(e.lease.Job.IndexDestination) > 0 {
		if json.Unmarshal(e.lease.Job.IndexDestination, &destination) != nil || destination.Dimension != dimension {
			return destination, errors.New("INDEX_DESTINATION_INVALID")
		}
		return destination, nil
	}
	return knowledgeIndexDestination(ctx, e.s, e.kb, dimension)
}

func knowledgeIndexDestination(ctx context.Context, s *knowledgeService, kb *types.KnowledgeBase, dimension int) (types.ProcessingIndexDestination, error) {
	var destination types.ProcessingIndexDestination
	tenant, ok := types.TenantInfoFromContext(ctx)
	if !ok || tenant.ID != kb.TenantID {
		return destination, errors.New("INDEX_TENANT_MISSING")
	}
	destination = types.ProcessingIndexDestination{VectorStoreID: kb.VectorStoreID, Engines: tenant.GetEffectiveEngines(), Dimension: dimension, KnowledgeType: kb.Type}
	if kb.IsVectorEnabled() {
		destination.Kinds = append(destination.Kinds, types.VectorRetrieverType)
	}
	if kb.IsKeywordEnabled() {
		destination.Kinds = append(destination.Kinds, types.KeywordsRetrieverType)
	}
	if destination.VectorStoreID == nil || *destination.VectorStoreID == "" {
		var err error
		destination.EnvironmentDigest, err = retriever.ProcessingEnvDestination(s.retrieveEngine, destination.Engines, destination.Kinds)
		if err != nil {
			return destination, err
		}
	}
	return destination, nil
}

func processingIndexEngine(ctx context.Context, s *knowledgeService, tenant uint64, destination types.ProcessingIndexDestination) (*retriever.CompositeRetrieveEngine, error) {
	if len(destination.Kinds) == 0 || destination.KnowledgeType == "" {
		return nil, errors.New("INDEX_DESTINATION_INVALID")
	}
	if destination.VectorStoreID == nil || *destination.VectorStoreID == "" {
		if destination.EnvironmentDigest == "" {
			return nil, errors.New("INDEX_DESTINATION_UNVERIFIED")
		}
		digest, err := retriever.ProcessingEnvDestination(s.retrieveEngine, destination.Engines, destination.Kinds)
		if err != nil {
			return nil, err
		}
		if digest != destination.EnvironmentDigest {
			return nil, errors.New("INDEX_DESTINATION_CHANGED")
		}
	}
	engine, err := retriever.CreateRetrieveEngineFromPayload(ctx, s.retrieveEngine, s.ownership, tenant, destination.Engines, destination.VectorStoreID)
	if err != nil {
		return nil, err
	}
	return engine.ForRetrieverTypes(destination.Kinds)
}

func (e *processingDocumentExecution) summarize(ctx context.Context) (types.ProcessingOutcome, error) {
	chunks, err := e.indexChunks(ctx, "chunk")
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	knowledge, err := e.s.repo.GetKnowledgeByID(ctx, e.lease.Job.TenantID, e.lease.Job.KnowledgeID)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	var summary string
	data, _, _, reused, err := e.reusableBytes(ctx, "summary")
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	if reused {
		var saved []*types.Chunk
		if json.Unmarshal(data, &saved) != nil || len(saved) != 1 || saved[0] == nil || saved[0].Content == "" {
			return types.ProcessingOutcome{}, errors.New("REUSE_SUMMARY_INVALID")
		}
		summary = saved[0].Content
	} else {
		model, err := e.s.modelService.GetChatModel(ctx, e.kb.SummaryModelID)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		summary, err = e.s.getSummary(ctx, model, knowledge, chunks)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
	}
	stamp, _ := json.Marshal(map[string]any{"processing_job_id": e.lease.Job.ID, "processing_step_id": e.lease.Step.ID, "processing_attempt": fmt.Sprint(e.lease.Ref.Attempt)})
	chunk := &types.Chunk{ID: uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("%s/%d/summary", e.lease.Step.ID, e.lease.Ref.Attempt))).String(),
		TenantID: knowledge.TenantID, KnowledgeBaseID: knowledge.KnowledgeBaseID, KnowledgeID: knowledge.ID,
		Content: summary, SourceContent: summary, ChunkType: types.ChunkTypeSummary, IsEnabled: true, Status: int(types.ChunkStatusStored), IndexStatus: "processing", Metadata: stamp}
	outcome, err := e.success(ctx, "summary", []*types.Chunk{chunk})
	outcome.Chunks, outcome.Description = []*types.Chunk{chunk}, &summary
	outcome.CopiedArtifact = reused
	return outcome, err
}

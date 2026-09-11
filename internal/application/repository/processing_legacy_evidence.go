package repository

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type ProcessingLegacyError struct {
	Identity types.ProcessingLegacyIdentity
	Error    types.SyncItemError
	// Only server verification sees these fields. RetryAttempt is a different
	// counter and is never substituted for the original processing attempt.
	KnowledgeID    string
	SourceRevision string
	Attempt        int
}

type ProcessingLegacySnapshot struct {
	Knowledge types.Knowledge
	Chunks    []*types.Chunk
	Spans     []types.KnowledgeProcessingSpan
	Attempt   int
	Digest    string
}

func (r *ProcessingRepository) InspectLegacyError(ctx context.Context, identity types.ProcessingLegacyIdentity) (*ProcessingLegacyError, error) {
	if identity.TenantID == 0 || identity.KnowledgeBaseID == "" || identity.DataSourceID == "" || identity.RunID == "" || identity.ErrorOrdinal < 1 {
		return nil, ErrProcessingScope
	}
	var source types.DataSource
	if err := r.db.WithContext(ctx).Where("id = ? AND tenant_id = ? AND knowledge_base_id = ? AND type = ?", identity.DataSourceID, identity.TenantID, identity.KnowledgeBaseID, types.ConnectorTypeTencentDocs).Take(&source).Error; err != nil {
		return nil, err
	}
	var encoded string
	query := r.db.WithContext(ctx).Table("sync_logs").Where("id = ? AND tenant_id = ? AND data_source_id = ?", identity.RunID, identity.TenantID, identity.DataSourceID)
	if r.db.Dialector.Name() == "postgres" {
		query = query.Select("COALESCE((result->'errors'->CAST(? AS INTEGER))::text, '')", identity.ErrorOrdinal-1)
	} else {
		path := fmt.Sprintf("$.errors[%d]", identity.ErrorOrdinal-1)
		query = query.Select("COALESCE(json_quote(json_extract(result, ?)), '')", path)
	}
	if err := query.Scan(&encoded).Error; err != nil {
		return nil, err
	}
	if encoded == "" || encoded == "null" {
		return nil, gorm.ErrRecordNotFound
	}
	result := ProcessingLegacyError{Identity: identity}
	if json.Unmarshal([]byte(encoded), &result.Error) != nil {
		return nil, errors.New("LEGACY_ERROR_INVALID")
	}
	result.Identity.ErrorDigest = fmt.Sprintf("%x", sha256.Sum256([]byte(encoded)))
	result.Identity.ExternalID, result.Identity.FileID = result.Error.ExternalID, result.Error.FileID
	var binding struct {
		KnowledgeID    string `json:"knowledge_id"`
		SourceRevision string `json:"source_revision"`
		Attempt        int    `json:"processing_attempt"`
	}
	_ = json.Unmarshal([]byte(encoded), &binding)
	result.KnowledgeID, result.SourceRevision, result.Attempt = binding.KnowledgeID, binding.SourceRevision, binding.Attempt
	if (identity.ErrorDigest != "" && identity.ErrorDigest != result.Identity.ErrorDigest) ||
		(identity.ExternalID != "" && identity.ExternalID != result.Identity.ExternalID) || (identity.FileID != "" && identity.FileID != result.Identity.FileID) {
		return nil, ErrProcessingConflict
	}
	return &result, nil
}

// The digest includes the exact current DB artifacts, not timestamps used as a
// proxy for source ingestion. It is checked again in the ownership transaction.
func (r *ProcessingRepository) LegacySnapshot(ctx context.Context, identity types.ProcessingLegacyIdentity, knowledgeID, revision string, attempt int) (*ProcessingLegacySnapshot, error) {
	return r.legacySnapshot(ctx, identity, knowledgeID, revision, attempt, "")
}

func (r *ProcessingRepository) legacySnapshot(ctx context.Context, identity types.ProcessingLegacyIdentity, knowledgeID, revision string, attempt int, owner string) (*ProcessingLegacySnapshot, error) {
	if knowledgeID == "" || revision == "" || attempt < 1 || (identity.ExternalID == "" && identity.FileID == "") {
		return nil, errors.New("LEGACY_IDENTITY_UNVERIFIED")
	}
	var out ProcessingLegacySnapshot
	err := r.db.WithContext(ctx).Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", knowledgeID, identity.TenantID, identity.KnowledgeBaseID).Take(&out.Knowledge).Error
	if err != nil {
		return nil, err
	}
	metadata := out.Knowledge.GetMetadata()
	if (metadata["processing_protocol"] == "2" && (owner == "" || metadata["processing_job_id"] != owner)) || (owner != "" && metadata["processing_job_id"] != owner) || metadata["datasource_id"] != identity.DataSourceID || metadata["datasource_version"] != revision ||
		(identity.ExternalID != "" && metadata["external_id"] != identity.ExternalID) || (identity.FileID != "" && metadata["file_id"] != identity.FileID) {
		return nil, ErrProcessingConflict
	}
	if err := r.db.WithContext(ctx).Model(&types.KnowledgeProcessingSpan{}).Where("knowledge_id = ?", knowledgeID).Select("COALESCE(MAX(attempt), 0)").Scan(&out.Attempt).Error; err != nil {
		return nil, err
	}
	if out.Attempt != attempt {
		return nil, ErrProcessingConflict
	}
	if out.Knowledge.FileSize > 100<<20 {
		return nil, errors.New("LEGACY_FILE_SIZE_EXCEEDED")
	}
	for _, check := range []struct {
		model   any
		fields  []string
		attempt bool
	}{
		{&types.Chunk{}, []string{"content", "source_content", "image_info", "metadata"}, false},
		{&types.KnowledgeProcessingSpan{}, []string{"input", "output", "metadata", "error_message", "error_detail"}, true},
	} {
		var terms []string
		for _, field := range check.fields {
			term := "LENGTH(CAST(COALESCE(" + field + ",'') AS BLOB))"
			if r.db.Dialector.Name() == "postgres" {
				term = "OCTET_LENGTH(COALESCE(CAST(" + field + " AS TEXT),''))"
			}
			terms = append(terms, term)
		}
		query := r.db.WithContext(ctx).Model(check.model).Where("knowledge_id = ?", knowledgeID)
		if check.attempt {
			query = query.Where("attempt = ?", attempt)
		}
		var bytes int64
		if err := query.Select("COALESCE(SUM(" + strings.Join(terms, "+") + "),0)").Scan(&bytes).Error; err != nil {
			return nil, err
		}
		if bytes > 100<<20 {
			return nil, errors.New("LEGACY_DATABASE_ARTIFACTS_TOO_LARGE")
		}
	}
	if err := r.db.WithContext(ctx).Where("knowledge_id = ? AND attempt = ?", knowledgeID, attempt).Order("id").Limit(100001).Find(&out.Spans).Error; err != nil {
		return nil, err
	}
	if err := r.db.WithContext(ctx).Where("knowledge_id = ? AND tenant_id = ? AND knowledge_base_id = ?", knowledgeID, identity.TenantID, identity.KnowledgeBaseID).Order("id").Limit(100001).Find(&out.Chunks).Error; err != nil {
		return nil, err
	}
	if len(out.Spans) == 0 || len(out.Spans) > 100000 || len(out.Chunks) == 0 || len(out.Chunks) > 100000 {
		return nil, errors.New("LEGACY_COVERAGE_INVALID")
	}
	digestKnowledge := out.Knowledge
	digestKnowledge.UpdatedAt = time.Time{}
	if owner != "" {
		var values map[string]any
		if json.Unmarshal(digestKnowledge.Metadata, &values) != nil {
			return nil, ErrProcessingConflict
		}
		delete(values, "processing_protocol")
		delete(values, "processing_job_id")
		digestKnowledge.Metadata, err = json.Marshal(values)
		if err != nil {
			return nil, err
		}
	}
	data, err := json.Marshal([]any{digestKnowledge, out.Spans, out.Chunks})
	if err != nil {
		return nil, err
	}
	// JSONB canonicalizes object order on write. Normalize nested JSON for
	// identical pre/post ownership digests without using mutable timestamps.
	var canonical any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&canonical); err != nil {
		return nil, err
	}
	data, err = json.Marshal(canonical)
	if err != nil {
		return nil, err
	}
	out.Digest = fmt.Sprintf("%x", sha256.Sum256(data))
	return &out, nil
}

func legacyDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}

// ArtifactDigest is produced by the service after reading the full file and
// actual index vectors. This boundary rechecks DB ownership and the original
// error before appending the resolution; it performs no remote I/O.
func (r *ProcessingRepository) RecordLegacyCompletion(ctx context.Context, evidence types.ProcessingLegacyEvidence) (*types.ProcessingLegacyEvidence, error) {
	if evidence.Action != "late_completion" && evidence.Action != "manual_confirmed" {
		return nil, ErrProcessingConflict
	}
	var result *types.ProcessingLegacyEvidence
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		configuration, err := lockProcessingConfiguration(tx, evidence.TenantID, evidence.KnowledgeBaseID)
		if err != nil {
			return err
		}
		if configuration != evidence.ConfigurationRevision {
			return ErrProcessingScope
		}
		var source types.DataSource
		query := tx.Where("id = ? AND tenant_id = ? AND knowledge_base_id = ?", evidence.DataSourceID, evidence.TenantID, evidence.KnowledgeBaseID)
		if tx.Dialector.Name() == "postgres" {
			query = query.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		if err := query.Take(&source).Error; err != nil {
			return err
		}
		if source.Status == types.DataSourceStatusDeleted {
			return ErrProcessingScope
		}
		repo := NewProcessingRepository(tx)
		original, err := repo.InspectLegacyError(ctx, evidence.ProcessingLegacyIdentity)
		if err != nil {
			return err
		}
		if original.Identity != evidence.ProcessingLegacyIdentity {
			return ErrProcessingConflict
		}
		if original.Error.Stage != "ingest" || original.Error.Code == "EXPORT_START_UNCERTAIN" || original.Error.Category == "EXPORT_START_UNCERTAIN" {
			return errors.New("LEGACY_ERROR_NOT_COMPLETION_WAIT")
		}
		if (original.KnowledgeID != "" && original.KnowledgeID != evidence.KnowledgeID) ||
			(original.SourceRevision != "" && original.SourceRevision != evidence.SourceRevision) || (original.Attempt != 0 && original.Attempt != evidence.Attempt) {
			return ErrProcessingConflict
		}
		if evidence.Action == "late_completion" && (original.KnowledgeID == "" || original.SourceRevision == "" || original.Attempt < 1) {
			return errors.New("LEGACY_ATTEMPT_UNATTRIBUTED")
		}
		// A completed row by itself is insufficient. The exact same attempt's
		// canonical stages and all persisted artifacts must still match.
		snapshot, err := repo.LegacySnapshot(ctx, evidence.ProcessingLegacyIdentity, evidence.KnowledgeID, evidence.SourceRevision, evidence.Attempt)
		if err != nil {
			return err
		}
		k := snapshot.Knowledge
		if snapshot.Digest != evidence.SnapshotDigest || k.ParseStatus != types.ParseStatusCompleted || k.EnableStatus != "enabled" || k.IsDataSourceCandidate() || k.PendingSubtasksCount != 0 || k.GetMetadata()["datasource_processing_failed"] != "" {
			return ErrProcessingConflict
		}
		if err := VerifyLegacyStages(snapshot, false); err != nil {
			return err
		}
		result, err = appendLegacyEvidence(tx, evidence)
		return err
	})
	return result, err
}

func VerifyLegacyStages(snapshot *ProcessingLegacySnapshot, candidate bool) error {
	seen := map[string]bool{}
	roots := 0
	for _, span := range snapshot.Spans {
		if span.Kind != types.SpanKindRoot && span.Status != types.SpanStatusDone && span.Status != types.SpanStatusSkipped {
			return errors.New("LEGACY_DESCENDANT_INCOMPLETE")
		}
		if span.Kind == types.SpanKindRoot {
			roots++
			if candidate && span.Status != types.SpanStatusRunning && span.Status != types.SpanStatusDone {
				return errors.New("LEGACY_ATTEMPT_INCOMPLETE")
			}
			if !candidate && span.Status != types.SpanStatusDone {
				return errors.New("LEGACY_ATTEMPT_INCOMPLETE")
			}
		}
		if span.Kind != types.SpanKindStage {
			continue
		}
		if seen[span.Name] {
			return errors.New("LEGACY_STAGE_AMBIGUOUS")
		}
		seen[span.Name] = true
		if candidate && span.Name == types.StagePostProcess && span.Status == types.SpanStatusSkipped {
			continue
		}
		if span.Name == types.StageMultimodal && span.Status == types.SpanStatusSkipped {
			continue
		}
		if span.Status != types.SpanStatusDone {
			return errors.New("LEGACY_STAGE_INCOMPLETE")
		}
	}
	if roots != 1 {
		return errors.New("LEGACY_ROOT_AMBIGUOUS")
	}
	for _, name := range types.AllStages {
		if !seen[name] {
			return errors.New("LEGACY_STAGE_MISSING:" + name)
		}
	}
	if !candidate {
		return VerifyLegacyProjectionCoverage(snapshot)
	}
	return nil
}

func VerifyLegacyProjectionCoverage(snapshot *ProcessingLegacySnapshot) error {
	var post types.JSONMap
	var summary []types.KnowledgeProcessingSpan
	var questions []types.KnowledgeProcessingSpan
	var questionGroups []types.KnowledgeProcessingSpan
	for _, span := range snapshot.Spans {
		if span.Kind == types.SpanKindStage && span.Name == types.StagePostProcess {
			post = span.Output
		}
		if span.Name == "postprocess.summary" {
			summary = append(summary, span)
		}
		if span.Name == "postprocess.question" {
			questionGroups = append(questionGroups, span)
		}
		if strings.HasPrefix(span.Name, "postprocess.question.batch[") {
			questions = append(questions, span)
		}
	}
	flags := map[string]bool{}
	for _, key := range []string{"enqueued_summary", "enqueued_question", "enqueued_graph", "enqueued_wiki", "wiki_slot_owned"} {
		value, ok := post[key].(bool)
		if !ok {
			return errors.New("LEGACY_POSTPROCESS_OBLIGATIONS_UNRECORDED")
		}
		flags[key] = value
	}
	// There is no legacy graph/Wiki readback receipt in this adapter. Do not
	// promote a counter/span to proof that those external projections exist.
	if flags["enqueued_graph"] || flags["enqueued_wiki"] || flags["wiki_slot_owned"] {
		return errors.New("LEGACY_GRAPH_WIKI_ARTIFACT_PROOF_UNAVAILABLE")
	}
	if len(questions) == 0 {
		questions = questionGroups
	}
	var summaryChunks []*types.Chunk
	questionCount := 0
	questionIDs := map[string]bool{}
	for _, chunk := range snapshot.Chunks {
		if chunk.ChunkType == types.ChunkTypeSummary {
			summaryChunks = append(summaryChunks, chunk)
		}
		meta, err := chunk.DocumentMetadata()
		if err != nil {
			return err
		}
		if meta == nil {
			continue
		}
		for _, q := range meta.GeneratedQuestions {
			id := types.GeneratedQuestionSourceID(chunk.ID, q.ID)
			if q.ID == "" || strings.TrimSpace(q.Question) == "" || questionIDs[id] {
				return errors.New("LEGACY_QUESTION_IDENTITY_INVALID")
			}
			questionIDs[id] = true
			questionCount++
		}
	}
	if flags["enqueued_summary"] {
		if len(summary) != 1 || len(summaryChunks) != 1 || summary[0].Status != types.SpanStatusDone || summary[0].Output["skipped"] != nil || summary[0].Output["status"] != "completed" || summary[0].Output["summary_chunk_indexed"] != true || snapshot.Knowledge.SummaryStatus != types.SummaryStatusCompleted || summaryChunks[0].Content != snapshot.Knowledge.Description || strings.TrimSpace(summaryChunks[0].Content) == "" {
			return errors.New("LEGACY_SUMMARY_INCOMPLETE")
		}
		count, err := strconv.Atoi(fmt.Sprint(summary[0].Output["summary_chars"]))
		if err != nil || count != utf8.RuneCountInString(summaryChunks[0].Content) {
			return errors.New("LEGACY_SUMMARY_COVERAGE_MISMATCH")
		}
	} else if len(summary) > 0 || len(summaryChunks) > 0 {
		return errors.New("LEGACY_SUMMARY_UNATTRIBUTED")
	}
	count, err := strconv.Atoi(fmt.Sprint(post["enqueued_question_count"]))
	if err != nil || count < 0 || (count > 0) != flags["enqueued_question"] || len(questions) != count {
		return errors.New("LEGACY_QUESTION_BATCHES_MISSING")
	}
	generated := 0
	seen := map[string]bool{}
	for _, span := range questions {
		n, err := strconv.Atoi(fmt.Sprint(span.Output["questions_generated"]))
		if err != nil || n <= 0 || span.Status != types.SpanStatusDone || seen[span.Name] || span.Output["status"] != "success" || span.Output["index_batch_succeeded"] != true {
			return errors.New("LEGACY_QUESTION_INDEX_INCOMPLETE")
		}
		seen[span.Name] = true
		values := map[string]int{}
		for _, key := range []string{"chunks_in_batch", "chunks_processed", "empty_chunks", "llm_failed", "index_entries_prepared"} {
			value, err := strconv.Atoi(fmt.Sprint(span.Output[key]))
			if err != nil || value < 0 {
				return errors.New("LEGACY_QUESTION_BATCH_UNVERIFIED")
			}
			values[key] = value
		}
		if values["chunks_in_batch"] != values["chunks_processed"]+values["empty_chunks"] || values["llm_failed"] != 0 || values["index_entries_prepared"] != n {
			return errors.New("LEGACY_QUESTION_BATCH_INCOMPLETE")
		}
		generated += n
	}
	if generated != questionCount {
		return errors.New("LEGACY_QUESTION_COVERAGE_MISMATCH")
	}
	return nil
}

func appendLegacyEvidence(tx *gorm.DB, input types.ProcessingLegacyEvidence) (*types.ProcessingLegacyEvidence, error) {
	if !legacyDigest(input.ErrorDigest) || !legacyDigest(input.SnapshotDigest) || !legacyDigest(input.ConfigurationRevision) || !legacyDigest(input.ArtifactDigest) || !legacyDigest(input.EvidenceDigest) ||
		strings.TrimSpace(input.Actor) == "" || len(input.Actor) > 128 || strings.TrimSpace(input.Reason) == "" || len(input.Reason) > 512 ||
		strings.TrimSpace(input.OperationRequestID) == "" || len(input.OperationRequestID) > 128 || strings.TrimSpace(input.EvidenceReference) == "" || len(input.EvidenceReference) > 256 {
		return nil, errors.New("LEGACY_EVIDENCE_REQUIRED")
	}
	// References are operator log/artifact labels, never credentials or URLs.
	if strings.ContainsAny(input.EvidenceReference, "\r\n?#") || strings.Contains(input.EvidenceReference, "://") {
		return nil, errors.New("LEGACY_EVIDENCE_REFERENCE_INVALID")
	}
	input.ID, input.RequestDigest = "", ""
	input.CreatedAt = types.ProcessingLegacyEvidence{}.CreatedAt
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	input.RequestDigest = fmt.Sprintf("%x", sha256.Sum256(encoded))
	var old types.ProcessingLegacyEvidence
	err = tx.Where("tenant_id = ? AND operation_request_id = ?", input.TenantID, input.OperationRequestID).Take(&old).Error
	if err == nil {
		if old.RequestDigest != input.RequestDigest {
			return nil, ErrProcessingConflict
		}
		return &old, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	input.ID = uuid.NewSHA1(uuid.NameSpaceOID, []byte(strconv.FormatUint(input.TenantID, 10)+"/legacy/"+input.OperationRequestID)).String()
	input.CreatedAt, err = processingDBTime(tx)
	if err != nil {
		return nil, err
	}
	if err := tx.Create(&input).Error; err != nil {
		return nil, err
	}
	return &input, nil
}

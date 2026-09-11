package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestProcessingLegacyCompletionRequiresExactAttemptAndImmutableEvidence(t *testing.T) {
	checkProcessingLegacyEvidence(t, processingTestStore(t))
}

func checkProcessingLegacyEvidence(t *testing.T, r *ProcessingRepository) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, r.db.AutoMigrate(&types.SyncLog{}, &types.KnowledgeProcessingSpan{}))
	var rawErrors []map[string]any
	for i := 1; i <= 5; i++ {
		id := fmt.Sprintf("legacy-proof-%d", i)
		errItem := map[string]any{"file_id": id, "external_id": "external:" + id, "stage": "ingest", "category": "INGEST_FAILED", "message": "candidate wait expired"}
		if i <= 4 {
			errItem["knowledge_id"], errItem["source_revision"], errItem["processing_attempt"] = id, "version-"+id, 1
		}
		rawErrors = append(rawErrors, errItem)
		metadata, _ := json.Marshal(map[string]string{"datasource_id": "source", "external_id": "external:" + id, "file_id": id, "datasource_version": "version-" + id})
		k := types.Knowledge{ID: id, TenantID: 1, KnowledgeBaseID: "kb", Title: id, Metadata: metadata, ParseStatus: types.ParseStatusCompleted, EnableStatus: "enabled"}
		require.NoError(t, r.db.Create(&k).Error)
		require.NoError(t, r.db.Create(&types.Chunk{ID: id + "-chunk", TenantID: 1, KnowledgeBaseID: "kb", KnowledgeID: id, Content: "synthetic", ChunkType: types.ChunkTypeText}).Error)
		require.NoError(t, r.db.Create(&types.KnowledgeProcessingSpan{KnowledgeID: id, Attempt: 1, SpanID: id + "-root", Kind: types.SpanKindRoot, Status: types.SpanStatusDone}).Error)
		for _, stage := range types.AllStages {
			output := types.JSONMap{}
			if stage == types.StagePostProcess {
				output = types.JSONMap{"enqueued_summary": false, "enqueued_question": false, "enqueued_question_count": 0, "enqueued_graph": false, "enqueued_wiki": false, "wiki_slot_owned": false}
			}
			require.NoError(t, r.db.Create(&types.KnowledgeProcessingSpan{KnowledgeID: id, Attempt: 1, SpanID: id + "-" + stage, Name: stage, Kind: types.SpanKindStage, Status: types.SpanStatusDone, Output: output}).Error)
		}
	}
	raw, _ := json.Marshal(map[string]any{"errors": rawErrors})
	run := types.SyncLog{ID: "legacy-proof-run", TenantID: 1, DataSourceID: "source", Status: "running", Result: raw, ItemsFailed: 5}
	require.NoError(t, r.db.Create(&run).Error)
	var before types.SyncLog
	require.NoError(t, r.db.First(&before, "id = ?", run.ID).Error)
	var first types.ProcessingLegacyEvidence
	configuration, configErr := r.ConfigurationRevision(ctx, 1, "kb")
	require.NoError(t, configErr)
	for i := 1; i <= 5; i++ {
		identity := types.ProcessingLegacyIdentity{TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", RunID: run.ID, ErrorOrdinal: i}
		original, err := r.InspectLegacyError(ctx, identity)
		require.NoError(t, err)
		id := fmt.Sprintf("legacy-proof-%d", i)
		snapshot, err := r.LegacySnapshot(ctx, original.Identity, id, "version-"+id, 1)
		require.NoError(t, err)
		e := types.ProcessingLegacyEvidence{ProcessingLegacyIdentity: original.Identity, Action: "late_completion", KnowledgeID: id, SourceRevision: "version-" + id, Attempt: 1, SnapshotDigest: snapshot.Digest,
			ArtifactDigest: strings.Repeat("a", 64), EvidenceReference: "synthetic-fixture", EvidenceDigest: strings.Repeat("b", 64), Actor: "test-operator", Reason: "exact recorded attempt verified", OperationRequestID: "resolve-" + id}
		e.ConfigurationRevision = configuration
		if i == 5 {
			_, err = r.RecordLegacyCompletion(ctx, e)
			require.ErrorContains(t, err, "LEGACY_ATTEMPT_UNATTRIBUTED")
			e.Action = "manual_confirmed"
		}
		if i == 1 {
			first = e
			for _, mutate := range []func(*types.ProcessingLegacyEvidence){
				func(e *types.ProcessingLegacyEvidence) { e.TenantID = 2 },
				func(e *types.ProcessingLegacyEvidence) { e.DataSourceID = "other-source" },
				func(e *types.ProcessingLegacyEvidence) { e.KnowledgeBaseID = "other-kb" },
				func(e *types.ProcessingLegacyEvidence) { e.ErrorDigest = strings.Repeat("c", 64) },
				func(e *types.ProcessingLegacyEvidence) { e.SourceRevision = "wrong-version" },
				func(e *types.ProcessingLegacyEvidence) { e.Attempt = 2 },
				func(e *types.ProcessingLegacyEvidence) { e.SnapshotDigest = strings.Repeat("d", 64) },
			} {
				bad := e
				mutate(&bad)
				_, err := r.RecordLegacyCompletion(ctx, bad)
				require.Error(t, err)
			}
		}
		saved, err := r.RecordLegacyCompletion(ctx, e)
		require.NoError(t, err)
		same, err := r.RecordLegacyCompletion(ctx, e)
		require.NoError(t, err)
		require.Equal(t, saved.ID, same.ID)
	}
	var rows int64
	require.NoError(t, r.db.Model(&types.ProcessingLegacyEvidence{}).Where("run_id = ? AND action = ?", run.ID, "late_completion").Count(&rows).Error)
	require.EqualValues(t, 4, rows)
	var after types.SyncLog
	require.NoError(t, r.db.First(&after, "id = ?", run.ID).Error)
	require.Equal(t, before, after)
	require.Error(t, r.db.Model(&types.ProcessingLegacyEvidence{}).Where("run_id = ?", run.ID).Update("reason", "rewrite").Error)
	require.Error(t, r.db.Where("run_id = ?", run.ID).Delete(&types.ProcessingLegacyEvidence{}).Error)
	first.Reason = "changed request under same operation id"
	_, err := r.RecordLegacyCompletion(ctx, first)
	require.ErrorIs(t, err, ErrProcessingConflict)
	// A changed historic error must not inherit the old resolution.
	require.NoError(t, r.db.Model(&run).Update("result", types.JSON(`{"errors":[{"file_id":"legacy-proof-1","external_id":"external:legacy-proof-1","stage":"export_start","code":"EXPORT_START_UNCERTAIN"}]}`)).Error)
	_, err = r.RecordLegacyCompletion(ctx, first)
	require.ErrorIs(t, err, ErrProcessingConflict)
	changed, err := r.InspectLegacyError(ctx, types.ProcessingLegacyIdentity{TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", RunID: run.ID, ErrorOrdinal: 1})
	require.NoError(t, err)
	first.ProcessingLegacyIdentity = changed.Identity
	first.Action = "manual_confirmed"
	_, err = r.RecordLegacyCompletion(ctx, first)
	require.ErrorContains(t, err, "LEGACY_ERROR_NOT_COMPLETION_WAIT")
}

func TestProcessingLegacyLateWritesCannotTakeBackAdoptedKnowledge(t *testing.T) {
	r := processingTestStore(t)
	require.NoError(t, r.db.AutoMigrate(&types.Knowledge{}))
	k := types.Knowledge{ID: "candidate", TenantID: 1, KnowledgeBaseID: "kb", Title: "original", ParseStatus: types.ParseStatusFailed,
		Metadata: types.JSON(`{"datasource_version":"old-version","datasource_candidate":"true"}`)}
	require.NoError(t, r.db.Create(&k).Error)
	stale := k
	require.NoError(t, r.db.Model(&k).Updates(map[string]any{"metadata": types.JSON(`{"processing_protocol":2,"processing_job_id":"new-job","datasource_version":"old-version"}`), "parse_status": types.ParseStatusCompleted}).Error)
	store := &knowledgeRepository{db: r.db}
	ctx := context.Background()
	legacy := types.WithLegacyProcessing(ctx)
	stale.Title = "late overwrite"
	require.ErrorIs(t, store.UpdateKnowledge(ctx, &stale), ErrProcessingConflict)
	require.ErrorIs(t, store.UpdateKnowledgeBatch(ctx, []*types.Knowledge{&stale}), ErrProcessingConflict)
	require.ErrorIs(t, store.UpdateKnowledgeColumn(legacy, k.ID, "parse_status", types.ParseStatusFailed), ErrProcessingConflict)
	require.ErrorIs(t, store.UpdateKnowledgeColumns(legacy, k.ID, map[string]interface{}{"metadata": stale.Metadata, "pending_subtasks_count": 4}), ErrProcessingConflict)
	var current types.Knowledge
	require.NoError(t, r.db.First(&current, "id = ?", k.ID).Error)
	require.Equal(t, "original", current.Title)
	require.Equal(t, types.ParseStatusCompleted, current.ParseStatus)
	require.Equal(t, "2", current.GetMetadata()["processing_protocol"])
	current.Title = "user edit"
	require.ErrorIs(t, store.UpdateKnowledge(legacy, &current), ErrProcessingConflict)
	require.NoError(t, store.UpdateKnowledge(ctx, &current))
	require.NoError(t, r.db.First(&current, "id = ?", k.ID).Error)
	require.Equal(t, "user edit", current.Title)
	current.ID = "missing"
	require.Error(t, store.UpdateKnowledge(ctx, &current))
	var count int64
	require.NoError(t, r.db.Model(&types.Knowledge{}).Where("id = ?", "missing").Count(&count).Error)
	require.Zero(t, count)
}

func TestProcessingLegacyStorageProofChecksActualOwner(t *testing.T) {
	r := processingTestStore(t)
	k := types.Knowledge{ID: "legacy-storage", TenantID: 1, KnowledgeBaseID: "kb", FilePath: "minio://bucket/1/legacy-storage/file.pdf"}
	require.NoError(t, r.db.Create(&k).Error)
	store := &knowledgeRepository{db: r.db}
	ctx := context.Background()
	require.NoError(t, store.ValidateLegacyStorageOwnership(ctx, 1, k.ID, k.FilePath, true))
	require.NoError(t, store.ValidateLegacyStorageOwnership(ctx, 1, k.ID, "minio://bucket/1/exports/image.png", false))
	for _, path := range []string{"minio://1/2/exports/image.png", "minio://bucket/1/other-knowledge/image.png", "minio://bucket/prefix/1/exports/image.png", "minio://user:pass@bucket/1/exports/image.png", "minio://bucket/1/exports/image.png?secret=x", "minio://bucket/1/exports/%2E%2E", "local://2/exports/image.png"} {
		require.Error(t, store.ValidateLegacyStorageOwnership(ctx, 1, k.ID, path, false), path)
	}
	resource := types.StoredResource{ID: "foreign-resource", Handle: "0123456789012345678901", TenantID: 2, PhysicalPath: "minio://bucket/2/exports/image.png", LocationHash: strings.Repeat("a", 64), Provider: "minio"}
	require.NoError(t, r.db.Create(&resource).Error)
	reference := types.BuildResourcePath(resource.Handle)
	require.Error(t, store.ValidateLegacyStorageOwnership(ctx, 1, k.ID, reference, false))
	resource.ID = "own-resource"
	resource.Handle = "1123456789012345678901"
	resource.TenantID = 1
	resource.PhysicalPath = k.FilePath
	require.NoError(t, r.db.Create(&resource).Error)
	reference = types.BuildResourcePath(resource.Handle)
	require.NoError(t, r.db.Model(&k).Update("file_path", reference).Error)
	require.Error(t, store.ValidateLegacyStorageOwnership(ctx, 1, k.ID, reference, true), "source resource requires its knowledge binding")
	require.NoError(t, r.db.Create(&types.ResourceBinding{ResourceID: resource.ID, TenantID: 1, OwnerType: "knowledge", OwnerID: k.ID, Relation: "source_file"}).Error)
	require.NoError(t, store.ValidateLegacyStorageOwnership(ctx, 1, k.ID, reference, true))
}

func TestProcessingLegacyDrainRequiresFullWorkerAndQueueProof(t *testing.T) {
	var queues []map[string]any
	for _, queue := range types.QueueDefinitions() {
		queues = append(queues, map[string]any{"queue": queue.Name, "inventory_digest": strings.Repeat("a", 64), "tasks": []any{}})
	}
	base := map[string]any{"complete": true, "workers": []map[string]any{{"old_instance_id": "old", "replacement_id": "new", "image_digest": strings.Repeat("b", 64), "exit_confirmed": true, "guard_protocol": 2}}, "queues": queues}
	encoded, _ := json.Marshal(base)
	require.NoError(t, validateLegacyDrainInventory(encoded))
	for _, name := range []string{"counter-only", "still-running", "unguarded-worker", "missing-queue", "live-task", "missing-payload-proof"} {
		t.Run(name, func(t *testing.T) {
			var candidate map[string]any
			require.NoError(t, json.Unmarshal(encoded, &candidate))
			switch name {
			case "counter-only":
				candidate = map[string]any{"complete": true, "active": 0}
			case "still-running":
				candidate["workers"].([]any)[0].(map[string]any)["exit_confirmed"] = false
			case "unguarded-worker":
				candidate["workers"].([]any)[0].(map[string]any)["guard_protocol"] = 1
			case "missing-queue":
				candidate["queues"] = candidate["queues"].([]any)[1:]
			case "live-task", "missing-payload-proof":
				task := map[string]any{"task_id": "old-delivery", "payload_digest": strings.Repeat("c", 64), "state": "running"}
				if name == "missing-payload-proof" {
					task["state"] = "absent"
					delete(task, "payload_digest")
				}
				candidate["queues"].([]any)[0].(map[string]any)["tasks"] = []any{task}
			}
			data, _ := json.Marshal(candidate)
			require.Error(t, validateLegacyDrainInventory(data))
		})
	}
}

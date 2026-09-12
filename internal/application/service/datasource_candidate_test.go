package service

import (
	"context"
	"encoding/json"
	"errors"
	"mime/multipart"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/require"
)

type candidateRepo struct {
	interfaces.KnowledgeRepository
	rows   map[string]*types.Knowledge
	events []string
}

func (r *candidateRepo) FindByMetadataKey(_ context.Context, _ uint64, _ string, key, value string) (*types.Knowledge, error) {
	for _, k := range r.rows {
		if k.GetMetadata()[key] == value {
			return k, nil
		}
	}
	return nil, nil
}
func (r *candidateRepo) FindByMetadataKeyPrefix(_ context.Context, _ uint64, _ string, key, value string) ([]*types.Knowledge, error) {
	var found []*types.Knowledge
	for _, k := range r.rows {
		if strings.HasPrefix(k.GetMetadata()[key], value) {
			found = append(found, k)
		}
	}
	return found, nil
}
func (r *candidateRepo) GetKnowledgeByID(_ context.Context, _ uint64, id string) (*types.Knowledge, error) {
	return r.rows[id], nil
}
func (r *candidateRepo) UpdateKnowledge(_ context.Context, k *types.Knowledge) error {
	r.rows[k.ID] = k
	r.events = append(r.events, "publish:"+k.ID)
	return nil
}

type candidateKS struct {
	interfaces.KnowledgeService
	r             *candidateRepo
	createErr     error
	status        string
	creates       int
	ready         bool
	failure       string
	reparses      int
	requireTenant bool
}

func (k *candidateKS) GetRepository() interfaces.KnowledgeRepository { return k.r }
func (k *candidateKS) CreateKnowledgeFromFile(_ context.Context, kb string, _ *multipart.FileHeader, metadata map[string]string, _ *bool, _ string, _ []string, _ string, _ *types.KnowledgeProcessOverrides) (*types.Knowledge, error) {
	k.creates++
	if k.createErr != nil {
		return nil, k.createErr
	}
	if k.ready {
		metadata["datasource_index_ready"] = "true"
	}
	if k.failure != "" {
		metadata["datasource_processing_failed"] = k.failure
	}
	b, _ := json.Marshal(metadata)
	n := &types.Knowledge{ID: "candidate", TenantID: 1, KnowledgeBaseID: kb, Metadata: b, ParseStatus: k.status, EnableStatus: "disabled"}
	k.r.rows[n.ID] = n
	return n, nil
}
func (k *candidateKS) DeleteKnowledge(ctx context.Context, id string) error {
	if k.requireTenant {
		tenant, ok := types.TenantInfoFromContext(ctx)
		if !ok || tenant.ID != 1 || ctx.Value(types.TenantIDContextKey) != uint64(1) {
			return errors.New("tenant info not found in cleanup context")
		}
	}
	k.r.events = append(k.r.events, "delete:"+id)
	delete(k.r.rows, id)
	return nil
}
func candidateFixture() (*DataSourceService, *candidateKS, *types.DataSource, *types.FetchedItem) {
	r := &candidateRepo{rows: map[string]*types.Knowledge{"old": {ID: "old", ParseStatus: "completed", EnableStatus: "enabled", Metadata: types.JSON(`{"external_id":"node","datasource_id":"ds"}`)}}}
	k := &candidateKS{r: r, status: "completed"}
	return &DataSourceService{knowledgeService: k}, k, &types.DataSource{ID: "ds", TenantID: 1, KnowledgeBaseID: "kb", Type: types.ConnectorTypeTencentDocs}, &types.FetchedItem{ExternalID: "node", FileName: "doc.md", Content: []byte("new body")}
}

func (r *candidateRepo) PublishSourceCandidate(ctx context.Context, _ *types.DataSource, id string) (bool, error) {
	k := r.rows[id]
	m := k.GetMetadata()
	delete(m, "datasource_candidate")
	k.Metadata, _ = json.Marshal(m)
	return true, r.UpdateKnowledge(ctx, k)
}

func TestTencentSourcePublicationRestoresTenantForCleanup(t *testing.T) {
	s, k, ds, item := candidateFixture()
	ds.TencentFileSync = true
	s.dsRepo = &prepushSourceRepo{row: ds}
	s.tenantRepo = &summaryRefreshTenantRepo{tenant: &types.Tenant{ID: 1}}
	k.requireTenant = true
	_, err := s.ingestItem(context.Background(), ds, item, nil)
	require.NoError(t, err)
	published, err := s.PublishSourceCandidate(context.Background(), 1, "candidate")
	require.NoError(t, err)
	require.True(t, published)
	require.Equal(t, []string{"publish:candidate", "delete:old"}, k.r.events)
}
func TestTencentCandidatePreservesOldOnCreateFailure(t *testing.T) {
	s, k, ds, item := candidateFixture()
	k.createErr = errors.New("storage unavailable")
	_, err := s.ingestItem(context.Background(), ds, item, nil)
	require.Error(t, err)
	require.NotNil(t, k.r.rows["old"])
	require.Empty(t, k.r.events)
}

func TestTencentCandidateFileSyncReturnsAfterSubmission(t *testing.T) {
	s, k, ds, item := candidateFixture()
	ds.TencentFileSync = true
	k.status = types.ParseStatusPending
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := s.ingestItem(ctx, ds, item, nil)
	require.NoError(t, err, "normal source fetch must not wait for parsing/embedding/summary")
	require.Equal(t, 1, k.creates)
	require.True(t, k.r.rows["candidate"].IsDataSourceCandidate())
	require.Equal(t, "true", k.r.rows["candidate"].GetMetadata()["datasource_async_publish"])
	require.NotNil(t, k.r.rows["old"])
	require.Empty(t, k.r.events, "old content stays published until postprocess confirms readiness")
}
func TestTencentCandidateRejectsFailedEnqueue(t *testing.T) {
	s, k, ds, item := candidateFixture()
	k.status = "failed"
	h := &streamSyncHandler{svc: s, ds: ds, result: &types.SyncResult{}}
	require.NoError(t, h.Emit(context.Background(), *item))
	require.Equal(t, 1, h.result.Failed)
	require.True(t, h.ItemRejected(item.ExternalID))
	require.NotNil(t, k.r.rows["old"])
	require.NotNil(t, h.ItemIngestError(item.ExternalID))
}
func TestTencentCandidatePublishesBeforeDeletingOld(t *testing.T) {
	s, k, ds, item := candidateFixture()
	updated, err := s.ingestItem(context.Background(), ds, item, nil)
	require.NoError(t, err)
	require.True(t, updated)
	require.Equal(t, []string{"publish:candidate", "delete:old"}, k.r.events)
	require.False(t, k.r.rows["candidate"].IsDataSourceCandidate())
}
func TestTencentCandidateResumesAfterWorkerRestart(t *testing.T) {
	s, k, ds, item := candidateFixture()
	k.status = "pending"
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.ingestItem(ctx, ds, item, nil)
	require.Error(t, err)
	require.NotNil(t, k.r.rows["old"])
	require.True(t, k.r.rows["candidate"].IsDataSourceCandidate())
	k.r.rows["candidate"].ParseStatus = "completed"
	_, err = s.ingestItem(context.Background(), ds, item, nil)
	require.NoError(t, err)
	require.Equal(t, 1, k.creates)
	require.Nil(t, k.r.rows["old"])
}
func TestTencentCandidateDoesNotDeleteAnotherSource(t *testing.T) {
	s, k, ds, item := candidateFixture()
	k.r.rows["other"] = &types.Knowledge{ID: "other", Metadata: types.JSON(`{"external_id":"node","datasource_id":"other-ds"}`)}
	_, err := s.ingestItem(context.Background(), ds, item, nil)
	require.NoError(t, err)
	require.NotNil(t, k.r.rows["other"])
}

func TestTencentCandidateCompletedWithFailedSubtaskKeepsOldVersion(t *testing.T) {
	s, k, ds, item := candidateFixture()
	k.failure = "wiki"
	_, err := s.ingestItem(context.Background(), ds, item, nil)
	require.Error(t, err)
	require.NotNil(t, k.r.rows["old"])
	require.Empty(t, k.r.events)
}

type sourceTerminalRepo struct {
	interfaces.KnowledgeRepository
	events  []string
	markErr error
}

func (r *sourceTerminalRepo) MarkDataSourceSubtaskFailed(context.Context, string, string) error {
	r.events = append(r.events, "failure")
	return r.markErr
}
func (r *sourceTerminalRepo) FinalizeSubtask(context.Context, string) (int, bool, error) {
	r.events = append(r.events, "finalize")
	return 0, true, nil
}
func TestTencentCandidateFailurePersistsBeforeSubtaskCompletion(t *testing.T) {
	r := &sourceTerminalRepo{}
	finalizeSubtaskDetached(context.Background(), r, "candidate", "graph", errors.New("index unavailable"), false, true)
	require.Equal(t, []string{"failure", "finalize"}, r.events)
	r = &sourceTerminalRepo{markErr: errors.New("database unavailable")}
	finalizeSubtaskDetached(context.Background(), r, "candidate", "graph", errors.New("index unavailable"), false, true)
	require.Equal(t, []string{"failure"}, r.events)
}

type candidatePostQueue struct{ r *candidateRepo }

func (q *candidatePostQueue) Enqueue(_ *asynq.Task, _ ...asynq.Option) (*asynq.TaskInfo, error) {
	if q.r.rows["candidate"].IsDataSourceCandidate() || q.r.rows["old"] == nil {
		return nil, errors.New("wrong publication order")
	}
	q.r.events = append(q.r.events, "postprocess")
	q.r.rows["candidate"].ParseStatus = types.ParseStatusCompleted
	return &asynq.TaskInfo{ID: "post"}, nil
}
func TestTencentCandidateDefersSharedDerivativesUntilPublication(t *testing.T) {
	s, k, ds, item := candidateFixture()
	k.status = "processing"
	k.ready = true
	s.taskEnqueuer = &candidatePostQueue{r: k.r}
	_, err := s.ingestItem(context.Background(), ds, item, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"publish:candidate", "postprocess", "delete:old"}, k.r.events)
}

type candidateReadyRepo struct {
	interfaces.KnowledgeRepository
	row *types.Knowledge
}

func (r *candidateReadyRepo) GetKnowledgeByIDOnly(context.Context, string) (*types.Knowledge, error) {
	return r.row, nil
}
func (r *candidateReadyRepo) UpdateKnowledgeColumn(_ context.Context, _ string, column string, value interface{}) error {
	if column != "metadata" {
		return errors.New("unexpected update")
	}
	r.row.Metadata = value.(types.JSON)
	return nil
}
func TestTencentCandidatePostProcessOnlyAnnouncesReadiness(t *testing.T) {
	now := time.Now()
	r := &candidateReadyRepo{row: &types.Knowledge{ID: "candidate", ParseStatus: types.ParseStatusProcessing, ProcessedAt: &now, Metadata: types.JSON(`{"datasource_candidate":"true"}`)}}
	s := &KnowledgePostProcessService{knowledgeRepo: r}
	body, _ := json.Marshal(types.KnowledgePostProcessPayload{KnowledgeID: "candidate", TenantID: 1, KnowledgeBaseID: "kb"})
	require.NoError(t, s.Handle(context.Background(), asynq.NewTask(types.TypeKnowledgePostProcess, body)))
	require.Equal(t, "true", r.row.GetMetadata()["datasource_index_ready"])
	require.True(t, r.row.IsDataSourceCandidate())
	require.Equal(t, types.ParseStatusProcessing, r.row.ParseStatus)
}

func (r *candidateRepo) UpdateKnowledgeColumns(_ context.Context, id string, values map[string]interface{}) error {
	k := r.rows[id]
	if v, ok := values["metadata"].(types.JSON); ok {
		k.Metadata = v
	}
	if v, ok := values["parse_status"].(string); ok {
		k.ParseStatus = v
	}
	if v, ok := values["enable_status"].(string); ok {
		k.EnableStatus = v
	}
	r.events = append(r.events, "publish:"+id)
	return nil
}
func (k *candidateKS) ReparseKnowledge(_ context.Context, id string, _ *types.KnowledgeProcessOverrides) (*types.Knowledge, error) {
	k.reparses++
	k.r.rows[id].ParseStatus = types.ParseStatusCompleted
	return k.r.rows[id], nil
}
func TestTencentCandidateFailedCompletionCanBeReparsed(t *testing.T) {
	s, k, ds, item := candidateFixture()
	k.failure = "wiki"
	_, err := s.ingestItem(context.Background(), ds, item, nil)
	require.Error(t, err)
	_, err = s.ingestItem(context.Background(), ds, item, nil)
	require.NoError(t, err)
	require.Equal(t, 1, k.creates)
	require.Equal(t, 1, k.reparses)
	require.Nil(t, k.r.rows["old"])
}

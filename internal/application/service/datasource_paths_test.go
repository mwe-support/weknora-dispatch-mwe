package service

import (
	"context"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

type sourcePathRepo struct{ createKnowledgeFileRepoStub }

func (r *sourcePathRepo) FindByMetadataKey(context.Context, uint64, string, string, string) (*types.Knowledge, error) {
	return nil, nil
}

func (r *sourcePathRepo) GetKnowledgeByID(context.Context, uint64, string) (*types.Knowledge, error) {
	r.createdKnowledge.ParseStatus = types.ParseStatusCompleted
	return r.createdKnowledge, nil
}
func (r *sourcePathRepo) UpdateKnowledge(_ context.Context, k *types.Knowledge) error {
	r.createdKnowledge = k
	return nil
}
func (r *sourcePathRepo) FindByMetadataKeyPrefix(context.Context, uint64, string, string, string) ([]*types.Knowledge, error) {
	return nil, nil
}

func TestTencentSourcePathUsesManualFolderIngestion(t *testing.T) {
	r := &sourcePathRepo{}
	k := &knowledgeService{repo: r, kbService: &createKnowledgeFileKBServiceStub{kb: &types.KnowledgeBase{ID: "kb-1"}}, fileSvc: &createKnowledgeFileServiceStub{}, task: &createKnowledgeTaskEnqueuerStub{}}
	s := &DataSourceService{knowledgeService: k}
	_, err := s.ingestItem(newCreateKnowledgeFileContext(), &types.DataSource{ID: "ds", KnowledgeBaseID: "kb-1", Type: types.ConnectorTypeTencentDocs}, &types.FetchedItem{
		ExternalID: "node", FileName: "guide.md", Content: []byte("fixture"), Metadata: map[string]string{"folder_path": "一级目录/二级目录", "source_path": "一级目录/二级目录/guide.md"},
	}, nil)
	require.NoError(t, err)
	require.Equal(t, "一级目录/二级目录", r.createdKnowledge.FolderPath)
	require.Equal(t, "guide.md", r.createdKnowledge.FileName)
	require.Equal(t, "一级目录/二级目录/guide.md", r.createdKnowledge.GetMetadata()["source_path"])
}

func (r *sourcePathRepo) UpdateKnowledgeColumns(_ context.Context, _ string, values map[string]interface{}) error {
	if v, ok := values["metadata"].(types.JSON); ok {
		r.createdKnowledge.Metadata = v
	}
	if v, ok := values["enable_status"].(string); ok {
		r.createdKnowledge.EnableStatus = v
	}
	return nil
}

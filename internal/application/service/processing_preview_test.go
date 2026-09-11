package service

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	files "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
)

type processingPreviewFiles struct {
	interfaces.FileService
	afterRead func()
}

func (s processingPreviewFiles) GetFile(ctx context.Context, path string) (io.ReadCloser, error) {
	file, err := s.FileService.GetFile(ctx, path)
	if s.afterRead != nil {
		s.afterRead()
	}
	return file, err
}

func TestProcessingPreviewReadsOnlyPublishedVerifiedFile(t *testing.T) {
	for _, fileType := range []string{"md", "docx", "pdf"} {
		t.Run(fileType, func(t *testing.T) {
			t.Setenv("SYSTEM_AES_KEY", "synthetic-32-byte-key-for-tests!")
			db := processingServiceTestDatabase(t)
			require.NoError(t, db.AutoMigrate(&types.Knowledge{}))
			ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
			fileService := files.NewLocalFileService(t.TempDir(), "")
			s := &knowledgeService{repo: repository.NewKnowledgeRepository(db), fileSvc: fileService,
				kbService: &knowledgeBaseService{repo: repository.NewKnowledgeBaseRepository(db)}}
			job := types.ProcessingJob{ID: "preview-job", TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", KnowledgeID: "preview-knowledge", Kind: types.ProcessingJobDocument, Generation: 1, IsPublished: true, PublicationEpoch: 1, RetirementState: "retained"}
			job.Metadata = types.JSON(`{"kind":"smartcanvas"}`)
			if fileType == "docx" {
				job.Metadata = types.JSON(`{"kind":"doc"}`)
			}
			if fileType == "pdf" {
				job.Metadata = types.JSON(`{"kind":"resource"}`)
			}
			step := types.ProcessingStep{ID: "preview-step", JobID: job.ID, Stage: "assets", UnitKey: "body", Status: types.ProcessingSucceeded, Attempt: 1, InputFingerprint: "input"}
			body := []byte("# Synthetic complete body\n\n![image](resource://AbCdEfGhIjKlMnOpQrStUv)\n\nTAIL-7391")
			stored, _ := json.Marshal(processingParsed{ReadResult: types.ReadResult{MarkdownContent: string(body)}})
			kind := "assets"
			if fileType == "docx" || fileType == "pdf" {
				step.Stage, kind = "download", "source_file"
				body = []byte("PK\x03\x04synthetic exact source bytes")
				if fileType == "pdf" {
					body = []byte("%PDF-1.4 synthetic exact source bytes")
				}
				stored = body
			}
			var err error
			step.OutputManifestRef, step.OutputDigest, err = NewProcessingArtifacts(fileService, nil).Save(ctx, job, step, kind, stored)
			require.NoError(t, err)
			manifest, _ := json.Marshal(map[string]types.ProcessingArtifactVersion{step.ID: {Attempt: 1, Digest: step.OutputDigest}})
			job.ActiveIndexManifest = string(manifest)
			require.NoError(t, db.Create(&job).Error)
			require.NoError(t, db.Create(&step).Error)
			knowledge := types.Knowledge{ID: job.KnowledgeID, TenantID: 1, KnowledgeBaseID: "kb", FileType: fileType, FileName: "synthetic." + fileType, EnableStatus: "enabled", Metadata: types.JSON(`{"processing_protocol":"2","processing_job_id":"preview-job"}`)}
			require.NoError(t, db.Create(&knowledge).Error)
			read := func() {
				reader, name, err := s.GetKnowledgeFile(ctx, knowledge.ID)
				require.NoError(t, err)
				defer reader.Close()
				actual, err := io.ReadAll(reader)
				require.NoError(t, err)
				require.Equal(t, body, actual)
				require.Equal(t, knowledge.FileName, name)
			}
			read()
			deny := func() {
				reader, _, err := s.GetKnowledgeFile(ctx, knowledge.ID)
				require.Error(t, err)
				require.Nil(t, reader)
			}
			for field, bad := range map[string]any{"is_published": false, "retirement_state": "deleting", "knowledge_base_id": "other", "knowledge_id": "other", "active_index_manifest": "{}"} {
				require.NoError(t, db.Model(&job).Update(field, bad).Error)
				deny()
				require.NoError(t, db.Model(&job).Updates(map[string]any{"is_published": true, "retirement_state": "retained", "knowledge_base_id": "kb", "knowledge_id": knowledge.ID, "active_index_manifest": string(manifest)}).Error)
			}
			goodDigest := step.OutputDigest
			require.NoError(t, db.Model(&step).Update("output_digest", strings.Repeat("0", 64)).Error)
			deny()
			badManifest, _ := json.Marshal(map[string]types.ProcessingArtifactVersion{step.ID: {Attempt: 1, Digest: step.OutputDigest}})
			require.NoError(t, db.Model(&job).Update("active_index_manifest", string(badManifest)).Error)
			deny() // Even a matching manifest must pass encrypted artifact verification.
			require.NoError(t, db.Model(&step).Update("output_digest", goodDigest).Error)
			require.NoError(t, db.Model(&job).Update("active_index_manifest", string(manifest)).Error)
			read()
			s.fileSvc = processingPreviewFiles{FileService: fileService, afterRead: func() {
				require.NoError(t, db.Model(&job).Update("is_published", false).Error)
			}}
			deny() // Publication changed during storage I/O; release no plaintext.
			s.fileSvc = fileService
			require.NoError(t, db.Model(&job).Update("is_published", true).Error)
			read()
			reader, _, err := s.GetKnowledgeFile(context.WithValue(ctx, types.TenantIDContextKey, uint64(2)), knowledge.ID)
			require.Error(t, err)
			require.Nil(t, reader)
			require.NoError(t, db.Delete(&types.KnowledgeBase{}, "id = ?", "kb").Error)
			deny()
		})
	}
}

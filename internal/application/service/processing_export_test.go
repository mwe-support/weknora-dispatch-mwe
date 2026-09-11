package service

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	files "github.com/Tencent/WeKnora/internal/application/service/file"
	"github.com/Tencent/WeKnora/internal/datasource/connector/tencentdocs"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

type processingExportClient struct {
	tencentdocs.Client
	refreshURL               bool
	starts, polls, downloads int
	startError               error
}

func (c *processingExportClient) StartExport(context.Context, string) (*tencentdocs.ExportTask, error) {
	c.starts++
	if c.startError != nil {
		err := c.startError
		c.startError = nil
		return nil, err
	}
	return &tencentdocs.ExportTask{ID: "fixed-provider-task"}, nil
}

func TestProcessingExportAdmissionWaitAndUncertainRequestHaveDifferentRecovery(t *testing.T) {
	for _, admission := range []bool{false, true} {
		t.Run(map[bool]string{false: "unknown-result", true: "not-admitted"}[admission], func(t *testing.T) {
			t.Setenv("SYSTEM_AES_KEY", "synthetic-32-byte-key-for-tests!")
			db := processingServiceTestDatabase(t)
			r := repository.NewProcessingRepository(db)
			ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
			job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", SourceRevision: "fixed", PipelineFingerprint: "fixed"})
			require.NoError(t, err)
			require.NoError(t, r.PlanSteps(ctx, 1, job.ID, []types.ProcessingStepSpec{{Stage: "export_start", UnitKey: "body", Phase: types.ProcessingPhasePrepare, RequiredForReady: true, InputFingerprint: "fixed"}}))
			ops, err := r.PendingDeliveries(ctx, 10)
			require.NoError(t, err)
			var ref types.ProcessingRef
			require.NoError(t, json.Unmarshal(ops[0].Payload, &ref))
			lease, err := r.ClaimStep(ctx, 1, ref, time.Minute)
			require.NoError(t, err)
			e := processingDocumentExecution{repo: r, lease: *lease, artifacts: NewProcessingArtifacts(files.NewLocalFileService(t.TempDir(), ""), nil), document: ProcessingDocumentSpec{FileID: "file", Kind: "doc"}}
			client := &processingExportClient{startError: context.DeadlineExceeded}
			if admission {
				client.startError = &tencentdocs.MCPBudgetWaitError{RetryAfter: time.Second}
			}
			verify := func(context.Context) error { return nil }
			outcome, err := e.exportStage(ctx, client, verify)
			if err != nil {
				outcome = tencentdocs.ProcessingFailure("export_start", err)
			}
			require.NoError(t, r.FinishStep(ctx, 1, *lease, outcome))
			step, err := r.GetStep(ctx, 1, job.ID, lease.Step.ID)
			require.NoError(t, err)
			require.NotEmpty(t, step.CheckpointRef)
			require.Zero(t, step.RetryCount)
			if !admission {
				require.Equal(t, "EXPORT_START_UNCERTAIN", step.ErrorCode)
				e.lease.Step = *step
				outcome, err = e.exportStage(ctx, client, verify)
				require.NoError(t, err)
				require.Equal(t, types.ProcessingBlocked, outcome.Status)
				require.Equal(t, 1, client.starts, "an unknown result must never reissue export")
				return
			}
			require.Equal(t, types.ProcessingWaitingExternal, step.Status)
			require.NoError(t, db.Model(step).Update("next_run_at", time.Now().Add(-time.Minute)).Error)
			require.NoError(t, r.ReconcileJob(ctx, 1, job.ID))
			ops, err = r.PendingDeliveries(ctx, 10)
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(ops[0].Payload, &ref))
			lease, err = r.ClaimStep(ctx, 1, ref, time.Minute)
			require.NoError(t, err)
			e.lease = *lease
			outcome, err = e.exportStage(ctx, client, verify)
			require.NoError(t, err)
			require.Equal(t, types.ProcessingSucceeded, outcome.Status)
			require.NoError(t, r.FinishStep(ctx, 1, *lease, outcome))
		})
	}
}

func TestProcessingDOCXCoverageRejectsLostTextAndImages(t *testing.T) {
	var buffer bytes.Buffer
	archive := zip.NewWriter(&buffer)
	for name, content := range map[string]string{
		"word/document.xml":    `<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body><w:p><w:r><w:t>` + strings.Repeat("LongParagraphBeyondPreview", 30) + `</w:t></w:r></w:p><w:tbl><w:tr><w:tc><w:p><w:r><w:t>TABLE-CELL-7391</w:t></w:r></w:p></w:tc></w:tr></w:tbl><w:p><w:r><w:drawing><w:txbxContent><w:p><w:r><w:t>TEXT-BOX-7391</w:t></w:r></w:p></w:txbxContent></w:drawing></w:r></w:p></w:body></w:document>`,
		"word/footnotes.xml":   `<w:footnotes xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:footnote><w:p><w:r><w:t>FOOTNOTE-END-7391</w:t></w:r></w:p></w:footnote></w:footnotes>`,
		"word/media/image.png": "synthetic image inventory",
	} {
		file, err := archive.Create(name)
		require.NoError(t, err)
		_, err = file.Write([]byte(content))
		require.NoError(t, err)
	}
	require.NoError(t, archive.Close())
	runs, images, err := processingDOCXInventory(buffer.Bytes())
	require.NoError(t, err)
	require.Len(t, runs, 4)
	require.Equal(t, 1, images)
	result := &types.ReadResult{MarkdownContent: strings.Repeat("LongParagraphBeyondPreview", 30) + "\nTABLE-CELL-7391\nTEXT-BOX-7391\nFOOTNOTE-END-7391", ImageRefs: []types.ImageRef{{Filename: "image.png"}}}
	require.NoError(t, validateProcessingDOCXText(runs, images, result))
	result.MarkdownContent = strings.ReplaceAll(result.MarkdownContent, "FOOTNOTE-END-7391", "")
	require.EqualError(t, validateProcessingDOCXText(runs, images, result), "DOCX_TEXT_COVERAGE_INCOMPLETE")
	result.MarkdownContent += "FOOTNOTE-END-7391"
	result.ImageRefs = nil
	require.EqualError(t, validateProcessingDOCXText(runs, images, result), "DOCX_IMAGE_COVERAGE_INCOMPLETE")
}

func TestProcessingDOCXCoverageAcceptsEscapedMarkdownWithoutLosingLiteralBackslashes(t *testing.T) {
	runs := map[string]int{"literal[link]*text": 1, `path\*file`: 1}
	result := &types.ReadResult{MarkdownContent: "literal\\[link\\]\\*text\n" + `path\\\*file`}
	require.NoError(t, validateProcessingDOCXText(runs, 0, result))
	result.MarkdownContent = "literal\\[link\\]\\*text\npath*file"
	require.EqualError(t, validateProcessingDOCXText(runs, 0, result), "DOCX_TEXT_COVERAGE_INCOMPLETE")
}
func (c *processingExportClient) GetExportProgress(context.Context, string) (*tencentdocs.ExportStatus, error) {
	c.polls++
	if c.polls == 1 {
		return nil, context.DeadlineExceeded
	}
	if c.polls == 2 {
		return &tencentdocs.ExportStatus{Progress: 40}, nil
	}
	if c.polls > 3 {
		if c.refreshURL && c.polls == 4 {
			return &tencentdocs.ExportStatus{Progress: 100, FileURL: "https://docs.qq.com/refreshed?response-content-disposition=attachment%3Bfilename%3Dsynthetic.docx&secret=must-not-enter-ledger"}, nil
		}
		return nil, &tencentdocs.MCPToolError{Code: 404}
	}
	return &tencentdocs.ExportStatus{Progress: 100, FileURL: "https://docs.qq.com/synthetic?response-content-disposition=attachment%3Bfilename%3Dsynthetic.docx&secret=must-not-enter-ledger"}, nil
}
func (c *processingExportClient) DownloadExport(context.Context, string) ([]byte, error) {
	c.downloads++
	if c.refreshURL {
		if c.downloads == 1 {
			return nil, tencentdocs.ErrExportDownloadURLExpired
		}
		if c.downloads == 2 {
			return nil, context.DeadlineExceeded
		}
		return []byte("synthetic-export-content"), nil
	}
	if c.downloads == 1 {
		return nil, context.DeadlineExceeded
	}
	return []byte("synthetic-export-content"), nil
}

func TestProcessingExportKeepsOriginalTaskAcrossPollingAndDownloadRetries(t *testing.T) {
	for _, refresh := range []bool{false, true} {
		t.Run(map[bool]string{false: "saved-url", true: "refreshed-url-checkpoint"}[refresh], func(t *testing.T) {
			t.Setenv("SYSTEM_AES_KEY", "synthetic-32-byte-key-for-tests!")
			db := processingServiceTestDatabase(t)
			r := repository.NewProcessingRepository(db)
			fs := files.NewLocalFileService(t.TempDir(), "")
			artifacts := NewProcessingArtifacts(fs, nil)
			ctx := context.WithValue(context.Background(), types.TenantIDContextKey, uint64(1))
			job, err := r.EnsureJob(ctx, types.ProcessingJob{Kind: types.ProcessingJobDocument, TenantID: 1, KnowledgeBaseID: "kb", DataSourceID: "source", ExternalID: "file", SourceRevision: "fixed", PipelineFingerprint: "fixed"})
			require.NoError(t, err)
			var specs []types.ProcessingStepSpec
			stages := []string{"export_start", "export_poll", "download"}
			for i, stage := range stages {
				spec := types.ProcessingStepSpec{Stage: stage, UnitKey: "body", Phase: types.ProcessingPhasePrepare, RequiredForReady: true, InputFingerprint: stage}
				if i > 0 {
					spec.DependsOn = []string{stages[i-1] + "/body"}
				}
				specs = append(specs, spec)
			}
			require.NoError(t, r.PlanSteps(ctx, 1, job.ID, specs))
			client := &processingExportClient{refreshURL: refresh}
			verifications := 0
			for n := 0; n < 12; n++ {
				require.NoError(t, db.Model(&types.ProcessingStep{}).Where("next_run_at IS NOT NULL").Update("next_run_at", time.Now().Add(-time.Minute)).Error)
				require.NoError(t, r.ReconcileJob(ctx, 1, job.ID))
				ops, err := r.PendingDeliveries(ctx, 10)
				require.NoError(t, err)
				if len(ops) == 0 {
					break
				}
				require.Len(t, ops, 1)
				var ref types.ProcessingRef
				require.NoError(t, json.Unmarshal(ops[0].Payload, &ref))
				lease, err := r.ClaimStep(ctx, 1, ref, time.Minute)
				require.NoError(t, err)
				steps, err := r.ListSteps(ctx, 1, job.ID)
				require.NoError(t, err)
				e := processingDocumentExecution{repo: r, lease: *lease, artifacts: artifacts, document: ProcessingDocumentSpec{FileID: "file", Kind: "doc"}, steps: steps}
				outcome, err := e.exportStage(ctx, client, func(context.Context) error { verifications++; return nil })
				if err != nil {
					outcome = tencentdocs.ProcessingFailure(lease.Step.Stage, err)
				}
				require.NotEqual(t, types.ProcessingBlocked, outcome.Status, "%+v", outcome)
				require.NoError(t, r.FinishStep(ctx, 1, *lease, outcome))
			}
			require.Equal(t, 1, client.starts)
			if refresh {
				require.Equal(t, 3, client.downloads)
				require.Equal(t, 4, client.polls)
			} else {
				require.Equal(t, 2, client.downloads)
				require.Equal(t, 3, client.polls, "provider removes task after first completed poll; download uses encrypted checkpoint")
			}
			require.GreaterOrEqual(t, verifications, 5, "source checked before and after external work")
			steps, err := r.ListSteps(ctx, 1, job.ID)
			require.NoError(t, err)
			for _, step := range steps {
				require.Equal(t, types.ProcessingSucceeded, step.Status, step.Stage)
				if step.Stage == "export_poll" {
					require.Equal(t, 1, step.RetryCount, "normal polling is not failure")
				}
				encoded, _ := json.Marshal(step)
				require.NotContains(t, string(encoded), "must-not-enter-ledger")
				if step.Stage == "download" {
					data, err := artifacts.Read(ctx, *job, step, "source_file", step.OutputManifestRef, step.OutputDigest)
					require.NoError(t, err)
					require.Equal(t, "synthetic-export-content", string(data))
				}
			}
			events, err := r.ListEvents(ctx, 1, job.ID, 0, 100)
			require.NoError(t, err)
			encoded, _ := json.Marshal(events)
			require.NotContains(t, string(encoded), "must-not-enter-ledger")
		})
	}
}

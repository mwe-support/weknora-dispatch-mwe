package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
)

func (s *knowledgeService) processingKnowledgeFile(ctx context.Context, knowledge *types.Knowledge) (io.ReadCloser, string, error) {
	repo, ok := s.repo.(interface {
		ProcessingFileSnapshot(context.Context, uint64, string) (types.ProcessingJob, types.ProcessingStep, error)
	})
	if !ok {
		return nil, "", errors.New("processing file snapshot is unavailable")
	}
	job, step, err := repo.ProcessingFileSnapshot(ctx, knowledge.TenantID, knowledge.ID)
	if err != nil {
		return nil, "", err
	}
	kb, err := s.kbService.GetKnowledgeBaseByID(ctx, job.KnowledgeBaseID)
	if err != nil {
		return nil, "", err
	}
	files := s.resolveFileServiceForPath(ctx, kb, step.OutputManifestRef)
	if files == nil {
		return nil, "", errors.New("processing file storage is unavailable")
	}
	kind := "assets"
	if step.Stage == "download" {
		kind = "source_file"
	} else if step.Stage == "legacy_snapshot" {
		kind = "legacy_snapshot"
	}
	body, err := NewProcessingArtifacts(files, s.resourceCatalog).Read(ctx, job, step, kind, step.OutputManifestRef, step.OutputDigest)
	if err != nil {
		return nil, "", err
	}
	if kind == "assets" {
		var parsed processingParsed
		if err := json.Unmarshal(body, &parsed); err != nil {
			return nil, "", errors.New("processing file artifact is invalid")
		}
		body = []byte(parsed.MarkdownContent)
	} else if kind == "legacy_snapshot" {
		var legacy processingLegacyManifest
		if json.Unmarshal(body, &legacy) != nil || legacy.SourceFile.Path == "" {
			return nil, "", errors.New("legacy file artifact is invalid")
		}
		body, err = NewProcessingArtifacts(files, s.resourceCatalog).Read(ctx, job, step, "source_file", legacy.SourceFile.Path, legacy.SourceFile.Digest)
		if err != nil {
			return nil, "", err
		}
	}
	// Storage I/O runs outside a DB transaction. Recheck publication and the
	// exact artifact before releasing any plaintext after a concurrent change.
	current, confirmed, err := repo.ProcessingFileSnapshot(ctx, knowledge.TenantID, knowledge.ID)
	if err != nil {
		return nil, "", err
	}
	if current.ID != job.ID || current.PublicationEpoch != job.PublicationEpoch || confirmed.ID != step.ID || confirmed.Attempt != step.Attempt || confirmed.OutputDigest != step.OutputDigest || confirmed.OutputManifestRef != step.OutputManifestRef {
		return nil, "", repository.ErrProcessingConflict
	}
	return io.NopCloser(bytes.NewReader(body)), knowledge.FileName, nil
}

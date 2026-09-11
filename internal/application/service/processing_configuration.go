package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/types"
)

func processingFingerprint(values ...any) string {
	encoded, _ := json.Marshal(values)
	return fmt.Sprintf("%x", sha256.Sum256(encoded))
}

// Each stage names its actual dependencies. Publication/cleanup are always
// executed for the new generation; model outputs can reuse equal inputs.
func processingStageInputs(kb *types.KnowledgeBase, cfg *config.Config, keys map[string]string) map[string]string {
	if cfg == nil {
		cfg = &config.Config{}
	}
	native := processingFingerprint("tencent-native-adapter-v2-20260911")
	parse := processingFingerprint(native, kb.ChunkingConfig.ParserEngineRules, cfg.DocReader, keys["tenant_parser"])
	assets := processingFingerprint(parse, keys["storage"], keys["backend"], keys["legacy_storage"])
	chunk := processingFingerprint(assets, keys["chunking"])
	embed := processingFingerprint(keys["embedding"], keys["model/"+kb.EmbeddingModelID], kb.IsVectorEnabled())
	vision := processingFingerprint(chunk, keys["vlm"], keys["images"], keys["model/"+kb.VLMConfig.ModelID])
	var summary, questions any
	if cfg.Conversation != nil {
		summary = []any{cfg.Conversation.Summary, cfg.Conversation.GenerateSummaryPrompt}
		questions = cfg.Conversation.GenerateQuestionsPrompt
	}
	chat := processingFingerprint(keys["summary"], keys["model/"+kb.SummaryModelID])
	return map[string]string{
		"native_read": native, "export_start": native, "export_poll": native, "download": native, "normalize": native,
		"parse": parse, "assets": assets, "chunk": chunk, "text_index": processingFingerprint(chunk, embed),
		"images": vision, "image_index": processingFingerprint(vision, embed),
		"summary": processingFingerprint(chunk, chat, summary), "summary_index": processingFingerprint(chunk, chat, summary, embed),
		"questions": processingFingerprint(vision, chat, questions, keys["questions"]), "question_index": processingFingerprint(vision, chat, questions, keys["questions"], embed),
		"graph":       processingFingerprint(vision, chat, keys["extract"], cfg.ExtractManager),
		"wiki":        processingFingerprint(vision, chat, keys["wiki"], embed),
		"faq_prepare": processingFingerprint(assets, keys["faq"]), "faq_index": processingFingerprint(assets, keys["faq"], embed),
	}
}

func (e *processingDocumentExecution) documentPlan(ctx context.Context, kind string) ([]types.ProcessingStepSpec, error) {
	if e.repo == nil {
		return ProcessingDocumentPlan(e.kb, kind, e.lease.Job.PipelineFingerprint+"/"+e.lease.Job.ConfigurationRevision)
	}
	revision, keys, err := e.repo.ConfigurationDigests(ctx, e.lease.Job.TenantID, e.kb.ID)
	if err != nil {
		return nil, err
	}
	if revision != e.lease.Job.ConfigurationRevision {
		return nil, repository.ErrProcessingScope
	}
	var cfg *config.Config
	if e.s != nil {
		cfg = e.s.config
		keys, err = e.s.processingParserKey(ctx, e.kb.TenantID, keys)
		if err != nil {
			return nil, err
		}
	}
	return ProcessingDocumentPlan(e.kb, kind, e.lease.Job.PipelineFingerprint+"/"+revision, processingStageInputs(e.kb, cfg, keys))
}

func (s *knowledgeService) processingParserKey(ctx context.Context, tenant uint64, keys map[string]string) (map[string]string, error) {
	if s.tenantRepo != nil {
		value, err := s.tenantRepo.GetTenantByID(ctx, tenant)
		if err != nil {
			return nil, err
		}
		keys["tenant_parser"] = processingFingerprint(value.ParserEngineConfig.ToOverridesMap())
	} else {
		keys["tenant_parser"] = processingFingerprint(s.getParserEngineOverridesFromContext(ctx))
	}
	return keys, nil
}

package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/config"
	"github.com/Tencent/WeKnora/internal/datasource/connector/tencentdocs"
	"github.com/Tencent/WeKnora/internal/types"
)

// Explicit source enrollment supports a pilot without switching unrelated
// sources. A fleet-wide cutover sets '*' after legacy workers have drained.
func ProcessingLifecycleEnabled(source *types.DataSource) bool {
	if source == nil || source.Type != types.ConnectorTypeTencentDocs {
		return false
	}
	if source.TencentFileSync {
		return false
	}
	for _, id := range strings.Split(os.Getenv("WEKNORA_PROCESSING_SOURCES"), ",") {
		if strings.TrimSpace(id) == "*" || (strings.TrimSpace(id) != "" && strings.TrimSpace(id) == source.ID) {
			return true
		}
	}
	return false
}

func BeginProcessingScan(ctx context.Context, repo *repository.ProcessingRepository, source *types.DataSource, run *types.SyncLog, cfg *config.Config) (*types.ProcessingJob, error) {
	if source == nil || run == nil || source.Type != types.ConnectorTypeTencentDocs || run.TenantID != source.TenantID || run.DataSourceID != source.ID {
		return nil, repository.ErrProcessingScope
	}
	configuration, err := source.ParseConfig()
	if err != nil {
		return nil, err
	}
	roots, scopeErr := tencentdocs.NativeScanRoots(configuration.ResourceIDs)
	scope, auth, err := repository.ProcessingSourceRevisions(source)
	if err != nil {
		return nil, err
	}
	var specs []types.ProcessingStepSpec
	if scopeErr != nil {
		// Persist configuration failures too, so a run cannot remain "running"
		// without an accountable stage after the delivery exhausts retries.
		specs = append(specs, processingScanSpec("scan_scope", "root", struct{}{}, false))
	}
	for _, root := range roots {
		specs = append(specs, processingScanSpec("scan_page", "root", root, true))
	}
	return repo.BeginScan(ctx, types.ProcessingJob{Kind: types.ProcessingJobScan, TenantID: source.TenantID, KnowledgeBaseID: source.KnowledgeBaseID, DataSourceID: source.ID,
		ExternalID: "run:" + run.ID, OriginRunID: run.ID, SourceRevision: run.ID, ScopeRevision: scope, AuthRevision: auth, PipelineFingerprint: ProcessingPipelineFingerprint(cfg)}, specs)
}

func processingScanSpec(stage, parent string, input any, barrier bool) types.ProcessingStepSpec {
	encoded, _ := json.Marshal(input)
	unit := fmt.Sprintf("%x", sha256.Sum256(append([]byte(parent+"\x00"), encoded...)))
	spec := types.ProcessingStepSpec{Stage: stage, UnitKey: unit, Phase: types.ProcessingPhaseScan, Input: encoded, InputFingerprint: unit, RequiredForCompletion: true}
	if barrier {
		spec.Kind = "barrier"
	}
	return spec
}

func ProcessingDocumentPlan(kb *types.KnowledgeBase, kind, input string, stageInputs ...map[string]string) ([]types.ProcessingStepSpec, error) {
	if kb == nil || input == "" {
		return nil, errors.New("PROCESSING_PLAN_INPUT_INVALID")
	}
	if kb.IndexingStrategy.GraphEnabled && !kb.IsGraphEnabled() {
		return nil, errors.New("GRAPH_CONFIGURATION_INVALID")
	}
	if kb.VLMConfig.Enabled && !kb.IsMultimodalEnabled() {
		return nil, errors.New("VLM_CONFIGURATION_INVALID")
	}
	switch kind {
	case "doc", "smartcanvas", "sheet", "smartsheet", "resource":
	default:
		return nil, errors.New("NATIVE_TYPE_UNSUPPORTED")
	}
	var plan []types.ProcessingStepSpec
	add := func(stage, phase string, barrier bool, dependencies ...string) {
		stageInput := input
		if len(stageInputs) == 1 && stageInputs[0][stage] != "" {
			stageInput = stageInputs[0][stage]
		}
		spec := types.ProcessingStepSpec{Stage: stage, UnitKey: "body", Phase: phase, RequiredForReady: phase == types.ProcessingPhasePrepare, RequiredForCompletion: true,
			InputFingerprint: fmt.Sprintf("%x", sha256.Sum256([]byte(stageInput+"/"+stage)))}
		if barrier {
			spec.Kind = "barrier"
		}
		for _, dep := range dependencies {
			spec.DependsOn = append(spec.DependsOn, dep+"/body")
		}
		plan = append(plan, spec)
	}
	prepare, projection := types.ProcessingPhasePrepare, types.ProcessingPhaseProjection
	add("native_read", prepare, false)
	if kind == "doc" || kind == "resource" {
		add("export_start", prepare, false, "native_read")
		add("export_poll", prepare, false, "export_start")
		add("download", prepare, false, "export_poll")
		add("normalize", prepare, false, "native_read", "export_poll")
		add("parse", prepare, false, "normalize", "download")
	} else {
		add("normalize", prepare, false, "native_read")
		add("parse", prepare, false, "normalize")
	}
	add("assets", prepare, true, "parse")
	if kb.Type == types.KnowledgeBaseTypeFAQ {
		add("faq_prepare", prepare, false, "assets")
		add("faq_index", prepare, true, "faq_prepare")
		add("publish", types.ProcessingPhasePublish, false, "faq_index")
		add("retire_previous", projection, false, "publish")
		return plan, nil
	}
	add("chunk", prepare, false, "assets")
	add("text_index", prepare, true, "chunk")
	add("images", prepare, true, "chunk")
	add("image_index", prepare, true, "images")
	add("publish", types.ProcessingPhasePublish, false, "text_index", "image_index")
	if kb.SummaryModelID != "" {
		add("summary", projection, false, "publish")
		add("summary_index", projection, true, "summary")
	}
	if kb.QuestionGenerationConfig.EffectiveCount() > 0 {
		add("questions", projection, true, "publish")
		add("question_index", projection, true, "questions")
	}
	if kb.IsGraphEnabled() {
		add("graph", projection, true, "publish")
	}
	if kb.IsWikiEnabled() {
		add("wiki", projection, true, "publish")
	}
	dependencies := []string{"publish"}
	for _, spec := range plan {
		if spec.Phase == projection {
			dependencies = append(dependencies, spec.Stage)
		}
	}
	add("retire_previous", projection, false, dependencies...)
	return plan, nil
}

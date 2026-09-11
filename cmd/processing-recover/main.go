// One-time, idempotent repair admission. Existing native workers own retries.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func main() {
	apply := flag.Bool("apply", false, "commit native recovery actions; default rolls each action back")
	mode := flag.String("mode", "documents", "documents, exports, models, docx-svg, or long-ocr")
	flag.Parse()
	filter := ""
	switch *mode {
	case "documents":
		filter = "(s.stage='parse' AND s.error_code IN ('DOCX_TEXT_COVERAGE_INCOMPLETE','DOCX_IMAGE_COVERAGE_INCOMPLETE')) OR (s.stage='chunk' AND s.error_code='CHUNK_PARENT_MAPPING_INVALID')"
	case "exports":
		filter = "(s.stage='export_poll' AND s.error_code='TENCENT_404') OR (s.stage='download' AND s.error_code='EXPORT_DOWNLOAD_URL_EXPIRED')"
	case "models":
		filter = "(s.stage IN ('image_ocr','image_caption') AND s.error_code IN ('IMAGE_MODEL_OUTPUT_INVALID','MODEL_OUTPUT_EMPTY')) OR (s.stage='summary' AND s.error_code IN ('STAGE_EXECUTION_ERROR','MODEL_OUTPUT_EMPTY'))"
	case "docx-svg":
		filter = "s.stage='parse' AND s.error_code='IMAGE_FORMAT_INVALID' AND j.metadata->>'kind'='doc'"
	case "long-ocr":
		filter = "s.stage='embedding' AND s.error_code='STAGE_EXECUTION_ERROR' AND s.input->>'stage'='images' AND EXISTS (SELECT 1 FROM jsonb_array_elements_text(s.input->'chunk_ids') cid JOIN chunks c ON c.id=cid.value WHERE c.chunk_type IN ('image_ocr','image_caption') AND length(c.content)>20000)"
	default:
		fmt.Fprintln(os.Stderr, "invalid recovery mode")
		os.Exit(1)
	}
	for _, key := range []string{"DB_HOST", "DB_USER", "DB_NAME"} {
		if os.Getenv(key) == "" {
			fmt.Fprintln(os.Stderr, "missing database environment")
			os.Exit(1)
		}
	}
	port := os.Getenv("DB_PORT")
	if port == "" {
		port = "5432"
	}
	u := url.URL{Scheme: "postgres", Host: os.Getenv("DB_HOST") + ":" + port, Path: "/" + os.Getenv("DB_NAME"), User: url.UserPassword(os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD"))}
	u.RawQuery = url.Values{"sslmode": {"disable"}}.Encode()
	db, err := gorm.Open(postgres.Open(u.String()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		fmt.Fprintln(os.Stderr, "database unavailable")
		os.Exit(1)
	}
	var items []struct {
		TenantID                        uint64
		JobID, StepID, Stage, ErrorCode string
	}
	err = db.Raw("SELECT j.tenant_id,j.id job_id,s.id step_id,s.stage,s.error_code FROM processing_steps s JOIN processing_jobs j ON j.id=s.job_id WHERE j.is_current AND s.status IN ('failed','blocked') AND (" + filter + ") ORDER BY j.id,s.id").Scan(&items).Error
	if err != nil {
		fmt.Fprintln(os.Stderr, "candidate query failed")
		os.Exit(1)
	}
	ctx := context.Background()
	rollback := errors.New("dry run rollback")
	counts := map[string]int{}
	results := []map[string]any{}
	for _, item := range items {
		status := "scheduled"
		replacement := ""
		operation := "blocker-repair-20260912/" + item.StepID
		if *mode == "docx-svg" {
			operation = "docx-svg-repair-20260912/" + item.StepID
		}
		if *mode == "exports" {
			operation = "export-repair-20260912/" + item.JobID
		}
		if *mode == "long-ocr" {
			operation = "long-ocr-repair-20260912/" + item.JobID
		}
		err := db.Transaction(func(tx *gorm.DB) error {
			action := "retry"
			if *mode == "exports" {
				action = "restart_export"
			}
			if *mode == "long-ocr" {
				action = "rebuild"
			}
			var prior int64
			if err := tx.Model(&types.ProcessingEvent{}).Where("tenant_id=? AND action=? AND operation_request_id=?", item.TenantID, action, operation).Count(&prior).Error; err != nil {
				return err
			}
			if prior > 0 {
				status = "already_scheduled"
				return nil
			}
			repo := repository.NewProcessingRepository(tx)
			job, err := repo.GetJob(ctx, item.TenantID, item.JobID)
			if err != nil {
				return err
			}
			if *mode == "exports" {
				err = repo.RestartExpiredExport(ctx, item.TenantID, item.JobID, types.ProcessingControlRequest{ExpectedRevision: job.Revision, OperationRequestID: operation, Actor: "mwe-release-20260912", Reason: "User-authorized replacement of unavailable acknowledged export; preserve prior receipts and reverify source identity"})
			} else if *mode == "long-ocr" {
				replacement, err = rebuildLongOCR(ctx, repo, job, operation)
			} else {
				err = repo.RetryStep(ctx, item.TenantID, item.JobID, item.StepID, job.Revision, operation, "mwe-release-20260912", "User-authorized recovery after DOCX, chunk and model-output fixes; retain bounded native retry budget")
			}
			if err != nil {
				return err
			}
			if !*apply {
				return rollback
			}
			return nil
		})
		failure := ""
		if err != nil && !errors.Is(err, rollback) {
			status = "rejected"
			switch {
			case errors.Is(err, repository.ErrProcessingScope):
				failure = "scope_changed"
			case errors.Is(err, repository.ErrProcessingConflict):
				failure = "state_conflict"
			case errors.Is(err, gorm.ErrRecordNotFound):
				failure = "record_not_found"
			default:
				failure = fmt.Sprintf("%T", err)
				var sqlError interface{ SQLState() string }
				if errors.As(err, &sqlError) {
					failure = "sqlstate_" + sqlError.SQLState()
				}
			}
		}
		counts[status]++
		results = append(results, map[string]any{"job_id": item.JobID, "step_id": item.StepID, "stage": item.Stage, "error_code": item.ErrorCode, "status": status, "failure": failure, "replacement_job_id": replacement})
	}
	json.NewEncoder(os.Stdout).Encode(map[string]any{"applied": *apply, "mode": *mode, "candidates": len(items), "counts": counts, "results": results})
	if counts["rejected"] > 0 {
		os.Exit(2)
	}
}

// Rebuild only a fully saved document snapshot with the same configuration.
// Reuse of authenticated source/OCR artifacts is handled by the native pipeline;
// derived image chunks and their index plan get fresh generation identities.
func rebuildLongOCR(ctx context.Context, repo *repository.ProcessingRepository, job *types.ProcessingJob, operation string) (string, error) {
	steps, err := repo.ListSteps(ctx, job.TenantID, job.ID)
	if err != nil {
		return "", err
	}
	roots := map[string]types.ProcessingStep{}
	children := map[string]string{}
	confirmed := map[string]bool{}
	for _, step := range steps {
		if step.ParentStepID != "" {
			children[step.ID] = step.ParentStepID
		}
		switch step.Stage {
		case "native_read", "export_start", "export_poll", "download":
			confirmed[step.Stage] = step.Status == types.ProcessingSucceeded && step.OutputManifestRef != "" && len(step.OutputDigest) == 64
		}
		if step.ParentStepID != "" || step.Phase == types.ProcessingPhaseRetire {
			continue
		}
		if step.Status == types.ProcessingSkipped && !step.RequiredForReady && !step.RequiredForCompletion {
			continue
		}
		roots[step.ID] = step
	}
	for _, stage := range []string{"native_read", "export_start", "export_poll", "download"} {
		if !confirmed[stage] {
			return "", repository.ErrProcessingConflict
		}
	}
	var plan []types.ProcessingStepSpec
	for _, step := range steps {
		if _, exists := roots[step.ID]; !exists {
			continue
		}
		spec := types.ProcessingStepSpec{Stage: step.Stage, UnitKey: step.UnitKey, Phase: step.Phase, Kind: step.Kind, Input: step.Input, InputFingerprint: step.InputFingerprint, RequiredForReady: step.RequiredForReady, RequiredForCompletion: step.RequiredForCompletion}
		// Match normal planning after a question-disable amendment changed the
		// configuration revision but deliberately retained old execution inputs.
		if step.Stage == "publish" || step.Stage == "retire_previous" {
			spec.InputFingerprint = fmt.Sprintf("%x", sha256.Sum256([]byte(job.PipelineFingerprint+"/"+job.ConfigurationRevision+"/"+step.Stage)))
		}
		var dependencies []string
		if len(step.Dependencies) > 0 && json.Unmarshal(step.Dependencies, &dependencies) != nil {
			return "", repository.ErrProcessingConflict
		}
		for _, id := range dependencies {
			dependency, exists := roots[id]
			if !exists {
				// A fresh barrier recreates its own dynamic children. Keeping
				// their old IDs would tie this plan to the failed generation.
				if step.Kind == "barrier" && children[id] == step.ID {
					continue
				}
				return "", repository.ErrProcessingConflict
			}
			spec.DependsOn = append(spec.DependsOn, dependency.Stage+"/"+dependency.UnitKey)
		}
		plan = append(plan, spec)
	}
	result, err := repo.RebuildJob(ctx, job.TenantID, job.ID, types.ProcessingControlRequest{ExpectedRevision: job.Revision, OperationRequestID: operation, Actor: "mwe-release-20260912", Reason: "Rebuild saved snapshot after lossless splitting of oversized OCR/caption text; retain source and successful model outputs"}, job.PipelineFingerprint, job.ConfigurationRevision, plan)
	if err != nil {
		return "", err
	}
	return result.ID, nil
}

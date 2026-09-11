package repository

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/Tencent/WeKnora/internal/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type QuestionDefaultsReport struct {
	KnowledgeBases int  `json:"knowledge_bases"`
	Jobs           int  `json:"jobs"`
	SkippedSteps   int  `json:"skipped_steps"`
	AlreadyApplied bool `json:"already_applied"`
}

// ResetQuestionDefaults is an explicit, one-time data operation. It is never
// called on ordinary application startup, so later user opt-ins stay intact.
func (r *ProcessingRepository) ResetQuestionDefaults(ctx context.Context) (report QuestionDefaultsReport, err error) {
	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if tx.Dialector.Name() == "postgres" {
			if err := tx.Exec("SELECT pg_advisory_xact_lock(hashtextextended(?,0))", types.QuestionDefaultsMigrationKey).Error; err != nil {
				return err
			}
		}
		var marker types.SystemSetting
		err := tx.Where("key = ?", types.QuestionDefaultsMigrationKey).Take(&marker).Error
		if err == nil {
			applied, valueErr := marker.AsBool()
			if valueErr != nil || !applied {
				return errors.New("question defaults migration receipt is invalid")
			}
			report.AlreadyApplied = true
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		var bases []types.KnowledgeBase
		if err := tx.Order("tenant_id,id").Find(&bases).Error; err != nil {
			return err
		}
		for i := range bases {
			kb := &bases[i]
			config := types.QuestionGenerationConfig{}
			if kb.QuestionGenerationConfig != nil {
				config.CustomInstructions = kb.QuestionGenerationConfig.CustomInstructions
			}
			kb.QuestionGenerationConfig = &config
			if err := updateKnowledgeBaseQuestionPolicy(tx, kb, "system:question-default-zero", &report, true); err != nil {
				return err
			}
			report.KnowledgeBases++
		}
		return tx.Create(&types.SystemSetting{Key: types.QuestionDefaultsMigrationKey, Value: types.JSON(`true`), ValueType: "bool", Category: "internal",
			Description: "One-time question defaults reset completed; later user choices are preserved."}).Error
	})
	return
}

func questionConfigurationDigest(inputs map[string]any) (string, error) {
	encoded, err := json.Marshal(inputs)
	return fmt.Sprintf("%x", sha256.Sum256(encoded)), err
}

// Only a question-disable change can amend an active generation. Other input
// changes retain the existing configuration fence and require a new version.
func updateKnowledgeBaseQuestionPolicy(tx *gorm.DB, kb *types.KnowledgeBase, actor string, report *QuestionDefaultsReport, questionOnly bool) error {
	save := func() error {
		if questionOnly {
			return tx.Model(&types.KnowledgeBase{}).Where("id = ? AND tenant_id = ?", kb.ID, kb.TenantID).
				Update("question_generation_config", kb.QuestionGenerationConfig).Error
		}
		return tx.Save(kb).Error
	}
	if kb.QuestionGenerationConfig.EffectiveCount() > 0 || !tx.Migrator().HasTable(&types.ProcessingJob{}) {
		return save()
	}
	var count int64
	if err := tx.Model(&types.ProcessingJob{}).Where("tenant_id = ? AND knowledge_base_id = ? AND is_current = ?", kb.TenantID, kb.ID, true).Count(&count).Error; err != nil {
		return err
	}
	if count == 0 {
		return save()
	}
	if err := lockTenantParserConfiguration(tx, kb.TenantID, true); err != nil {
		return err
	}
	var old types.KnowledgeBase
	query := tx.Where("tenant_id = ? AND id = ?", kb.TenantID, kb.ID)
	if tx.Dialector.Name() == "postgres" {
		query = query.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	if err := query.Take(&old).Error; err != nil {
		return err
	}
	oldQuestions, _ := json.Marshal(old.QuestionGenerationConfig)
	newQuestions, _ := json.Marshal(kb.QuestionGenerationConfig)
	if bytes.Equal(oldQuestions, newQuestions) {
		return save()
	}
	before, err := lockProcessingConfigurationInputs(tx, kb.TenantID, kb.ID)
	if err != nil {
		return err
	}
	oldRevision, err := questionConfigurationDigest(before)
	if err != nil {
		return err
	}
	if err := save(); err != nil {
		return err
	}
	after, err := lockProcessingConfigurationInputs(tx, kb.TenantID, kb.ID)
	if err != nil {
		return err
	}
	newRevision, err := questionConfigurationDigest(after)
	if err != nil || oldRevision == newRevision {
		return err
	}
	delete(before, "questions")
	delete(after, "questions")
	oldInputs, _ := json.Marshal(before)
	newInputs, _ := json.Marshal(after)
	if !bytes.Equal(oldInputs, newInputs) {
		return nil // A broader edit must not bypass normal version fencing.
	}
	var jobs []types.ProcessingJob
	if err := tx.Where("tenant_id = ? AND knowledge_base_id = ? AND is_current = ?", kb.TenantID, kb.ID, true).Order("datasource_id,id").Find(&jobs).Error; err != nil {
		return err
	}
	for i := range jobs {
		job, err := lockProcessingRetirement(tx, kb.TenantID, jobs[i].ID)
		if err != nil {
			return err
		}
		if job.ConfigurationRevision != oldRevision {
			return fmt.Errorf("question policy cannot amend an already mismatched job: %w", ErrProcessingScope)
		}
		if err := disableProcessingQuestions(tx, job, newRevision, actor, report); err != nil {
			return err
		}
		jobs[i] = *job
	}
	// Project run totals only after every member of this KB is updated.
	for i := range jobs {
		if err := refreshProcessingRuns(tx, &jobs[i]); err != nil {
			return err
		}
		if err := scheduleProcessingSteps(tx, &jobs[i]); err != nil {
			return err
		}
	}
	return nil
}

func disableProcessingQuestions(tx *gorm.DB, job *types.ProcessingJob, revision, actor string, report *QuestionDefaultsReport) error {
	var steps []types.ProcessingStep
	if err := tx.Where("job_id = ?", job.ID).Order("id").Find(&steps).Error; err != nil {
		return err
	}
	disabled := map[string]bool{}
	for _, step := range steps {
		if step.Stage == "questions" || step.Stage == "question" || step.Stage == "question_index" {
			disabled[step.ID] = true
		}
	}
	for changed := true; changed; {
		changed = false
		for _, step := range steps {
			if disabled[step.ParentStepID] && !disabled[step.ID] {
				disabled[step.ID], changed = true, true
			}
		}
	}
	newPlanDigest, err := questionDisabledPlanDigest(job, steps, disabled, revision)
	if err != nil {
		return err
	}
	now, err := processingDBTime(tx)
	if err != nil {
		return err
	}
	skipped := 0
	for i := range steps {
		step := &steps[i]
		if disabled[step.ID] {
			if step.Phase != types.ProcessingPhaseProjection || step.RequiredForReady {
				return errors.New("question policy cannot cancel a primary processing stage")
			}
			changes := map[string]any{"required_for_completion": false}
			if step.Status != types.ProcessingSucceeded && step.Status != types.ProcessingSkipped {
				event := stepEvent(step, "step_skipped_by_policy", step.Status)
				event.ToState, event.Actor, event.Message = types.ProcessingSkipped, actor, "Question generation disabled by user policy"
				if err := appendProcessingEvent(tx, job, event); err != nil {
					return err
				}
				changes["status"], changes["finished_at"] = types.ProcessingSkipped, now
				changes["lease_token"], changes["lease_expires_at"], changes["next_run_at"] = "", nil, nil
				changes["error_class"], changes["error_code"], changes["error_message"] = "", "", ""
				skipped++
			}
			if err := tx.Model(step).Updates(changes).Error; err != nil {
				return err
			}
			var incidents []int64
			if err := tx.Model(&types.ProcessingEvent{}).Where("job_id = ? AND step_id = ? AND error_code <> ''", job.ID, step.ID).
				Where("id NOT IN (?)", tx.Model(&types.ProcessingEvent{}).Select("resolves_event_id").Where("resolves_event_id IS NOT NULL")).Order("id").Pluck("id", &incidents).Error; err != nil {
				return err
			}
			for _, id := range incidents {
				if err := appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "incident_resolved", StepID: step.ID, Attempt: step.Attempt, ResolvesEventID: &id, ResolutionType: "feature_disabled", Actor: actor}); err != nil {
					return err
				}
			}
			continue
		}
		var dependencies []string
		if len(step.Dependencies) > 0 && json.Unmarshal(step.Dependencies, &dependencies) != nil {
			return ErrProcessingConflict
		}
		remaining := slices.DeleteFunc(slices.Clone(dependencies), func(id string) bool { return disabled[id] })
		if len(remaining) != len(dependencies) {
			if step.Stage != "retire_previous" {
				return errors.New("unexpected non-question consumer of generated questions")
			}
			encoded, _ := json.Marshal(remaining)
			if err := tx.Model(step).Update("dependencies", types.JSON(encoded)).Error; err != nil {
				return err
			}
		}
	}
	detail, _ := json.Marshal(map[string]any{"previous_configuration_revision": job.ConfigurationRevision, "configuration_revision": revision, "compatible_plan_digest": newPlanDigest, "skipped_steps": skipped})
	job.ConfigurationRevision = revision
	if err := tx.Model(job).Update("configuration_revision", revision).Error; err != nil {
		return err
	}
	if err := appendProcessingEvent(tx, job, types.ProcessingEvent{Type: "questions_disabled", Actor: actor, Action: "disable_questions", Detail: detail}); err != nil {
		return err
	}
	if job.PlanSealed && job.Status != types.ProcessingCanceled && job.Status != types.ProcessingSuperseded {
		if err := refreshProcessingJob(tx, job); err != nil {
			return err
		}
	}
	if report != nil {
		report.Jobs++
		report.SkippedSteps += skipped
	}
	return nil
}

// Preserve the original sealed plan and immutable step input fingerprints.
// Record an equivalent question-free admission digest for later source scans.
func questionDisabledPlanDigest(job *types.ProcessingJob, steps []types.ProcessingStep, disabled map[string]bool, revision string) (string, error) {
	roots := map[string]types.ProcessingStep{}
	for _, step := range steps {
		if step.ParentStepID == "" && !disabled[step.ID] {
			roots[step.ID] = step
		}
	}
	var specs []types.ProcessingStepSpec
	for _, step := range roots {
		spec := types.ProcessingStepSpec{Stage: step.Stage, UnitKey: step.UnitKey, Kind: step.Kind, Phase: step.Phase, Input: step.Input,
			InputFingerprint: step.InputFingerprint, RequiredForReady: step.RequiredForReady, RequiredForCompletion: step.RequiredForCompletion}
		if step.Stage == "publish" || step.Stage == "retire_previous" {
			spec.InputFingerprint = fmt.Sprintf("%x", sha256.Sum256([]byte(job.PipelineFingerprint+"/"+revision+"/"+step.Stage)))
		}
		var dependencies []string
		if len(step.Dependencies) > 0 && json.Unmarshal(step.Dependencies, &dependencies) != nil {
			return "", ErrProcessingConflict
		}
		for _, id := range dependencies {
			if root, ok := roots[id]; ok {
				spec.DependsOn = append(spec.DependsOn, root.Stage+"/"+root.UnitKey)
			}
		}
		specs = append(specs, spec)
	}
	if len(specs) == 0 {
		return "", nil
	}
	_, digest, err := processingPlan(job.ID, specs)
	return digest, err
}

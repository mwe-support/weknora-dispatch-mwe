package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/searchutil"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
)

func (e *processingDocumentExecution) questionBarrier(ctx context.Context) (types.ProcessingOutcome, error) {
	chunks, err := e.indexChunks(ctx, "chunk")
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	if !e.lease.Step.PlanSealed {
		images, err := e.indexChunks(ctx, "images")
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		var contents []string
		for _, chunk := range chunks {
			contents = append(contents, chunk.Content)
		}
		for _, image := range images {
			contents = append(contents, image.Content, image.ImageInfo)
		}
		contextFingerprint := processingFingerprint(contents)
		var specs []types.ProcessingStepSpec
		for i, chunk := range chunks {
			input, _ := json.Marshal(map[string]string{"chunk_id": chunk.ID})
			specs = append(specs, types.ProcessingStepSpec{Stage: "question", UnitKey: chunk.ID, Phase: e.lease.Step.Phase, Input: input, InputFingerprint: processingFingerprint(e.lease.Step.InputFingerprint, contextFingerprint, i, chunk.ContentRevision, chunk.Content)})
		}
		next := time.Now().UTC().Add(time.Second)
		return types.ProcessingOutcome{Status: types.ProcessingWaitingExternal, NextRunAt: &next, SealPlan: true, ChildSteps: specs}, nil
	}
	var groups []types.ProcessingChunkQuestions
	for _, chunk := range chunks {
		var group types.ProcessingChunkQuestions
		if err := e.dependency(ctx, "question", chunk.ID, "question", &group); err != nil {
			return types.ProcessingOutcome{}, err
		}
		if group.ChunkID != chunk.ID || group.ContentRevision != chunk.ContentRevision || len(group.Questions) == 0 {
			return types.ProcessingOutcome{}, errors.New("QUESTION_COVERAGE_INVALID")
		}
		groups = append(groups, group)
	}
	return e.success(ctx, "questions", groups)
}

func (e *processingDocumentExecution) generateChunkQuestions(ctx context.Context) (types.ProcessingOutcome, error) {
	var input struct {
		ChunkID string `json:"chunk_id"`
	}
	if json.Unmarshal(e.lease.Step.Input, &input) != nil || input.ChunkID == "" || input.ChunkID != e.lease.Step.UnitKey {
		return types.ProcessingOutcome{}, errors.New("QUESTION_INPUT_INVALID")
	}
	cfg := e.kb.QuestionGenerationConfig
	if cfg.EffectiveCount() == 0 || e.kb.SummaryModelID == "" {
		return types.ProcessingOutcome{}, errors.New("QUESTION_CONFIGURATION_INVALID")
	}
	chunks, err := e.indexChunks(ctx, "chunk")
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	sort.SliceStable(chunks, func(i, j int) bool { return chunks[i].StartAt < chunks[j].StartAt })
	index := -1
	for i, chunk := range chunks {
		if chunk.ID == input.ChunkID {
			index = i
			break
		}
	}
	if index < 0 {
		return types.ProcessingOutcome{}, errors.New("QUESTION_CHUNK_MISSING")
	}
	chunk := chunks[index]
	current, err := e.s.chunkRepo.GetChunkByID(ctx, e.lease.Job.TenantID, chunk.ID)
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	if current.ContentRevision != chunk.ContentRevision || current.Content != chunk.Content {
		return types.ProcessingOutcome{}, errors.New("QUESTION_SOURCE_CHANGED")
	}
	// Read only the confirmed image artifact. A best-effort live image query
	// could hide a read failure or mix in another attempt's OCR/caption output.
	images, err := e.indexChunks(ctx, "images")
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	infos := map[string][]*types.ImageInfo{}
	seen := map[string]bool{}
	for _, image := range images {
		var values []*types.ImageInfo
		if json.Unmarshal([]byte(image.ImageInfo), &values) != nil {
			return types.ProcessingOutcome{}, errors.New("QUESTION_IMAGE_CONTEXT_INVALID")
		}
		for _, value := range values {
			if value == nil {
				continue
			}
			key := image.ParentChunkID + "/" + value.URL
			if !seen[key] {
				infos[image.ParentChunkID] = append(infos[image.ParentChunkID], value)
				seen[key] = true
			}
		}
	}
	content := func(i int) string {
		if i < 0 || i >= len(chunks) {
			return ""
		}
		c := chunks[i]
		if len(infos[c.ID]) == 0 {
			return c.Content
		}
		data, _ := json.Marshal(infos[c.ID])
		return searchutil.EnrichContentWithImageInfo(c.Content, string(data))
	}
	count := cfg.EffectiveCount()
	var questions []string
	data, _, _, reused, err := e.reusableBytes(ctx, "question")
	if err != nil {
		return types.ProcessingOutcome{}, err
	}
	if reused {
		var saved types.ProcessingChunkQuestions
		if json.Unmarshal(data, &saved) != nil || len(saved.Questions) == 0 {
			return types.ProcessingOutcome{}, errors.New("REUSE_QUESTIONS_INVALID")
		}
		for _, question := range saved.Questions {
			questions = append(questions, question.Question)
		}
	} else {
		model, err := e.s.modelService.GetChatModel(ctx, e.kb.SummaryModelID)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
		questions, err = e.s.generateQuestionsWithContext(ctx, model, content(index), content(index-1), content(index+1), e.document.Title, count, cfg.CustomInstructions)
		if err != nil {
			return types.ProcessingOutcome{}, err
		}
	}
	questions = types.SanitizeStrings(questions)
	if len(questions) == 0 {
		return types.ProcessingOutcome{}, errors.New("QUESTION_OUTPUT_EMPTY")
	}
	group := types.ProcessingChunkQuestions{ChunkID: chunk.ID, ContentRevision: chunk.ContentRevision}
	for i, text := range questions {
		if len(strings.TrimSpace(text)) == 0 || len(text) > 16<<10 {
			return types.ProcessingOutcome{}, errors.New("QUESTION_OUTPUT_INVALID")
		}
		id := uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("%s/%d/question/%d", e.lease.Step.ID, e.lease.Ref.Attempt, i))).String()
		revision := chunk.ContentRevision
		group.Questions = append(group.Questions, types.GeneratedQuestion{ID: id, Question: text, ContentRevision: &revision})
	}
	outcome, err := e.success(ctx, "question", group)
	outcome.Questions = &group
	outcome.CopiedArtifact = reused
	return outcome, err
}

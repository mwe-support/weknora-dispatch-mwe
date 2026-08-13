package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
)

type recordingProcessingFailureRepo struct {
	interfaces.KnowledgeRepository
	updated *types.Knowledge
}

func (r *recordingProcessingFailureRepo) UpdateKnowledge(_ context.Context, knowledge *types.Knowledge) error {
	r.updated = knowledge
	return nil
}

func TestFinalizeIndexedKnowledgeState(t *testing.T) {
	now := time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name                 string
		hasPendingMultimodal bool
		textChunkCount       int
		wantParseStatus      string
		wantSummaryStatus    string
		wantErrorMessage     string
	}{
		{
			name:              "text document stays processing so post-process can fan out enrichment",
			textChunkCount:    2,
			wantParseStatus:   types.ParseStatusProcessing,
			wantSummaryStatus: types.SummaryStatusNone,
			// Still processing: FinalizeSubtask clears the column when this
			// row is eventually promoted to completed.
			wantErrorMessage: "previous attempt failed",
		},
		{
			name:              "empty indexed document is completed without summary work",
			textChunkCount:    0,
			wantParseStatus:   types.ParseStatusCompleted,
			wantSummaryStatus: types.SummaryStatusNone,
			wantErrorMessage:  "",
		},
		{
			name:                 "document waits while multimodal image work is pending",
			hasPendingMultimodal: true,
			textChunkCount:       2,
			wantParseStatus:      types.ParseStatusProcessing,
			wantSummaryStatus:    types.SummaryStatusNone,
			wantErrorMessage:     "previous attempt failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			knowledge := &types.Knowledge{
				ParseStatus:   types.ParseStatusProcessing,
				SummaryStatus: types.SummaryStatusCompleted,
				ErrorMessage:  "previous attempt failed",
			}

			finalizeIndexedKnowledgeState(knowledge, 4096, tt.textChunkCount, tt.hasPendingMultimodal, now)

			if knowledge.ErrorMessage != tt.wantErrorMessage {
				t.Fatalf("ErrorMessage = %q, want %q", knowledge.ErrorMessage, tt.wantErrorMessage)
			}

			if knowledge.ParseStatus != tt.wantParseStatus {
				t.Fatalf("ParseStatus = %q, want %q", knowledge.ParseStatus, tt.wantParseStatus)
			}
			if knowledge.SummaryStatus != tt.wantSummaryStatus {
				t.Fatalf("SummaryStatus = %q, want %q", knowledge.SummaryStatus, tt.wantSummaryStatus)
			}
			if knowledge.EnableStatus != "enabled" {
				t.Fatalf("EnableStatus = %q, want enabled", knowledge.EnableStatus)
			}
			if knowledge.StorageSize != 4096 {
				t.Fatalf("StorageSize = %d, want 4096", knowledge.StorageSize)
			}
			if knowledge.ProcessedAt == nil || !knowledge.ProcessedAt.Equal(now) {
				t.Fatalf("ProcessedAt = %v, want %v", knowledge.ProcessedAt, now)
			}
			if !knowledge.UpdatedAt.Equal(now) {
				t.Fatalf("UpdatedAt = %v, want %v", knowledge.UpdatedAt, now)
			}
		})
	}
}

func TestMarkKnowledgeProcessingFailedRecordsTerminalError(t *testing.T) {
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	knowledge := &types.Knowledge{
		ParseStatus:  types.ParseStatusProcessing,
		ErrorMessage: "",
	}

	markKnowledgeProcessingFailed(knowledge, errors.New("model ID cannot be empty"), now)

	if knowledge.ParseStatus != types.ParseStatusFailed {
		t.Fatalf("ParseStatus = %q, want %q", knowledge.ParseStatus, types.ParseStatusFailed)
	}
	if knowledge.ErrorMessage != "model ID cannot be empty" {
		t.Fatalf("ErrorMessage = %q", knowledge.ErrorMessage)
	}
	if !knowledge.UpdatedAt.Equal(now) {
		t.Fatalf("UpdatedAt = %v, want %v", knowledge.UpdatedAt, now)
	}
}

func TestPersistKnowledgeProcessingFailureWritesTerminalState(t *testing.T) {
	repo := &recordingProcessingFailureRepo{}
	service := &knowledgeService{repo: repo}
	knowledge := &types.Knowledge{ID: "knowledge-1", ParseStatus: types.ParseStatusProcessing}

	service.persistKnowledgeProcessingFailure(context.Background(), knowledge, errors.New("model unavailable"))

	if repo.updated != knowledge {
		t.Fatal("UpdateKnowledge was not called with the failed knowledge")
	}
	if knowledge.ParseStatus != types.ParseStatusFailed || knowledge.ErrorMessage != "model unavailable" {
		t.Fatalf("terminal state = %q / %q", knowledge.ParseStatus, knowledge.ErrorMessage)
	}
}

// TestMarkKnowledgeProcessingClearsPreviousAttemptError covers the transition
// every worker performs before it starts a new attempt. A row that failed
// earlier still carries that attempt's error_message, and leaving it in place
// makes the UI report a failure on a document it is simultaneously showing as
// processing.
func TestMarkKnowledgeProcessingClearsPreviousAttemptError(t *testing.T) {
	now := time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC)
	knowledge := &types.Knowledge{
		ParseStatus:  types.ParseStatusFailed,
		ErrorMessage: "Task interrupted due to application restart",
	}

	markKnowledgeProcessing(knowledge, now)

	if knowledge.ParseStatus != types.ParseStatusProcessing {
		t.Fatalf("ParseStatus = %q, want %q", knowledge.ParseStatus, types.ParseStatusProcessing)
	}
	if knowledge.ErrorMessage != "" {
		t.Fatalf("ErrorMessage = %q, want empty", knowledge.ErrorMessage)
	}
	if !knowledge.UpdatedAt.Equal(now) {
		t.Fatalf("UpdatedAt = %v, want %v", knowledge.UpdatedAt, now)
	}
}

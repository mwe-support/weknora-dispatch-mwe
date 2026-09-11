package service

import (
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestProcessingQuestionZeroPlansNoQuestionWork(t *testing.T) {
	for _, count := range []int{0, 1, 3, 10} {
		kb := &types.KnowledgeBase{QuestionGenerationConfig: &types.QuestionGenerationConfig{Enabled: true, QuestionCount: count}}
		plan, err := ProcessingDocumentPlan(kb, "smartcanvas", "input")
		require.NoError(t, err)
		stages := map[string]bool{}
		for _, step := range plan {
			stages[step.Stage] = true
		}
		require.Equal(t, count > 0, stages["questions"])
		require.Equal(t, count > 0, stages["question_index"])
		require.True(t, stages["publish"])
		require.True(t, stages["text_index"])
	}
	defaults := ResolveProcessConfig(&types.KnowledgeBase{}, nil)
	require.Zero(t, defaults.QuestionGenerationConfig.EffectiveCount())
	override := &types.KnowledgeProcessOverrides{QuestionGenerationConfig: &types.QuestionGenerationConfig{Enabled: true, QuestionCount: 2}}
	chosen := ResolveProcessConfig(&types.KnowledgeBase{}, override)
	require.Equal(t, 2, chosen.QuestionGenerationConfig.EffectiveCount())
	override.QuestionGenerationConfig.QuestionCount = 0
	off := ResolveProcessConfig(&types.KnowledgeBase{}, override)
	require.False(t, off.QuestionGenerationConfig.Enabled)
}

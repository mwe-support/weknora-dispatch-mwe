package types

import "testing"

func TestQuestionGenerationZeroIsDisabled(t *testing.T) {
	for _, test := range []struct {
		config *QuestionGenerationConfig
		want   int
	}{
		{nil, 0}, {&QuestionGenerationConfig{}, 0},
		{&QuestionGenerationConfig{Enabled: true, QuestionCount: 0}, 0},
		{&QuestionGenerationConfig{Enabled: false, QuestionCount: 3}, 0},
		{&QuestionGenerationConfig{Enabled: true, QuestionCount: 1}, 1},
		{&QuestionGenerationConfig{Enabled: true, QuestionCount: 10}, 10},
	} {
		if got := test.config.EffectiveCount(); got != test.want {
			t.Fatalf("effective count=%d want %d", got, test.want)
		}
	}
}

package router

import (
	"fmt"
	"github.com/Tencent/WeKnora/internal/datasource"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/hibiken/asynq"
	"testing"
	"time"
)

func TestTencentDocsTaskRetryIsAtLeastTwoMinutes(t *testing.T) {
	task := asynq.NewTask(types.TypeDataSourceSync, nil)
	err := fmt.Errorf("transport unavailable: %w", datasource.ErrTencentDocsSyncRetry)
	for n := 0; n < 6; n++ {
		if delay := asynqRetryDelayFunc(n, err, task); delay < 2*time.Minute {
			t.Fatalf("retry %d is only %v", n, delay)
		}
	}
}

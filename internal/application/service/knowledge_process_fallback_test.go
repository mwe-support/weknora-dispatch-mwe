package service

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/Tencent/WeKnora/internal/infrastructure/docparser"
	"github.com/Tencent/WeKnora/internal/types"
)

type fallbackReaderStub struct {
	result *types.ReadResult
	err    error
	calls  int
	engine string
}

func (r *fallbackReaderStub) Read(_ context.Context, req *types.ReadRequest) (*types.ReadResult, error) {
	r.calls++
	r.engine = req.ParserEngine
	return r.result, r.err
}

func TestCallDocReaderWithMinerUFallback(t *testing.T) {
	t.Run("transient MinerU failure uses builtin", func(t *testing.T) {
		primary := &fallbackReaderStub{err: fmt.Errorf("%w: cuda busy", docparser.ErrMinerUTransient)}
		fallback := &fallbackReaderStub{result: &types.ReadResult{MarkdownContent: "fallback"}}
		req := &types.ReadRequest{FileName: "doc.pdf", ParserEngine: "mineru"}

		result, engine, err := (&knowledgeService{}).callDocReaderWithMinerUFallback(
			t.Context(), primary, fallback, req,
		)
		if err != nil || result == nil || result.MarkdownContent != "fallback" {
			t.Fatalf("result/error=%+v/%v", result, err)
		}
		if engine != "builtin" || primary.calls != 1 || fallback.calls != 1 || fallback.engine != "builtin" {
			t.Fatalf("engine/calls=%s/%d/%d fallback_engine=%s", engine, primary.calls, fallback.calls, fallback.engine)
		}
		if req.ParserEngine != "mineru" {
			t.Fatalf("original request mutated: engine=%s", req.ParserEngine)
		}
	})

	t.Run("ordinary parsing error is preserved", func(t *testing.T) {
		ordinary := errors.New("invalid document")
		primary := &fallbackReaderStub{err: ordinary}
		fallback := &fallbackReaderStub{result: &types.ReadResult{MarkdownContent: "must not run"}}

		_, engine, err := (&knowledgeService{}).callDocReaderWithMinerUFallback(
			t.Context(), primary, fallback, &types.ReadRequest{ParserEngine: "mineru"},
		)
		if !errors.Is(err, ordinary) || engine != "mineru" || fallback.calls != 0 {
			t.Fatalf("engine/error/fallback_calls=%s/%v/%d", engine, err, fallback.calls)
		}
	})
}

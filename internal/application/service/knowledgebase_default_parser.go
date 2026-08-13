package service

import "github.com/Tencent/WeKnora/internal/types"

func applyDefaultParserEngineRules(kb *types.KnowledgeBase) {
	if kb == nil || kb.ParserEngineRulesProvided || len(kb.ChunkingConfig.ParserEngineRules) > 0 {
		return
	}
	kb.ChunkingConfig.ParserEngineRules = types.DefaultKnowledgeBaseParserEngineRules()
}

package handler

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHasParserEngineRulesFieldDistinguishesExplicitEmptyArray(t *testing.T) {
	tests := []struct {
		name string
		body string
		want bool
	}{
		{name: "omitted chunking config", body: `{}`, want: false},
		{name: "omitted parser rules", body: `{"chunking_config":{}}`, want: false},
		{name: "explicit empty rules", body: `{"chunking_config":{"parser_engine_rules":[]}}`, want: true},
		{name: "explicit null rules", body: `{"chunking_config":{"parser_engine_rules":null}}`, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var raw map[string]json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(tt.body), &raw))
			require.Equal(t, tt.want, hasParserEngineRulesField(raw))
		})
	}
}

package types

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTenantDeploymentMinerUDefaultsApplyToNewAndLoadedTenants(t *testing.T) {
	t.Setenv(DefaultMinerUEndpointEnv, " http://mineru-api:8000 ")

	created := &Tenant{}
	require.NoError(t, created.BeforeCreate(nil))
	require.Nil(t, created.ParserEngineConfig, "deployment defaults must not be written during insert")
	require.NoError(t, created.AfterCreate(nil))
	require.NotNil(t, created.ParserEngineConfig)
	require.Equal(t, "http://mineru-api:8000", created.ParserEngineConfig.MinerUEndpoint)
	require.Equal(t, "pipeline", created.ParserEngineConfig.MinerUModel)
	require.Equal(t, MinerUParseMethodAuto, created.ParserEngineConfig.MinerUParseMethod)
	require.Equal(t, "ch", created.ParserEngineConfig.MinerULanguage)
	require.NotNil(t, created.ParserEngineConfig.MinerUEnableFormula)
	require.True(t, *created.ParserEngineConfig.MinerUEnableFormula)
	require.NotNil(t, created.ParserEngineConfig.MinerUEnableTable)
	require.True(t, *created.ParserEngineConfig.MinerUEnableTable)

	loaded := &Tenant{}
	require.NoError(t, loaded.AfterFind(nil))
	require.Equal(t, "http://mineru-api:8000", loaded.ParserEngineConfig.MinerUEndpoint)
	require.Nil(t, loaded.ParserEngineConfigForPersistence(), "inherited defaults must not leak into unrelated updates")
}

func TestTenantDeploymentMinerUDefaultsKeepPersistedAndExplicitValuesSeparate(t *testing.T) {
	t.Setenv(DefaultMinerUEndpointEnv, "http://mineru-api:8000")
	persisted := &ParserEngineConfig{MinerULanguage: "en"}
	tenant := &Tenant{ParserEngineConfig: persisted}

	require.NoError(t, tenant.AfterFind(nil))
	require.Equal(t, "http://mineru-api:8000", tenant.ParserEngineConfig.MinerUEndpoint)
	require.Equal(t, "en", tenant.ParserEngineConfig.MinerULanguage)
	require.Empty(t, tenant.ParserEngineConfigForPersistence().MinerUEndpoint)
	require.Equal(t, "en", tenant.ParserEngineConfigForPersistence().MinerULanguage)

	tenant.ParserEngineConfig.MinerUEndpoint = "http://workspace-mineru:8000"
	tenant.MarkParserEngineConfigExplicit()
	require.Equal(t, "http://workspace-mineru:8000", tenant.ParserEngineConfigForPersistence().MinerUEndpoint)
}

func TestTenantDeploymentMinerUDefaultsPreserveExplicitOverrides(t *testing.T) {
	t.Setenv(DefaultMinerUEndpointEnv, "http://mineru-api:8000")
	disabled := false
	tenant := &Tenant{ParserEngineConfig: &ParserEngineConfig{
		MinerUEndpoint:      "http://custom-mineru:9000",
		MinerUModel:         "vlm-http-client",
		MinerUEnableFormula: &disabled,
		MinerUEnableTable:   &disabled,
		MinerUParseMethod:   MinerUParseMethodText,
		MinerULanguage:      "en",
	}}

	require.NoError(t, tenant.AfterFind(nil))
	require.Equal(t, "http://custom-mineru:9000", tenant.ParserEngineConfig.MinerUEndpoint)
	require.Equal(t, "vlm-http-client", tenant.ParserEngineConfig.MinerUModel)
	require.False(t, *tenant.ParserEngineConfig.MinerUEnableFormula)
	require.False(t, *tenant.ParserEngineConfig.MinerUEnableTable)
	require.Equal(t, MinerUParseMethodText, tenant.ParserEngineConfig.MinerUParseMethod)
	require.Equal(t, "en", tenant.ParserEngineConfig.MinerULanguage)
}

func TestDefaultKnowledgeBaseParserEngineRulesKeepExcelBuiltin(t *testing.T) {
	t.Setenv(DefaultMinerUEndpointEnv, "http://mineru-api:8000")
	rules := DefaultKnowledgeBaseParserEngineRules()
	config := ChunkingConfig{ParserEngineRules: rules}

	for _, fileType := range []string{"pdf", "jpg", "jpeg", "png", "bmp", "tiff", "pptx"} {
		require.Equal(t, "mineru", config.ResolveParserEngine(fileType), fileType)
	}
	require.Equal(t, "markitdown", config.ResolveParserEngine("ppt"))
	for _, fileType := range []string{"xls", "xlsx"} {
		require.Equal(t, "builtin", config.ResolveParserEngine(fileType), fileType)
	}
	require.Empty(t, config.ResolveParserEngine("docx"))
	require.Empty(t, config.ResolveParserEngine("gif"))
	require.Empty(t, config.ResolveParserEngine("webp"))
}

func TestAnnotateDefaultParserEngineFileTypes(t *testing.T) {
	t.Setenv(DefaultMinerUEndpointEnv, "http://mineru-api:8000")
	engines := []ParserEngineInfo{{Name: "builtin"}, {Name: "mineru"}, {Name: "markitdown"}}

	engines = AnnotateDefaultParserEngineFileTypes(engines)

	require.ElementsMatch(t, []string{"xls", "xlsx"}, engines[0].DefaultFileTypes)
	require.ElementsMatch(t,
		[]string{"pdf", "jpg", "jpeg", "png", "bmp", "tiff", "pptx"},
		engines[1].DefaultFileTypes,
	)
	require.ElementsMatch(t, []string{"ppt"}, engines[2].DefaultFileTypes)
}

func TestAnnotateDefaultParserEnginesKeepsMissingRemoteDefaultVisible(t *testing.T) {
	t.Setenv(DefaultMinerUEndpointEnv, "http://mineru-api:8000")
	engines := []ParserEngineInfo{
		{Name: "builtin", FileTypes: []string{"xls", "xlsx"}, Available: true},
		{Name: "mineru", FileTypes: []string{"pdf", "pptx"}, Available: true},
	}

	engines = AnnotateDefaultParserEngineFileTypes(engines)
	require.Len(t, engines, 3)
	require.Equal(t, "markitdown", engines[2].Name)
	require.Equal(t, []string{"ppt"}, engines[2].FileTypes)
	require.Equal(t, []string{"ppt"}, engines[2].DefaultFileTypes)
	require.False(t, engines[2].Available)
	require.NotEmpty(t, engines[2].UnavailableReason)
}

func TestDeploymentParserDefaultsDisabledPreservesUpstreamBehavior(t *testing.T) {
	t.Setenv(DefaultMinerUEndpointEnv, "")
	tenant := &Tenant{}
	require.NoError(t, tenant.BeforeCreate(nil))
	require.Nil(t, tenant.ParserEngineConfig)
	require.NoError(t, tenant.AfterCreate(nil))
	require.Nil(t, tenant.ParserEngineConfig)
	require.NoError(t, tenant.AfterFind(nil))
	require.Nil(t, tenant.ParserEngineConfig)
	require.Nil(t, DefaultKnowledgeBaseParserEngineRules())

	engines := []ParserEngineInfo{{Name: "builtin"}, {Name: "mineru"}}
	engines = AnnotateDefaultParserEngineFileTypes(engines)
	require.Empty(t, engines[0].DefaultFileTypes)
	require.Empty(t, engines[1].DefaultFileTypes)
	payload, err := json.Marshal(engines)
	require.NoError(t, err)
	require.NotContains(t, string(payload), "DefaultFileTypes")
}

func TestDeploymentParserDefaultAnnotationIsPresentOnTheWire(t *testing.T) {
	t.Setenv(DefaultMinerUEndpointEnv, "http://mineru-api:8000")
	engines := AnnotateDefaultParserEngineFileTypes([]ParserEngineInfo{{
		Name: "builtin", FileTypes: []string{"xls", "xlsx"}, Available: true,
	}})
	payload, err := json.Marshal(engines)
	require.NoError(t, err)
	require.True(t, strings.Contains(string(payload), `"DefaultFileTypes":["xls","xlsx"]`))
}

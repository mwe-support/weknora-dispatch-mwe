package types

import (
	"os"
	"strconv"
	"strings"
)

const (
	// DefaultMinerUEndpointEnv enables the deployment-wide self-hosted MinerU
	// policy. When it is empty, upstream tenant and knowledge-base behaviour is
	// unchanged.
	DefaultMinerUEndpointEnv      = "WEKNORA_DEFAULT_MINERU_ENDPOINT"
	defaultMinerUModelEnv         = "WEKNORA_DEFAULT_MINERU_MODEL"
	defaultMinerUParseMethodEnv   = "WEKNORA_DEFAULT_MINERU_PARSE_METHOD"
	defaultMinerULanguageEnv      = "WEKNORA_DEFAULT_MINERU_LANGUAGE"
	defaultMinerUEnableFormulaEnv = "WEKNORA_DEFAULT_MINERU_ENABLE_FORMULA"
	defaultMinerUEnableTableEnv   = "WEKNORA_DEFAULT_MINERU_ENABLE_TABLE"
)

var (
	defaultMinerUFileTypes = []string{"pdf", "jpg", "jpeg", "png", "bmp", "tiff", "pptx"}
	defaultMarkItDownPPT   = []string{"ppt"}
	defaultBuiltinExcel    = []string{"xls", "xlsx"}
)

func deploymentDefaultMinerUConfig(getenv func(string) string) *ParserEngineConfig {
	endpoint := strings.TrimSpace(getenv(DefaultMinerUEndpointEnv))
	if endpoint == "" {
		return nil
	}

	formula := parseDeploymentBool(getenv(defaultMinerUEnableFormulaEnv), true)
	table := parseDeploymentBool(getenv(defaultMinerUEnableTableEnv), true)
	model := strings.TrimSpace(getenv(defaultMinerUModelEnv))
	if model == "" {
		model = "pipeline"
	}
	parseMethod := ResolveMinerUParseMethod(getenv(defaultMinerUParseMethodEnv), nil)
	language := strings.TrimSpace(getenv(defaultMinerULanguageEnv))
	if language == "" {
		language = "ch"
	}

	return &ParserEngineConfig{
		MinerUEndpoint:      endpoint,
		MinerUModel:         model,
		MinerUEnableFormula: &formula,
		MinerUEnableTable:   &table,
		MinerUParseMethod:   parseMethod,
		MinerULanguage:      language,
	}
}

func parseDeploymentBool(raw string, fallback bool) bool {
	value, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		return fallback
	}
	return value
}

// ApplyDeploymentParserEngineDefaults fills only missing tenant fields. A
// workspace can still override individual values, including explicit false
// booleans, while blank workspaces inherit the operator-managed local MinerU.
func (t *Tenant) ApplyDeploymentParserEngineDefaults() {
	if t == nil {
		return
	}
	defaults := deploymentDefaultMinerUConfig(os.Getenv)
	if defaults == nil {
		return
	}
	if t.ParserEngineConfig == nil {
		t.ParserEngineConfig = &ParserEngineConfig{}
	}
	config := t.ParserEngineConfig
	if strings.TrimSpace(config.MinerUEndpoint) == "" {
		config.MinerUEndpoint = defaults.MinerUEndpoint
	}
	if strings.TrimSpace(config.MinerUModel) == "" {
		config.MinerUModel = defaults.MinerUModel
	}
	if config.MinerUEnableFormula == nil {
		config.MinerUEnableFormula = boolPointer(*defaults.MinerUEnableFormula)
	}
	if config.MinerUEnableTable == nil {
		config.MinerUEnableTable = boolPointer(*defaults.MinerUEnableTable)
	}
	if strings.TrimSpace(config.MinerUParseMethod) == "" && config.MinerUEnableOCR == nil {
		config.MinerUParseMethod = defaults.MinerUParseMethod
	}
	if strings.TrimSpace(config.MinerULanguage) == "" {
		config.MinerULanguage = defaults.MinerULanguage
	}
}

func cloneParserEngineConfig(config *ParserEngineConfig) *ParserEngineConfig {
	if config == nil {
		return nil
	}
	cloned := *config
	if config.MinerUEnableFormula != nil {
		cloned.MinerUEnableFormula = boolPointer(*config.MinerUEnableFormula)
	}
	if config.MinerUEnableTable != nil {
		cloned.MinerUEnableTable = boolPointer(*config.MinerUEnableTable)
	}
	if config.MinerUEnableOCR != nil {
		cloned.MinerUEnableOCR = boolPointer(*config.MinerUEnableOCR)
	}
	if config.ChatParserEngineRules != nil {
		cloned.ChatParserEngineRules = append([]ParserEngineRule(nil), config.ChatParserEngineRules...)
	}
	return &cloned
}

// ApplyInheritedParserEngineDefaults exposes operator-managed defaults without
// turning them into tenant-owned data. The captured database value is used by
// repositories when another tenant field is updated, so a routine name/quota
// change cannot accidentally freeze the current environment defaults in JSON.
func (t *Tenant) ApplyInheritedParserEngineDefaults() {
	if t == nil {
		return
	}
	t.persistedParserEngineConfig = cloneParserEngineConfig(t.ParserEngineConfig)
	t.parserEngineConfigLoaded = true
	t.parserEngineConfigExplicit = false
	t.ApplyDeploymentParserEngineDefaults()
}

// MarkParserEngineConfigExplicit is called only by the parser settings write
// path. Explicit workspace overrides must be persisted instead of stripped as
// inherited deployment defaults.
func (t *Tenant) MarkParserEngineConfigExplicit() {
	if t == nil {
		return
	}
	t.parserEngineConfigExplicit = true
}

// ParserEngineConfigForPersistence returns the tenant-owned value. Read-time
// deployment defaults remain effective in memory but never leak into an
// unrelated database update.
func (t *Tenant) ParserEngineConfigForPersistence() *ParserEngineConfig {
	if t == nil {
		return nil
	}
	if t.parserEngineConfigLoaded && !t.parserEngineConfigExplicit {
		return cloneParserEngineConfig(t.persistedParserEngineConfig)
	}
	return cloneParserEngineConfig(t.ParserEngineConfig)
}

func boolPointer(value bool) *bool {
	return &value
}

// DefaultKnowledgeBaseParserEngineRules returns a fresh copy of the
// deployment policy. Excel deliberately stays on the builtin parser.
func DefaultKnowledgeBaseParserEngineRules() []ParserEngineRule {
	if strings.TrimSpace(os.Getenv(DefaultMinerUEndpointEnv)) == "" {
		return nil
	}
	return []ParserEngineRule{
		{FileTypes: append([]string(nil), defaultMinerUFileTypes...), Engine: "mineru"},
		{FileTypes: append([]string(nil), defaultMarkItDownPPT...), Engine: "markitdown"},
		{FileTypes: append([]string(nil), defaultBuiltinExcel...), Engine: "builtin"},
	}
}

// AnnotateDefaultParserEngineFileTypes tells the UI which available engine is
// the deployment default for each file type without reordering the registry.
func AnnotateDefaultParserEngineFileTypes(engines []ParserEngineInfo) []ParserEngineInfo {
	rules := DefaultKnowledgeBaseParserEngineRules()
	for _, rule := range rules {
		found := false
		for i := range engines {
			if strings.EqualFold(strings.TrimSpace(rule.Engine), strings.TrimSpace(engines[i].Name)) {
				found = true
				break
			}
		}
		if !found {
			engines = append(engines, ParserEngineInfo{
				Name:              rule.Engine,
				Description:       "Deployment default parser engine",
				FileTypes:         append([]string(nil), rule.FileTypes...),
				Available:         false,
				UnavailableReason: "Configured deployment default is not currently discoverable",
			})
		}
	}
	for i := range engines {
		engines[i].DefaultFileTypes = nil
		for _, rule := range rules {
			if strings.EqualFold(strings.TrimSpace(rule.Engine), strings.TrimSpace(engines[i].Name)) {
				engines[i].DefaultFileTypes = append(engines[i].DefaultFileTypes, rule.FileTypes...)
			}
		}
	}
	return engines
}

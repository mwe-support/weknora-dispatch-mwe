package types

import "strings"

// supportedKnowledgeFileExtensions is the single source of truth for binary
// file extensions that WeKnora can accept into a knowledge base. Connectors
// use the same gate as direct upload so an exported attachment cannot be
// reported as a sync failure only after it reaches the ingestion pipeline.
var supportedKnowledgeFileExtensions = []string{
	"pdf", "txt", "docx", "doc", "epub",
	"html", "htm", "mhtml", "md", "markdown",
	"png", "jpg", "jpeg", "gif",
	"csv", "xlsx", "xls", "pptx", "ppt", "json",
	"mp3", "wav", "m4a", "flac", "ogg",
}

var supportedKnowledgeFileExtensionSet = func() map[string]struct{} {
	extensions := make(map[string]struct{}, len(supportedKnowledgeFileExtensions))
	for _, extension := range supportedKnowledgeFileExtensions {
		extensions[extension] = struct{}{}
	}
	return extensions
}()

// IsSupportedKnowledgeFileExtension reports whether an extension is accepted
// by the knowledge ingestion pipeline. The input may be dotted or uppercase.
func IsSupportedKnowledgeFileExtension(extension string) bool {
	extension = strings.ToLower(strings.TrimPrefix(strings.TrimSpace(extension), "."))
	_, ok := supportedKnowledgeFileExtensionSet[extension]
	return ok
}

// SupportedKnowledgeFileExtensions returns a defensive copy for validation,
// documentation, and tests without exposing mutable package state.
func SupportedKnowledgeFileExtensions() []string {
	return append([]string(nil), supportedKnowledgeFileExtensions...)
}

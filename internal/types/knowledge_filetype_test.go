package types

import "testing"

func TestIsSupportedKnowledgeFileExtension(t *testing.T) {
	for _, ext := range []string{
		"pdf", "txt", "docx", "doc", "epub", "html", "htm", "mhtml", "md", "markdown",
		"png", "jpg", "jpeg", "gif", "csv", "xlsx", "xls", "pptx", "ppt", "json",
		"mp3", "wav", "m4a", "flac", "ogg",
	} {
		if !IsSupportedKnowledgeFileExtension(ext) {
			t.Errorf("IsSupportedKnowledgeFileExtension(%q) = false, want true", ext)
		}
	}
	for _, ext := range []string{"", "unknown", "zip", "exe", "mp4"} {
		if IsSupportedKnowledgeFileExtension(ext) {
			t.Errorf("IsSupportedKnowledgeFileExtension(%q) = true, want false", ext)
		}
	}
	if !IsSupportedKnowledgeFileExtension(" .PDF ") {
		t.Error("extension normalization rejected uppercase dotted PDF")
	}
}

func TestSupportedKnowledgeFileExtensionsReturnsDefensiveCopy(t *testing.T) {
	first := SupportedKnowledgeFileExtensions()
	if len(first) == 0 {
		t.Fatal("SupportedKnowledgeFileExtensions() returned no extensions")
	}
	first[0] = "mutated"
	second := SupportedKnowledgeFileExtensions()
	if second[0] == "mutated" {
		t.Fatal("SupportedKnowledgeFileExtensions() exposed mutable shared state")
	}
}

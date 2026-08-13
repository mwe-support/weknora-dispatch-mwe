package docparser

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/utils"
	"github.com/stretchr/testify/require"
)

func TestMinerUReadPreservesOriginalOfficeFilename(t *testing.T) {
	var uploadedFilename string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseMultipartForm(1<<20))
		_, header, err := r.FormFile("files")
		require.NoError(t, err)
		uploadedFilename = header.Filename
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"results": map[string]any{"document": map[string]any{"md_content": "ok"}},
		}))
	}))
	defer server.Close()

	t.Setenv("SSRF_WHITELIST", "127.0.0.1,localhost")
	utils.ResetSSRFWhitelistForTest()
	t.Cleanup(utils.ResetSSRFWhitelistForTest)

	reader := NewMinerUReader(map[string]string{"mineru_endpoint": server.URL})
	result, err := reader.Read(t.Context(), &types.ReadRequest{
		FileName:    "财务 汇报.pptx",
		FileType:    "pptx",
		FileContent: []byte("pptx payload"),
	})
	require.NoError(t, err)
	require.Equal(t, "ok", result.MarkdownContent)
	require.Equal(t, "财务 汇报.pptx", uploadedFilename)
}

func TestMinerUReadAcceptsVersion344FilenameKey(t *testing.T) {
	png := createTestPNG(200, 150)
	encodedImage := base64.StdEncoding.EncodeToString(png)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseMultipartForm(1<<20))
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"status":     "completed",
			"file_names": []string{"财务制度"},
			"results": map[string]any{
				"财务制度": map[string]any{
					"md_content": "# 财务制度\n\n![](images/第 1 页.jpg)",
					"images": map[string]string{
						"第 1 页.jpg": "data:image/png;base64," + encodedImage,
					},
				},
			},
		}))
	}))
	defer server.Close()

	t.Setenv("SSRF_WHITELIST", "127.0.0.1,localhost")
	utils.ResetSSRFWhitelistForTest()
	t.Cleanup(utils.ResetSSRFWhitelistForTest)

	reader := NewMinerUReader(map[string]string{"mineru_endpoint": server.URL})
	result, err := reader.Read(t.Context(), &types.ReadRequest{
		FileName:    "财务制度.pdf",
		FileType:    "pdf",
		FileContent: []byte("pdf payload"),
	})

	require.NoError(t, err)
	require.Equal(t, "# 财务制度\n\n![](images/第 1 页.jpg)", result.MarkdownContent)
	require.Len(t, result.ImageRefs, 1)
	require.Equal(t, "images/第 1 页.jpg", result.ImageRefs[0].OriginalRef)
}

func TestMinerUReadRejectsCompletedResponseWithoutContent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseMultipartForm(1<<20))
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"status":     "completed",
			"file_names": []string{"空白文档"},
			"results": map[string]any{
				"空白文档": map[string]any{
					"md_content": "",
					"images":     map[string]string{},
				},
			},
		}))
	}))
	defer server.Close()

	t.Setenv("SSRF_WHITELIST", "127.0.0.1,localhost")
	utils.ResetSSRFWhitelistForTest()
	t.Cleanup(utils.ResetSSRFWhitelistForTest)

	reader := NewMinerUReader(map[string]string{"mineru_endpoint": server.URL})
	result, err := reader.Read(t.Context(), &types.ReadRequest{
		FileName:    "空白文档.pdf",
		FileType:    "pdf",
		FileContent: []byte("pdf payload"),
	})

	require.Nil(t, result)
	require.ErrorContains(t, err, "completed response contains no usable markdown or images")
}

func TestMinerUReadSkipsEmptyLegacyResultBeforeValidFilesResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseMultipartForm(1<<20))
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"results": map[string]any{
				"document": map[string]any{},
				"files":    map[string]any{"md_content": "legacy files content"},
			},
		}))
	}))
	defer server.Close()

	t.Setenv("SSRF_WHITELIST", "127.0.0.1,localhost")
	utils.ResetSSRFWhitelistForTest()
	t.Cleanup(utils.ResetSSRFWhitelistForTest)

	reader := NewMinerUReader(map[string]string{"mineru_endpoint": server.URL})
	result, err := reader.Read(t.Context(), &types.ReadRequest{
		FileName:    "legacy.pdf",
		FileType:    "pdf",
		FileContent: []byte("pdf payload"),
	})

	require.NoError(t, err)
	require.Equal(t, "legacy files content", result.MarkdownContent)
}

func TestMinerUReadRejectsImagesThatProduceNoUsableReferences(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseMultipartForm(1<<20))
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"file_names": []string{"empty-image"},
			"results": map[string]any{
				"empty-image": map[string]any{
					"md_content": "",
					"images": map[string]string{
						"unreferenced.png": "not-valid-base64",
					},
				},
			},
		}))
	}))
	defer server.Close()

	t.Setenv("SSRF_WHITELIST", "127.0.0.1,localhost")
	utils.ResetSSRFWhitelistForTest()
	t.Cleanup(utils.ResetSSRFWhitelistForTest)

	reader := NewMinerUReader(map[string]string{"mineru_endpoint": server.URL})
	result, err := reader.Read(t.Context(), &types.ReadRequest{
		FileName:    "empty-image.pdf",
		FileType:    "pdf",
		FileContent: []byte("pdf payload"),
	})

	require.Nil(t, result)
	require.ErrorContains(t, err, "completed response contains no usable markdown or images")
}

func TestMinerUUploadFilenameStripsControlCharacters(t *testing.T) {
	require.Equal(t, "report.pptx", mineruUploadFilename("../report\r\n\x00.pptx", "pptx"))
	require.Equal(t, "document.pdf", mineruUploadFilename("\r\n\x7f", "pdf"))
}

func TestNewMinerUReaderResolvesParseMethod(t *testing.T) {
	tests := []struct {
		name      string
		overrides map[string]string
		want      string
	}{
		{name: "default is auto", overrides: map[string]string{}, want: "auto"},
		{name: "legacy enabled becomes auto", overrides: map[string]string{"mineru_enable_ocr": "true"}, want: "auto"},
		{name: "legacy disabled becomes text", overrides: map[string]string{"mineru_enable_ocr": "false"}, want: "txt"},
		{name: "explicit method wins", overrides: map[string]string{"mineru_parse_method": "ocr", "mineru_enable_ocr": "false"}, want: "ocr"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewMinerUReader(tt.overrides)
			if reader.parseMethod != tt.want {
				t.Fatalf("parseMethod = %q, want %q", reader.parseMethod, tt.want)
			}
		})
	}
}

func TestNormalizeMinerUMarkdownPreservesMarkdownAndHTML(t *testing.T) {
	input := strings.Join([]string{
		"# Heading",
		"",
		"![](images/cover.jpg)",
		"",
		`<details><summary>text_image</summary>caption</details>`,
		"",
		`<table><tr><td><img src="images/profile.jpg"/></td></tr></table>`,
	}, "\n")

	got := normalizeMinerUMarkdown(input)

	if !strings.Contains(got, "# Heading") {
		t.Fatalf("expected heading to stay intact, got: %q", got)
	}
	if strings.Contains(got, `\# Heading`) {
		t.Fatalf("expected heading to avoid escaped form, got: %q", got)
	}
	if !strings.Contains(got, "![](images/cover.jpg)") {
		t.Fatalf("expected markdown image syntax to stay intact, got: %q", got)
	}
	if strings.Contains(got, `!\[](images/cover.jpg)`) {
		t.Fatalf("expected markdown image syntax to avoid escaped form, got: %q", got)
	}
	if !strings.Contains(got, `<details><summary>text_image</summary>caption</details>`) {
		t.Fatalf("expected details/summary block to be preserved, got: %q", got)
	}
	if !strings.Contains(got, `<img src="images/profile.jpg"/>`) {
		t.Fatalf("expected html img tag to be preserved, got: %q", got)
	}
}

func TestProcessImagesKeepsReferencedVariants(t *testing.T) {
	reader := &MinerUReader{}
	mdContent := strings.Join([]string{
		"![](images/cover.jpg)",
		`<img src="./images/profile.jpg"/>`,
		`![](plain.jpg)`,
	}, "\n")

	png := createTestPNG(200, 150)
	b64 := base64.StdEncoding.EncodeToString(png)
	images := map[string]string{
		"cover.jpg":   "data:image/png;base64," + b64,
		"profile.jpg": "data:image/png;base64," + b64,
		"plain.jpg":   "data:image/png;base64," + b64,
	}

	refs, gotMarkdown := reader.processImages(mdContent, images)

	if gotMarkdown != mdContent {
		t.Fatalf("processImages should not rewrite markdown content")
	}
	if len(refs) != 3 {
		t.Fatalf("expected 3 image refs, got %d", len(refs))
	}
}

// TestProcessImagesMatchesPathsWithSpaces guards against a regression where
// MinerU image filenames containing spaces (common on Chinese documents,
// e.g. "images/第 1 页.jpg") would be silently dropped because the markdown
// regex used to extract refs disallowed whitespace inside the URL group.
func TestProcessImagesMatchesPathsWithSpaces(t *testing.T) {
	reader := &MinerUReader{}
	mdContent := "![](images/第 1 页.jpg)"

	png := createTestPNG(200, 150)
	b64 := base64.StdEncoding.EncodeToString(png)
	images := map[string]string{
		"第 1 页.jpg": "data:image/png;base64," + b64,
	}

	refs, _ := reader.processImages(mdContent, images)
	if len(refs) != 1 {
		t.Fatalf("expected 1 image ref for path with spaces, got %d", len(refs))
	}
	if refs[0].OriginalRef != "images/第 1 页.jpg" {
		t.Fatalf("unexpected OriginalRef: %q", refs[0].OriginalRef)
	}
}

package tencentdocs

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNativeMDXRecordedProviderPage(t *testing.T) {
	data, err := os.ReadFile("testdata/lifecycle-native-pages.json")
	require.NoError(t, err)
	var fixture struct {
		SmartCanvas struct {
			Content string `json:"content"`
		} `json:"smartcanvas"`
	}
	require.NoError(t, json.Unmarshal(data, &fixture))
	result, err := NormalizeNativeMDX(fixture.SmartCanvas.Content)
	require.NoError(t, err)
	require.Contains(t, result.Markdown, "MDX-P-45")
	require.Contains(t, result.Markdown, "| MDX-TABLE-7391 | 0 |")
	require.Len(t, result.Assets, 1)
	require.Len(t, result.Unsupported, 2)
	require.Equal(t, "todo_state", result.Unsupported[0].Kind)
	require.Equal(t, "code", result.Unsupported[1].Kind)
}

func TestNativeMDXNormalizesStructureAndRetainsEveryMediaUnit(t *testing.T) {
	source := `<Heading level="2" id="h">标题</Heading><Callout id="callout"><Paragraph>提示 **重点**。</Paragraph></Callout><Todo checked id="checked">完成</Todo><Table id="table"><TableRow><TableCell><Paragraph>产品</Paragraph></TableCell><TableCell>数量</TableCell></TableRow><TableRow><TableCell>合成样本</TableCell><TableCell>0</TableCell></TableRow></Table>`
	for i := 0; i < 48; i++ {
		source += fmt.Sprintf(`<Image id="image-%d" src="https://docimg5.docs.qq.com/image/test-%d.png" width={640} height={180} />`, i, i)
	}
	result, err := NormalizeNativeMDX(source)
	require.NoError(t, err)
	require.Empty(t, result.Unsupported)
	require.Contains(t, result.Markdown, "## 标题")
	require.Contains(t, result.Markdown, "> 提示 **重点**。")
	require.Contains(t, result.Markdown, "- [x] 完成")
	require.Contains(t, result.Markdown, "| 合成样本 | 0 |")
	require.Len(t, result.Assets, 48)
	require.Equal(t, 48, strings.Count(result.Markdown, "!["))
	require.NotContains(t, result.Markdown, "<Image")
}

func TestNativeMDXUnknownCodeAndTodoStateCannotPretendComplete(t *testing.T) {
	result, err := NormalizeNativeMDX(`<Unsupported type="code" id="code" readonly /><Todo id="unknown">原生读取遗漏状态</Todo>`)
	require.NoError(t, err)
	require.Len(t, result.Unsupported, 2)
	require.Equal(t, "code", result.Unsupported[0].Kind)
	require.Equal(t, "todo_state", result.Unsupported[1].Kind)
	require.NotContains(t, result.Markdown, "- [ ]")
	require.Contains(t, result.Markdown, "状态未知")
}

func TestNativeMDXRejectsUnsafeAssetsAndMalformedNesting(t *testing.T) {
	for _, source := range []string{`<Image src="javascript:alert(1)" />`, `<Paragraph><Mark>broken</Paragraph>`, `<Image src="http://127.0.0.1/secret" />`} {
		_, err := NormalizeNativeMDX(source)
		require.Error(t, err)
	}
	result, err := NormalizeNativeMDX(`<Paragraph>{dangerousExpression()}</Paragraph><script>alert(1)</script>`)
	require.NoError(t, err)
	require.NotContains(t, result.Markdown, "<script>")
	require.Len(t, result.Unsupported, 1)
}

func TestNativeMDXEscapedAnglesRemainText(t *testing.T) {
	result, err := NormalizeNativeMDX(`<Paragraph id="literal">\<Page> literal \</Page> and \<Image src="https://docs.qq.com/private" /></Paragraph>`)
	require.NoError(t, err)
	require.Empty(t, result.Unsupported)
	require.Empty(t, result.Assets)
	require.Contains(t, result.Markdown, "&lt;Page&gt;")
	require.Contains(t, result.Markdown, "&lt;/Page&gt;")
	ids, err := nativeMDXIDs(`<Paragraph id="real">\<Image id="fake" /></Paragraph>`)
	require.NoError(t, err)
	require.Equal(t, []string{"real"}, ids)
}

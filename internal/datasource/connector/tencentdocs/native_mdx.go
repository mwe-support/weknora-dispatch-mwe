package tencentdocs

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"

	"golang.org/x/net/html"
)

type NativeAsset struct {
	ID      string `json:"id"`
	URL     string `json:"url"`
	Caption string `json:"caption,omitempty"`
}
type NativeUnsupported struct {
	Kind    string `json:"kind"`
	BlockID string `json:"block_id"`
}
type NativeNormalized struct {
	Markdown    string              `json:"markdown"`
	Assets      []NativeAsset       `json:"assets"`
	Unsupported []NativeUnsupported `json:"unsupported,omitempty"`
}
type nativeMDXNode struct {
	tag, text string
	attrs     map[string]string
	children  []*nativeMDXNode
}

func NormalizeNativeMDX(content string) (*NativeNormalized, error) {
	if len(content) > maxNativeDocumentBytes {
		bytes := int64(len(content))
		return nil, &NativeLimitError{Kind: "text", LimitBytes: maxNativeDocumentBytes, ActualBytes: &bytes}
	}
	root := &nativeMDXNode{tag: "root"}
	stack := []*nativeMDXNode{root}
	tokenizer := html.NewTokenizer(strings.NewReader(nativeMDXEscapedAngles(content)))
	nodes := 0
	for {
		kind := tokenizer.Next()
		if kind == html.ErrorToken {
			if tokenizer.Err() != io.EOF {
				return nil, tokenizer.Err()
			}
			if len(stack) != 1 {
				return nil, errors.New("MDX_COMPONENT_NESTING_INCOMPLETE")
			}
			break
		}
		if kind == html.CommentToken || kind == html.DoctypeToken {
			continue
		}
		token := tokenizer.Token()
		if kind == html.EndTagToken {
			if len(stack) < 2 || stack[len(stack)-1].tag != token.Data {
				return nil, errors.New("MDX_COMPONENT_NESTING_MISMATCH")
			}
			stack = stack[:len(stack)-1]
			continue
		}
		node := &nativeMDXNode{}
		if kind == html.TextToken {
			node.text = token.Data
		} else {
			node.tag = token.Data
			node.attrs = map[string]string{}
			for _, attr := range token.Attr {
				node.attrs[attr.Key] = attr.Val
			}
		}
		nodes++
		if nodes > 1000000 {
			return nil, errors.New("MDX_NODE_LIMIT_EXCEEDED")
		}
		parent := stack[len(stack)-1]
		parent.children = append(parent.children, node)
		if kind == html.StartTagToken && node.tag != "br" && node.tag != "img" && node.tag != "hr" {
			if len(stack) >= 64 {
				return nil, errors.New("MDX_NESTING_LIMIT_EXCEEDED")
			}
			stack = append(stack, node)
		}
	}
	result := &NativeNormalized{}
	rendered, err := renderNativeMDX(root, result)
	if err != nil {
		return nil, err
	}
	result.Markdown = strings.TrimSpace(rendered)
	return result, nil
}

// The native reader emits Markdown escapes for literal tag-shaped text.
// Honor odd backslash runs before HTML tokenization; a literal <Image> must
// never become a provider asset or alter the component nesting stack.
func nativeMDXEscapedAngles(content string) string {
	var out strings.Builder
	out.Grow(len(content))
	for i := 0; i < len(content); {
		if content[i] != '\\' {
			out.WriteByte(content[i])
			i++
			continue
		}
		start := i
		for i < len(content) && content[i] == '\\' {
			i++
		}
		if (i-start)%2 == 1 && i < len(content) && (content[i] == '<' || content[i] == '>') {
			out.WriteString(content[start : i-1])
			out.WriteString(html.EscapeString(content[i : i+1]))
			i++
		} else {
			out.WriteString(content[start:i])
		}
	}
	return out.String()
}

func renderNativeMDX(node *nativeMDXNode, result *NativeNormalized) (string, error) {
	if node.tag == "" {
		text := node.text
		if strings.Contains(text, "\n") {
			lines := strings.Split(text, "\n")
			for i := range lines {
				lines[i] = strings.TrimSpace(lines[i])
			}
			text = strings.Trim(strings.Join(lines, "\n"), "\n")
		}
		return html.EscapeString(text), nil
	}
	if node.tag == "table" {
		return renderNativeMDXTable(node, result)
	}
	var content strings.Builder
	for _, child := range node.children {
		rendered, err := renderNativeMDX(child, result)
		if err != nil {
			return "", err
		}
		content.WriteString(rendered)
	}
	body := strings.TrimSpace(content.String())
	block := func(value string) string { return "\n\n" + value + "\n\n" }
	switch node.tag {
	case "root", "page", "columnlist", "column":
		return block(body), nil
	case "paragraph", "p":
		return block(body), nil
	case "heading":
		level, err := strconv.Atoi(strings.Trim(node.attrs["level"], "{}"))
		if err != nil || level < 1 || level > 6 {
			return "", errors.New("MDX_HEADING_LEVEL_INVALID")
		}
		return block(strings.Repeat("#", level) + " " + body), nil
	case "callout", "blockquote":
		return block("> " + strings.ReplaceAll(body, "\n", "\n> ")), nil
	case "mark":
		for _, style := range []struct{ attr, mark string }{{"bold", "**"}, {"italic", "*"}, {"strikethrough", "~~"}, {"code", "`"}} {
			if value, ok := node.attrs[style.attr]; ok && value != "false" {
				body = style.mark + body + style.mark
			}
		}
		return body, nil
	case "link", "a":
		target := node.attrs["href"]
		if target == "" {
			target = node.attrs["url"]
		}
		return nativeMarkdownLink(body, target)
	case "bulletedlist", "numberedlist", "todo":
		prefix := "- "
		if node.tag == "numberedlist" {
			prefix = "1. "
		}
		if node.tag == "todo" {
			checked, present := node.attrs["checked"]
			if !present {
				// Verified live: the read tool omits checked on a checked browser item;
				// DOCX also exports it as an empty box. Do not invent an unchecked state.
				result.Unsupported = append(result.Unsupported, NativeUnsupported{Kind: "todo_state", BlockID: node.attrs["id"]})
				prefix = "- [状态未知] "
			} else if checked == "" || checked == "true" || checked == "{true}" {
				prefix = "- [x] "
			} else if checked == "false" || checked == "{false}" {
				prefix = "- [ ] "
			} else {
				return "", errors.New("MDX_TODO_STATE_INVALID")
			}
		}
		return "\n" + prefix + strings.ReplaceAll(body, "\n", "\n  ") + "\n", nil
	case "image", "img":
		target := node.attrs["src"]
		id := node.attrs["id"]
		if id == "" {
			id = fmt.Sprintf("image-position-%d", len(result.Assets))
		}
		caption := node.attrs["alt"]
		image, err := nativeImageMarkdown(id, target, caption, result)
		if err != nil {
			return "", err
		}
		return block(image), nil
	case "divider", "hr":
		return block("---"), nil
	case "br":
		return "\n", nil
	case "text":
		return content.String(), nil
	default:
		kind := node.tag
		if kind == "unsupported" {
			kind = node.attrs["type"]
			if kind == "" {
				kind = "unknown"
			}
		}
		result.Unsupported = append(result.Unsupported, NativeUnsupported{Kind: kind, BlockID: node.attrs["id"]})
		return block(body), nil
	}
}

func nativeMarkdownLink(label, target string) (string, error) {
	parsed, err := url.Parse(target)
	if err != nil || !((parsed.Scheme == "https" || parsed.Scheme == "http") && parsed.Host != "" || parsed.Scheme == "mailto" && parsed.Opaque != "") {
		return "", errors.New("NATIVE_LINK_SCHEME_INVALID")
	}
	return "[" + strings.ReplaceAll(label, "]", "\\]") + "](<" + html.EscapeString(target) + ">)", nil
}

func nativeImageMarkdown(id, target, caption string, result *NativeNormalized) (string, error) {
	if err := validateExportURL(target); err != nil {
		return "", errors.New("NATIVE_IMAGE_URL_INVALID")
	}
	result.Assets = append(result.Assets, NativeAsset{ID: id, URL: target, Caption: caption})
	target = strings.NewReplacer(" ", "%20", "(", "%28", ")", "%29").Replace(target)
	return "![" + strings.NewReplacer("[", "\\[", "]", "\\]", "\n", " ").Replace(caption) + "](" + target + ")", nil
}

func renderNativeMDXTable(table *nativeMDXNode, result *NativeNormalized) (string, error) {
	var rows [][]string
	width := 0
	for _, row := range table.children {
		if row.tag == "" && strings.TrimSpace(row.text) == "" {
			continue
		}
		if row.tag != "tablerow" && row.tag != "tr" {
			return "", errors.New("MDX_TABLE_ROW_CONTRACT_INVALID")
		}
		var cells []string
		for _, cell := range row.children {
			if cell.tag == "" && strings.TrimSpace(cell.text) == "" {
				continue
			}
			if cell.tag != "tablecell" && cell.tag != "td" && cell.tag != "th" {
				return "", errors.New("MDX_TABLE_CELL_CONTRACT_INVALID")
			}
			for _, attr := range []string{"rowspan", "colspan"} {
				if span := cell.attrs[attr]; span != "" && span != "1" {
					result.Unsupported = append(result.Unsupported, NativeUnsupported{Kind: "table_span", BlockID: table.attrs["id"]})
				}
			}
			var body strings.Builder
			for _, child := range cell.children {
				text, err := renderNativeMDX(child, result)
				if err != nil {
					return "", err
				}
				body.WriteString(text)
			}
			cells = append(cells, strings.NewReplacer("|", "\\|", "\n", "<br>").Replace(strings.TrimSpace(body.String())))
		}
		if width == 0 {
			width = len(cells)
		}
		if len(cells) != width || width == 0 {
			return "", errors.New("MDX_TABLE_COLUMN_COUNT_MISMATCH")
		}
		rows = append(rows, cells)
	}
	if len(rows) == 0 {
		return "", errors.New("MDX_TABLE_EMPTY")
	}
	var out strings.Builder
	out.WriteString("\n\n")
	for i, row := range rows {
		out.WriteString("| " + strings.Join(row, " | ") + " |\n")
		if i == 0 {
			out.WriteString("|" + strings.Repeat(" --- |", width) + "\n")
		}
	}
	out.WriteString("\n")
	return out.String(), nil
}

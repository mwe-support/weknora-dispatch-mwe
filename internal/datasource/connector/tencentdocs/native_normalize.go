package tencentdocs

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/net/html"
)

func NormalizeNativeSnapshot(snapshot *NativeSnapshot) (*NativeNormalized, error) {
	if snapshot == nil || !snapshot.CoverageComplete {
		return nil, errors.New("NATIVE_SNAPSHOT_INCOMPLETE")
	}
	result := &NativeNormalized{}
	switch snapshot.Kind {
	case "doc":
		result.Unsupported = append(result.Unsupported, NativeUnsupported{Kind: "doc_full_body", BlockID: snapshot.FileID})
	case "smartcanvas":
		var text strings.Builder
		for _, page := range snapshot.Pages {
			if page.Tool != "smartcanvas.read" {
				continue
			}
			var content struct {
				Content string `json:"content"`
			}
			if err := json.Unmarshal(page.Data, &content); err != nil {
				return nil, err
			}
			normalized, err := NormalizeNativeMDX(content.Content)
			if err != nil {
				return nil, err
			}
			text.WriteString(normalized.Markdown)
			text.WriteString("\n\n")
			result.Assets = append(result.Assets, normalized.Assets...)
			result.Unsupported = append(result.Unsupported, normalized.Unsupported...)
		}
		result.Markdown = strings.TrimSpace(text.String())
	case "smartsheet":
		if err := normalizeNativeSmartSheet(snapshot, result); err != nil {
			return nil, err
		}
	case "sheet":
		sheet, err := normalizeNativeSheet(snapshot)
		if err != nil {
			return nil, err
		}
		result.Markdown = sheet.Text
	default:
		return nil, errors.New("NATIVE_NORMALIZER_UNAVAILABLE")
	}
	if len(result.Markdown) > maxNativeDocumentBytes {
		bytes := int64(len(result.Markdown))
		return nil, &NativeLimitError{Kind: "text", LimitBytes: maxNativeDocumentBytes, ActualBytes: &bytes}
	}
	return result, nil
}

type nativeSmartField struct {
	ID    string `json:"field_id"`
	Title string `json:"field_title"`
	Type  string `json:"field_type"`
}
type nativeSmartRecord struct {
	ID     string                       `json:"record_id"`
	Values []map[string]json.RawMessage `json:"field_values"`
}
type nativeSmartTable struct {
	title   string
	fields  []nativeSmartField
	records []nativeSmartRecord
}

func normalizeNativeSmartSheet(snapshot *NativeSnapshot, result *NativeNormalized) error {
	tables := map[string]*nativeSmartTable{}
	var order []string
	for _, page := range snapshot.Pages {
		switch page.Tool {
		case "smartsheet.list_tables":
			var catalog nativeSmartCatalog
			if err := json.Unmarshal(page.Data, &catalog); err != nil {
				return err
			}
			for _, table := range catalog.Sheets {
				if tables[table.ID] != nil {
					return errors.New("SMARTSHEET_DUPLICATE_TABLE")
				}
				tables[table.ID] = &nativeSmartTable{title: table.Title}
				order = append(order, table.ID)
			}
		case "smartsheet.list_fields", "smartsheet.list_records":
			id, _ := page.Args["sheet_id"].(string)
			table := tables[id]
			if table == nil {
				return errors.New("SMARTSHEET_TABLE_MISSING")
			}
			var data struct {
				Fields  []nativeSmartField  `json:"fields"`
				Records []nativeSmartRecord `json:"records"`
			}
			if err := json.Unmarshal(page.Data, &data); err != nil {
				return err
			}
			table.fields = append(table.fields, data.Fields...)
			table.records = append(table.records, data.Records...)
		}
	}
	var out strings.Builder
	for _, id := range order {
		table := tables[id]
		out.WriteString("## " + html.EscapeString(table.title) + "\n\n")
		if len(table.fields) == 0 {
			return errors.New("SMARTSHEET_FIELDS_MISSING")
		}
		byID, byTitle := map[string]int{}, map[string]int{}
		titles := make([]string, len(table.fields))
		for i, field := range table.fields {
			if field.ID == "" {
				return errors.New("SMARTSHEET_FIELD_ID_MISSING")
			}
			if _, ok := byID[field.ID]; ok {
				return errors.New("SMARTSHEET_DUPLICATE_FIELD_ID")
			}
			byID[field.ID] = i
			if _, ok := byTitle[field.Title]; ok {
				byTitle[field.Title] = -1
			} else {
				byTitle[field.Title] = i
			}
			titles[i] = nativeTableCell(field.Title)
		}
		out.WriteString("| " + strings.Join(titles, " | ") + " |\n")
		out.WriteString("|" + strings.Repeat(" --- |", len(titles)) + "\n")
		for _, record := range table.records {
			values := make([]string, len(titles))
			present := map[int]bool{}
			for _, value := range record.Values {
				var fieldID, title string
				_ = json.Unmarshal(value["field_id"], &fieldID)
				_ = json.Unmarshal(value["field"], &title)
				index, ok := byID[fieldID]
				if fieldID == "" {
					index, ok = byTitle[title]
				}
				if !ok || index < 0 {
					return errors.New("SMARTSHEET_FIELD_ID_AMBIGUOUS_OR_CHANGED")
				}
				if present[index] {
					return errors.New("SMARTSHEET_DUPLICATE_FIELD_VALUE")
				}
				present[index] = true
				field := table.fields[index]
				if nativeComputedType(field.Type) && len(value["computed_value"]) == 0 {
					return errors.New("SMARTSHEET_COMPUTED_VALUE_UNAVAILABLE")
				}
				text, err := nativeSmartValue(value, id+"/"+record.ID+"/"+field.ID, result)
				if err != nil {
					return err
				}
				values[index] = nativeTableCell(text)
			}
			for i, field := range table.fields {
				if nativeComputedType(field.Type) && !present[i] {
					return errors.New("SMARTSHEET_COMPUTED_VALUE_UNAVAILABLE")
				}
			}
			out.WriteString("| " + strings.Join(values, " | ") + " |\n")
			if out.Len() > maxNativeDocumentBytes {
				return &NativeLimitError{Kind: "text", LimitBytes: maxNativeDocumentBytes, ObservedAtLeastBytes: int64(out.Len())}
			}
		}
		out.WriteString("\n")
	}
	result.Markdown = strings.TrimSpace(out.String())
	return nil
}

func nativeComputedType(kind string) bool {
	switch strings.ToLower(kind) {
	case "formula", "lookup", "rollup", "reference", "bidirectionalreference", "twowaylink":
		return true
	default:
		return false
	}
}

func nativeSmartValue(value map[string]json.RawMessage, locator string, result *NativeNormalized) (string, error) {
	if raw, ok := value["computed_value"]; ok {
		var computed struct {
			Text      string      `json:"text"`
			IsError   bool        `json:"is_error"`
			HasNumber bool        `json:"has_number"`
			Number    json.Number `json:"number"`
			HasBool   bool        `json:"has_bool"`
			Bool      bool        `json:"bool_value"`
		}
		if err := json.Unmarshal(raw, &computed); err != nil {
			return "", err
		}
		if computed.IsError {
			return "[计算错误] " + html.EscapeString(computed.Text), nil
		}
		if computed.Text != "" {
			return html.EscapeString(computed.Text), nil
		}
		if computed.HasNumber {
			if computed.Number == "" {
				return "", errors.New("SMARTSHEET_COMPUTED_NUMBER_MISSING")
			}
			return computed.Number.String(), nil
		}
		if computed.HasBool {
			return strconv.FormatBool(computed.Bool), nil
		}
		return "", nil // Explicit empty computed result; distinct from absent result.
	}
	for _, key := range []string{"number_value", "bool_value", "string_value"} {
		if raw, ok := value[key]; ok {
			if key == "string_value" {
				var text string
				if err := json.Unmarshal(raw, &text); err != nil {
					return "", err
				}
				return html.EscapeString(text), nil
			}
			if string(raw) == "null" {
				return "", nil
			}
			if key == "number_value" {
				var number json.Number
				if err := json.Unmarshal(raw, &number); err != nil {
					return "", err
				}
				return number.String(), nil
			}
			var boolean bool
			if err := json.Unmarshal(raw, &boolean); err != nil {
				return "", err
			}
			return strconv.FormatBool(boolean), nil
		}
	}
	for _, key := range []string{"text_value", "url_value", "option_value", "reference_value", "image_value"} {
		raw, ok := value[key]
		if !ok {
			continue
		}
		var list struct {
			Items []json.RawMessage `json:"items"`
		}
		if err := json.Unmarshal(raw, &list); err != nil {
			return "", err
		}
		var parts []string
		for i, item := range list.Items {
			var text string
			if json.Unmarshal(item, &text) == nil {
				parts = append(parts, html.EscapeString(text))
				continue
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(item, &fields); err != nil {
				return "", err
			}
			_ = json.Unmarshal(fields["text"], &text)
			var link string
			_ = json.Unmarshal(fields["link"], &link)
			if key == "image_value" {
				var imageID, imageURL string
				_ = json.Unmarshal(fields["image_id"], &imageID)
				_ = json.Unmarshal(fields["url"], &imageURL)
				if imageURL == "" && strings.HasPrefix(imageID, "https://") {
					imageURL = imageID
				}
				assetID := fmt.Sprintf("%s/image/%d", locator, i)
				if imageURL == "" {
					result.Unsupported = append(result.Unsupported, NativeUnsupported{Kind: "image_reference", BlockID: assetID})
					parts = append(parts, "[图片地址待解析]")
					continue
				}
				image, err := nativeImageMarkdown(assetID, imageURL, text, result)
				if err != nil {
					return "", err
				}
				parts = append(parts, image)
				continue
			}
			if link != "" {
				rendered, err := nativeMarkdownLink(text, link)
				if err != nil {
					return "", err
				}
				parts = append(parts, rendered)
			} else if text != "" {
				parts = append(parts, html.EscapeString(text))
			} else {
				parts = append(parts, html.EscapeString(string(item)))
			}
		}
		return strings.Join(parts, " "), nil
	}
	if raw, ok := value["auto_number_value"]; ok {
		var number struct {
			Text string `json:"text"`
			Seq  string `json:"seq"`
		}
		if err := json.Unmarshal(raw, &number); err != nil {
			return "", err
		}
		if number.Text != "" {
			return html.EscapeString(number.Text), nil
		}
		return html.EscapeString(number.Seq), nil
	}
	// Preserve a future value in the diagnostic output but block completeness;
	// never silently drop a new attachment or structured field kind.
	var keys []string
	for key := range value {
		if key != "field" && key != "field_id" {
			keys = append(keys, key)
		}
	}
	if len(keys) > 0 {
		sort.Strings(keys)
		result.Unsupported = append(result.Unsupported, NativeUnsupported{Kind: "smart_field/" + strings.Join(keys, ","), BlockID: locator})
		raw, _ := json.Marshal(value)
		return html.EscapeString(string(raw)), nil
	}
	return "", nil
}

func nativeTableCell(text string) string {
	return strings.NewReplacer("|", "\\|", "\r\n", "<br>", "\n", "<br>").Replace(strings.TrimSpace(text))
}

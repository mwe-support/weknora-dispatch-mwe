package tencentdocs

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"golang.org/x/net/html"
)

// NativeReadFunc is also the checkpoint seam: a worker can return a verified
// saved page for the same request, and persist each newly read page before the
// next call. Verify requests must always reach the provider.
type NativeReadFunc func(context.Context, string, map[string]interface{}, bool) (*NativeResponse, error)

type NativePage struct {
	Tool    string                 `json:"tool"`
	Args    map[string]interface{} `json:"args"`
	Data    json.RawMessage        `json:"data"`
	TraceID string                 `json:"trace_id,omitempty"`
}
type NativeSnapshot struct {
	LocatorID        string       `json:"locator_id"`
	FileID           string       `json:"file_id"`
	Kind             string       `json:"kind"`
	Pages            []NativePage `json:"pages"`
	RawBytes         int64        `json:"raw_bytes"`
	Units            int          `json:"units"`
	CoverageComplete bool         `json:"coverage_complete"`
	RequiresExport   bool         `json:"requires_export"`
	SourceRevision   uint64       `json:"source_revision"`
	RevisionKey      string       `json:"revision_key"`
}

type nativeCollector struct {
	snapshot NativeSnapshot
	read     NativeReadFunc
	requests int
}

// CollectNative verifies the full pagination contract, not a text preview.
// A DOC snapshot still requires full export; structure coverage is not body
// completeness. MDX unknown components are evaluated by the normalizer.
func CollectNative(ctx context.Context, fileID, kind string, read NativeReadFunc) (*NativeSnapshot, error) {
	if err := requireID("file ID", fileID); err != nil {
		return nil, err
	}
	if read == nil {
		return nil, errors.New("native reader is unavailable")
	}
	c := nativeCollector{snapshot: NativeSnapshot{LocatorID: fileID, FileID: fileID, Kind: kind}, read: read}
	var before FileInfo
	if err := c.call(ctx, toolQueryFileInfo, map[string]interface{}{}, false, &before); err != nil {
		return nil, err
	}
	if before.ID == "" || before.Status != "normal" || before.ModifiedAt == 0 || before.Type != kind {
		return nil, errors.New("SOURCE_METADATA_INCOMPLETE_OR_UNAVAILABLE")
	}
	// Only a canonical ID explicitly returned by the metadata contract may be
	// used to resolve a locator; never infer an ID from an error or document title.
	c.snapshot.FileID = before.ID
	c.snapshot.SourceRevision = before.ModifiedAt
	var err error
	switch kind {
	case "doc":
		err = c.doc(ctx)
	case "smartcanvas":
		err = c.canvas(ctx)
	case "smartsheet":
		err = c.smartSheet(ctx)
	case "sheet":
		err = c.sheet(ctx)
	default:
		return nil, fmt.Errorf("unsupported native document type %q", kind)
	}
	if err != nil {
		return nil, err
	}
	var after FileInfo
	if err := c.call(ctx, toolQueryFileInfo, map[string]interface{}{}, true, &after); err != nil {
		return nil, err
	}
	if before.ID != after.ID || before.ModifiedAt != after.ModifiedAt || before.Title != after.Title || before.Type != after.Type || before.Status != after.Status {
		return nil, errors.New("SOURCE_CHANGED_DURING_READ")
	}
	c.snapshot.CoverageComplete = true
	c.snapshot.RevisionKey, err = nativeSnapshotRevision(&c.snapshot)
	if err != nil {
		return nil, err
	}
	return &c.snapshot, nil
}

func (c *nativeCollector) call(ctx context.Context, tool string, args map[string]interface{}, verify bool, out interface{}) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	c.requests++
	if c.requests > maxMCPPagination {
		return errors.New("NATIVE_PAGE_LIMIT_EXCEEDED")
	}
	args["file_id"] = c.snapshot.FileID
	response, err := c.read(ctx, tool, args, verify)
	if err != nil {
		return err
	}
	if response == nil || !json.Valid(response.Data) {
		return errors.New("native read returned no valid page")
	}
	bytes := max(response.Bytes, int64(len(response.Data)))
	if bytes > maxNativeResponseBytes {
		return &NativeLimitError{Kind: "response", LimitBytes: maxNativeResponseBytes, ActualBytes: &bytes}
	}
	c.snapshot.RawBytes += bytes
	if c.snapshot.RawBytes > maxNativeDocumentBytes {
		return &NativeLimitError{Kind: "document", LimitBytes: maxNativeDocumentBytes, ObservedAtLeastBytes: c.snapshot.RawBytes}
	}
	if err := json.Unmarshal(response.Data, out); err != nil {
		return fmt.Errorf("native page contract: %w", err)
	}
	if !verify {
		c.snapshot.Pages = append(c.snapshot.Pages, NativePage{Tool: tool, Args: args, Data: response.Data, TraceID: response.TraceID})
	}
	return nil
}

type nativeDocPage struct {
	Nodes      []json.RawMessage `json:"nodes"`
	Version    json.RawMessage   `json:"version"`
	Pagination struct {
		Total    *int `json:"total_nodes"`
		Returned *int `json:"returned_nodes"`
		HasMore  bool `json:"has_more"`
	} `json:"pagination"`
}

func (c *nativeCollector) doc(ctx context.Context) error {
	seen := map[string]bool{}
	total, offset := -1, 0
	version := ""
	for {
		var page nativeDocPage
		args := map[string]interface{}{"mode": 2, "offset": offset, "limit": 150, "include_table_cells": true, "include_textbox_children": true, "has_text_preview_length": true, "text_preview_length": 200}
		if err := c.call(ctx, "doc.resolve_document_structure", args, false, &page); err != nil {
			return err
		}
		if page.Pagination.Total == nil || page.Pagination.Returned == nil || *page.Pagination.Returned != len(page.Nodes) || len(page.Version) == 0 || string(page.Version) == "null" {
			return errors.New("DOC_STRUCTURE_CONTRACT_INCOMPLETE")
		}
		if total < 0 {
			total, version = *page.Pagination.Total, string(page.Version)
		}
		if total != *page.Pagination.Total || version != string(page.Version) {
			return errors.New("SOURCE_CHANGED_DURING_READ")
		}
		for i, node := range page.Nodes {
			var loc struct {
				ID    string `json:"paragraph_id"`
				Index int    `json:"paragraph_index"`
			}
			if err := json.Unmarshal(node, &loc); err != nil {
				return err
			}
			key := loc.ID
			if loc.Index > 0 {
				key = "paragraph/" + strconv.Itoa(loc.Index)
			}
			if key == "" {
				key = "position/" + strconv.Itoa(offset+i)
			}
			if seen[key] {
				return errors.New("DOC_PAGINATION_DUPLICATE_NODE")
			}
			seen[key] = true
		}
		offset += len(page.Nodes)
		if offset > total {
			return errors.New("DOC_STRUCTURE_TOTAL_MISMATCH")
		}
		if offset == total {
			if page.Pagination.HasMore {
				return errors.New("DOC_PAGINATION_CONTRADICTION")
			}
			break
		}
		if !page.Pagination.HasMore || len(page.Nodes) == 0 {
			return errors.New("DOC_STRUCTURE_TRUNCATED")
		}
	}
	var last nativeDocPage
	if err := c.call(ctx, "doc.resolve_document_structure", map[string]interface{}{"mode": 2, "offset": 0, "limit": 1}, true, &last); err != nil {
		return err
	}
	if last.Pagination.Total == nil || *last.Pagination.Total != total || string(last.Version) != version {
		return errors.New("SOURCE_CHANGED_DURING_READ")
	}
	c.snapshot.Units = offset
	c.snapshot.RequiresExport = true // text_preview is never a complete body contract.
	return nil
}

type nativeCanvasCatalog struct {
	Pages []struct {
		ID        string          `json:"id"`
		Children  []string        `json:"children"`
		Version   json.RawMessage `json:"version"`
		UpdatedAt json.RawMessage `json:"updated_at"`
	} `json:"top_level_pages"`
}

func (c *nativeCollector) canvas(ctx context.Context) error {
	var catalog nativeCanvasCatalog
	if err := c.call(ctx, "smartcanvas.get_top_level_pages", map[string]interface{}{}, false, &catalog); err != nil {
		return err
	}
	if len(catalog.Pages) == 0 {
		return errors.New("MDX_PAGE_CATALOG_EMPTY")
	}
	allIDs := map[string]bool{}
	pageIDs := map[string]bool{}
	for _, page := range catalog.Pages {
		if page.ID == "" || pageIDs[page.ID] {
			return errors.New("MDX_PAGE_ID_INVALID")
		}
		pageIDs[page.ID] = true
		expected := map[string]bool{}
		for _, id := range page.Children {
			if id == "" || expected[id] {
				return errors.New("MDX_CHILD_ID_INVALID")
			}
			expected[id] = true
		}
		token := ""
		cursors := map[string]bool{}
		for {
			var part struct {
				Content string `json:"content"`
				Next    string `json:"next_token"`
			}
			if err := c.call(ctx, "smartcanvas.read", map[string]interface{}{"page_id": page.ID, "size": 20, "next_token": token}, false, &part); err != nil {
				return err
			}
			ids, err := nativeMDXIDs(part.Content)
			if err != nil {
				return err
			}
			progress := 0
			for _, id := range ids {
				if id == page.ID {
					continue
				} // Providers may repeat the enclosing page wrapper.
				if allIDs[id] {
					return errors.New("MDX_PAGINATION_DUPLICATE_BLOCK")
				}
				allIDs[id] = true
				progress++
				delete(expected, id)
			}
			if part.Next == "" {
				break
			}
			if part.Next == token || cursors[part.Next] || progress == 0 {
				return errors.New("MDX_PAGINATION_NO_PROGRESS")
			}
			cursors[part.Next] = true
			token = part.Next
		}
		if len(expected) > 0 {
			return errors.New("MDX_PAGE_CONTENT_TRUNCATED")
		}
	}
	var last nativeCanvasCatalog
	if err := c.call(ctx, "smartcanvas.get_top_level_pages", map[string]interface{}{}, true, &last); err != nil {
		return err
	}
	before, _ := json.Marshal(catalog)
	after, _ := json.Marshal(last)
	if string(before) != string(after) {
		return errors.New("SOURCE_CHANGED_DURING_READ")
	}
	c.snapshot.Units = len(allIDs)
	return nil
}

// Tokenize components without evaluating JSX, expressions, imports or scripts.
func nativeMDXIDs(content string) ([]string, error) {
	tokenizer := html.NewTokenizer(strings.NewReader(nativeMDXEscapedAngles(content)))
	var ids []string
	for {
		kind := tokenizer.Next()
		if kind == html.ErrorToken {
			if tokenizer.Err() == io.EOF {
				return ids, nil
			}
			return nil, tokenizer.Err()
		}
		if kind == html.StartTagToken || kind == html.SelfClosingTagToken {
			for _, attr := range tokenizer.Token().Attr {
				if attr.Key == "id" && attr.Val != "" {
					ids = append(ids, attr.Val)
				}
			}
		}
	}
}

type nativeSmartCatalog struct {
	Sheets []struct {
		ID      string `json:"sheet_id"`
		Title   string `json:"title"`
		Visible bool   `json:"is_visible"`
	} `json:"sheets"`
}
type nativeSmartPage struct {
	Fields  []json.RawMessage `json:"fields"`
	Records []json.RawMessage `json:"records"`
	HasMore bool              `json:"has_more"`
	Next    int               `json:"next"`
	Total   *int              `json:"total"`
}

func (c *nativeCollector) smartSheet(ctx context.Context) error {
	var catalog nativeSmartCatalog
	if err := c.call(ctx, "smartsheet.list_tables", map[string]interface{}{}, false, &catalog); err != nil {
		return err
	}
	if len(catalog.Sheets) == 0 {
		return errors.New("SMARTSHEET_TABLE_CATALOG_EMPTY")
	}
	tables := map[string]bool{}
	for _, table := range catalog.Sheets {
		if table.ID == "" || tables[table.ID] {
			return errors.New("SMARTSHEET_TABLE_ID_INVALID")
		}
		tables[table.ID] = true
		for _, tool := range []string{"smartsheet.list_fields", "smartsheet.list_records"} {
			total, offset := -1, 0
			seen := map[string]bool{}
			var pageDigests []string
			var pageOffsets []int
			for {
				var page nativeSmartPage
				args := map[string]interface{}{"sheet_id": table.ID, "offset": offset, "limit": 100}
				if tool == "smartsheet.list_records" {
					args["include_computed_values"] = true
				}
				if err := c.call(ctx, tool, args, false, &page); err != nil {
					return err
				}
				items := page.Fields
				idKey := "field_id"
				if tool == "smartsheet.list_records" {
					items = page.Records
					idKey = "record_id"
				}
				if page.Total == nil || *page.Total < 0 {
					return errors.New("SMARTSHEET_TOTAL_MISSING")
				}
				if total < 0 {
					total = *page.Total
				}
				if total != *page.Total {
					return errors.New("SOURCE_CHANGED_DURING_READ")
				}
				for _, item := range items {
					var fields map[string]json.RawMessage
					if err := json.Unmarshal(item, &fields); err != nil {
						return err
					}
					var id string
					_ = json.Unmarshal(fields[idKey], &id)
					if id == "" || seen[id] {
						return errors.New("SMARTSHEET_PAGINATION_DUPLICATE_OR_MISSING_ID")
					}
					seen[id] = true
				}
				encoded, _ := json.Marshal(items)
				pageDigests = append(pageDigests, fmt.Sprintf("%x", sha256.Sum256(encoded)))
				pageOffsets = append(pageOffsets, offset)
				offset += len(items)
				if offset > total {
					return errors.New("SMARTSHEET_TOTAL_MISMATCH")
				}
				if offset == total {
					if page.HasMore {
						return errors.New("SMARTSHEET_PAGINATION_CONTRADICTION")
					}
					break
				}
				if !page.HasMore || len(items) == 0 || page.Next != offset {
					return errors.New("SMARTSHEET_PAGINATION_NO_PROGRESS")
				}
			}
			// Fields are a small structural contract: verify every page again so a
			// same-count rename/type change cannot pass as an unchanged schema.
			if tool == "smartsheet.list_fields" {
				for i, digest := range pageDigests {
					var verify nativeSmartPage
					if err := c.call(ctx, tool, map[string]interface{}{"sheet_id": table.ID, "offset": pageOffsets[i], "limit": 100}, true, &verify); err != nil {
						return err
					}
					encoded, _ := json.Marshal(verify.Fields)
					if verify.Total == nil || *verify.Total != total || fmt.Sprintf("%x", sha256.Sum256(encoded)) != digest {
						return errors.New("SOURCE_CHANGED_DURING_READ")
					}
				}
			} else {
				var verify nativeSmartPage
				if err := c.call(ctx, tool, map[string]interface{}{"sheet_id": table.ID, "offset": 0, "limit": 1, "include_computed_values": true}, true, &verify); err != nil {
					return err
				}
				if verify.Total == nil || *verify.Total != total {
					return errors.New("SOURCE_CHANGED_DURING_READ")
				}
				c.snapshot.Units += total
			}
		}
	}
	var last nativeSmartCatalog
	if err := c.call(ctx, "smartsheet.list_tables", map[string]interface{}{}, true, &last); err != nil {
		return err
	}
	before, _ := json.Marshal(catalog)
	after, _ := json.Marshal(last)
	if string(before) != string(after) {
		return errors.New("SOURCE_CHANGED_DURING_READ")
	}
	return nil
}

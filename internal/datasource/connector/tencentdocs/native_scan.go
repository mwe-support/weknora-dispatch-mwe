package tencentdocs

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
)

type NativeScanRequest struct {
	ResourceID     string   `json:"resource_id"`
	ParentID       string   `json:"parent_id,omitempty"`
	FolderPath     string   `json:"folder_path,omitempty"`
	Ancestors      []string `json:"ancestors,omitempty"`
	Offset         int      `json:"offset"`
	Page           int      `json:"page"`
	Metadata       bool     `json:"metadata,omitempty"`
	PreviousDigest string   `json:"previous_digest,omitempty"`
}

type NativeScanEntry struct {
	ExternalID   string `json:"external_id"`
	FileID       string `json:"file_id"`
	Title        string `json:"title"`
	DocumentType string `json:"document_type"`
	FolderPath   string `json:"folder_path"`
	Disposition  string `json:"disposition"`
}

type NativeScanPage struct {
	Entries  []NativeScanEntry   `json:"entries"`
	Children []NativeScanRequest `json:"children"`
	Next     *NativeScanRequest  `json:"next,omitempty"`
	Digest   string              `json:"digest"`
	Response NativeResponse      `json:"response"`
}

// Each request is one durable enumeration unit. File bodies are read by
// separate document jobs; a failed body never repeats a completed listing.
func NativeScanRoots(resources []string) ([]NativeScanRequest, error) {
	if len(resources) == 0 || len(resources) > maxMCPPagination {
		return nil, errors.New("SCAN_SCOPE_REQUIRED")
	}
	ids := slices.Clone(resources)
	slices.Sort(ids)
	roots := make([]NativeScanRequest, 0, len(ids))
	for _, id := range slices.Compact(ids) {
		ref, err := decodeResourceID(id)
		if err != nil {
			return nil, err
		}
		request := NativeScanRequest{ResourceID: id, ParentID: ref.nodeID, Metadata: ref.nodeID != ""}
		if ref.nodeID != "" {
			request.Ancestors = []string{ref.nodeID}
		}
		roots = append(roots, request)
	}
	return roots, nil
}

func (r NativeScanRequest) Arguments() (string, map[string]interface{}, error) {
	ref, err := decodeResourceID(r.ResourceID)
	if err != nil {
		return "", nil, err
	}
	if r.Page < 0 || r.Page >= maxMCPPagination || r.Offset < 0 || r.Offset > 10000000 || len(r.Ancestors) > 256 {
		return "", nil, errors.New("SCAN_LIMIT_EXCEEDED")
	}
	if (r.ParentID == "" && len(r.Ancestors) != 0) || (r.ParentID != "" && (len(r.Ancestors) == 0 || r.Ancestors[len(r.Ancestors)-1] != r.ParentID)) ||
		(ref.nodeID != "" && (len(r.Ancestors) == 0 || r.Ancestors[0] != ref.nodeID)) {
		return "", nil, errors.New("SCAN_SCOPE_INVALID")
	}
	seen := map[string]bool{}
	for _, id := range r.Ancestors {
		if requireID("node ID", id) != nil || seen[id] {
			return "", nil, errors.New("SCAN_CYCLE")
		}
		seen[id] = true
	}
	if r.Metadata {
		if ref.nodeID == "" || r.ParentID != ref.nodeID || r.Offset != 0 || r.Page != 0 {
			return "", nil, errors.New("SCAN_SCOPE_INVALID")
		}
		return toolQueryFileInfo, map[string]interface{}{"file_id": r.ParentID}, nil
	}
	if ref.kind == resourceKindSpace || ref.kind == resourceKindNode {
		args := map[string]interface{}{"space_id": ref.spaceID, "num": r.Offset}
		if r.ParentID != "" {
			args["parent_id"] = r.ParentID
		}
		return toolQuerySpaceNode, args, nil
	}
	args := map[string]interface{}{"start": r.Offset}
	if r.ParentID != "" {
		args["folder_id"] = r.ParentID
	}
	return toolManageFolderList, args, nil
}

// ReadScan shares the bounded transport and JSON envelope validation with
// document reads. Its request type cannot invoke a write tool or follow a URL.
func (c *TencentDocsMCPClient) ReadScan(ctx context.Context, request NativeScanRequest) (*NativeResponse, error) {
	tool, args, err := request.Arguments()
	if err != nil {
		return nil, err
	}
	return c.readNativeResponse(ctx, tool, args)
}

func ReadNativeScanPage(ctx context.Context, request NativeScanRequest, read NativeReadFunc) (*NativeScanPage, error) {
	tool, args, err := request.Arguments()
	if err != nil {
		return nil, err
	}
	response, err := read(ctx, tool, args, false)
	if err != nil {
		return nil, err
	}
	if response == nil || !json.Valid(response.Data) {
		return nil, errors.New("SCAN_RESPONSE_INVALID")
	}
	actual := max(response.Bytes, int64(len(response.Data)))
	if actual > maxNativeResponseBytes {
		return nil, &NativeLimitError{Kind: "scan_page", LimitBytes: maxNativeResponseBytes, ActualBytes: &actual}
	}
	ref, _ := decodeResourceID(request.ResourceID)
	page := &NativeScanPage{Response: *response, Entries: []NativeScanEntry{}, Children: []NativeScanRequest{}}
	var nodes []Node
	finished := true
	if request.Metadata {
		var info FileInfo
		if err := json.Unmarshal(response.Data, &info); err != nil {
			return nil, err
		}
		if info.ID != request.ParentID || (ref.spaceID != "" && info.SpaceID != ref.spaceID) {
			return nil, errors.New("SCAN_IDENTITY_MISMATCH")
		}
		if info.Status != "normal" {
			return nil, errors.New("SCAN_SOURCE_NOT_NORMAL")
		}
		if info.IsFolder {
			child := request
			child.Metadata, child.FolderPath = false, sourceFolder(request.FolderPath, info.Title)
			page.Children = append(page.Children, child)
			page.Entries = append(page.Entries, NativeScanEntry{ExternalID: fetchedNodeResourceID(ref.spaceID, info.ID), FileID: info.ID, Title: info.Title, FolderPath: request.FolderPath, Disposition: "container"})
		} else {
			nodes = []Node{{ID: info.ID, Title: info.Title, Type: "file", DocumentType: info.Type}}
		}
	} else if tool == toolQuerySpaceNode {
		var payload struct {
			Children *[]Node `json:"children"`
			HasNext  *bool   `json:"has_next"`
		}
		if err := json.Unmarshal(response.Data, &payload); err != nil {
			return nil, err
		}
		if payload.Children == nil || payload.HasNext == nil {
			return nil, errors.New("SCAN_RESPONSE_INVALID")
		}
		nodes, finished = *payload.Children, !*payload.HasNext
	} else {
		var payload struct {
			List   *[]HomeNode `json:"list"`
			Finish *bool       `json:"finish"`
		}
		if err := json.Unmarshal(response.Data, &payload); err != nil {
			return nil, err
		}
		if payload.List == nil || payload.Finish == nil {
			return nil, errors.New("SCAN_RESPONSE_INVALID")
		}
		finished = *payload.Finish
		for _, home := range *payload.List {
			kind := "file"
			if home.IsFolder {
				kind = "folder"
			}
			nodes = append(nodes, Node{ID: home.ID, Title: home.Title, Type: kind, HasChildren: home.IsFolder})
		}
	}
	if len(nodes) == 0 && !finished {
		return nil, errors.New("SCAN_EMPTY_UNFINISHED")
	}
	if len(nodes) > maxMCPPagination {
		return nil, errors.New("SCAN_LIMIT_EXCEEDED")
	}
	seen := map[string]bool{}
	for _, node := range nodes {
		if requireID("node ID", node.ID) != nil || seen[node.ID] {
			return nil, errors.New("SCAN_NODE_IDENTITY_INVALID")
		}
		seen[node.ID] = true
		entry := NativeScanEntry{ExternalID: fetchedNodeResourceID(ref.spaceID, node.ID), FileID: node.ID, Title: node.Title, DocumentType: strings.ToLower(node.DocumentType), FolderPath: request.FolderPath}
		switch {
		case isLinkNode(node):
			entry.Disposition = "link"
		case strings.EqualFold(node.Type, "folder") || strings.EqualFold(node.Type, "wiki_folder"):
			entry.Disposition = "container"
		case isSyncableNode(node):
			entry.Disposition = "document"
		default:
			return nil, errors.New("SCAN_NODE_TYPE_UNSUPPORTED")
		}
		page.Entries = append(page.Entries, entry)
		if entry.Disposition != "link" && (node.HasChildren || entry.Disposition == "container") {
			if slices.Contains(request.Ancestors, node.ID) {
				return nil, errors.New("SCAN_CYCLE")
			}
			child := NativeScanRequest{ResourceID: request.ResourceID, ParentID: node.ID, FolderPath: sourceFolder(request.FolderPath, node.Title), Ancestors: append(slices.Clone(request.Ancestors), node.ID)}
			page.Children = append(page.Children, child)
		}
	}
	// Ignore trace IDs and arbitrary provider URLs when comparing pages.
	encoded, _ := json.Marshal([]any{page.Entries, page.Children, finished})
	page.Digest = fmt.Sprintf("%x", sha256.Sum256(encoded))
	if page.Digest == request.PreviousDigest {
		return nil, errors.New("SCAN_PAGE_REPEATED")
	}
	if !finished {
		next := request
		next.Page++
		next.Offset++
		if tool == toolManageFolderList {
			next.Offset = request.Offset + len(nodes)
		}
		next.PreviousDigest = page.Digest
		page.Next = &next
	}
	return page, nil
}

package tencentdocs

import (
	"context"
	"errors"
	"strings"
)

// Start at the selected root: access to an old parent alone does not prove
// that a document still belongs to that root. This proves membership only,
// never that a previously exported byte snapshot is the current file version.
func ReadNativeMembership(ctx context.Context, resourceID, fileID, externalID string, read NativeScanReadFunc) (*NativeScanEntry, error) {
	if read == nil || fileID == "" || externalID == "" {
		return nil, errors.New("RESOURCE_LISTING_PROOF_REQUIRED")
	}
	queue, err := NativeScanRoots([]string{resourceID})
	if err != nil {
		return nil, err
	}
	var bytes int64
	for i := 0; i < len(queue); i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if i >= maxMCPPagination {
			return nil, errors.New("SCAN_LIMIT_EXCEEDED")
		}
		request := queue[i]
		page, err := ReadNativeScanPage(ctx, request, func(ctx context.Context, _ string, _ map[string]interface{}, _ bool) (*NativeResponse, error) {
			return read(ctx, request)
		})
		if err != nil {
			return nil, err
		}
		bytes += max(page.Response.Bytes, int64(len(page.Response.Data)))
		if bytes > maxExportBytes {
			return nil, errors.New("SCAN_LIMIT_EXCEEDED")
		}
		for _, entry := range page.Entries {
			if entry.FileID == fileID {
				if entry.ExternalID != externalID || entry.Disposition != "document" {
					return nil, errors.New("SCAN_IDENTITY_MISMATCH")
				}
				return &entry, nil
			}
		}
		queue = append(queue, page.Children...)
		if page.Next != nil {
			queue = append(queue, *page.Next)
		}
		if len(queue) > maxMCPPagination {
			return nil, errors.New("SCAN_LIMIT_EXCEEDED")
		}
	}
	return nil, errors.New("RESOURCE_NO_LONGER_IN_SCOPE")
}

// A provider error alone never identifies an attachment. The original entry
// must come from a confirmed listing, and only the existing exact unsupported
// metadata contract permits this fallback.
func NativeResourceFallback(entry NativeScanEntry, err error) bool {
	return entry.Listing != nil && shouldExportUnclassifiedNode(Node{ID: entry.FileID, Type: entry.NodeType, DocumentType: entry.DocumentType}, err)
}

type NativeScanReadFunc func(context.Context, NativeScanRequest) (*NativeResponse, error)

// Re-enumerate only the recorded parent inside the selected source. Listing
// proves identity/access, not byte immutability: resource jobs use a distinct
// export snapshot per scan and record the full downloaded content digest.
func VerifyNativeResource(ctx context.Context, expected NativeScanEntry, read NativeScanReadFunc) error {
	if expected.Listing == nil || expected.Listing.Metadata || read == nil || expected.FileID == "" || expected.ExternalID == "" {
		return errors.New("RESOURCE_LISTING_PROOF_REQUIRED")
	}
	request := *expected.Listing
	request.Offset, request.Page, request.PreviousDigest = 0, 0, ""
	for {
		page, err := ReadNativeScanPage(ctx, request, func(ctx context.Context, _ string, _ map[string]interface{}, _ bool) (*NativeResponse, error) {
			return read(ctx, request)
		})
		if err != nil {
			return err
		}
		for _, actual := range page.Entries {
			if actual.FileID != expected.FileID {
				continue
			}
			if actual.ExternalID != expected.ExternalID || actual.Disposition != "document" || actual.Title != expected.Title || !strings.EqualFold(actual.NodeType, expected.NodeType) || !strings.EqualFold(actual.DocumentType, expected.DocumentType) {
				return errors.New("SOURCE_CHANGED_DURING_READ")
			}
			return nil
		}
		if page.Next == nil {
			return errors.New("RESOURCE_NO_LONGER_IN_SCOPE")
		}
		request = *page.Next
	}
}

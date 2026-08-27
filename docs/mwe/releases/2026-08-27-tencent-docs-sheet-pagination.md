# Tencent Docs Sheet range pagination fix

Date: 2026-08-27
Upstream: WeKnora v0.7.2
Target branch: weknora-v0.7.2
Repository: mwe-support/weknora-dispatch-mwe

## Scope

This release fixes silent truncation when a Tencent Docs Sheet contains more rows than the generic get_content response returns.

## Root cause

The Tencent Docs connector used one generic get_content call for every online document type. For Sheet documents, that response stopped at about 100 records, exposed no cursor or truncation marker, and was accepted as a successful complete document. WeKnora therefore indexed incomplete Markdown while the sync log reported success.

## Changes

- Detect sheet, excel, and tencentsheet document types.
- Call sheet.get_sheet_info to obtain worksheet dimensions.
- Read each worksheet with sheet.get_cell_data using explicit inclusive row ranges.
- Keep each request below the 20,000-cell MCP limit and prefer 100 rows per call.
- Render non-empty rows to Markdown with an explicit source-row column.
- Store source_sheet_count, source_row_count, exported_row_count, and non_empty_row_count in knowledge metadata.
- Fail the document sync if worksheet metadata, dimensions, range reads, or returned coordinates are invalid.
- Preserve generic get_content behavior for non-Sheet online documents.

## Follow-up hardening

Status: deployed and production-verified on 2026-08-27 in marvel/weknora-app:v0.7.2-tencentdocs-sheet-pagination-v2-20260827.

The targeted production repair exposed two additional edge cases, now covered in the same implementation:

- Pagination is planned per worksheet: small sheets use one range request; larger or wider sheets are split by the 100-row preference and the 20,000-cell hard limit.
- Every declared row range is still scanned. Empty intermediate pages never terminate scanning, because sparse sheets can contain valid data hundreds of rows later.
- Completely empty worksheets emit an explicit `_Empty worksheet_` marker instead of an empty Markdown table.
- Non-empty rows are buffered per worksheet so trailing unused columns are removed from Markdown while source row numbers and interior empty cells remain intact.
- Compatibility metadata (`source_row_count`, `exported_row_count`, `non_empty_row_count`) is preserved, with explicit scanned/emitted row, page-count, empty-sheet, and used-column metrics added.
- Final scan coverage is checked before content is accepted; incomplete planning fails the document instead of advancing the sync cursor.

## Verification

Automated:
- go test ./internal/datasource/connector/tencentdocs -count=1
- go test ./internal/datasource/... ./internal/application/service ./internal/types -count=1
- Connector fixture: 191 rows and 26 columns; verifies two range calls (0-99 and 100-190), a row-119 sentinel, the final-page sentinel, and exact metadata counts.
- MCP adapter fixture: verifies exact sheet.get_sheet_info and sheet.get_cell_data tool names and arguments.

Read-only real document check:
- Target file ID: PMJIUYDBGCPD
- Worksheet ID: 000001
- Source rows: 191
- Exported rows: 191
- Generated Markdown: 48,292 bytes
- The sentinel record beyond row 100 is present.
- No source document or production knowledge was modified by the connector-level test.

## Deployment

Image:
- marvel/weknora-app:v0.7.2-tencentdocs-sheet-pagination-20260827

Only WeKnora-app was recreated. Database, frontend, object storage, model services, and other containers were not recreated.

Configuration backup:
- /public/knowledgebase/weknora/override.yml.bak.20260827T094437Z-sheet-pagination

Follow-up image:
- marvel/weknora-app:v0.7.2-tencentdocs-sheet-pagination-v2-20260827
- Code commit: 6e4a2d876f965e7f47d6e962e2ee5cd23a709706
- Configuration backup: /public/knowledgebase/weknora/override.yml.bak.20260827T145552Z-sheet-pagination-v2

Only WeKnora-app was recreated for the follow-up; all other container IDs were unchanged.

## Rollback

1. Restore the follow-up backup override or replace the app image with:
   marvel/weknora-app:v0.7.2-tencentdocs-sheet-pagination-20260827
2. Run:
   docker compose --profile minio -f docker-compose.yml -f ../override.yml up -d --no-deps app
3. Confirm WeKnora-app is healthy.

## Residual risk

- Existing Sheet knowledges remain unchanged until their data source is synchronized again.
- The production inventory currently contains 157 Tencent Docs Sheet knowledges; not all have more than 100 rows.
- Very wide worksheets above 20,000 columns fail explicitly rather than silently truncating.
- Physical row_count can include blank rows; they are included in scanned/exported coverage but omitted from Markdown and emitted-row counts.
- The production knowledge reindex for the reported data source requires an authorized owner/admin to trigger the sync.

## Production acceptance

Accepted on 2026-08-27 after a targeted repair of the affected production inventory.

- Original refined inventory: 157 Tencent Docs Sheet knowledges.
- Pagination-affected candidates: 29.
- User-confirmed out-of-scope personal-home documents excluded from repair: 2; existing knowledge was not deleted.
- Final targeted repair plan: 27 documents across 6 data sources and 6 successful sync batches.
- Sync result: 27 updated, 0 failed.
- Final knowledge state: 27 completed, 27 summaries completed, 0 pending subtasks.
- Row coverage: 42,288 source rows and 42,288 scanned/exported rows; 6,721 non-empty rows emitted to Markdown.
- Row mismatches: 0.
- Reported file PMJIUYDBGCPD: 191/191 rows, completed, and two indexed chunks contain the row-119 sentinel supplier name.
- Production image: marvel/weknora-app:v0.7.2-tencentdocs-sheet-pagination-20260827; WeKnora-app healthy.
- Follow-up image: marvel/weknora-app:v0.7.2-tencentdocs-sheet-pagination-v2-20260827; code commit 6e4a2d87; container health endpoint returned `{"status":"ok"}`; restart count 0.
- Follow-up deployment recreated only WeKnora-app; no other container ID changed; startup logs contained no panic/fatal/startup failure.
- Post-deployment active knowledge processing: 0; running/pending data-source syncs: 0.
- Final audit: /public/knowledgebase/results/tencent-sheet-repair-scan/20260827T100149Z/final-acceptance.json
- Exclusion audit: /public/knowledgebase/results/tencent-sheet-repair-scan/20260827T100149Z/repair-plan-exclusions.json

## Push target

Repository: mwe-support/weknora-dispatch-mwe; branch: weknora-v0.7.2. This release record is included in the deployment push batch.

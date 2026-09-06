# Tencent Docs source paths and FAQ sources — production acceptance

- Upstream: WeKnora v0.7.2; target branch: `weknora-v0.7.2`.
- Repository: `mwe-support/weknora-dispatch-mwe`; no MCP-server changes.
- Status: deployed with explicit user approval; real MCP/API acceptance passed. Source changes are included in the [2026-09-06 reviewed publish batch](2026-09-06-prepush-review-and-publish.md).
- Release identifier: `v0.7.2-tencent-paths-faq-20260904` (app and frontend).

## 1. Preserve source folders

Previously the connector discarded parent folders and ingestion supplied only a basename. Traversal now carries the selected folder and descendant names through normal fetches, error records and file compensation retries. The resulting relative filename uses the existing manual-folder-upload ingestion path; no schema migration or parallel folder storage is introduced.

- Selecting a folder includes that folder itself; selecting a whole space/home preserves folders below that root. Selecting an individual file places it at the KB root. Ancestors outside the selected scope are not fetched or invented.
- `folder_path` is the normalized KB-relative folder; `source_path` is the selected-scope relative path plus source title, not a full absolute Tencent location.
- Literal slash/backslash characters in a source folder name become full-width characters so they do not create unintended levels. Existing 16-level, 128-byte-segment and 1024-byte-path limits apply. UTF-8 truncation is corrected in the shared normalizer.
- New cursors remember folder paths; a later folder rename/move triggers re-ingestion even if the document modification time is unchanged.
- Old cursors retain their incremental behavior. Existing flattened documents need an explicitly requested full or targeted resync to adopt paths; this change does not silently re-import them.
- Existing content deduplication and update/delete behavior are unchanged. Normalized name collisions and content deduplication retain the same limitations as manual uploads.

## 2. FAQ sources

FAQ settings expose the existing datasource editor. The UI filters types to Tencent Docs; create, update and sync execution enforce the same restriction on the server.

Supported inputs: FAQ-template CSV, TSV, XLSX, JSON arrays, and Tencent Docs Markdown tables (including the paginated Sheet exporter). XLSX processes every nonempty sheet; Sheet errors preserve original worksheet row numbers. Legacy binary XLS is explicitly rejected with instructions to save as XLSX. PDF, images and unstructured prose are not inferred into FAQs.

Required fields follow the existing FAQ validator: standard question and at least one nonempty answer. Table headers accept `问题`/`standard_question`/`question` and `机器人回答`/`answers`; parentheses notes in template headers are ignored. Existing optional tags, similar/negative questions, answer strategy, enabled/recommended flags are supported. Multiple text values use `##`, not comma/semicolon. Internal exported IDs are ignored, like manual JSON import; labels are resolved in the target KB.

All rows in a source file pass format validation before any import. Invalid header, missing answer, bad boolean, extra nonempty columns, malformed data or unsupported type records `FAQ_FORMAT_INVALID` / `faq_validate`. Failures identify the source file/path and worksheet/row or JSON entry. Other source files continue.

Valid files use the existing FAQ append/standard-question merge and embedding implementation synchronously. Sync success requires terminal import progress with no failed or partially failed entries. Downstream failures record `FAQ_IMPORT_FAILED` / `faq_import`, task ID and available failure details; they do not advance successful source cursors. Inline import running markers are released even on early returns/cancellation.

This is append/merge synchronization, not source-owned replacement: deleting or renaming a source question does not remove the old FAQ, and source removal never deletes manual FAQs. Conflicting standard questions across sources use existing KB-wide merge semantics. A downstream partial import can leave successful entries present; rerunning merges them rather than replacing the KB. Format errors do not enter the transient-network compensation loop; a later regular/manual sync reads rejected files again.

## 3. Logs and Grafana

- Existing per-file `SyncItemError` fields retain identity/category/stage, with added `source_path`. The frontend prefers the path when displaying failure samples.
- Grafana displays `FAQ格式校验` and `FAQ入库` stages with the source path and error message.
- Only confirmed successful FAQ imports add `result.faq_completed[external_id]` timestamps. A later matching success under the same tenant and datasource resolves an older file failure; an unrelated or empty incremental success does not. This is needed because FAQ entries share one knowledge record rather than one knowledge record per source file.
- Existing per-run error sample caps, datasource retention, paging and tenant isolation are unchanged. No source bodies, tokens or signed URLs are added to this release record.

## Verification

Development tests use synthetic files only, never production credentials or databases:

```sh
go test ./internal/application/service ./internal/datasource/connector/tencentdocs ./internal/types
go test -race -count=3 ./internal/application/service ./internal/datasource/connector/tencentdocs -run 'TestFAQSource|TestTencentSourceFolder|TestTencentSourcePath|TestTencentRejectedItem|TestTencentMovedFolder|TestTencentRetryKeepsFolder'
npm --prefix frontend run type-check
npm --prefix frontend run build
python scripts/render-observability-failure-queries.py
python scripts/test-observability-failure-queries.py --emit /tmp/faq-observability-fixture.sql
```

Go runs in an isolated Linux container with `--network none`, 2 CPUs and 4 GiB RAM. Windows full Go tests cannot compile the pre-existing CGO `pg_query` dependency; this is not counted as a pass. Public dependencies already declared by go.mod are reused from the server cache.

Coverage: nested/selected-root folders, recursive sibling isolation, folder moves, retry path retention, UTF-8 limits, real manual ingestion reuse, all FAQ formats, multi-sheet validation, source row 119/201, malformed values, no import before validation, partial downstream failure, rejected cursor acknowledgements and connector restrictions. Grafana synthetic PostgreSQL assertions cover capture, presentation, successful-file recovery, old-success rejection, tenant isolation and existing pagination/count behavior. The database container has no network/host port, uses tmpfs and is removed after the test.

Full ordinary Go package regression passed for all three packages. Frontend type checking, final build and asset integrity (532 retained/generated assets) passed. Synthetic SQL assertions passed, including FAQ failure display and per-file recovery evidence.

The broader `go test -race -count=3 ./internal/application/service` is **not green**: `TestTenantAPIKeyServiceAuthenticateThrottlesLastUsedUpdates` races between its fake repository writer and test reader; repeated Wiki tests reuse SQLite state and fail `UNIQUE constraint failed: wiki_pages.slug`. Both failures were reproduced in the pre-change isolated baseline. These unrelated files are unchanged; this release does not claim a clean whole-service race gate. Evidence is in `faq-full-race.log` and `faq-baseline-race.log` under the isolated `file-retry-20260904` test directory.

All nine new targeted tests (including their table-driven cases) passed three race-enabled repetitions on the final candidate. Evidence is recorded in `faq-targeted-race-final.log` in the same isolated directory. Real Tencent datasource acceptance subsequently passed as recorded below. Signed-in browser visual/interaction acceptance was not performed; frontend validation comprises type checking, build integrity and deployed frontend health.

## Production acceptance — 2026-09-04

Deployment waited for all nine Asynq queues to have zero active/pending/scheduled/retry tasks, zero nonterminal knowledge records, zero running syncs and zero MinerU processing/queued tasks. The checks were repeated immediately before cutover. Only app/frontend were recreated; all other container IDs/start times remained unchanged. Grafana merged only the document-failure SQL and its page-count query into the existing dashboard, preserving other production panel changes.

- App: `marvel/weknora-app:v0.7.2-tencent-paths-faq-20260904`.
- Frontend: `marvel/weknora-ui:v0.7.2-tencent-paths-faq-20260904`.
- Binary SHA256: `1ac87f0c0bd718d2cb8a30a8d8390856e0afdb16c782172cfc7913bb6f6fee8f`.
- Final app/frontend health: healthy, restart count 0. Grafana health and real datasource query: HTTP 200; dashboard version 25.
- Backup/audit directory: `/public/knowledgebase/results/tencent-paths-faq-release-20260904/` (`override.yml.before`, `dashboard.before.json`, `containers.before.json`, `deployment.json`, `grafana-*.json`).

The user explicitly approved a new synthetic Tencent personal-home test folder. The current WeKnora MCP key was reused through HTTPS for standard datasource APIs (native MCP has no datasource-management tools), while native MCP verified resulting knowledge and FAQ retrieval. No existing business datasource was synced or modified, and no key/token was logged or stored in local test artifacts.

| Scenario | Verified result | Sync log |
| --- | --- | --- |
| Nested document source | 1/1 created, completed, zero pending subtasks; `folder_path=一级目录/二级目录`; API folder tree has both levels | `72c81124-4d58-4106-8612-8d282f652459` |
| FAQ first run: valid sheet plus missing-answer sheet | 2 source files: 1 updated, 1 failed; two FAQ entries from the valid sheet, including source row 119 | `f89873ea-e432-47b9-803c-e8906fc0c713` |
| Invalid FAQ observability | `FAQ_FORMAT_INVALID`, `faq_validate`, source-relative path and `工作表1 row 2: 答案不能为空`; real Grafana query displayed one unresolved row | same first-run log |
| Fix only the missing answer, incremental retry | Exactly 1 source file processed successfully; total FAQ entries became 3; Grafana unresolved test rows became 0 | `71a3f7ea-5f30-4c4c-a057-d6724b9f4cd8` |
| Incremental sync without further changes | 0 source files processed, no failures; exactly 3 FAQ entries remain | `b4b847cc-f78b-49ad-adb2-357825fdc02f` |
| Non-Tencent type on FAQ KB | HTTP 400 with explicit Tencent-only explanation; no datasource created | standard create API |
| Native MCP retrieval | Row-119 answer ranked first (score 0.949885); repaired answer ranked first (0.932317) | `hybrid_search` |

Generic MCP `list_chunks` defaults to text chunks and therefore does not enumerate FAQ chunks; the existing FAQ entries API and MCP `hybrid_search` verified them. No MCP server modification was made for this pre-existing tool limitation.

Test KBs `MWE验收-目录保留-20260904` and `MWE验收-FAQ同步-20260904` and their synthetic sources remain available for user review; their schedules are empty (manual sync only). Final queues/nonterminal knowledge/running syncs/MinerU processing and queued counts were all zero. Real-source coverage here is Tencent Sheet; CSV/TSV/JSON/XLSX and parser edge cases remain covered by isolated tests, not claimed as live cloud tests.

## Rollout and rollback

The approved rollout and targeted acceptance above are complete. Do not resync all existing sources automatically. Further rollout of source paths to existing flattened documents requires an explicitly selected resync scope.

Rollback restores the backed-up override (app/UI `v0.7.2-file-compensation-20260904`) and `dashboard.before.json`, then runs the existing Compose command with `--no-deps app frontend`; verify both healthy. Exact prior image IDs and container snapshots are in `deployment.json`. There is no database migration. Pause newly created FAQ datasources before rolling back to code that does not understand FAQ datasource ingestion. Imported FAQs and folder paths are data and are not removed by image rollback. Release notes are Chinese/English technical text in this single existing release directory; no root governance or unrelated documentation changes.

# Tencent file-source synchronization simplification

- Upstream: WeKnora v0.7.2.
- Repository/branch: `mwe-support/weknora-dispatch-mwe`, `weknora-v0.7.2`.
- Base: `9f26fd173d6c21be5b46d655f68e01daaf6d5249`.
- Build: `marvel/weknora-app:v0.7.2-tencent-file-sync-20260912-r2`.
- Image: `sha256:e40b8393f2405bcb1fc13f248a3f7627070fde0c10ddc72dd992aaa1019c5f8d`.
- Application binary SHA-256: `531cc84e387e6db38f17610cab56ec7b9d61346215aba25bd71e359b411a48ae`.
- Final tested source archive: `593f827f9b8e43cbd0acaa4b23920fab0c7e7a2d3ed6021d8910c4579fdb526a`.

## Motivation and approved scope

The customization was intended to handle Tencent MCP failures and missing Smart Sheet/Smart Canvas compatibility. Native per-page reads, repeated version/membership checks and a second whole-document lifecycle consumed requests and delayed work reaching existing WeKnora processors. The source layer now exports files and reuses the existing asynchronous file-processing path.

No daily account quota accounting, fixed daily allowance or next-day budget scheduler is introduced. Normal export polling remains distinct from failure compensation. Retries are scoped to the same data source's completed normal fetch/submission pass, not all sources or downstream model completion. Empty folders are not retained.

## Change groups

1. **Connector export and failure handling** — DOC/Smart Canvas export DOCX; Sheet/Smart Sheet export XLSX. Upload resources keep their file export route. No source-body `get_content`, MDX, field/record or cell-range reads are used by normal synchronization. Known unsupported online types and oversized files are permanent failures. Each failed file gets at most one compensation attempt; terminal records survive later/full scans. Export intents/task IDs and encrypted URLs survive retries. Confirmed pre-send failures are distinguished from unknown sent exports. Same-run completed files are retained in the cursor. Office archive timestamps/generated properties do not force identical content to reindex. Directory-only changes update local paths for completed files.
2. **Existing pipeline handoff and source queue isolation** — normal source work returns after file submission instead of waiting up to ten minutes for parsing/postprocessing. Existing postprocess fan-in publishes candidates only after core/image readiness, with tenant/source/configuration and newer-candidate checks; old good content remains until replacement readiness. The `sync_retry` queue uses one dedicated `source_retry` worker, preserving normal worker capacities. Source batch timeout is 24 hours; individual MCP timeout and export polling deadline remain 30 seconds and 45 minutes. Task recovery is distinct from the persistent single file-attempt budget. PostgreSQL migration 90 / SQLite migration 13 add one server-owned `tencent_file_sync` cutover flag, no new task-state tables. Switching the flag fences old execution while preserving published versions and historical evidence.
3. **Operational visibility** — overview activity/queue cards use actual Redis queue sizes with availability/freshness guards. New panels separate source submission, file compensation/permanent errors and downstream WeKnora state. The permanent/error view reads all retained file records rather than the old 100-error sync-log sample. First/latest errors, file identity, directory, stage and attempt are exposed through restricted views. Credentials, raw configuration, signed URLs and encrypted URL contents are excluded. Redis exporter reads fixed queue keys with key-value export disabled.
4. **Exported XLSX compatibility** — DuckDB rejects Tencent style records that omit the default `numFmtId`. On that specific local reader error, add the explicit zero default in a temporary ZIP copy and retry the local table load once. Cell data, non-default formats and the saved source workbook are unchanged; no Tencent request is made. Use standard-library ZIP/XML processing with a bounded style-entry read, without another dependency.

## Verification

- Existing synthetic Tencent samples were actually exported: four metadata calls, four export starts, four progress calls; four ordinary HTTPS downloads and zero native-body calls. Confirmed real XLSX/DOCX ZIP structure, end markers and DOCX images. Existing DocReader parsed all four in network-disabled test containers; images resolved to local files. This validates representative formats, not every interactive Tencent component.
- Full Tencent connector package: **108 top-level PASS, 0 FAIL, 3 environment-dependent SKIP**, 6.761 seconds. Historical Sheet renderer tests still validate the retained legacy renderer directly; new source tests assert export rather than native range reads.
- Extended source/service/repository/types/router/handler/container checks: **149 top-level PASS, 0 FAIL, 12 environment-dependent SKIP**. Command:
  `go test ./internal/datasource ./internal/application/service ./internal/application/repository ./internal/types ./internal/router ./internal/handler ./internal/container -run '^(TestDataSource|TestTencent|TestFile|TestSource|TestRuntime|TestQueue|TestWorker|TestProcessing)' -count=1 -timeout=150s -v`.
- Checks cover same-source continuation, one file compensation, terminal persistence, cached export URL reuse, same-run restart, path retention, asynchronous candidate handoff, atomic publication and engine-change fencing without loss of published artifacts.
- Isolated PostgreSQL monitor-view probe: file identity and final error detail PASS; direct raw-configuration access denied; signed URL and ciphertext not returned. Test container restored to its original stopped state.
- Installed Alloy image validated the modified configuration successfully. `render-processing-observability.py --check` and `git diff --check` PASS.
- Normal worker concurrency/settings, models and question-generation preferences are preserved. The source retry worker adds one isolated execution slot.
- Final source also passed `go test ./internal/agent/tools -run '^(TestLoadFromExcel|TestBuildExcelCreateTableSQL)' -count=1 -timeout=150s -v`: eight tests passed, none skipped, using the installed DuckDB extension cache with networking disabled. The new test reproduces the original style error, verifies the repaired row/value, and checks that the stored workbook remains byte-identical.
- The synthetic live pilot exposed missing tenant information in asynchronous old-version cleanup. Publication now restores tenant context through the existing worker helper before publication/cleanup. The regression test checks both tenant ID and tenant object at the actual cleanup call.

## Rollout and retained evidence

The deployment drains activity under owned queue pauses, backs up the existing override and source cursor/state on the host with restricted permissions, starts the new image/migration, and dry-runs the repository-based source cutover before committing. Source-mode change and old task invalidation use repository source locks and event history. Confirmed size/type failures and unknown export obligations are transferred to persistent file refusals; proven-unsent intents are not treated as accepted exports. Published version count must remain unchanged across cutover. Old source payloads referring to ledger-owned/finished logs are no-ops.

Host evidence directory: `/public/knowledgebase/results/tencent-streaming-20260912/`. Attempts have separate directories; private backups must not be copied into Git.

The first startup was blocked by a roughly five-minute observer aggregation query holding a DDL-conflicting read lock. The deployment restored the previous app and released all owned pauses before any source-mode cutover. After canceling only the identified read-only query, migration 90's actual schema and zero cutover flags were verified before repairing its interrupted dirty marker. The next rollout stopped collection during schema changes. Alloy now identifies its connections and limits each statement to ten seconds; expensive historical aggregation may time out instead of holding locks indefinitely. Historical query optimization is not implied by this timeout guard.

Attempt 2 committed all 54 source flags and retained all 1,036 versions published immediately before cutover. All 12 owned pauses were released. Live checks verified application health, migration 90 clean, zero running old lifecycle steps, the actual Grafana PostgreSQL query, `redis_up=1`, and all 48 fixed queue metrics. Existing permanent/uncertain cursor records were retained; no claim is made that historical incidents are resolved.

One existing synthetic directory source was manually synchronized. Its normal source pass succeeded in 14.91 seconds, submitting one XLSX with the relative path `一级目录/二级目录`. Source submission ended before core indexing finished, demonstrating asynchronous handoff. The initial pilot found the tenant-context and XLSX compatibility defects described above; attempt 3 installs those fixes without rerunning the source cutover. The saved synthetic file is reparsed for final validation without another source export.

Final live verification on the r2 image: the same synthetic file reached `parse_status=completed`, `summary_status=completed`, enabled and published, with its directory intact. The new attempt's parsing, indexing and postprocess spans finished; logs confirmed table extraction and indexed summary completion. Failed prior-attempt evidence remains historical. The r2 rollout again released all 12 owned pauses. Final checks found 54/54 sources in file mode and migration 90 clean. Source schedules and paused-source settings were preserved; this rollout did not start a bulk rescan.

Commit mapping: `3cf9107d` implements group 1; `5a825d48` implements group 2 including tenant-context restoration; `ded35b29` implements group 4. The final monitoring/release-record commit implements group 3. All belong to the main application repository; no MCP-server or upstream push is involved.

## Rollback and residual limits

Before source cutover commits, the previous image can be restored directly. After cutover, retain a binary that understands asynchronous file candidates; do not blindly restore an old binary while new-mode work is queued. Pause/drain the file-source work, preserve published content, explicitly reconcile pending candidates/cursors and source mode, then restore the previous engine/image if required. Historical lifecycle tables and evidence remain available.

Permanent failures require explicit operator action to reopen. Unknown export results are never blindly restarted. File downloads are retried from the saved URL/task rather than byte-range resumed. Directory/connection failures can end a source pass; completed-file cursor evidence and final error logs remain. The existing per-process source lock assumes the established single-app deployment; a multi-replica deployment still requires shared source locking. Downstream parsing/model retry policies remain WeKnora's existing behavior and are not duplicated in the Tencent source layer.

Alloy option reference: https://grafana.com/docs/alloy/latest/reference/components/prometheus/prometheus.exporter.redis/ .

# Ingestion resilience for Tencent Docs and MinerU

Date: 2026-09-01
Upstream: WeKnora v0.7.2
Target branch: weknora-v0.7.2
Repository: mwe-support/weknora-dispatch-mwe

## Incident summary

- One Tencent Docs sync processed 170 items and reported 73 document failures: 64 HTTP 429 responses, 6 upstream drive errors, 2 network timeouts, and 1 export above the existing 100 MiB limit.
- An incomplete directory traversal also reported 97 deletions. No knowledge rows were deleted, but unseen cursor entries would have been removed and re-imported later.
- The existing MinerU container lost CUDA access while the host GPU remained healthy. A fresh one-shot container passed CUDA allocation, proving the failure was isolated to stale container GPU state.
- 204 knowledge rows across 6 tenants and 8 knowledge bases had already failed with the same CUDA-unavailable error when the incident was measured.
- WeKnora core task concurrency was 8 while MinerU accepted only 2 concurrent requests.

## Changes

- Retry Tencent Docs MCP HTTP 429 and selected transient read/list/progress failures with context-aware exponential backoff: 2, 4, 8, and 16 seconds.
- Do not retry ambiguous timeout errors for `manage.export_file`; HTTP 429 remains retryable because the server explicitly rejected the request.
- Disable incremental deletion detection for a sync whenever any selected subtree could not be listed. Existing cursor entries remain intact for the next sync.
- Classify MinerU network, 429/5xx, queue, CUDA-unavailable, NVML, and CUDA OOM failures as transient service errors.
- Retry a transient MinerU failure with the existing built-in parser. Ordinary content and validation failures remain failures.
- Record `parser_engine` and `parser_fallback_from` in the document-reader trace output.

## Production capacity settings

- Recreate only the stale MinerU container; CUDA allocation and seven real parsing jobs succeeded after recovery.
- Disable unused MinerU VLM preload for the pipeline backend, releasing approximately 4.5 GiB and leaving about 15 GiB available on GPU 0 before pipeline models load.
- Keep MinerU concurrency at 2.
- Set `WEKNORA_ASYNQ_CORE_CONCURRENCY=2` so WeKnora cannot submit more concurrent core parse jobs than MinerU accepts. Pending work remains in the durable Asynq queue instead of the MinerU process queue.

## Release target

- App image: `marvel/weknora-app:v0.7.2-ingestion-resilience-20260901`.
- MinerU capacity backup: `/public/knowledgebase/gpu/compose.yml.bak.20260901T112107Z-mineru-capacity`.
- The MinerU recreation and VLM-preload change are already production-verified; the app concurrency setting is applied with the new image deployment.

## Verification

Commands:

- `go test ./internal/datasource/connector/tencentdocs -count=1`
- `go test ./internal/infrastructure/docparser -count=1`
- `go test ./internal/application/service -count=1`
- `go test ./internal/datasource/... ./internal/infrastructure/docparser ./internal/application/service ./internal/types -count=1`

- Tencent Connector regression: incomplete child traversal emits a failure but no deletion and preserves the unseen child cursor.
- Tencent MCP regression: two HTTP 429 failures followed by success produce three calls and 2/4-second backoffs.
- MinerU regression: CUDA-unavailable 409 responses are classified as transient.
- Parser regression: transient MinerU failure uses built-in parsing; ordinary parsing errors do not fall back.
- Package tests: Tencent Docs connector, docparser, and application service.
- Live recovery: MinerU health returned healthy, CUDA tensor allocation succeeded, and seven production parsing jobs completed with zero failures after recreation.

## Production rollout

- App image deployed: `marvel/weknora-app:v0.7.2-ingestion-resilience-20260901`.
- App override backup: `/public/knowledgebase/weknora/override.yml.bak.20260901T115409Z-ingestion-resilience`.
- Startup confirmed `asynq core-pool server starting with concurrency=2`; health endpoint returned `{"status":"ok"}` and restart count remained 0.
- MinerU VLM preload was disabled using backup `/public/knowledgebase/gpu/compose.yml.bak.20260901T112107Z-mineru-capacity`; GPU 0 idle/preload usage dropped from about 14.1 GiB to 9.5 GiB.
- MinerU recovered with CUDA available and completed 15 production parsing jobs with zero failures during rollout verification.
- 222 CUDA-failed knowledge rows were grouped into six tenant-scoped batch-reparse tasks. Audit: `/public/knowledgebase/results/ingestion-recovery/20260901T120000Z/enqueue-report.json`.
- The affected Tencent Docs data source was retried as sync log `de98374a-07c6-4b1e-ab9a-06b4001c2cdb`. Audit: `/public/knowledgebase/results/ingestion-recovery/20260901T120000Z/pm13-sync-retry.json`.

## Rollback

1. Restore the previous WeKnora app image and set `WEKNORA_ASYNQ_CORE_CONCURRENCY=8` if required.
2. Restore `/public/knowledgebase/gpu/compose.yml` from the timestamped MinerU capacity backup to re-enable VLM preload.
3. Recreate only the affected app or MinerU service and confirm health.

## Residual risk

- Built-in parsing preserves availability but can be less accurate than MinerU for complex layouts; fallback is limited to availability/capacity faults.
- The existing 100 MiB Tencent export limit remains intentional.
- Previously failed knowledge rows require a controlled batch reparse after deployment.
- Tencent Docs upstream may remain rate-limited for longer than the 30-second total retry window; those items remain failed and retryable on the next sync.

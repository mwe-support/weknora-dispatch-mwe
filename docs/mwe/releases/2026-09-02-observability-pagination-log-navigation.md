# Observability pagination and container log navigation

## Release target

- WeKnora base: v0.7.2
- Target branch: `weknora-v0.7.2`
- Deployment host: `marvel-kb` (`192.168.18.25`)
- Project directory: `/public/knowledgebase/observability`
- Dashboard revision: 8

## Change scope and motivation

- Add independent module-local pagers for unresolved document failures,
  unresolved data-source sync failures and task dead letters.
- Keep page variables hidden, enforce `LIMIT 50/OFFSET` in PostgreSQL and add
  previous/next buttons plus an Enter-to-jump numeric input below each table.
- Pin the signed Grafana Labs Business Text plugin at version `6.3.0`; use its
  supported `locationService.partial()` API rather than disabling Grafana HTML
  sanitization.
- Add a single horizontal, scrollable container navigation bar above the log
  panel, matching a top-control-bar interaction rather than separate cards.
- Keep the navigation on Grafana's default theme colors and prevent container
  names from wrapping inside the bar.
- Restrict the related-error log query to the selected container and display
  that container in the panel title.

## Pagination behavior

- Each table query reads at most 50 records using its hidden page variable.
- The pager shows current/total pages, previous/next controls and `50 条/页`.
- Entering a page updates only that module's variable and reruns its table.
- Values outside `1..total pages` do not change the page and show an error.

## Log navigation behavior

- Each navigation item maps to one deployed application or observability
  container; overflow stays on the same horizontal bar and scrolls.
- The backing Loki variable remains hidden and accepts only `WeKnora-*` and
  `kb-*` container values.
- Default is `WeKnora-app`.
- Browser validation switched to `kb-mineru-api`; the URL variable, panel title
  and Loki selector all updated to the selected container.

## Deployment impact

- Updated the provisioned dashboard, Compose plugin pin and documentation.
- Recreated only `kb-observability-grafana` to load the signed plugin; no
  knowledge-base business container or other monitoring container restarted.
- No database schema or application code changed.

## Validation

- Dashboard JSON parse and Git diff check: passed.
- PostgreSQL read-only queries for all three complete result sets: passed
  (`2` current non-deleted document failures, `9` data-source failures,
  `658` dead letters).
- Loki container label query: passed.
- Grafana HTTP: `200`; provisioning log errors in the final five-minute check:
  `0`.
- Browser QA in the authenticated Grafana session: passed.
  - No document, data-source or dead-letter page selector remains at the
    dashboard top.
  - All three modules rendered previous/next buttons and numeric page inputs.
  - Entering dead-letter page `3` changed the first row and URL page variable.
  - Entering invalid page `15` kept page `3` and showed `请输入 1 到 14`.
  - Previous changed page `3` to `2`; browser console stayed clean.

## Rollback

Restore the previous dashboard and Compose files, remove the Business Text
panel dependency, then recreate only the Grafana service. The plugin directory
is stored in `grafana-data`; uninstall it separately only if disk cleanup is
required.

## Residual risks

- Pager state is stored in dashboard URL variables, so copied URLs retain the
  selected table pages.
- Business Text is a pinned third-party panel dependency maintained and signed
  by Grafana Labs; upgrades require compatibility review before changing the
  pinned version.
- Data-source failure resolution remains per latest data-source sync because
  current item errors do not persist `external_id` or `knowledge_id`.

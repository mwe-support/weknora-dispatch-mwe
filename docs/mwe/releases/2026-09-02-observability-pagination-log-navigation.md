# Observability pagination and container log navigation

## Release target

- WeKnora base: v0.7.2
- Target branch: `weknora-v0.7.2`
- Deployment host: `marvel-kb` (`192.168.18.25`)
- Project directory: `/public/knowledgebase/observability`
- Dashboard revision: 3

## Change scope and motivation

- Add independent navigation variables for unresolved document failures,
  unresolved data-source sync failures and task dead letters.
- Enforce `LIMIT 50` and a calculated `OFFSET` in PostgreSQL for every table.
  Grafana 13 native table pagination remains height-dependent and does not
  reliably honor the legacy fixed `pageSize` option, so pagination is applied
  at the authoritative query boundary instead.
- Add a single horizontal, scrollable container navigation bar above the log
  panel, matching a top-control-bar interaction rather than separate cards.
- Keep the navigation on Grafana's default theme colors and prevent container
  names from wrapping inside the bar.
- Restrict the related-error log query to the selected container and display
  that container in the panel title.

## Pagination behavior

- Each page selector is independently calculated from the current unresolved
  record count.
- Default page is 1; page selection persists in the dashboard URL.
- Maximum query result per table page is 50 rows.
- At validation time: document failures exposed 1 page, data-source failures
  exposed 1 page, and dead letters exposed 10 pages. Dead-letter page 2
  returned exactly 50 records in the read-only SQL check.

## Log navigation behavior

- Each navigation item maps to one deployed application or observability
  container; overflow stays on the same horizontal bar and scrolls.
- The backing Loki variable remains hidden and accepts only `WeKnora-*` and
  `kb-*` container values.
- Default is `WeKnora-app`.
- Browser validation switched to `kb-mineru-api`; the URL variable, panel title
  and Loki selector all updated to the selected container.

## Deployment impact

- Updated only the provisioned Grafana dashboard JSON and documentation.
- Grafana loaded the file automatically; no monitoring or business container
  was restarted.
- No database schema or application code changed.

## Validation

- Dashboard JSON parse and Git diff check: passed.
- PostgreSQL variable/count queries using `weknora_observer`: passed.
- PostgreSQL page-2 dead-letter query: 50 rows.
- Loki container label query: passed.
- Browser QA in the user's authenticated Chrome session:
  - Three table page selectors rendered at the dashboard top.
  - One horizontal per-container navigation bar rendered above the log panel
    with 19 clickable container items and Grafana's default theme colors.
  - Dead-letter page 2 updated the URL to `var-dead_page=2`.
  - `kb-mineru-api` selection updated the URL and log panel heading.
  - GPU, host-resource and operational panels remained populated.
  - Browser console: zero errors and warnings in the final validation pass.

## Rollback

Restore the previous repository version of
`deploy/mwe-observability/grafana/dashboards/knowledgebase-overview.json`.
Grafana's file provisioner reloads it without restarting a container.

## Residual risks

- Page selectors are global dashboard controls rather than panel-local arrows;
  this is the smallest reliable fixed-50 implementation in Grafana 13.
- Data-source failure resolution remains per latest data-source sync because
  current item errors do not persist `external_id` or `knowledge_id`.

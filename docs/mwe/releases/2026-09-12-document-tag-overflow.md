# Document-list tag overflow fix

- Upstream/branch: WeKnora v0.7.2 / `weknora-v0.7.2`.
- Repository: `mwe-support/weknora-dispatch-mwe` (application frontend only).
- Implementation commit: `52110432c7135243e010c627068c4ba8c2c58b6c`.
- Production image: `marvel/weknora-ui:v0.7.2-tag-overflow-20260912`.
- Image ID: `sha256:331fdb7ef4455b43f13793ef517ba9de8915cfb04c4b0b6da10fb931ce34cc9b`.
- Deployed at: 2026-09-12 19:39:54 Asia/Shanghai.

## Problem and change

Long document tags overflowed their grid column and overlapped the source column and other tags. The text rule targeted `.t-tag__text`, which the installed TDesign tag does not render. Its native ellipsis class is enabled by `maxWidth`; the tag group also lacked a bounded width, so its resize-based visible-tag estimate measured overflowing content rather than available space.

`DocumentListView.vue` now uses TDesign's native `max-width="100%"` and full-name title, bounds the tag group to its cell, allows the tag to shrink, and prevents the `+N` count from shrinking. The obsolete selector was removed. Existing overflow counting and tag editing are reused. No dependency, backend, source-sync or data change is included.

## Verification

The real Vue component and installed TDesign were mounted with synthetic data in a temporary local fixture at `http://127.0.0.1:5188/tag-qa`. In-app Browser validation covered 1280×720 and 1024×700 viewports, a narrower list, single long Chinese tags, multiple long tags, unbroken English tags, short tags and empty tags.

- Before: the tag cell was 154 px wide, while long/multiple tag groups extended to 186/346/349 px and crossed into the source column.
- After: all groups remained inside their cells; `+N` had no intersection with the visible tag. Long text used ellipsis and retained the complete title. Hovering the multi-tag group exposed the full list.
- Clicking a tag emitted tag editing without opening the document; read-only mode did not emit editing. No relevant console warning/error or framework overlay was observed.
- `VITE_IS_DOCKER=true VITE_FRONTEND_COMMIT=52110432 npm run build`: PASS. Dist integrity verifier passed for 955 retained/current assets (2 HTML, 791 JS, 75 CSS). Existing large-chunk size warnings remain; they are unrelated to this CSS fix.
- `git diff --check`: PASS.
- Production private HTTP and public HTTPS entry matched the built index SHA-256 `79065bd710f240a33bf6d68ae35a7f496f821b0a5066d2f0e8a1a727e3531208`; knowledge-page JS/CSS assets were read back and matched the bundle manifest.
- Only the frontend image changed. Frontend environment remained identical; the app container ID/start time remained unchanged. No queue pauses or source calls were made.

Local evidence: `results/tag-overlap-20260912/desktop.png`, `narrow-tooltip.png`, `build.log`. Host deployment receipt and restricted rollback backup: `/public/knowledgebase/results/tag-overlap-20260912/`. Evidence and temporary fixture files are outside the application repository; no business document contents or credentials are included.

## Rollback and limits

Restore only the frontend image in the existing Compose override to `marvel/weknora-ui:v0.7.2-question-zero-20260912` (image ID `sha256:8262030898febbcd2dae10f38b981884b66f8a0b1bcf8e7eb222d8dec88b35ea`) and recreate only `frontend` with `--no-deps`. Do not overwrite unrelated later override changes. Previously built static assets are retained for already-open pages.

Visual validation uses the production component with synthetic fixtures, not a signed-in business document page. Phone-sized table layout and other browsers were not tested; the existing fixed-width list layout is unchanged. The shared tag-count estimator remains an estimate, while actual tag text is now constrained by the available cell width.

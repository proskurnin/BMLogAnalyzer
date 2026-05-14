# Changelog

## 1.0.0 - 2026-05-14

### Added

- AI analysis scaffold for report-level hypotheses based on factual report context.
- Dedicated AI proxy settings via `BM_AI_HTTPS_PROXY` and `BM_AI_HTTP_PROXY`.
- Upload report rebuild action from the uploads table.
- Suspicious findings section in HTML reports.
- Archive retention policy settings in the admin area.

### Changed

- Empty report sections are hidden when there is no data to show.
- Analyzer version bumped to `1.0.0`.

## 0.34.0 - 2026-05-12

### Added

- Authentication for the web resource.
- Role-based access for users and administrators.
- Admin user management page at `/admin`.
- `/profile` page with profile updates and per-user uploaded file list.
- Owner metadata for uploaded files and generated reports.
- `Пользователь` column on `/uploads`.
- Production environment settings via `BM_*` variables.
- Upload limits for file count, per-file size, and total session size.
- Deployment documentation and production env example.

### Changed

- The web UI is now hidden behind authorization.
- Administrator navigation includes upload, upload history, and administration links.
- Report, upload, and run-history access is scoped by owner for non-admin users.
- Analyzer version bumped to `0.34.0`.

### Security

- Production startup requires explicit admin credentials.
- Production refuses the default `admin` password.
- Session cookies can be marked secure via `BM_COOKIE_SECURE`.
- Expired sessions are cleaned up at app startup and during session reads.

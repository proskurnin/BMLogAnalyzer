# Changelog

## 1.2.4 - 2026-05-15

### Changed

- `Версии ПО ридеров` in HTML reports now also uses reader firmware values found by log inventory outside PaymentStart response lines.
- Analyzer version bumped to `1.2.4`.

## 1.2.3 - 2026-05-15

### Changed

- HTML reports now show `Версии ПО ридеров` in `BM сведения`.
- Reader firmware parsing now recognizes `ReaderVersion` markers in PaymentStart response lines.
- Analyzer version bumped to `1.2.3`.

## 1.2.2 - 2026-05-15

### Changed

- Admin sections now preserve open or closed state for the current browser session and stay open after actions inside them.
- Admin users table now shows Moscow creation time and supports sorting by name, email, and creation date.
- Analyzer version bumped to `1.2.2`.

## 1.2.1 - 2026-05-15

### Changed

- HTML reports now label the generator version as `отчёт создан в версии сервиса X.Y.Z`.
- Uploads page now displays product name and service version on separate lines.
- Analyzer version bumped to `1.2.1`.

## 1.2.0 - 2026-05-15

### Added

- Admins can add custom validation checks based on code, message text, duration, and repeat timing conditions.
- HTML reports now include a collapsed `Проверки` section when validation checks match log evidence.
- Report manifests now expose validation check results.

### Changed

- Validation check catalog rows now show editable condition type and value.
- Analyzer version bumped to `1.2.0`.

## 1.1.1 - 2026-05-15

### Changed

- Admin page sections are now collapsed by default and ordered as users, storage policies, and validation checks.
- Analyzer version bumped to `1.1.1`.

## 1.1.0 - 2026-05-15

### Added

- Admin-managed validation check catalog stored in `BM_DATA_DIR/web_settings/check_cases.json`.
- Admin forms to edit validation check title, description, severity and enabled state.
- Admin action to reset validation checks back to built-in defaults.

### Changed

- Built-in validation checks now read the active catalog, so disabled checks no longer produce CSV or AI-context results.
- Analyzer version bumped to `1.1.0`.

## 1.0.6 - 2026-05-15

### Changed

- Suspicious analysis now detects bursts of identical non-success events in one source log.
- AI analytics panel now supports status refresh, repeat runs, and renders `what_to_check` and limitations.
- Admin page now shows the current built-in validation check catalog.
- Analyzer version bumped to `1.0.6`.

## 1.0.5 - 2026-05-14

### Changed

- HTML reports now hide the `Подозрительно` section when no suspicious log lines were found.
- Analyzer version bumped to `1.0.5`.

## 1.0.4 - 2026-05-14

### Changed

- Upload completion actions now render as a separate centered button row below the upload result message.
- Analyzer version bumped to `1.0.4`.

## 1.0.3 - 2026-05-14

### Changed

- AI analytics status now formats generated time as Moscow time: `ДД.ММ.ГГГГ (ЧЧ:ММ:СС) (Мск)`.
- Analyzer version bumped to `1.0.3`.

## 1.0.2 - 2026-05-14

### Changed

- Upload completion now shows actions to open the latest upload-session report or go to `/uploads`.
- Multi-file upload sessions now return a combined report URL covering all files uploaded in that session.
- Analyzer version bumped to `1.0.2`.

## 1.0.1 - 2026-05-14

### Changed

- Upload history now labels upload time as Moscow time and formats it as `ДД.ММ.ГГГГ (ЧЧ:ММ:СС)`.
- Analyzer version bumped to `1.0.1`.

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

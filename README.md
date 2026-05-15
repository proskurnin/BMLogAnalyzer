# BM Log Analyzer

CLI-first analyzer for BM PaymentStart response logs.

Current analyzer version: `1.2.4`.

Deployment notes are in [README_DEPLOY.md](README_DEPLOY.md). Release changes are tracked in [CHANGELOG.md](CHANGELOG.md).

## Run

```bash
python3 main.py --path ./_workdir/input
```

By default the CLI reads `config/config.yaml`:

```bash
python3 main.py --config ./config/config.yaml
```

Optional filters:

```bash
python3 main.py --path ./_workdir/input --date 2026-04-29 --reader TT --bm 4.4.12
```

Print analyzer version:

```bash
python3 main.py --version
```

## Versioning

BM Log Analyzer uses semantic versioning: `major.minor.patch`.

| Change type | Bump | Example |
| --- | --- | --- |
| Bug fix | `patch` | `4.7.0` -> `4.7.1` |
| New pipeline step or new backward-compatible feature | `minor` | `4.7.0` -> `4.8.0` |
| Breaking backward compatibility | `major` | `4.7.0` -> `5.0.0` |

Use the bump helper to update the analyzer version in code and README:

```bash
python3 tools/bump_version.py patch
python3 tools/bump_version.py minor
python3 tools/bump_version.py major
```

## Web

The project now has a web service layer in `web/` that reuses the same core analysis engine as the CLI.
It exposes a snapshot API for later UI work and a lightweight health/readiness surface.

The web adapter is currently optional and expects `fastapi` and `uvicorn` to be installed in the runtime environment.
Without those packages, the core service layer still works and is covered by tests.

Install the runtime dependencies:

```bash
python3 -m pip install -r requirements-web.txt
```

Run the web service after installing the optional dependencies:

```bash
python3 -m web --host 127.0.0.1 --port 8000
```

On shells where `python3` still resolves to the system interpreter, use:

```bash
.venv/bin/python -m web --host 127.0.0.1 --port 8000
```

The web page now uses a single upload flow: choose files or a folder, click `Загрузить`, watch the progress bar, then wait for the upload and report generation to finish.
Every upload session is stored in `_workdir/web_history`, and the HTML report is served from `/report/{run_id}`.
Each HTML report also writes a versioned JSON manifest next to it and exposes it at `/report/{run_id}/manifest`.
The latest saved report is available at `/api/runs/latest/report`.
The history list can be filtered by run mode (`analysis` or `summary`), searched by date/path/version, sorted by time, and each history entry exposes direct HTML and manifest URLs. Sessions can be deleted from the history view.
The HTML report also includes a collapsed validator analytics section grouped by validator number, BM version, and date range.

The root page `/` is upload-only. It does not expose analytics, reports, or history.
The admin upload registry is available at `/uploads`. It lists uploaded files, lets an admin download each file locally, shows the current report state, and can generate a report for multiple selected uploads.
Upload storage is kept in `_workdir/upload_store`.
The retention period for uploaded archives and extracted workspaces is configurable in the admin panel and defaults to 10 days. When it expires, the file data is removed from disk, but the registry records remain.
Role-based access control is planned separately: the product flow already separates upload and admin views, but authentication is not implemented yet.

Stable contracts exposed by the core/web layer:

* snapshot schema: `bm-log-analyzer.snapshot.v1`
* report manifest schema: `bm-log-analyzer.analysis-report.v1`
* stable report sections are declared in the manifest `sections` list
* validator analytics is exposed in the HTML report and manifest as `validator_analytics`

Archives are extracted into `_workdir/extracted` before analysis. The scanner reads `.log`, `.gz`, `.zip`, `.tar.gz`, `.tgz`, and `.rar` sources. RAR extraction requires `bsdtar` (`libarchive-tools` in the Docker image).

## Report Configuration

Reports can be enabled or disabled in `config/config.yaml` under the `reports` section.
Each key maps to one generated report. Disabled reports are not written during the run.

Example:

```yaml
reports:
  parsed_events: true
  analysis_report_html: true
  technical_error_events: false
  read_error_repeat_outcomes: true
  no_card_repeat_outcomes: true
  card_check_markers: true
  card_identity_markers: true
  matrix_reader_type_by_code: false
```

CLI path arguments override config paths when provided:

```bash
python3 main.py --config ./config/config.yaml --path ./_workdir/input --reports-dir ./_workdir/reports
```

Generated reports:

* `_workdir/reports/report_metadata.csv`
* `_workdir/reports/parsed_events.csv`
* `_workdir/reports/summary_by_code.csv`
* `_workdir/reports/summary_by_message.csv`
* `_workdir/reports/summary_by_bm_version.csv`
* `_workdir/reports/summary_by_reader_type.csv`
* `_workdir/reports/summary_by_reader_firmware.csv`
* `_workdir/reports/summary_by_classification.csv`
* `_workdir/reports/summary_by_duration_bucket.csv`
* `_workdir/reports/known_codes.csv`
* `_workdir/reports/unknown_codes.csv`
* `_workdir/reports/diagnostics.csv`
* `_workdir/reports/file_diagnostics.csv`
* `_workdir/reports/bundle_manifest.csv`
* `_workdir/reports/bundle_manifest.json`
* `_workdir/reports/archive_inventory.csv`
* `_workdir/reports/summary_by_archive_category.csv`
* `_workdir/reports/log_inventory.csv`
* `_workdir/reports/summary_by_log_type.csv`
* `_workdir/reports/bm_log_versions.csv`
* `_workdir/reports/bm_status_summary.csv`
* `_workdir/reports/reader_models.csv`
* `_workdir/reports/reader_firmware_versions.csv`
* `_workdir/reports/reader_firmware_timeline.csv`
* `_workdir/reports/summary_reader_firmware_timeline.csv`
* `_workdir/reports/reader_error_summary.csv`
* `_workdir/reports/system_error_summary.csv`
* `_workdir/reports/other_logs.csv`
* `_workdir/reports/error_events.csv`
* `_workdir/reports/technical_error_events.csv`
* `_workdir/reports/repeat_attempts_after_failure.csv`
* `_workdir/reports/summary_repeat_attempts_after_failure.csv`
* `_workdir/reports/read_error_repeat_outcomes.csv`
* `_workdir/reports/summary_read_error_repeat_outcomes.csv`
* `_workdir/reports/timeout_repeat_outcomes.csv`
* `_workdir/reports/summary_timeout_repeat_outcomes.csv`
* `_workdir/reports/no_card_repeat_outcomes.csv`
* `_workdir/reports/summary_no_card_repeat_outcomes.csv`
* `_workdir/reports/card_check_markers.csv`
* `_workdir/reports/summary_card_check_markers.csv`
* `_workdir/reports/oda_cda_repeat_outcomes.csv`
* `_workdir/reports/summary_oda_cda_repeat_outcomes.csv`
* `_workdir/reports/card_identity_markers.csv`
* `_workdir/reports/summary_card_identity_markers.csv`
* `_workdir/reports/card_fingerprint_events.csv`
* `_workdir/reports/read_error_card_history.csv`
* `_workdir/reports/summary_read_error_card_history.csv`
* `_workdir/reports/timeout_card_history.csv`
* `_workdir/reports/summary_timeout_card_history.csv`
* `_workdir/reports/no_card_card_history.csv`
* `_workdir/reports/summary_no_card_card_history.csv`
* `_workdir/reports/check_results.csv`
* `_workdir/reports/check_summary.csv`
* `_workdir/reports/analysis_report.html`
* `_workdir/reports/errors_by_file.csv`
* `_workdir/reports/file_error_overview.csv`
* `_workdir/reports/comparison_by_bm_version.csv`
* `_workdir/reports/comparison_by_reader_type.csv`
* `_workdir/reports/matrix_bm_version_by_code.csv`
* `_workdir/reports/matrix_reader_type_by_code.csv`
* `_workdir/reports/matrix_bm_version_by_classification.csv`
* `_workdir/reports/matrix_reader_type_by_classification.csv`

## Questions Answered By The Analysis

BM Log Analyzer answers factual questions based on parsed log lines and calculated statistics.
It does not automatically answer why an issue happened in BM code; such statements must be kept separate as hypotheses.
Each generated report set includes `report_metadata.csv` with the analyzer version used for the run.

### Analysis Scope

* How many log lines were scanned?
* How many `PaymentStart resp` events were found and parsed?
* How many `PaymentStart resp` lines could not be parsed?
* Which files were actually included in the analysis?
* Which archives were extracted?
* Which archives were skipped?
* Which source files have diagnostics or parsing problems?
* How many files are classified as BM logs, reader logs, system logs, and other logs?
* Which detection evidence was used for each log file?

### Archive Inventory

The archive inventory separates files by archive path before PaymentStart parsing:

* `BM rotate` - BM application rotate logs such as `logs/bm/bm-rotate-*.log.gz`.
* `BM stdout` - BM stdout/current logs such as `logs/bm-std/*`.
* `Stopper rotate` - stopper rotate logs such as `logs/stopper/stopper-rotate*.log.gz`.
* `Stopper stdout` - stopper stdout/current logs such as `logs/stopper-std/*`.
* `VIL logs` - VIL logs such as `vil.logs/*.log`.
* `Reader logs` - explicit reader log files, if present.
* `Reader firmware binary` - reader firmware files such as `reader-*.bin.P.signed`; these are not logs.
* `System logs` - explicit OS/system logs such as `syslog`, `journal`, `kernel.log`, or files under `var/log`.
* `Service config` - service unit files such as `bm.service` and `stopper.service`; these are not logs.
* `Other log-like` - log files that do not match known BM/stopper/VIL/reader/system categories.
* `Other` - databases, binaries, configs, keys, JSON files, libraries, and other non-log files.

The parsed-log inventory separately classifies files that were actually scanned by content:

* `bm` - BM logs detected by `mgt_nbs-*` packages, `PaymentStart` events, or BM path markers.
* `reader` - reader logs detected by reader model, reader firmware, or reader path markers.
* `system` - system logs detected by system path/content markers such as `systemd`, `kernel:`, `service`, `syslog`, or `journal`.
* `other` - files without known BM, reader, or system markers.

For the archive as a whole, reports answer:

* How many BM log files are in the archive?
* How many stopper log files are in the archive?
* Which BM versions are present and how many BM log files belong to each version?
* Which dates are present in BM logs?
* Are explicit reader log files present in the archive?
* Are reader firmware binaries present in the archive?
* Which reader models are present and how many files mention each model?
* Which reader firmware versions are present by reader model?
* Are explicit system log files present in the archive?
* Which files are other logs and what evidence is available for them?

### Operation Results

* How many total `PaymentStart resp` operations were found?
* How many operations were successful?
* What percentage of operations were successful?
* How many operations were classified as `decline`?
* What percentage of operations were classified as `decline`?
* How many operations were classified as `technical_error`?
* What percentage of operations were classified as `technical_error`?
* How many operations had `unknown` codes?
* What percentage of operations had `unknown` codes?

### BM Status Table

The BM status summary contains `status`, `count`, and `percent` columns.
Statuses are assigned only when the parsed event line contains a supported code or explicit text marker.
For `Code:0`, the analyzer also uses the explicit response template together with `PaymentType` and `AuthType` values when they are present in the log line.
Events that cannot be mapped factually are reported as `Не классифицировано`.

Current BM status rows:

* `Успешный онлайн (БЕЗ МИР)`
* `Успешный онлайн МИР`
* `Успешный оффлайн`
* `Проход зафейлен (онлайн - не получили конфирм и зарегали как фейл)`
* `Отказ, повторное предъявление`
* `Отказ, ошибка чтения карты`
* `Отказ, карта в стоп листе`
* `Отказ, коллизия`
* `Отказ, ошибка ODA/CDA`
* `Отказ, нет карты в поле`
* `Не классифицировано`

### Codes And Messages

* Which `Code` values appeared in logs?
* How many times did each `Code` appear?
* Which codes are known to the analyzer?
* Which codes are unknown to the analyzer?
* Which `Message` values appeared in logs?
* How many times did each `Message` appear?
* Which events belong to `success`, `decline`, `technical_error`, and `unknown` classifications?

### BM Versions And Readers

* Which BM versions appeared in logs?
* How many operations were found for each BM version?
* Which reader types appeared in logs?
* How many operations were found for each reader type?
* Which reader firmware versions appeared in logs?
* How many operations were found for each reader firmware version?
* Were explicit reader firmware values found in `PaymentStart resp` events?
* Did the explicit reader firmware value change over time within the same source file?
* Are there reader firmware binaries in the archive, and are they only binary evidence rather than runtime reader-log evidence?

### Comparisons

* What are success, decline, technical error, and unknown statistics for each BM version?
* What are success, decline, technical error, and unknown statistics for each reader type?
* Which codes appeared for each BM version?
* Which codes appeared for each reader type?
* How are classifications distributed by BM version?
* How are classifications distributed by reader type?

### Durations

* How many operations are in the `<300 ms` duration bucket?
* How many operations are in the `300-500 ms` duration bucket?
* How many operations are in the `500-1000 ms` duration bucket?
* How many operations are in the `1000-2000 ms` duration bucket?
* How many operations are in the `>2000 ms` duration bucket?
* How many operations have missing duration?
* What is the P90 duration?
* What is the P95 duration?

### Errors By File

* Which files contain non-success events?
* How many error events are in each file?
* Which error codes and messages appeared in each file?
* Which files should be checked first by number of errors?

### Repeat Attempts

* Were there repeat `PaymentStart resp` events after non-success events?
* Was a repeat found within 3 seconds after a failure?
* How many seconds passed before the repeat?
* Which code did the repeat attempt return?
* Which failure codes most often had repeat attempts?
* Which failure codes did not have repeat attempts?

### Read Error Investigation

For `Code:3` / `Ошибка чтения карты`, the analyzer answers:

* How many card read errors were found?
* Was a repeat tap found within 3 seconds?
* Did the repeat tap end with success?
* Did the repeat tap return the same read error again?
* Did the repeat tap return another decline, technical error, or unknown code?
* Does the failed event contain a technical card key from `HashPan`, `VirtualUid`, `VirtualAppCode`, or `Bin`?
* Was the same technical card key seen earlier in the analyzed logs?
* Was the same technical card key seen later in the analyzed logs?
* Did the same technical card key have a previous or later success?
* Which source file, line number, timestamp, and raw log lines confirm each outcome?

### Timeout Investigation

For `Code:16` / `Истек таймаут`, the analyzer answers:

* How many timeout events were found?
* Was a repeat tap found within 3 seconds?
* Did the repeat tap end with success?
* Did the repeat tap return the same timeout again?
* Does the timeout event contain a technical card key from `HashPan`, `VirtualUid`, `VirtualAppCode`, or `Bin`?
* Was the same technical card key seen earlier or later in the analyzed logs?
* Did the same technical card key have a previous or later success?
* Which source file, line number, timestamp, and raw log lines confirm each outcome?

### No Card Investigation

For `Code:17` / `Нет карты`, the analyzer answers:

* How many no-card events were found?
* Was a repeat tap found within 3 seconds?
* Did the repeat tap end with success?
* Did the repeat tap return `Code:17` again?
* Did the repeat tap return another decline, technical error, or unknown code?
* Does the no-card event contain a technical card key from `HashPan`, `VirtualUid`, `VirtualAppCode`, or `Bin`?
* Was the same technical card key seen earlier or later in the analyzed logs?
* Did the same technical card key have a previous or later success?
* Which source file, line number, timestamp, and raw log lines confirm each outcome?

### ODA/CDA And Basic Check Investigation

For explicit `ODA`, `CDA`, and `basic check` markers, the analyzer answers:

* Were explicit ODA/CDA/basic-check markers found in parsed `PaymentStart resp` events?
* Which event lines contain those markers?
* Which code, message, BM version, reader type, source file, and raw line confirm each marker?
* Was a repeat tap found within 3 seconds after an explicit ODA/CDA/basic-check failure marker?
* Did the repeat tap end with success, the same error, another decline, technical error, or unknown code?
* If no explicit markers are found, the summary states that fact directly instead of inferring a cause from unknown codes.

### Card Identity Investigation

For explicit card-type markers and technical card fields, the analyzer answers:

* Were explicit `MIFARE`, `Troika`, `Тройка`, `card type`, or transport-card markers found in parsed `PaymentStart resp` events?
* Which event lines contain those explicit markers?
* Which event lines contain technical fields such as `Bin`, `HashPan`, `VirtualCard`, `VirtualUid`, and `VirtualAppCode`?
* Which code, message, BM version, reader type, source file, and raw line confirm each marker?
* If only technical fields are present, the report states them as technical evidence only and does not infer the card type.
* If no explicit card-type markers are found, the summary states that fact directly.

### Built-In Checks

* Is there `Code:3` / `Ошибка чтения карты`?
* Is there `Code:16` / timeout?
* Is there `Code:255` / operation declined?
* Are there unknown result codes?
* Is there repeat card presentation after non-success within 3 seconds?
* In which file and line was each check result found?
* Which raw log line confirms each check result?

## Tests

```bash
.venv/bin/python -m pip install -r requirements-dev.txt
.venv/bin/python -m pytest
```

## Verification

For a single post-change check that prints tests, healthchecks, and readiness status:

```bash
.venv/bin/python tools/verify.py
```

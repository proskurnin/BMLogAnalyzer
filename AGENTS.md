# AGENTS.md - BM Log Analyzer

## Project Name
BM Log Analyzer

## Goal
Build a Python-based log analysis system for BM logs.

The final product must become a web service, but the first implementation should be a stable CLI/core engine similar in style and structure to the existing deTilda project.

The system must analyze BM logs factually, without assumptions, and produce reproducible reports.

## Critical Rule
Facts from logs and hypotheses from code analysis must always be separated.

Do not invent values.
Do not approximate exact numbers.
Do not make conclusions unless they are supported by parsed log lines or calculated statistics.

## Product Vision
BM Log Analyzer should:

1. Accept folders with BM logs and archives.
2. Unpack archives.
3. Scan `.log`, `.gz`, `.zip` sources.
4. Parse PaymentStart events.
5. Extract codes, messages, durations, BM versions, reader types and reader firmware versions.
6. Aggregate statistics.
7. Compare BM versions and reader types.
8. Export reports.
9. Later provide a Web UI.
10. Later connect to local Git repositories for read-only code analysis.
11. Later integrate with Codex only for analysis, not for code changes.

## Web Product Vision

The future web product must work as a log archive and analysis workspace.

The organization often requests logs from real BM devices. After authorization, one group of users should be able to upload logs and archives into the system for later analysis. For these users, the product is primarily a file storage/archive for operational log handoff.

Another group of users should be able to open the uploaded logs, download them when needed, and immediately view analytics generated from the same stored files. For these users, the product is both a log archive and an analytics tool.

Core web concepts:

* authenticated access
* uploaded log/archive storage
* separation between upload-only and analysis users
* list/search/filter uploaded log bundles
* download original uploaded files
* run analysis on selected uploaded files
* view factual analytics reports from stored logs
* keep uploaded raw logs and generated reports connected
* admin area for managing reusable validation/check cases

The web layer must still call the same core analysis engine that CLI uses.

## Web Admin Future

The web product should include an admin area where maintainers can add and manage different check cases for log validation and analysis.

This needs architectural design before implementation. Initial concepts:

* check case catalog
* case title, description, severity and expected evidence
* configurable matching rules based on parsed facts such as code, message, timing, BM version, reader type and source log
* checks for sequence/timing patterns, for example repeat tap within 0-3 seconds after a failed event
* enable/disable cases without code changes
* versioned case definitions where possible
* case results linked back to source file, line numbers and raw log evidence

Admin-managed cases must not replace factual reports. They should add a separate validation layer on top of parsed facts.

## Architecture Principle
The analysis engine must not depend on the web layer.

Correct dependency direction:

```text
core -> parsers -> analytics -> reports -> cli
web -> core
code_analysis -> core
```

The web layer should call the same core engine that CLI uses.

## Planned Stack

* Python 3.11+
* CLI first
* FastAPI later for backend
* HTML/Jinja or simple frontend first
* PostgreSQL later if persistent storage is needed
* CSV / Excel / HTML reports
* pytest for tests

## Initial CLI Workflow

1. Read input path from CLI or config.
2. Extract archives into `_workdir/extracted`.
3. Find log files.
4. Parse PaymentStart req/resp events.
5. Extract fields.
6. Normalize records.
7. Calculate statistics.
8. Generate reports into `_workdir/reports`.

Example command:

```bash
python main.py --path ./_workdir/input --date 2026-04-29 --reader TT --bm 4.4.12
```

## Known Log Concepts

BM logs contain transaction events.

Main event lines:

* `PaymentStart req`
* `PaymentStart resp`
* `PaymentStart, resp`

Typical response line example:

```text
2026-04-29 20:50:41.343 PaymentStart, resp: {Code:3 Message:Ошибка чтения карты} duration=412 ms p: mgt_nbs-oti-4.4.12
```

Fields to parse:

```text
timestamp      = 2026-04-29 20:50:41.343
event_type     = PaymentStart resp
code           = 3
message        = Ошибка чтения карты
duration_ms    = 412
package        = mgt_nbs-oti-4.4.12
bm_type        = oti
bm_version     = 4.4.12
reader_type    = OTI or TT
```

Reader type can be inferred from package:

* `oti` -> `OTI`
* `tt` -> `TT`

Known result codes:

* `Code:0` -> success
* `Code:1` -> decline / Следующий проход через 20 минут
* `Code:3` -> technical_error
* `Code:4` -> decline / Карта в стоп-листе
* `Code:6` -> decline / Приложите одну карту
* `Code:16` -> technical_error
* `Code:17` -> technical_error
* `Code:255` -> decline / Операция отклонена

Other codes should not be thrown away. They must be stored and reported as separate codes.

Unknown codes must be reported separately.

## Metrics

Minimum metrics:

* total operations
* success count and percent
* decline count and percent
* technical error count and percent
* unknown count and percent
* count by Code
* count by Message
* count by BM version
* count by reader type
* count by reader firmware
* duration buckets
* P90 duration
* P95 duration

Duration buckets:

* `<300 ms`
* `300-500 ms`
* `500-1000 ms`
* `1000-2000 ms`
* `>2000 ms`
* `missing duration`

Percentages should be rounded to 2 decimals.

## Reports

Initial reports:

* console summary
* CSV raw parsed events
* CSV aggregated statistics

Later reports:

* Excel report
* HTML report
* web dashboard
* Grafana metrics export

Reports must be factual and concise.

Use language like:

* `Факт из логов:`
* `Гипотеза:`
* `Что проверить:`

Never mix facts and assumptions.

Reports must make it easy to answer:

* which exact source log file was analyzed
* which error codes and messages were found in that file
* which line numbers contain those errors
* which events are declines, technical errors, or unknowns
* what raw log line supports each reported error
* whether another `PaymentStart resp` happened in the same source log in the same second or within 3 seconds after each non-success event

Archive handling rules:

* do not analyze the same uploaded archive both as `input.zip!...` and as extracted files
* if an archive is extracted, analyze the extracted log files as the factual source files
* direct `.log` files in the input folder may be analyzed directly
* `.tar.gz` / `.tgz` must not be treated as plain gzip text logs; add explicit tar extraction support before analyzing them
* generated manifests must list input files, extracted files, analyzed files, and skipped archives separately

## Code Analysis Future

The project may later inspect a local Git repository behind OpenVPN to find code related to log errors.

Code analysis must be read-only.

Codex integration future rules:

* Codex must not modify files.
* Codex must not create patches.
* Codex must not commit.
* Codex must not push.
* Codex must only answer with findings, file paths, functions, line numbers and hypotheses.

Preferred Codex runtime mode:

```bash
codex --sandbox read-only --ask-for-approval never
```

## Coding Style

* Keep modules small.
* Keep core logic pure where possible.
* Prefer dataclasses or pydantic-style models for parsed records.
* Functions should be testable.
* Do not mix parsing, analytics and reporting in one file.
* Avoid hardcoded absolute paths.
* All file operations must handle `.log`, `.gz`, `.zip`.
* Encoding problems must not crash the whole run.
* Bad lines should be counted and optionally saved to diagnostics.

## First Implementation Task

Create the initial project skeleton and implement MVP:

1. `main.py` CLI entry point
2. `config/config.yaml`
3. `core/log_scanner.py`
4. `parsers/payment_parser.py`
5. `parsers/version_parser.py`
6. `analytics/counters.py`
7. `reports/console_report.py`
8. `reports/csv_report.py`
9. tests for parser and counters

MVP should be able to scan a folder and produce:

* console summary
* `parsed_events.csv`
* `summary_by_code.csv`
* `summary_by_bm_version.csv`

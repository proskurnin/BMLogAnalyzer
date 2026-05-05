# BM Log Analyzer

CLI-first analyzer for BM PaymentStart response logs.

## Run

```bash
python3 main.py --path ./_workdir/input
```

Optional filters:

```bash
python3 main.py --path ./_workdir/input --date 2026-04-29 --reader TT --bm 4.4.12
```

Archives are extracted into `_workdir/extracted` before analysis. The scanner reads `.log`, `.gz`, and `.zip` sources.

Generated reports:

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
* `_workdir/reports/error_events.csv`
* `_workdir/reports/technical_error_events.csv`
* `_workdir/reports/errors_by_file.csv`
* `_workdir/reports/file_error_overview.csv`
* `_workdir/reports/comparison_by_bm_version.csv`
* `_workdir/reports/comparison_by_reader_type.csv`
* `_workdir/reports/matrix_bm_version_by_code.csv`
* `_workdir/reports/matrix_reader_type_by_code.csv`
* `_workdir/reports/matrix_bm_version_by_classification.csv`
* `_workdir/reports/matrix_reader_type_by_classification.csv`

## Tests

```bash
.venv/bin/python -m pip install -r requirements-dev.txt
.venv/bin/python -m pytest
```

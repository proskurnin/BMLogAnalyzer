# BM Log Analyzer

CLI-first analyzer for BM PaymentStart response logs.

## Run

```bash
python main.py --path ./_workdir/input
```

Optional filters:

```bash
python main.py --path ./_workdir/input --date 2026-04-29 --reader TT --bm 4.4.12
```

Generated reports:

* `_workdir/reports/parsed_events.csv`
* `_workdir/reports/summary_by_code.csv`
* `_workdir/reports/summary_by_bm_version.csv`

## Tests

```bash
python -m pytest
```

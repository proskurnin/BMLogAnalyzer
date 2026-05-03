from __future__ import annotations

import argparse
from pathlib import Path

from analytics.counters import analyze_events
from core.log_scanner import scan_logs
from parsers.payment_parser import parse_payment_start_response
from reports.console_report import render_console_summary
from reports.csv_report import write_csv_reports


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze BM PaymentStart response logs.")
    parser.add_argument("--path", default="./_workdir/input", help="Input file or folder with .log, .gz, .zip sources.")
    parser.add_argument("--date", help="Filter by event date in YYYY-MM-DD format.")
    parser.add_argument("--reader", choices=["OTI", "TT", "oti", "tt"], help="Filter by reader type.")
    parser.add_argument("--bm", help="Filter by BM version, for example 4.4.12.")
    parser.add_argument("--reports-dir", default="./_workdir/reports", help="Directory for CSV reports.")
    return parser


def matches_filters(event, date_filter: str | None, reader_filter: str | None, bm_filter: str | None) -> bool:
    if date_filter and (event.timestamp is None or event.timestamp.date().isoformat() != date_filter):
        return False
    if reader_filter and event.reader_type != reader_filter.upper():
        return False
    if bm_filter and event.bm_version != bm_filter:
        return False
    return True


def main() -> int:
    args = build_parser().parse_args()
    input_path = Path(args.path)
    reports_dir = Path(args.reports_dir)

    events = []
    scanned_lines = 0
    malformed_payment_lines = 0

    for log_line in scan_logs(input_path):
        scanned_lines += 1
        event = parse_payment_start_response(log_line.text, log_line.source_file, log_line.line_number)
        if event is None:
            if "PaymentStart" in log_line.text and "resp" in log_line.text:
                malformed_payment_lines += 1
            continue
        if matches_filters(event, args.date, args.reader, args.bm):
            events.append(event)

    result = analyze_events(events)
    write_csv_reports(events, result, reports_dir)
    print(render_console_summary(result, scanned_lines=scanned_lines, malformed_payment_lines=malformed_payment_lines))
    print(f"CSV reports: {reports_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

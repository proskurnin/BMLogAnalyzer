from __future__ import annotations

import argparse
from pathlib import Path
from time import perf_counter

from core.models import PipelineStepResult
from core.pipeline import run_analysis
from reports.console_report import render_console_summary
from reports.csv_report import write_csv_reports
from reports.pipeline_report import ConsolePipelineReporter


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze BM PaymentStart response logs.")
    parser.add_argument("--path", default="./_workdir/input", help="Input file or folder with .log, .gz, .zip sources.")
    parser.add_argument("--date", help="Filter by event date in YYYY-MM-DD format.")
    parser.add_argument("--reader", choices=["OTI", "TT", "oti", "tt"], help="Filter by reader type.")
    parser.add_argument("--bm", help="Filter by BM version, for example 4.4.12.")
    parser.add_argument("--reports-dir", default="./_workdir/reports", help="Directory for CSV reports.")
    parser.add_argument("--extracted-dir", default="./_workdir/extracted", help="Directory for extracted archives.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    input_path = Path(args.path)
    reports_dir = Path(args.reports_dir)
    extracted_dir = Path(args.extracted_dir)
    pipeline_reporter = ConsolePipelineReporter()

    events, result, stats = run_analysis(
        input_path,
        extracted_dir=extracted_dir,
        date_filter=args.date,
        reader_filter=args.reader,
        bm_filter=args.bm,
        progress_callback=pipeline_reporter.callback,
    )
    csv_started_at = perf_counter()
    try:
        pipeline_reporter.callback("start", "write_csv_reports")
        write_csv_reports(events, result, reports_dir, diagnostics=stats.diagnostics, file_stats=stats.files)
        csv_step = PipelineStepResult(
            name="write_csv_reports",
            status="ok",
            duration_ms=(perf_counter() - csv_started_at) * 1000,
            errors=0,
            details={
                "reports_dir": str(reports_dir),
                "files": 22,
            },
        )
        pipeline_reporter.callback("finish", csv_step)
        stats.steps.append(csv_step)
    except Exception as exc:
        csv_step = PipelineStepResult(
            name="write_csv_reports",
            status="failed",
            duration_ms=(perf_counter() - csv_started_at) * 1000,
            errors=1,
            details={"reports_dir": str(reports_dir), "error": str(exc)},
        )
        pipeline_reporter.callback("finish", csv_step)
        stats.steps.append(csv_step)
        raise
    print(render_console_summary(result, stats=stats))
    print(f"CSV reports: {reports_dir}")
    pipeline_reporter.print_total()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

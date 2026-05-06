from __future__ import annotations

import argparse
from pathlib import Path
from time import perf_counter

from core.config import load_app_config
from core.models import PipelineStepResult
from core.pipeline import run_analysis
from core.version import format_version
from reports.console_report import render_console_summary
from reports.csv_report import write_csv_reports
from reports.html_report import write_html_report
from reports.pipeline_report import ConsolePipelineReporter


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze BM PaymentStart response logs.")
    parser.add_argument("--version", action="version", version=format_version())
    parser.add_argument("--config", default="./config/config.yaml", help="Path to YAML config file.")
    parser.add_argument(
        "--path",
        default=None,
        help="Input file or folder with .log, .gz, .zip, .tar.gz, .tgz, .rar sources.",
    )
    parser.add_argument("--date", help="Filter by event date in YYYY-MM-DD format.")
    parser.add_argument("--reader", choices=["OTI", "TT", "oti", "tt"], help="Filter by reader type.")
    parser.add_argument("--bm", help="Filter by BM version, for example 4.4.12.")
    parser.add_argument("--reports-dir", default=None, help="Directory for CSV reports.")
    parser.add_argument("--extracted-dir", default=None, help="Directory for extracted archives.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    config = load_app_config(args.config)
    input_path = Path(args.path or config.input_path)
    reports_dir = Path(args.reports_dir or config.reports_dir)
    extracted_dir = Path(args.extracted_dir or config.extracted_dir)
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
        pipeline_reporter.callback("start", "write_reports")
        written_reports = write_csv_reports(
            events,
            result,
            reports_dir,
            diagnostics=stats.diagnostics,
            file_stats=stats.files,
            pipeline_stats=stats,
            report_config=config.report_config,
        )
        if config.report_config.enabled("analysis_report_html"):
            html_report_path = reports_dir / "analysis_report.html"
            write_html_report(events, result, html_report_path, stats=stats)
            written_reports.append(html_report_path)
        csv_step = PipelineStepResult(
            name="write_reports",
            status="ok",
            duration_ms=(perf_counter() - csv_started_at) * 1000,
            errors=0,
            details={
                "reports_dir": str(reports_dir),
                "files": len(written_reports),
            },
        )
        pipeline_reporter.callback("finish", csv_step)
        stats.steps.append(csv_step)
    except Exception as exc:
        csv_step = PipelineStepResult(
            name="write_reports",
            status="failed",
            duration_ms=(perf_counter() - csv_started_at) * 1000,
            errors=1,
            details={"reports_dir": str(reports_dir), "error": str(exc)},
        )
        pipeline_reporter.callback("finish", csv_step)
        stats.steps.append(csv_step)
        raise
    print(render_console_summary(result, stats=stats))
    print(f"Reports: {reports_dir}")
    pipeline_reporter.print_total()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

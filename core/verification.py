from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from core.config import load_app_config
from core.pipeline import run_analysis
from core.version import format_version
from main import build_parser
from reports.csv_report import write_csv_reports
from reports.html_report import write_html_report


CheckStatus = Literal["ok", "failed"]


@dataclass(frozen=True)
class CheckOutcome:
    name: str
    status: CheckStatus
    details: str = ""


def run_healthchecks(config_path: Path | str = "./config/config.yaml") -> list[CheckOutcome]:
    outcomes: list[CheckOutcome] = []

    try:
        config = load_app_config(config_path)
        outcomes.append(
            CheckOutcome(
                name="config_load",
                status="ok",
                details=f"input_path={config.input_path} reports_dir={config.reports_dir} enabled_reports={config.report_config.enabled_count()}",
            )
        )
    except Exception as exc:
        outcomes.append(CheckOutcome(name="config_load", status="failed", details=str(exc)))

    try:
        parser = build_parser()
        option_dests = {action.dest for action in parser._actions}
        required_options = {"version", "config", "path", "date", "reader", "bm", "reports_dir", "extracted_dir"}
        missing = sorted(required_options - option_dests)
        if missing:
            raise ValueError(f"missing parser options: {', '.join(missing)}")
        outcomes.append(CheckOutcome(name="cli_bootstrap", status="ok", details=format_version()))
    except Exception as exc:
        outcomes.append(CheckOutcome(name="cli_bootstrap", status="failed", details=str(exc)))

    try:
        version = format_version()
        outcomes.append(CheckOutcome(name="version_format", status="ok", details=version))
    except Exception as exc:
        outcomes.append(CheckOutcome(name="version_format", status="failed", details=str(exc)))

    return outcomes


def run_readiness_check(workdir: Path | None = None) -> list[CheckOutcome]:
    if workdir is None:
        with tempfile.TemporaryDirectory(prefix="bm-log-analyzer-readiness-") as tmp:
            return _run_readiness_check(Path(tmp))
    return _run_readiness_check(Path(workdir))


def _run_readiness_check(workdir: Path) -> list[CheckOutcome]:
    input_dir = workdir / "input"
    extracted_dir = workdir / "extracted"
    reports_dir = workdir / "reports"
    input_dir.mkdir(parents=True, exist_ok=True)
    extracted_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)

    sample_log = input_dir / "sample.log"
    sample_log.write_text(
        "\n".join(
            [
                "2026-04-29 20:50:41.343 PaymentStart, resp: {Code:0 Message:OK} duration=412 ms p: mgt_nbs-oti-4.4.12",
                "2026-04-29 20:50:42.000 PaymentStart, resp: {Code:3 Message:Ошибка чтения карты} duration=412 ms p: mgt_nbs-oti-4.4.12",
            ]
        ),
        encoding="utf-8",
    )

    events, result, stats = run_analysis(input_dir, extracted_dir=extracted_dir)
    written_reports = write_csv_reports(events, result, reports_dir, diagnostics=stats.diagnostics, file_stats=stats.files, pipeline_stats=stats)
    write_html_report(events, result, reports_dir / "analysis_report.html", stats=stats)
    written_reports.append(reports_dir / "analysis_report.html")

    outcomes = [
        CheckOutcome(name="analysis_smoke", status="ok", details=f"events={len(events)} total={result.total}"),
        CheckOutcome(name="report_generation", status="ok", details=f"reports={len(written_reports)} dir={reports_dir}"),
        CheckOutcome(name="pipeline_steps", status="ok", details=" -> ".join(step.name for step in stats.steps)),
    ]
    return outcomes

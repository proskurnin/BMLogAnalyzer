from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Any

from analytics.counters import analyze_events
from core.archive_extractor import extract_archives
from core.log_scanner import scan_logs
from core.models import AnalysisResult, DiagnosticLine, PaymentEvent, PipelineStats, PipelineStepResult
from parsers.payment_parser import is_payment_start_response_line, parse_payment_start_response

PipelineProgressCallback = Callable[[str, PipelineStepResult | str], None]


@dataclass
class _Stage:
    name: str
    callback: PipelineProgressCallback | None
    started_at: float = 0.0

    def __enter__(self) -> "_Stage":
        self.started_at = perf_counter()
        if self.callback:
            self.callback("start", self.name)
        return self

    def finish(self, *, errors: int = 0, details: dict[str, Any] | None = None) -> PipelineStepResult:
        result = PipelineStepResult(
            name=self.name,
            status="ok" if errors == 0 else "completed_with_errors",
            duration_ms=(perf_counter() - self.started_at) * 1000,
            errors=errors,
            details=details or {},
        )
        if self.callback:
            self.callback("finish", result)
        return result

    def __exit__(self, exc_type, exc, traceback) -> bool:
        if exc is not None:
            result = PipelineStepResult(
                name=self.name,
                status="failed",
                duration_ms=(perf_counter() - self.started_at) * 1000,
                errors=1,
                details={"error": str(exc)},
            )
            if self.callback:
                self.callback("finish", result)
        return False


def run_analysis(
    input_path: Path | str,
    *,
    extracted_dir: Path | str,
    date_filter: str | None = None,
    reader_filter: str | None = None,
    bm_filter: str | None = None,
    progress_callback: PipelineProgressCallback | None = None,
) -> tuple[list[PaymentEvent], AnalysisResult, PipelineStats]:
    steps: list[PipelineStepResult] = []

    with _Stage("extract_archives", progress_callback) as stage:
        extraction = extract_archives(input_path, extracted_dir)
        steps.append(
            stage.finish(
                errors=len(extraction.skipped_files),
                details={
                    "input_path": str(input_path),
                    "extracted_files": len(extraction.extracted_files),
                    "skipped_archives": len(extraction.skipped_files),
                    "extracted_dir": str(extracted_dir),
                },
            )
        )

    scan_roots = [Path(input_path)]
    if extraction.extracted_files:
        scan_roots.append(Path(extraction.extracted_dir))

    events: list[PaymentEvent] = []
    diagnostics: list[DiagnosticLine] = []
    scanned_lines = 0

    with _Stage("scan_and_parse_logs", progress_callback) as stage:
        for scan_root in scan_roots:
            for log_line in scan_logs(scan_root):
                scanned_lines += 1
                event = parse_payment_start_response(log_line.text, log_line.source_file, log_line.line_number)
                if event is None:
                    if is_payment_start_response_line(log_line.text):
                        diagnostics.append(
                            DiagnosticLine(
                                source_file=log_line.source_file,
                                line_number=log_line.line_number,
                                reason="payment_start_resp_parse_failed",
                                raw_line=log_line.text,
                            )
                        )
                    continue
                if matches_filters(event, date_filter, reader_filter, bm_filter):
                    events.append(event)

        steps.append(
            stage.finish(
                errors=len(diagnostics),
                details={
                    "scan_roots": len(scan_roots),
                    "scanned_lines": scanned_lines,
                    "parsed_events": len(events),
                    "malformed_payment_lines": len(diagnostics),
                    "date_filter": date_filter,
                    "reader_filter": reader_filter,
                    "bm_filter": bm_filter,
                },
            )
        )

    with _Stage("aggregate_statistics", progress_callback) as stage:
        result = analyze_events(events)
        steps.append(
            stage.finish(
                details={
                    "total_operations": result.total,
                    "codes": len(result.by_code),
                    "bm_versions": len(result.by_bm_version),
                    "reader_types": len(result.by_reader_type),
                },
            )
        )

    stats = PipelineStats(
        scanned_lines=scanned_lines,
        malformed_payment_lines=len(diagnostics),
        extracted_files=len(extraction.extracted_files),
        skipped_archives=len(extraction.skipped_files),
        diagnostics=diagnostics,
        steps=steps,
    )
    return events, result, stats


def matches_filters(
    event: PaymentEvent,
    date_filter: str | None,
    reader_filter: str | None,
    bm_filter: str | None,
) -> bool:
    if date_filter and (event.timestamp is None or event.timestamp.date().isoformat() != date_filter):
        return False
    if reader_filter and event.reader_type != reader_filter.upper():
        return False
    if bm_filter and event.bm_version != bm_filter:
        return False
    return True

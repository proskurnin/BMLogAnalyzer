from __future__ import annotations

from pathlib import Path

from analytics.counters import analyze_events
from core.archive_extractor import extract_archives
from core.log_scanner import scan_logs
from core.models import AnalysisResult, DiagnosticLine, PaymentEvent, PipelineStats
from parsers.payment_parser import is_payment_start_response_line, parse_payment_start_response


def run_analysis(
    input_path: Path | str,
    *,
    extracted_dir: Path | str,
    date_filter: str | None = None,
    reader_filter: str | None = None,
    bm_filter: str | None = None,
) -> tuple[list[PaymentEvent], AnalysisResult, PipelineStats]:
    extraction = extract_archives(input_path, extracted_dir)
    scan_roots = [Path(input_path)]
    if extraction.extracted_files:
        scan_roots.append(Path(extraction.extracted_dir))

    events: list[PaymentEvent] = []
    diagnostics: list[DiagnosticLine] = []
    scanned_lines = 0

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

    result = analyze_events(events)
    stats = PipelineStats(
        scanned_lines=scanned_lines,
        malformed_payment_lines=len(diagnostics),
        extracted_files=len(extraction.extracted_files),
        diagnostics=diagnostics,
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

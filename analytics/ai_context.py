from __future__ import annotations

from collections import Counter
from dataclasses import asdict
from datetime import datetime, timezone
import re

from analytics.bm_statuses import bm_status_summary_rows
from analytics.check_cases import run_builtin_checks
from analytics.protocol_scenarios import run_protocol_scenarios
from analytics.repeats import repeat_attempt_rows
from analytics.suspicious import suspicious_line_payloads
from core.models import AnalysisResult, PaymentEvent, PipelineStats
from core.version import __version__


MAX_CONTEXT_ROWS = 80


def build_ai_context(
    events: list[PaymentEvent],
    result: AnalysisResult,
    *,
    stats: PipelineStats | None = None,
) -> dict[str, object]:
    suspicious_rows = suspicious_line_payloads(events)[:MAX_CONTEXT_ROWS]
    repeat_rows = [row for row in repeat_attempt_rows(events) if row.get("repeat_found_within_3s")][:MAX_CONTEXT_ROWS]
    check_rows = [_json_ready(asdict(row)) for row in run_builtin_checks(events)[:MAX_CONTEXT_ROWS]]
    protocol_rows = [_json_ready(asdict(row)) for row in run_protocol_scenarios(events)[:MAX_CONTEXT_ROWS]]
    return {
        "schema_version": "bm-log-analyzer.ai-context.v1",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "version": __version__,
        "instruction": (
            "Контекст содержит только факты, извлечённые BM Log Analyzer из логов. "
            "AI должен ссылаться только на эти факты и evidence refs."
        ),
        "summary": {
            "events": result.total,
            "success": result.success_count,
            "decline": result.decline_count,
            "technical_error": result.technical_error_count,
            "unknown": result.unknown_count,
            "p90_ms": result.p90_ms,
            "p95_ms": result.p95_ms,
            "by_code": result.by_code,
            "by_message": result.by_message,
            "by_bm_version": result.by_bm_version,
            "by_reader_type": result.by_reader_type,
            "physical_carriers": _physical_carriers(stats),
            "physical_reader_types": _physical_reader_types(stats),
            "package_reader_types": _package_reader_types(events),
            "duration_buckets": result.duration_buckets,
        },
        "bm_statuses": bm_status_summary_rows(events),
        "suspicious_lines": suspicious_rows,
        "repeat_after_failure_3s": repeat_rows,
        "builtin_check_results": check_rows,
        "protocol_scenario_results": protocol_rows,
        "pipeline": {
            "input_files": stats.input_files if stats else [],
            "analyzed_files": stats.analyzed_files if stats else [],
            "malformed_payment_lines": stats.malformed_payment_lines if stats else 0,
        },
        "input_sources": [_input_source_context(item) for item in (stats.input_source_summaries if stats else [])],
        "limits": {
            "max_rows_per_section": MAX_CONTEXT_ROWS,
            "raw_log_lines_are_truncated_to_selected_evidence": True,
        },
    }


def _input_source_context(item) -> dict[str, object]:
    return {
        "source_file": item.source_file,
        "input_kind": item.input_kind,
        "log_types": item.log_types,
        "log_type_labels": item.log_type_labels,
        "log_type_counts": item.log_type_counts,
        "log_type_evidence": {log_type: values[:3] for log_type, values in item.log_type_evidence.items()},
        "archive_file_count": item.archive_file_count,
        "log_file_count": item.log_file_count,
        "other_file_count": item.other_file_count,
        "extracted_file_count": item.extracted_file_count,
        "analyzed_file_count": item.analyzed_file_count,
        "skipped_file_count": item.skipped_file_count,
        "skipped_reasons": item.skipped_reasons,
    }


def _physical_reader_types(stats: PipelineStats | None) -> dict[str, int]:
    if not stats:
        return {}
    return dict(Counter(report.reader_type for report in stats.device_boot_reports if report.reader_type))


def _physical_carriers(stats: PipelineStats | None) -> dict[str, int]:
    if not stats:
        return {}
    return dict(Counter(_device_boot_carrier(report) for report in stats.device_boot_reports if _device_boot_carrier(report)))


def _device_boot_carrier(report) -> str:
    text = " ".join([report.title, *report.source_files]).casefold()
    if "аскп" in text or "askp" in text:
        return "АСКП"
    return ""


def _package_reader_types(events: list[PaymentEvent]) -> dict[str, int]:
    return dict(Counter(reader for event in events if (reader := _event_package_reader_type(event))))


def _event_package_reader_type(event: PaymentEvent) -> str:
    platform = (event.platform or "").lower()
    if platform in {"oti", "tt"}:
        return platform.upper()
    match = re.search(r"\b[A-Za-z0-9_]+-(oti|tt)-\d", event.package or "", re.IGNORECASE)
    return match.group(1).upper() if match else ""


def _json_ready(value):
    if hasattr(value, "isoformat"):
        return value.isoformat(sep=" ")
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value

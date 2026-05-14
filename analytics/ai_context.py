from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone

from analytics.bm_statuses import bm_status_summary_rows
from analytics.check_cases import run_builtin_checks
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
            "duration_buckets": result.duration_buckets,
        },
        "bm_statuses": bm_status_summary_rows(events),
        "suspicious_lines": suspicious_rows,
        "repeat_after_failure_3s": repeat_rows,
        "builtin_check_results": check_rows,
        "pipeline": {
            "input_files": stats.input_files if stats else [],
            "analyzed_files": stats.analyzed_files if stats else [],
            "malformed_payment_lines": stats.malformed_payment_lines if stats else 0,
        },
        "limits": {
            "max_rows_per_section": MAX_CONTEXT_ROWS,
            "raw_log_lines_are_truncated_to_selected_evidence": True,
        },
    }


def _json_ready(value):
    if hasattr(value, "isoformat"):
        return value.isoformat(sep=" ")
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value

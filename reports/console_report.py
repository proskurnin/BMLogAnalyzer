from __future__ import annotations

from core.models import AnalysisResult


def render_console_summary(
    result: AnalysisResult,
    *,
    scanned_lines: int = 0,
    malformed_payment_lines: int = 0,
) -> str:
    lines = [
        "Факт из логов:",
        f"scanned lines: {scanned_lines}",
        f"parsed PaymentStart resp events: {result.total}",
        f"malformed PaymentStart resp lines: {malformed_payment_lines}",
        f"success: {result.success_count} ({result.success_percent:.2f}%)",
        f"decline: {result.decline_count} ({result.decline_percent:.2f}%)",
        f"technical_error: {result.technical_error_count} ({result.technical_error_percent:.2f}%)",
        f"unknown: {result.unknown_count} ({result.unknown_percent:.2f}%)",
        f"P90 duration ms: {_format_optional(result.p90_ms)}",
        f"P95 duration ms: {_format_optional(result.p95_ms)}",
        "",
        "By Code:",
    ]
    lines.extend(f"{code}: {count}" for code, count in result.by_code.items())
    lines.extend(["", "By BM version:"])
    lines.extend(f"{version}: {count}" for version, count in result.by_bm_version.items())
    return "\n".join(lines)


def _format_optional(value: float | None) -> str:
    if value is None:
        return "missing"
    if value.is_integer():
        return str(int(value))
    return f"{value:.2f}"

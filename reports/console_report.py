from __future__ import annotations

from analytics.archive_inventory import (
    archive_category_date_range,
    archive_category_totals,
    bm_log_count,
    explicit_reader_log_count,
    explicit_system_log_count,
    stopper_log_count,
)
from analytics.classifiers import CODE_CLASSIFICATIONS, CODE_DESCRIPTIONS, is_known_code
from core.models import AnalysisResult, PipelineStats
from core.version import format_version
from reports.pipeline_report import format_pipeline_step


def render_console_summary(
    result: AnalysisResult,
    *,
    stats: PipelineStats | None = None,
) -> str:
    scanned_lines = stats.scanned_lines if stats else 0
    malformed_payment_lines = stats.malformed_payment_lines if stats else 0
    extracted_files = stats.extracted_files if stats else 0
    skipped_archives = stats.skipped_archives if stats else 0
    lines = [
        "Факт из логов:",
        f"Analyzer version: {format_version()}",
        "",
        "=== Pipeline ===",
        f"• Extracted files                   : {extracted_files}",
        f"• Skipped archives                  : {skipped_archives}",
        f"• Scanned lines                     : {scanned_lines}",
        f"• Parsed PaymentStart resp events   : {result.total}",
        f"• Malformed PaymentStart resp lines : {malformed_payment_lines}",
        "",
        "=== Result ===",
        f"• success           : {result.success_count} ({result.success_percent:.2f}%)",
        f"• decline           : {result.decline_count} ({result.decline_percent:.2f}%)",
        f"• technical_error   : {result.technical_error_count} ({result.technical_error_percent:.2f}%)",
        f"• unknown           : {result.unknown_count} ({result.unknown_percent:.2f}%)",
        f"• P90 duration (ms) : {_format_optional(result.p90_ms)}",
        f"• P95 duration (ms) : {_format_optional(result.p95_ms)}",
    ]
    if stats and stats.steps:
        lines.extend(["", "=== Pipeline steps ==="])
        lines.extend(f"• {format_pipeline_step(step).removeprefix('[PIPELINE] ')}" for step in stats.steps)
    if stats:
        lines.extend(["", "=== Archive inventory ==="])
        lines.append(f"• Archives processed: {len(stats.input_files)}")
        lines.append(f"• BM logs           : {bm_log_count(stats.archive_inventory)}")
        lines.append(f"• Stopper logs      : {stopper_log_count(stats.archive_inventory)}")
        lines.append(f"• Reader logs       : {explicit_reader_log_count(stats.archive_inventory)}")
        lines.append(f"• System logs       : {explicit_system_log_count(stats.archive_inventory)}")
        lines.append(
            "• BM log dates      : "
            f"{archive_category_date_range(stats.archive_inventory, {'BM rotate', 'BM stdout'})}"
        )
        visible_categories = archive_category_totals(stats.archive_inventory)
        visible_categories.pop("Other", None)
        lines.extend(_render_mapping_section("Archive categories", visible_categories))
    lines.extend(_render_mapping_section("By Code", result.by_code, formatter=_format_code_label))
    lines.extend(_render_mapping_section("By BM version", result.by_bm_version))
    unknown_codes = {
        code: count
        for code, count in result.by_code.items()
        if not is_known_code(code)
    }
    lines.extend(_render_mapping_section("Unknown codes", unknown_codes))
    return "\n".join(lines)


def _format_optional(value: float | None) -> str:
    if value is None:
        return "missing"
    if value.is_integer():
        return str(int(value))
    return f"{value:.2f}"


def _render_mapping_section(title: str, values: dict[int | str, int], formatter=str) -> list[str]:
    lines = ["", f"=== {title} ==="]
    if not values:
        lines.append("• no data")
        return lines
    for key, count in sorted(values.items(), key=lambda item: str(item[0])):
        lines.append(f"• {formatter(key)}: {count}")
    return lines


def _format_code_label(code: int | str) -> str:
    try:
        normalized = int(code)
    except (TypeError, ValueError):
        return f"{code} (unknown)"
    classification = CODE_CLASSIFICATIONS.get(normalized, "unknown")
    description = CODE_DESCRIPTIONS.get(normalized)
    if description:
        return f"{normalized} ({classification}: {description})"
    return f"{normalized} ({classification})"

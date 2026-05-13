from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path

from analytics.archive_inventory import archive_category_totals
from analytics.bm_statuses import bm_status_summary_rows
from analytics.card_checks import card_check_marker_rows, card_check_marker_summary_rows
from analytics.card_history import (
    card_fingerprint_event_rows,
    no_card_card_history_rows,
    no_card_card_history_summary_rows,
    read_error_card_history_rows,
    read_error_card_history_summary_rows,
    timeout_card_history_rows,
    timeout_card_history_summary_rows,
)
from analytics.card_identity import card_identity_marker_rows, card_identity_marker_summary_rows
from analytics.classifiers import CODE_CLASSIFICATIONS, CODE_DESCRIPTIONS, classify_code, is_known_code
from analytics.check_cases import run_builtin_checks
from analytics.comparisons import (
    classification_matrix_rows,
    code_matrix_rows,
    comparison_rows,
    error_summary_rows,
    file_error_overview_rows,
)
from analytics.log_inventory import (
    bm_log_version_counts,
    error_status_counts_by_type,
    log_type_counts,
    other_log_descriptions,
    reader_firmware_counts,
    reader_model_counts,
)
from analytics.no_card import no_card_repeat_rows, no_card_repeat_summary_rows
from analytics.oda_cda import oda_cda_repeat_rows, oda_cda_repeat_summary_rows
from analytics.reader_firmware_timeline import (
    reader_firmware_timeline_rows,
    reader_firmware_timeline_summary_rows,
)
from analytics.read_errors import read_error_repeat_rows, read_error_repeat_summary_rows
from analytics.repeats import repeat_attempt_rows, repeat_attempt_summary_rows
from analytics.timeouts import timeout_repeat_rows, timeout_repeat_summary_rows
from core.models import (
    AnalysisResult,
    ArchiveInventoryRow,
    CheckResult,
    DiagnosticLine,
    LogFileInventory,
    PaymentEvent,
    PipelineStats,
)
from core.config import ReportConfig
from core.version import APP_NAME, __version__


def write_csv_reports(
    events: list[PaymentEvent],
    result: AnalysisResult,
    reports_dir: Path | str,
    diagnostics: list[DiagnosticLine] | None = None,
    file_stats=None,
    pipeline_stats: PipelineStats | None = None,
    report_config: ReportConfig | None = None,
) -> list[Path]:
    output_dir = Path(reports_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    config = report_config or ReportConfig()
    written: list[Path] = []
    _write_if_enabled(config, written, "report_metadata", output_dir / "report_metadata.csv", write_report_metadata)
    _write_if_enabled(config, written, "parsed_events", output_dir / "parsed_events.csv", write_parsed_events, events)
    _write_if_enabled(config, written, "summary_by_code", output_dir / "summary_by_code.csv", write_summary_report, result.by_code, "code")
    _write_if_enabled(config, written, "summary_by_message", output_dir / "summary_by_message.csv", write_summary_report, result.by_message, "message")
    _write_if_enabled(config, written, "summary_by_bm_version", output_dir / "summary_by_bm_version.csv", write_summary_report, result.by_bm_version, "bm_version")
    _write_if_enabled(config, written, "summary_by_reader_type", output_dir / "summary_by_reader_type.csv", write_summary_report, result.by_reader_type, "reader_type")
    _write_if_enabled(config, written, "summary_by_reader_firmware", output_dir / "summary_by_reader_firmware.csv", write_summary_report, result.by_reader_firmware, "reader_firmware")
    _write_if_enabled(config, written, "summary_by_classification", output_dir / "summary_by_classification.csv", write_summary_report, result.by_classification, "classification")
    _write_if_enabled(config, written, "summary_by_duration_bucket", output_dir / "summary_by_duration_bucket.csv", write_summary_report, result.duration_buckets, "duration_bucket")
    _write_if_enabled(config, written, "unknown_codes", output_dir / "unknown_codes.csv", write_unknown_codes, result.by_code)
    _write_if_enabled(config, written, "known_codes", output_dir / "known_codes.csv", write_known_codes)
    _write_if_enabled(config, written, "diagnostics", output_dir / "diagnostics.csv", write_diagnostics, diagnostics or [])
    _write_if_enabled(config, written, "file_diagnostics", output_dir / "file_diagnostics.csv", write_file_diagnostics, file_stats or [])
    _write_if_enabled(config, written, "bundle_manifest", output_dir / "bundle_manifest.csv", write_bundle_manifest, pipeline_stats)
    _write_if_enabled(config, written, "bundle_manifest_json", output_dir / "bundle_manifest.json", write_bundle_manifest_json, pipeline_stats)
    archive_inventory = pipeline_stats.archive_inventory if pipeline_stats else []
    _write_if_enabled(config, written, "archive_inventory", output_dir / "archive_inventory.csv", write_archive_inventory, archive_inventory)
    _write_if_enabled(config, written, "summary_by_archive_category", output_dir / "summary_by_archive_category.csv", write_summary_report, archive_category_totals(archive_inventory), "category")
    inventory = pipeline_stats.log_inventory if pipeline_stats else []
    _write_if_enabled(config, written, "log_inventory", output_dir / "log_inventory.csv", write_log_inventory, inventory)
    _write_if_enabled(config, written, "summary_by_log_type", output_dir / "summary_by_log_type.csv", write_summary_report, log_type_counts(inventory), "log_type")
    _write_if_enabled(config, written, "bm_log_versions", output_dir / "bm_log_versions.csv", write_summary_report, bm_log_version_counts(inventory), "bm_version")
    _write_if_enabled(config, written, "bm_status_summary", output_dir / "bm_status_summary.csv", write_bm_status_summary, events)
    _write_if_enabled(config, written, "reader_models", output_dir / "reader_models.csv", write_reader_models, inventory)
    _write_if_enabled(config, written, "reader_firmware_versions", output_dir / "reader_firmware_versions.csv", write_reader_firmwares, inventory)
    _write_reader_firmware_timeline_reports(config, written, events, inventory, archive_inventory, output_dir)
    _write_if_enabled(config, written, "reader_error_summary", output_dir / "reader_error_summary.csv", write_error_status_summary_report, inventory, "reader")
    _write_if_enabled(config, written, "system_error_summary", output_dir / "system_error_summary.csv", write_error_status_summary_report, inventory, "system")
    _write_if_enabled(config, written, "other_logs", output_dir / "other_logs.csv", write_other_logs, inventory)
    _write_if_enabled(config, written, "error_events", output_dir / "error_events.csv", write_error_events, events)
    _write_if_enabled(config, written, "technical_error_events", output_dir / "technical_error_events.csv", write_technical_error_events, events)
    _write_if_enabled(config, written, "errors_by_file", output_dir / "errors_by_file.csv", write_error_summary_by_file, events)
    _write_if_enabled(config, written, "file_error_overview", output_dir / "file_error_overview.csv", write_file_error_overview, events)
    _write_repeat_reports(config, written, events, output_dir)
    _write_read_error_reports(config, written, events, output_dir)
    _write_timeout_reports(config, written, events, output_dir)
    _write_no_card_reports(config, written, events, output_dir)
    _write_card_check_reports(config, written, events, output_dir)
    _write_oda_cda_reports(config, written, events, output_dir)
    _write_card_identity_reports(config, written, events, output_dir)
    _write_card_history_reports(config, written, events, output_dir)
    _write_check_reports(config, written, events, output_dir)
    _write_if_enabled(config, written, "comparison_by_bm_version", output_dir / "comparison_by_bm_version.csv", write_comparison_report_configured, events, "bm_version")
    _write_if_enabled(config, written, "comparison_by_reader_type", output_dir / "comparison_by_reader_type.csv", write_comparison_report_configured, events, "reader_type")
    _write_if_enabled(config, written, "matrix_bm_version_by_code", output_dir / "matrix_bm_version_by_code.csv", write_code_matrix_report_configured, events, "bm_version")
    _write_if_enabled(config, written, "matrix_reader_type_by_code", output_dir / "matrix_reader_type_by_code.csv", write_code_matrix_report_configured, events, "reader_type")
    _write_if_enabled(config, written, "matrix_bm_version_by_classification", output_dir / "matrix_bm_version_by_classification.csv", write_classification_matrix_report_configured, events, "bm_version")
    _write_if_enabled(config, written, "matrix_reader_type_by_classification", output_dir / "matrix_reader_type_by_classification.csv", write_classification_matrix_report_configured, events, "reader_type")
    return written


def write_parsed_events(events: list[PaymentEvent], path: Path) -> None:
    fieldnames = [
        "source_file",
        "line_number",
        "timestamp",
        "event_type",
        "code",
        "message",
        "duration_ms",
        "package",
        "carrier",
        "platform",
        "bm_type",
        "bm_version",
        "reader_type",
        "reader_firmware",
        "payment_type",
        "auth_type",
        "raw_line",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for event in events:
            writer.writerow(
                {
                    "source_file": event.source_file,
                    "line_number": event.line_number,
                    "timestamp": event.timestamp.isoformat(sep=" ") if event.timestamp else "",
                    "event_type": event.event_type,
                    "code": event.code if event.code is not None else "",
                    "message": event.message or "",
                    "duration_ms": _format_number(event.duration_ms),
                    "package": event.package or "",
                    "carrier": event.carrier or "",
                    "platform": event.platform or "",
                    "bm_type": event.bm_type or "",
                    "bm_version": event.bm_version or "",
                    "reader_type": event.reader_type or "",
                    "reader_firmware": event.reader_firmware or "",
                    "payment_type": event.payment_type if event.payment_type is not None else "",
                    "auth_type": event.auth_type if event.auth_type is not None else "",
                    "raw_line": event.raw_line,
                }
            )


def write_report_metadata(path: Path) -> None:
    rows = [
        {"key": "app_name", "value": APP_NAME},
        {"key": "app_version", "value": __version__},
        {"key": "generated_at", "value": datetime.now().astimezone().isoformat(timespec="seconds")},
    ]
    _write_dict_rows(path, ["key", "value"], rows)


def write_summary(values: dict[object, int], path: Path, key_name: str) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=[key_name, "count"])
        writer.writeheader()
        for key, count in values.items():
            writer.writerow({key_name: key, "count": count})


def write_summary_report(values: dict[object, int], key_name: str, path: Path) -> None:
    write_summary(values, path, key_name)


def write_unknown_codes(values: dict[object, int], path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["code", "count"])
        writer.writeheader()
        for key, count in values.items():
            if not is_known_code(key):
                writer.writerow({"code": key, "count": count})


def write_known_codes(path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["code", "classification", "description"])
        writer.writeheader()
        for code, classification in sorted(CODE_CLASSIFICATIONS.items()):
            writer.writerow(
                {
                    "code": code,
                    "classification": classification,
                    "description": CODE_DESCRIPTIONS.get(code, ""),
                }
            )


def write_diagnostics(diagnostics: list[DiagnosticLine], path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["source_file", "line_number", "reason", "raw_line"])
        writer.writeheader()
        for item in diagnostics:
            writer.writerow(
                {
                    "source_file": item.source_file,
                    "line_number": item.line_number,
                    "reason": item.reason,
                    "raw_line": item.raw_line,
                }
            )


def write_file_diagnostics(file_stats, path: Path) -> None:
    fieldnames = [
        "source_file",
        "scanned_lines",
        "payment_resp_lines",
        "parsed_payment_resp_lines",
        "selected_payment_resp_events",
        "malformed_payment_resp_lines",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for item in file_stats:
            writer.writerow({field: getattr(item, field) for field in fieldnames})


def write_log_inventory(inventory: list[LogFileInventory], path: Path) -> None:
    fieldnames = [
        "source_file",
        "log_type",
        "detection_method",
        "evidence",
        "dates",
        "bm_versions",
        "reader_models",
        "reader_firmware_versions",
        "error_lines",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for item in inventory:
            writer.writerow(
                {
                    "source_file": item.source_file,
                    "log_type": item.log_type,
                    "detection_method": item.detection_method,
                    "evidence": item.evidence,
                    "dates": ";".join(item.dates),
                    "bm_versions": ";".join(item.bm_versions),
                    "reader_models": ";".join(item.reader_models),
                    "reader_firmware_versions": ";".join(item.reader_firmware_versions),
                    "error_lines": sum(item.error_status_counts.values()),
                }
            )


def write_archive_inventory(rows: list[ArchiveInventoryRow], path: Path) -> None:
    fieldnames = ["archive", "category", "count", "date_from", "date_to", "examples"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "archive": row.archive,
                    "category": row.category,
                    "count": row.count,
                    "date_from": row.date_from or "",
                    "date_to": row.date_to or "",
                    "examples": " | ".join(row.examples),
                }
            )


def write_bm_status_summary(events: list[PaymentEvent], path: Path) -> None:
    _write_dict_rows(path, ["status", "count", "percent"], bm_status_summary_rows(events))


def write_reader_models(inventory: list[LogFileInventory], path: Path) -> None:
    _write_dict_rows(
        path,
        ["model", "count"],
        [{"model": model, "count": count} for model, count in reader_model_counts(inventory).items()],
    )


def write_reader_firmwares(inventory: list[LogFileInventory], path: Path) -> None:
    _write_dict_rows(
        path,
        ["model", "firmware_version", "count"],
        [
            {"model": model, "firmware_version": firmware, "count": count}
            for (model, firmware), count in reader_firmware_counts(inventory).items()
        ],
    )


def _write_reader_firmware_timeline_reports(
    config: ReportConfig,
    written: list[Path],
    events: list[PaymentEvent],
    inventory: list[LogFileInventory],
    archive_inventory: list[ArchiveInventoryRow],
    output_dir: Path,
) -> None:
    rows = reader_firmware_timeline_rows(events)
    _write_if_enabled(
        config,
        written,
        "reader_firmware_timeline",
        output_dir / "reader_firmware_timeline.csv",
        write_dict_rows_report,
        [
            "source_file",
            "line_number",
            "timestamp",
            "reader_type",
            "reader_firmware",
            "bm_version",
            "code",
            "message",
            "raw_line",
        ],
        rows,
    )
    _write_if_enabled(
        config,
        written,
        "summary_reader_firmware_timeline",
        output_dir / "summary_reader_firmware_timeline.csv",
        write_dict_rows_report,
        ["metric", "value", "message"],
        reader_firmware_timeline_summary_rows(events, inventory, archive_inventory),
    )


def write_error_status_summary(inventory: list[LogFileInventory], path: Path, log_type: str) -> None:
    counts = error_status_counts_by_type(inventory, log_type)
    total = sum(counts.values())
    rows = [
        {"status": status, "count": count, "percent": _percent(count, total)}
        for status, count in counts.items()
    ]
    _write_dict_rows(path, ["status", "count", "percent"], rows)


def write_error_status_summary_report(inventory: list[LogFileInventory], log_type: str, path: Path) -> None:
    write_error_status_summary(inventory, path, log_type)


def write_other_logs(inventory: list[LogFileInventory], path: Path) -> None:
    _write_dict_rows(path, ["source_file", "description", "evidence"], other_log_descriptions(inventory))


def write_bundle_manifest(stats: PipelineStats | None, path: Path) -> None:
    fieldnames = ["kind", "path"]
    rows = _manifest_rows(stats)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_bundle_manifest_json(stats: PipelineStats | None, path: Path) -> None:
    data = {
        "input_files": stats.input_files if stats else [],
        "extracted_files": stats.extracted_file_paths if stats else [],
        "analyzed_files": stats.analyzed_files if stats else [],
        "skipped_archives": stats.skipped_archive_paths if stats else [],
    }
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _manifest_rows(stats: PipelineStats | None) -> list[dict[str, str]]:
    if stats is None:
        return []
    rows: list[dict[str, str]] = []
    for kind, paths in (
        ("input", stats.input_files),
        ("extracted", stats.extracted_file_paths),
        ("analyzed", stats.analyzed_files),
        ("skipped_archive", stats.skipped_archive_paths),
    ):
        rows.extend({"kind": kind, "path": path} for path in paths)
    return rows


def write_error_events(events: list[PaymentEvent], path: Path) -> None:
    write_filtered_events(
        [event for event in events if classify_code(event.code) != "success"],
        path,
        include_classification=True,
    )


def write_technical_error_events(events: list[PaymentEvent], path: Path) -> None:
    write_filtered_events(
        [event for event in events if classify_code(event.code) == "technical_error"],
        path,
        include_classification=True,
    )


def write_filtered_events(events: list[PaymentEvent], path: Path, *, include_classification: bool = False) -> None:
    fieldnames = [
        "source_file",
        "line_number",
        "timestamp",
        "classification",
        "code",
        "message",
        "duration_ms",
        "bm_version",
        "reader_type",
        "reader_firmware",
        "payment_type",
        "auth_type",
        "raw_line",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for event in events:
            writer.writerow(
                {
                    "source_file": event.source_file,
                    "line_number": event.line_number,
                    "timestamp": event.timestamp.isoformat(sep=" ") if event.timestamp else "",
                    "classification": classify_code(event.code) if include_classification else "",
                    "code": event.code if event.code is not None else "",
                    "message": event.message or "",
                    "duration_ms": _format_number(event.duration_ms),
                    "bm_version": event.bm_version or "",
                    "reader_type": event.reader_type or "",
                    "reader_firmware": event.reader_firmware or "",
                    "payment_type": event.payment_type if event.payment_type is not None else "",
                    "auth_type": event.auth_type if event.auth_type is not None else "",
                    "raw_line": event.raw_line,
                }
            )


def write_error_summary_by_file(events: list[PaymentEvent], path: Path) -> None:
    fieldnames = ["source_file", "classification", "code", "message", "count"]
    _write_dict_rows(path, fieldnames, error_summary_rows(events))


def write_file_error_overview(events: list[PaymentEvent], path: Path) -> None:
    fieldnames = ["source_file", "total_events", "error_events", "success", "decline", "technical_error", "unknown"]
    _write_dict_rows(path, fieldnames, file_error_overview_rows(events))


def _write_repeat_reports(
    config: ReportConfig,
    written: list[Path],
    events: list[PaymentEvent],
    output_dir: Path,
) -> None:
    rows = repeat_attempt_rows(events)
    fieldnames = [
        "source_file",
        "failure_line_number",
        "failure_timestamp",
        "failure_classification",
        "failure_code",
        "failure_message",
        "repeat_found_within_3s",
        "repeat_delay_seconds",
        "repeat_line_number",
        "repeat_timestamp",
        "repeat_code",
        "repeat_message",
        "repeat_classification",
        "failure_raw_line",
        "repeat_raw_line",
    ]
    _write_if_enabled(
        config,
        written,
        "repeat_attempts_after_failure",
        output_dir / "repeat_attempts_after_failure.csv",
        write_dict_rows_report,
        fieldnames,
        rows,
    )
    _write_if_enabled(
        config,
        written,
        "summary_repeat_attempts_after_failure",
        output_dir / "summary_repeat_attempts_after_failure.csv",
        write_dict_rows_report,
        ["metric", "value", "message"],
        repeat_attempt_summary_rows(rows),
    )


def write_repeat_attempt_reports(events: list[PaymentEvent], output_dir: Path) -> None:
    rows = repeat_attempt_rows(events)
    fieldnames = [
        "source_file",
        "failure_line_number",
        "failure_timestamp",
        "failure_classification",
        "failure_code",
        "failure_message",
        "repeat_found_within_3s",
        "repeat_delay_seconds",
        "repeat_line_number",
        "repeat_timestamp",
        "repeat_code",
        "repeat_message",
        "repeat_classification",
        "failure_raw_line",
        "repeat_raw_line",
    ]
    _write_dict_rows(output_dir / "repeat_attempts_after_failure.csv", fieldnames, rows)
    _write_dict_rows(
        output_dir / "summary_repeat_attempts_after_failure.csv",
        ["metric", "value", "message"],
        repeat_attempt_summary_rows(rows),
    )


def _write_read_error_reports(
    config: ReportConfig,
    written: list[Path],
    events: list[PaymentEvent],
    output_dir: Path,
) -> None:
    rows = read_error_repeat_rows(events)
    fieldnames = [
        "source_file",
        "failure_line_number",
        "failure_timestamp",
        "failure_code",
        "failure_message",
        "repeat_found_within_3s",
        "repeat_outcome",
        "repeat_delay_seconds",
        "repeat_line_number",
        "repeat_timestamp",
        "repeat_code",
        "repeat_message",
        "repeat_classification",
        "failure_raw_line",
        "repeat_raw_line",
    ]
    _write_if_enabled(
        config,
        written,
        "read_error_repeat_outcomes",
        output_dir / "read_error_repeat_outcomes.csv",
        write_dict_rows_report,
        fieldnames,
        rows,
    )
    _write_if_enabled(
        config,
        written,
        "summary_read_error_repeat_outcomes",
        output_dir / "summary_read_error_repeat_outcomes.csv",
        write_dict_rows_report,
        ["metric", "value", "percent", "message"],
        read_error_repeat_summary_rows(rows),
    )


def _write_timeout_reports(
    config: ReportConfig,
    written: list[Path],
    events: list[PaymentEvent],
    output_dir: Path,
) -> None:
    rows = timeout_repeat_rows(events)
    fieldnames = [
        "source_file",
        "failure_line_number",
        "failure_timestamp",
        "failure_code",
        "failure_message",
        "repeat_found_within_3s",
        "repeat_outcome",
        "repeat_delay_seconds",
        "repeat_line_number",
        "repeat_timestamp",
        "repeat_code",
        "repeat_message",
        "repeat_classification",
        "failure_raw_line",
        "repeat_raw_line",
    ]
    _write_if_enabled(
        config,
        written,
        "timeout_repeat_outcomes",
        output_dir / "timeout_repeat_outcomes.csv",
        write_dict_rows_report,
        fieldnames,
        rows,
    )
    _write_if_enabled(
        config,
        written,
        "summary_timeout_repeat_outcomes",
        output_dir / "summary_timeout_repeat_outcomes.csv",
        write_dict_rows_report,
        ["metric", "value", "percent", "message"],
        timeout_repeat_summary_rows(rows),
    )


def _write_no_card_reports(
    config: ReportConfig,
    written: list[Path],
    events: list[PaymentEvent],
    output_dir: Path,
) -> None:
    rows = no_card_repeat_rows(events)
    fieldnames = [
        "source_file",
        "failure_line_number",
        "failure_timestamp",
        "failure_code",
        "failure_message",
        "repeat_found_within_3s",
        "repeat_outcome",
        "repeat_delay_seconds",
        "repeat_line_number",
        "repeat_timestamp",
        "repeat_code",
        "repeat_message",
        "repeat_classification",
        "failure_raw_line",
        "repeat_raw_line",
    ]
    _write_if_enabled(
        config,
        written,
        "no_card_repeat_outcomes",
        output_dir / "no_card_repeat_outcomes.csv",
        write_dict_rows_report,
        fieldnames,
        rows,
    )
    _write_if_enabled(
        config,
        written,
        "summary_no_card_repeat_outcomes",
        output_dir / "summary_no_card_repeat_outcomes.csv",
        write_dict_rows_report,
        ["metric", "value", "percent", "message"],
        no_card_repeat_summary_rows(rows),
    )


def _write_card_check_reports(
    config: ReportConfig,
    written: list[Path],
    events: list[PaymentEvent],
    output_dir: Path,
) -> None:
    rows = card_check_marker_rows(events)
    _write_if_enabled(
        config,
        written,
        "card_check_markers",
        output_dir / "card_check_markers.csv",
        write_dict_rows_report,
        [
            "source_file",
            "line_number",
            "timestamp",
            "markers",
            "code",
            "message",
            "bm_version",
            "reader_type",
            "raw_line",
        ],
        rows,
    )
    _write_if_enabled(
        config,
        written,
        "summary_card_check_markers",
        output_dir / "summary_card_check_markers.csv",
        write_dict_rows_report,
        ["metric", "value", "message"],
        card_check_marker_summary_rows(rows),
    )


def _write_oda_cda_reports(
    config: ReportConfig,
    written: list[Path],
    events: list[PaymentEvent],
    output_dir: Path,
) -> None:
    rows = oda_cda_repeat_rows(events)
    fieldnames = [
        "source_file",
        "failure_line_number",
        "failure_timestamp",
        "markers",
        "failure_code",
        "failure_message",
        "repeat_found_within_3s",
        "repeat_outcome",
        "repeat_delay_seconds",
        "repeat_line_number",
        "repeat_timestamp",
        "repeat_code",
        "repeat_message",
        "repeat_classification",
        "failure_raw_line",
        "repeat_raw_line",
    ]
    _write_if_enabled(
        config,
        written,
        "oda_cda_repeat_outcomes",
        output_dir / "oda_cda_repeat_outcomes.csv",
        write_dict_rows_report,
        fieldnames,
        rows,
    )
    _write_if_enabled(
        config,
        written,
        "summary_oda_cda_repeat_outcomes",
        output_dir / "summary_oda_cda_repeat_outcomes.csv",
        write_dict_rows_report,
        ["metric", "value", "percent", "message"],
        oda_cda_repeat_summary_rows(rows),
    )


def _write_card_identity_reports(
    config: ReportConfig,
    written: list[Path],
    events: list[PaymentEvent],
    output_dir: Path,
) -> None:
    rows = card_identity_marker_rows(events)
    _write_if_enabled(
        config,
        written,
        "card_identity_markers",
        output_dir / "card_identity_markers.csv",
        write_dict_rows_report,
        [
            "source_file",
            "line_number",
            "timestamp",
            "code",
            "message",
            "bm_version",
            "reader_type",
            "explicit_card_type_markers",
            "technical_markers",
            "bin",
            "hashpan_present",
            "virtual_card_present",
            "virtual_uid_present",
            "virtual_app_code",
            "raw_line",
        ],
        rows,
    )
    _write_if_enabled(
        config,
        written,
        "summary_card_identity_markers",
        output_dir / "summary_card_identity_markers.csv",
        write_dict_rows_report,
        ["metric", "value", "message"],
        card_identity_marker_summary_rows(rows, len(events)),
    )


def _write_card_history_reports(
    config: ReportConfig,
    written: list[Path],
    events: list[PaymentEvent],
    output_dir: Path,
) -> None:
    _write_if_enabled(
        config,
        written,
        "card_fingerprint_events",
        output_dir / "card_fingerprint_events.csv",
        write_dict_rows_report,
        [
            "card_key",
            "key_source",
            "source_file",
            "line_number",
            "timestamp",
            "classification",
            "code",
            "message",
            "bm_version",
            "reader_type",
            "bin",
            "hashpan_present",
            "virtual_uid_present",
            "virtual_app_code",
            "raw_line",
        ],
        card_fingerprint_event_rows(events),
    )
    rows = read_error_card_history_rows(events)
    _write_if_enabled(
        config,
        written,
        "read_error_card_history",
        output_dir / "read_error_card_history.csv",
        write_dict_rows_report,
        [
            "source_file",
            "line_number",
            "timestamp",
            "card_key",
            "key_source",
            "same_card_events_total",
            "same_card_previous_events",
            "same_card_previous_success",
            "same_card_later_events",
            "same_card_later_success",
            "repeat_found_within_3s",
            "repeat_outcome",
            "repeat_code",
            "repeat_message",
            "bin",
            "hashpan_present",
            "virtual_uid_present",
            "virtual_app_code",
            "raw_line",
        ],
        rows,
    )
    _write_if_enabled(
        config,
        written,
        "summary_read_error_card_history",
        output_dir / "summary_read_error_card_history.csv",
        write_dict_rows_report,
        ["metric", "value", "percent", "message"],
        read_error_card_history_summary_rows(rows),
    )
    timeout_rows = timeout_card_history_rows(events)
    _write_if_enabled(
        config,
        written,
        "timeout_card_history",
        output_dir / "timeout_card_history.csv",
        write_dict_rows_report,
        [
            "source_file",
            "line_number",
            "timestamp",
            "card_key",
            "key_source",
            "same_card_events_total",
            "same_card_previous_events",
            "same_card_previous_success",
            "same_card_later_events",
            "same_card_later_success",
            "repeat_found_within_3s",
            "repeat_outcome",
            "repeat_code",
            "repeat_message",
            "bin",
            "hashpan_present",
            "virtual_uid_present",
            "virtual_app_code",
            "raw_line",
        ],
        timeout_rows,
    )
    _write_if_enabled(
        config,
        written,
        "summary_timeout_card_history",
        output_dir / "summary_timeout_card_history.csv",
        write_dict_rows_report,
        ["metric", "value", "percent", "message"],
        timeout_card_history_summary_rows(timeout_rows),
    )
    no_card_rows = no_card_card_history_rows(events)
    _write_if_enabled(
        config,
        written,
        "no_card_card_history",
        output_dir / "no_card_card_history.csv",
        write_dict_rows_report,
        [
            "source_file",
            "line_number",
            "timestamp",
            "card_key",
            "key_source",
            "same_card_events_total",
            "same_card_previous_events",
            "same_card_previous_success",
            "same_card_later_events",
            "same_card_later_success",
            "repeat_found_within_3s",
            "repeat_outcome",
            "repeat_code",
            "repeat_message",
            "bin",
            "hashpan_present",
            "virtual_uid_present",
            "virtual_app_code",
            "raw_line",
        ],
        no_card_rows,
    )
    _write_if_enabled(
        config,
        written,
        "summary_no_card_card_history",
        output_dir / "summary_no_card_card_history.csv",
        write_dict_rows_report,
        ["metric", "value", "percent", "message"],
        no_card_card_history_summary_rows(no_card_rows),
    )


def _write_check_reports(
    config: ReportConfig,
    written: list[Path],
    events: list[PaymentEvent],
    output_dir: Path,
) -> None:
    results = run_builtin_checks(events)
    _write_if_enabled(config, written, "check_results", output_dir / "check_results.csv", write_check_results, results)
    _write_if_enabled(config, written, "check_summary", output_dir / "check_summary.csv", write_check_summary, results)


def write_check_reports(events: list[PaymentEvent], output_dir: Path) -> None:
    results = run_builtin_checks(events)
    write_check_results(results, output_dir / "check_results.csv")
    write_check_summary(results, output_dir / "check_summary.csv")


def write_check_results(results: list[CheckResult], path: Path) -> None:
    fieldnames = [
        "check_id",
        "title",
        "severity",
        "status",
        "source_file",
        "line_number",
        "timestamp",
        "code",
        "message",
        "evidence",
        "raw_line",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for result in results:
            writer.writerow(
                {
                    "check_id": result.check_id,
                    "title": result.title,
                    "severity": result.severity,
                    "status": result.status,
                    "source_file": result.source_file,
                    "line_number": result.line_number if result.line_number is not None else "",
                    "timestamp": result.timestamp.isoformat(sep=" ") if result.timestamp else "",
                    "code": result.code if result.code is not None else "",
                    "message": result.message or "",
                    "evidence": result.evidence,
                    "raw_line": result.raw_line,
                }
            )


def write_check_summary(results: list[CheckResult], path: Path) -> None:
    counts: dict[tuple[str, str, str, str], int] = {}
    for result in results:
        key = (result.check_id, result.title, result.severity, result.status)
        counts[key] = counts.get(key, 0) + 1

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["check_id", "title", "severity", "status", "count"])
        writer.writeheader()
        for (check_id, title, severity, status), count in sorted(counts.items()):
            writer.writerow(
                {
                    "check_id": check_id,
                    "title": title,
                    "severity": severity,
                    "status": status,
                    "count": count,
                }
            )


def write_comparison_report(events: list[PaymentEvent], path: Path, dimension: str) -> None:
    rows = comparison_rows(events, dimension)
    fieldnames = [
        dimension,
        "total",
        "success_count",
        "success_percent",
        "decline_count",
        "decline_percent",
        "technical_error_count",
        "technical_error_percent",
        "unknown_count",
        "unknown_percent",
        "p90_ms",
        "p95_ms",
    ]
    _write_dict_rows(path, fieldnames, rows)


def write_comparison_report_configured(events: list[PaymentEvent], dimension: str, path: Path) -> None:
    write_comparison_report(events, path, dimension)


def write_code_matrix_report(events: list[PaymentEvent], path: Path, dimension: str) -> None:
    codes, rows = code_matrix_rows(events, dimension)
    fieldnames = [dimension, *[f"code_{code}" for code in codes]]
    _write_dict_rows(path, fieldnames, rows)


def write_code_matrix_report_configured(events: list[PaymentEvent], dimension: str, path: Path) -> None:
    write_code_matrix_report(events, path, dimension)


def write_classification_matrix_report(events: list[PaymentEvent], path: Path, dimension: str) -> None:
    fieldnames = [dimension, "success", "decline", "technical_error", "unknown"]
    _write_dict_rows(path, fieldnames, classification_matrix_rows(events, dimension))


def write_classification_matrix_report_configured(events: list[PaymentEvent], dimension: str, path: Path) -> None:
    write_classification_matrix_report(events, path, dimension)


def _write_if_enabled(
    config: ReportConfig,
    written: list[Path],
    report_name: str,
    path: Path,
    writer,
    *args,
) -> None:
    if not config.enabled(report_name):
        return
    writer(*args, path)
    written.append(path)


def write_dict_rows_report(fieldnames: list[str], rows: list[dict[str, object]], path: Path) -> None:
    _write_dict_rows(path, fieldnames, rows)


def _write_dict_rows(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _format_number(value: float | None) -> str:
    if value is None:
        return ""
    if value.is_integer():
        return str(int(value))
    return str(value)


def _percent(count: int, total: int) -> float:
    if total == 0:
        return 0.0
    return round((count / total) * 100, 2)

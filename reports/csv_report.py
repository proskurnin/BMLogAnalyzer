from __future__ import annotations

import csv
from pathlib import Path

from analytics.classifiers import CODE_CLASSIFICATIONS, CODE_DESCRIPTIONS, is_known_code
from analytics.comparisons import classification_matrix_rows, code_matrix_rows, comparison_rows
from core.models import AnalysisResult, DiagnosticLine, PaymentEvent


def write_csv_reports(
    events: list[PaymentEvent],
    result: AnalysisResult,
    reports_dir: Path | str,
    diagnostics: list[DiagnosticLine] | None = None,
    file_stats=None,
) -> None:
    output_dir = Path(reports_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    write_parsed_events(events, output_dir / "parsed_events.csv")
    write_summary(result.by_code, output_dir / "summary_by_code.csv", "code")
    write_summary(result.by_message, output_dir / "summary_by_message.csv", "message")
    write_summary(result.by_bm_version, output_dir / "summary_by_bm_version.csv", "bm_version")
    write_summary(result.by_reader_type, output_dir / "summary_by_reader_type.csv", "reader_type")
    write_summary(result.by_reader_firmware, output_dir / "summary_by_reader_firmware.csv", "reader_firmware")
    write_summary(result.by_classification, output_dir / "summary_by_classification.csv", "classification")
    write_summary(result.duration_buckets, output_dir / "summary_by_duration_bucket.csv", "duration_bucket")
    write_unknown_codes(result.by_code, output_dir / "unknown_codes.csv")
    write_known_codes(output_dir / "known_codes.csv")
    write_diagnostics(diagnostics or [], output_dir / "diagnostics.csv")
    write_file_diagnostics(file_stats or [], output_dir / "file_diagnostics.csv")
    write_comparison_report(events, output_dir / "comparison_by_bm_version.csv", "bm_version")
    write_comparison_report(events, output_dir / "comparison_by_reader_type.csv", "reader_type")
    write_code_matrix_report(events, output_dir / "matrix_bm_version_by_code.csv", "bm_version")
    write_code_matrix_report(events, output_dir / "matrix_reader_type_by_code.csv", "reader_type")
    write_classification_matrix_report(events, output_dir / "matrix_bm_version_by_classification.csv", "bm_version")
    write_classification_matrix_report(events, output_dir / "matrix_reader_type_by_classification.csv", "reader_type")


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
        "bm_type",
        "bm_version",
        "reader_type",
        "reader_firmware",
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
                    "bm_type": event.bm_type or "",
                    "bm_version": event.bm_version or "",
                    "reader_type": event.reader_type or "",
                    "reader_firmware": event.reader_firmware or "",
                    "raw_line": event.raw_line,
                }
            )


def write_summary(values: dict[object, int], path: Path, key_name: str) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=[key_name, "count"])
        writer.writeheader()
        for key, count in values.items():
            writer.writerow({key_name: key, "count": count})


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


def write_code_matrix_report(events: list[PaymentEvent], path: Path, dimension: str) -> None:
    codes, rows = code_matrix_rows(events, dimension)
    fieldnames = [dimension, *[f"code_{code}" for code in codes]]
    _write_dict_rows(path, fieldnames, rows)


def write_classification_matrix_report(events: list[PaymentEvent], path: Path, dimension: str) -> None:
    fieldnames = [dimension, "success", "decline", "technical_error", "unknown"]
    _write_dict_rows(path, fieldnames, classification_matrix_rows(events, dimension))


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

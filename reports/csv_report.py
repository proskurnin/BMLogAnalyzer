from __future__ import annotations

import csv
from pathlib import Path

from core.models import AnalysisResult, DiagnosticLine, PaymentEvent


def write_csv_reports(
    events: list[PaymentEvent],
    result: AnalysisResult,
    reports_dir: Path | str,
    diagnostics: list[DiagnosticLine] | None = None,
) -> None:
    output_dir = Path(reports_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    write_parsed_events(events, output_dir / "parsed_events.csv")
    write_summary(result.by_code, output_dir / "summary_by_code.csv", "code")
    write_summary(result.by_bm_version, output_dir / "summary_by_bm_version.csv", "bm_version")
    write_summary(result.by_reader_type, output_dir / "summary_by_reader_type.csv", "reader_type")
    write_summary(result.by_reader_firmware, output_dir / "summary_by_reader_firmware.csv", "reader_firmware")
    write_summary(result.by_classification, output_dir / "summary_by_classification.csv", "classification")
    write_summary(result.duration_buckets, output_dir / "summary_by_duration_bucket.csv", "duration_bucket")
    write_unknown_codes(result.by_code, output_dir / "unknown_codes.csv")
    write_diagnostics(diagnostics or [], output_dir / "diagnostics.csv")


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
                    "duration_ms": event.duration_ms if event.duration_ms is not None else "",
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
    known_codes = {"0", "3", "16", "17"}
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["code", "count"])
        writer.writeheader()
        for key, count in values.items():
            if str(key) not in known_codes:
                writer.writerow({"code": key, "count": count})


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

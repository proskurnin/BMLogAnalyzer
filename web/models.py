from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class RequestModel:
    input_path: str
    extracted_dir: str
    reports_dir: str
    date: str | None
    reader: str | None
    bm: str | None
    generate_reports: bool


@dataclass(frozen=True)
class AnalysisModel:
    total: int
    success_count: int
    success_percent: float
    decline_count: int
    decline_percent: float
    technical_error_count: int
    technical_error_percent: float
    unknown_count: int
    unknown_percent: float
    by_code: dict[int | str, int] = field(default_factory=dict)
    by_message: dict[str, int] = field(default_factory=dict)
    by_bm_version: dict[str, int] = field(default_factory=dict)
    by_reader_type: dict[str, int] = field(default_factory=dict)
    by_reader_firmware: dict[str, int] = field(default_factory=dict)
    by_classification: dict[str, int] = field(default_factory=dict)
    duration_buckets: dict[str, int] = field(default_factory=dict)
    p90_ms: float | None = None
    p95_ms: float | None = None


@dataclass(frozen=True)
class PipelineModel:
    scanned_lines: int
    malformed_payment_lines: int
    extracted_files: int
    skipped_archives: int
    input_files: list[str] = field(default_factory=list)
    analyzed_files: list[str] = field(default_factory=list)
    diagnostics: list[dict[str, Any]] = field(default_factory=list)
    steps: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class ArchiveModel:
    processed: int
    bm_logs: int
    stopper_logs: int
    reader_logs: int
    system_logs: int
    archive_categories: dict[str, int] = field(default_factory=dict)
    log_type_counts: dict[str, int] = field(default_factory=dict)
    bm_log_version_counts: dict[str, int] = field(default_factory=dict)
    reader_model_counts: dict[str, int] = field(default_factory=dict)
    reader_firmware_counts: dict[str, int] = field(default_factory=dict)
    other_logs: list[dict[str, str]] = field(default_factory=list)


@dataclass(frozen=True)
class ReportsModel:
    written: list[str] = field(default_factory=list)
    reports_dir: str = ""


@dataclass(frozen=True)
class SnapshotModel:
    version: str
    request: RequestModel
    analysis: AnalysisModel
    pipeline: PipelineModel
    archives: ArchiveModel
    reports: ReportsModel
    facts: dict[str, Any] = field(default_factory=dict)
    stats: dict[str, Any] | None = None


@dataclass(frozen=True)
class HistoryItemModel:
    run_id: str
    created_at: str
    mode: str
    source: str
    version: str
    input_path: str
    reports_dir: str
    total: int
    success_count: int
    decline_count: int
    technical_error_count: int
    unknown_count: int
    bm_logs: int
    reader_logs: int
    system_logs: int
    report_path: str = ""

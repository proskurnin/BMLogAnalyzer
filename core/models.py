from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class LogLine:
    source_file: str
    line_number: int
    text: str


@dataclass(frozen=True)
class DiagnosticLine:
    source_file: str
    line_number: int
    reason: str
    raw_line: str


@dataclass(frozen=True)
class ExtractionResult:
    input_path: str
    extracted_dir: str
    source_archives: list[str] = field(default_factory=list)
    extracted_files: list[str] = field(default_factory=list)
    skipped_files: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class PaymentEvent:
    source_file: str
    line_number: int
    timestamp: datetime | None
    event_type: str
    code: int | None
    message: str | None
    duration_ms: float | None
    package: str | None
    raw_line: str
    carrier: str | None = None
    platform: str | None = None
    bm_type: str | None = None
    bm_version: str | None = None
    reader_type: str | None = None
    reader_firmware: str | None = None
    payment_type: int | None = None
    auth_type: int | None = None


@dataclass(frozen=True)
class AnalysisResult:
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
class PipelineStepResult:
    name: str
    status: str
    duration_ms: float
    errors: int
    details: dict[str, str | int | float | None] = field(default_factory=dict)


@dataclass
class FileProcessingStats:
    source_file: str
    scanned_lines: int = 0
    payment_resp_lines: int = 0
    parsed_payment_resp_lines: int = 0
    selected_payment_resp_events: int = 0
    malformed_payment_resp_lines: int = 0


@dataclass(frozen=True)
class LogFileInventory:
    source_file: str
    log_type: str
    detection_method: str
    evidence: str
    dates: list[str] = field(default_factory=list)
    bm_versions: list[str] = field(default_factory=list)
    reader_models: list[str] = field(default_factory=list)
    reader_firmware_versions: list[str] = field(default_factory=list)
    error_status_counts: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class ArchiveInventoryRow:
    archive: str
    category: str
    count: int
    size_bytes: int = 0
    date_from: str | None = None
    date_to: str | None = None
    examples: list[str] = field(default_factory=list)
    files: list[str] = field(default_factory=list)
    file_sizes: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class PipelineStats:
    scanned_lines: int
    malformed_payment_lines: int
    extracted_files: int
    skipped_archives: int = 0
    input_files: list[str] = field(default_factory=list)
    analyzed_files: list[str] = field(default_factory=list)
    extracted_file_paths: list[str] = field(default_factory=list)
    skipped_archive_paths: list[str] = field(default_factory=list)
    diagnostics: list[DiagnosticLine] = field(default_factory=list)
    steps: list[PipelineStepResult] = field(default_factory=list)
    files: list[FileProcessingStats] = field(default_factory=list)
    log_inventory: list[LogFileInventory] = field(default_factory=list)
    archive_inventory: list[ArchiveInventoryRow] = field(default_factory=list)


@dataclass(frozen=True)
class CheckCase:
    check_id: str
    title: str
    description: str
    severity: str
    enabled: bool = True
    version: str = "1"


@dataclass(frozen=True)
class CheckResult:
    check_id: str
    title: str
    severity: str
    status: str
    source_file: str
    line_number: int | None
    timestamp: datetime | None
    code: int | None
    message: str | None
    evidence: str
    raw_line: str

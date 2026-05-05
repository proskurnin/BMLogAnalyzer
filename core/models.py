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
    bm_type: str | None
    bm_version: str | None
    reader_type: str | None
    reader_firmware: str | None
    raw_line: str


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

from __future__ import annotations

from dataclasses import asdict, dataclass
import tempfile
from pathlib import Path
from typing import Any

from analytics.archive_inventory import (
    archive_category_date_range,
    archive_category_totals,
    bm_log_count,
    explicit_reader_log_count,
    explicit_system_log_count,
    stopper_log_count,
)
from analytics.log_inventory import (
    bm_log_version_counts,
    log_type_counts,
    other_log_descriptions,
    reader_firmware_counts,
    reader_model_counts,
)
from core.config import load_app_config
from core.contracts import SNAPSHOT_SCHEMA_VERSION
from core.pipeline import run_analysis
from core.version import format_version
from web.models import AnalysisModel, ArchiveModel, PipelineModel, ReportsModel, RequestModel, SnapshotModel
from reports.csv_report import write_csv_reports
from reports.html_report import write_html_report


@dataclass(frozen=True)
class AnalysisRequest:
    input_path: str | None = None
    config_path: str = "./config/config.yaml"
    extracted_dir: str | None = None
    reports_dir: str | None = None
    date: str | None = None
    reader: str | None = None
    bm: str | None = None
    generate_reports: bool = False


@dataclass(frozen=True)
class AnalysisBundle:
    snapshot: SnapshotModel
    events: list[Any]
    result: Any
    stats: Any


def execute_analysis(request: AnalysisRequest) -> AnalysisBundle:
    config = load_app_config(request.config_path)
    input_path = Path(request.input_path or config.input_path)
    extracted_dir = Path(request.extracted_dir or config.extracted_dir)
    reports_dir = Path(request.reports_dir or config.reports_dir)
    events, result, stats = run_analysis(
        input_path,
        extracted_dir=extracted_dir,
        date_filter=request.date,
        reader_filter=request.reader,
        bm_filter=request.bm,
    )

    written_reports: list[str] = []
    if request.generate_reports:
        written = write_csv_reports(
            events,
            result,
            reports_dir,
            diagnostics=stats.diagnostics,
            file_stats=stats.files,
            pipeline_stats=stats,
            report_config=config.report_config,
        )
        written_reports.extend(str(path) for path in written)
        if config.report_config.enabled("analysis_report_html"):
            html_report_path = reports_dir / "analysis_report.html"
            write_html_report(events, result, html_report_path, stats=stats)
            written_reports.append(str(html_report_path))

    snapshot = SnapshotModel(
        version=format_version(),
        request=RequestModel(
            input_path=str(input_path),
            extracted_dir=str(extracted_dir),
            reports_dir=str(reports_dir),
            date=request.date,
            reader=request.reader,
            bm=request.bm,
            generate_reports=request.generate_reports,
        ),
        analysis=AnalysisModel(
            total=result.total,
            success_count=result.success_count,
            success_percent=result.success_percent,
            decline_count=result.decline_count,
            decline_percent=result.decline_percent,
            technical_error_count=result.technical_error_count,
            technical_error_percent=result.technical_error_percent,
            unknown_count=result.unknown_count,
            unknown_percent=result.unknown_percent,
            by_code=dict(result.by_code),
            by_message=dict(result.by_message),
            by_bm_version=dict(result.by_bm_version),
            by_reader_type=dict(result.by_reader_type),
            by_reader_firmware=dict(result.by_reader_firmware),
            by_classification=dict(result.by_classification),
            duration_buckets=dict(result.duration_buckets),
            p90_ms=result.p90_ms,
            p95_ms=result.p95_ms,
        ),
        pipeline=PipelineModel(
            scanned_lines=stats.scanned_lines,
            malformed_payment_lines=stats.malformed_payment_lines,
            extracted_files=stats.extracted_files,
            skipped_archives=stats.skipped_archives,
            input_files=list(stats.input_files),
            analyzed_files=list(stats.analyzed_files),
            diagnostics=[asdict(item) for item in stats.diagnostics],
            steps=[asdict(step) for step in stats.steps],
        ),
        archives=ArchiveModel(
            processed=len(stats.input_files),
            bm_logs=bm_log_count(stats.archive_inventory),
            stopper_logs=stopper_log_count(stats.archive_inventory),
            reader_logs=explicit_reader_log_count(stats.archive_inventory),
            system_logs=explicit_system_log_count(stats.archive_inventory),
            archive_categories=archive_category_totals(stats.archive_inventory),
            log_type_counts=log_type_counts(stats.log_inventory),
            bm_log_version_counts=bm_log_version_counts(stats.log_inventory),
            reader_model_counts=reader_model_counts(stats.log_inventory),
            reader_firmware_counts={
                f"{model}::{firmware}": count
                for (model, firmware), count in reader_firmware_counts(stats.log_inventory).items()
            },
            other_logs=other_log_descriptions(stats.log_inventory),
        ),
        reports=ReportsModel(written=written_reports, reports_dir=str(reports_dir)),
        facts={
            "archive_date_range": archive_category_date_range(stats.archive_inventory, {"BM rotate", "BM stdout"}),
        },
        stats={
            "archive_inventory": [asdict(row) for row in stats.archive_inventory],
            "log_inventory": [asdict(item) for item in stats.log_inventory],
        },
        schema_version=SNAPSHOT_SCHEMA_VERSION,
    )
    return AnalysisBundle(snapshot=snapshot, events=events, result=result, stats=stats)


def analyze_request(request: AnalysisRequest) -> SnapshotModel:
    return execute_analysis(request).snapshot


def build_analysis_snapshot(request: AnalysisRequest) -> SnapshotModel:
    return analyze_request(request)


def build_summary_snapshot(request: AnalysisRequest) -> SnapshotModel:
    snapshot = analyze_request(request)
    return SnapshotModel(
        version=snapshot.version,
        request=snapshot.request,
        analysis=snapshot.analysis,
        pipeline=snapshot.pipeline,
        archives=snapshot.archives,
        reports=snapshot.reports,
        facts=snapshot.facts,
        stats=None,
        schema_version=snapshot.schema_version,
    )


def stage_uploaded_files(files: list[tuple[str, bytes]], base_dir: Path | None = None) -> tuple[Path, Path]:
    temp_root = Path(tempfile.mkdtemp(prefix="bm-log-analyzer-upload-", dir=str(base_dir) if base_dir else None))
    input_dir = temp_root / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    for relative_name, content in files:
        target = input_dir / Path(relative_name)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    return temp_root, input_dir


def analyze_uploaded_files(
    request: AnalysisRequest,
    files: list[tuple[str, bytes]],
    *,
    summary: bool = False,
    storage_dir: Path | None = None,
) -> SnapshotModel:
    storage_base = Path(storage_dir or "./_workdir/uploads")
    storage_base.mkdir(parents=True, exist_ok=True)
    upload_root, input_dir = stage_uploaded_files(files, storage_base)
    staged_request = AnalysisRequest(
        input_path=str(input_dir),
        config_path=request.config_path,
        extracted_dir=str(upload_root / "extracted"),
        reports_dir=request.reports_dir,
        date=request.date,
        reader=request.reader,
        bm=request.bm,
        generate_reports=request.generate_reports,
    )
    return build_summary_snapshot(staged_request) if summary else analyze_request(staged_request)


def execute_uploaded_analysis(
    request: AnalysisRequest,
    files: list[tuple[str, bytes]],
    *,
    summary: bool = False,
    storage_dir: Path | None = None,
) -> AnalysisBundle:
    storage_base = Path(storage_dir or "./_workdir/uploads")
    storage_base.mkdir(parents=True, exist_ok=True)
    upload_root, input_dir = stage_uploaded_files(files, storage_base)
    staged_request = AnalysisRequest(
        input_path=str(input_dir),
        config_path=request.config_path,
        extracted_dir=str(upload_root / "extracted"),
        reports_dir=request.reports_dir,
        date=request.date,
        reader=request.reader,
        bm=request.bm,
        generate_reports=request.generate_reports,
    )
    bundle = execute_analysis(staged_request)
    if summary:
        return AnalysisBundle(
            snapshot=SnapshotModel(
                version=bundle.snapshot.version,
                request=bundle.snapshot.request,
                analysis=bundle.snapshot.analysis,
                pipeline=bundle.snapshot.pipeline,
                archives=bundle.snapshot.archives,
                reports=bundle.snapshot.reports,
                facts=bundle.snapshot.facts,
                stats=None,
                schema_version=bundle.snapshot.schema_version,
            ),
            events=bundle.events,
            result=bundle.result,
            stats=bundle.stats,
        )
    return bundle

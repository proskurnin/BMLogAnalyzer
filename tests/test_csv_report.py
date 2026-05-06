from core.models import ArchiveInventoryRow, DiagnosticLine, PipelineStats
from core.config import ReportConfig
from core.version import __version__
from reports.csv_report import write_csv_reports
from tests.test_counters import make_event
from analytics.counters import analyze_events


def test_writes_extended_csv_reports(tmp_path):
    events = [make_event(0, 100, "4.4.12"), make_event(3, None, "4.4.12"), make_event(999, None, "4.4.12")]
    result = analyze_events(events)
    diagnostics = [DiagnosticLine("sample.log", 2, "payment_start_resp_parse_failed", "raw")]
    stats = PipelineStats(
        scanned_lines=3,
        malformed_payment_lines=1,
        extracted_files=0,
        input_files=["input/sample.log"],
        analyzed_files=["input/sample.log"],
        archive_inventory=[
            ArchiveInventoryRow(
                archive="input/archive.zip",
                category="BM rotate",
                count=1,
                date_from="2026-04-29",
                date_to="2026-04-29",
                examples=["bm/logs/bm/bm-rotate.log"],
            )
        ],
    )

    write_csv_reports(events, result, tmp_path, diagnostics=diagnostics, pipeline_stats=stats)

    assert (tmp_path / "report_metadata.csv").exists()
    assert (tmp_path / "parsed_events.csv").exists()
    assert (tmp_path / "summary_by_reader_type.csv").exists()
    assert (tmp_path / "summary_by_duration_bucket.csv").exists()
    assert (tmp_path / "known_codes.csv").exists()
    assert (tmp_path / "file_diagnostics.csv").exists()
    assert (tmp_path / "comparison_by_bm_version.csv").exists()
    assert (tmp_path / "matrix_bm_version_by_code.csv").exists()
    assert (tmp_path / "matrix_bm_version_by_classification.csv").exists()
    assert (tmp_path / "error_events.csv").exists()
    assert (tmp_path / "technical_error_events.csv").exists()
    assert (tmp_path / "repeat_attempts_after_failure.csv").exists()
    assert (tmp_path / "summary_repeat_attempts_after_failure.csv").exists()
    assert (tmp_path / "read_error_repeat_outcomes.csv").exists()
    assert (tmp_path / "summary_read_error_repeat_outcomes.csv").exists()
    assert (tmp_path / "timeout_repeat_outcomes.csv").exists()
    assert (tmp_path / "summary_timeout_repeat_outcomes.csv").exists()
    assert (tmp_path / "no_card_repeat_outcomes.csv").exists()
    assert (tmp_path / "summary_no_card_repeat_outcomes.csv").exists()
    assert (tmp_path / "card_check_markers.csv").exists()
    assert (tmp_path / "summary_card_check_markers.csv").exists()
    assert (tmp_path / "oda_cda_repeat_outcomes.csv").exists()
    assert (tmp_path / "summary_oda_cda_repeat_outcomes.csv").exists()
    assert (tmp_path / "card_identity_markers.csv").exists()
    assert (tmp_path / "summary_card_identity_markers.csv").exists()
    assert (tmp_path / "card_fingerprint_events.csv").exists()
    assert (tmp_path / "read_error_card_history.csv").exists()
    assert (tmp_path / "summary_read_error_card_history.csv").exists()
    assert (tmp_path / "timeout_card_history.csv").exists()
    assert (tmp_path / "summary_timeout_card_history.csv").exists()
    assert (tmp_path / "no_card_card_history.csv").exists()
    assert (tmp_path / "summary_no_card_card_history.csv").exists()
    assert (tmp_path / "check_results.csv").exists()
    assert (tmp_path / "check_summary.csv").exists()
    assert (tmp_path / "errors_by_file.csv").exists()
    assert (tmp_path / "file_error_overview.csv").exists()
    assert (tmp_path / "bundle_manifest.csv").exists()
    assert (tmp_path / "bundle_manifest.json").exists()
    assert (tmp_path / "archive_inventory.csv").exists()
    assert (tmp_path / "summary_by_archive_category.csv").exists()
    assert (tmp_path / "log_inventory.csv").exists()
    assert (tmp_path / "summary_by_log_type.csv").exists()
    assert (tmp_path / "bm_log_versions.csv").exists()
    assert (tmp_path / "bm_status_summary.csv").exists()
    assert (tmp_path / "reader_models.csv").exists()
    assert (tmp_path / "reader_firmware_versions.csv").exists()
    assert (tmp_path / "reader_firmware_timeline.csv").exists()
    assert (tmp_path / "summary_reader_firmware_timeline.csv").exists()
    assert (tmp_path / "reader_error_summary.csv").exists()
    assert (tmp_path / "system_error_summary.csv").exists()
    assert (tmp_path / "other_logs.csv").exists()
    assert "input/sample.log" in (tmp_path / "bundle_manifest.csv").read_text(encoding="utf-8")
    assert f"app_version,{__version__}" in (tmp_path / "report_metadata.csv").read_text(encoding="utf-8")
    assert "BM rotate" in (tmp_path / "archive_inventory.csv").read_text(encoding="utf-8")
    assert (tmp_path / "summary_by_archive_category.csv").read_text(encoding="utf-8").splitlines() == [
        "category,count",
        "BM rotate,1",
    ]
    assert "Отказ, ошибка чтения карты" in (tmp_path / "bm_status_summary.csv").read_text(encoding="utf-8")
    assert "read_error_events" in (tmp_path / "summary_read_error_repeat_outcomes.csv").read_text(encoding="utf-8")
    assert "timeout_events" in (tmp_path / "summary_timeout_repeat_outcomes.csv").read_text(encoding="utf-8")
    assert "no_card_events" in (tmp_path / "summary_no_card_repeat_outcomes.csv").read_text(encoding="utf-8")
    assert "explicit_card_check_marker_events" in (tmp_path / "summary_card_check_markers.csv").read_text(encoding="utf-8")
    assert "oda_cda_or_basic_check_events" in (tmp_path / "summary_oda_cda_repeat_outcomes.csv").read_text(encoding="utf-8")
    assert "explicit_card_type_marker_events" in (tmp_path / "summary_card_identity_markers.csv").read_text(encoding="utf-8")
    assert "same_card_had_later_success" in (tmp_path / "summary_read_error_card_history.csv").read_text(encoding="utf-8")
    assert "timeout_events_with_card_key" in (tmp_path / "summary_timeout_card_history.csv").read_text(encoding="utf-8")
    assert "no_card_events_with_card_key" in (tmp_path / "summary_no_card_card_history.csv").read_text(encoding="utf-8")
    assert "payment_events_with_reader_firmware" in (tmp_path / "summary_reader_firmware_timeline.csv").read_text(encoding="utf-8")
    assert (tmp_path / "unknown_codes.csv").read_text(encoding="utf-8").splitlines() == ["code,count", "999,1"]
    error_events = (tmp_path / "error_events.csv").read_text(encoding="utf-8")
    assert "technical_error" in error_events
    assert "unknown" in error_events
    assert "success" not in error_events
    assert (tmp_path / "technical_error_events.csv").read_text(encoding="utf-8").count("technical_error") == 1
    assert "payment_start_resp_parse_failed" in (tmp_path / "diagnostics.csv").read_text(encoding="utf-8")


def test_skips_disabled_csv_reports(tmp_path):
    events = [make_event(0, 100, "4.4.12")]
    result = analyze_events(events)
    config = ReportConfig(
        reports={
            "report_metadata": True,
            "parsed_events": False,
            "summary_by_code": True,
        }
    )

    written = write_csv_reports(events, result, tmp_path, report_config=config)

    assert tmp_path / "report_metadata.csv" in written
    assert tmp_path / "summary_by_code.csv" in written
    assert not (tmp_path / "parsed_events.csv").exists()

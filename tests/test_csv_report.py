from core.models import DiagnosticLine, PipelineStats
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
    )

    write_csv_reports(events, result, tmp_path, diagnostics=diagnostics, pipeline_stats=stats)

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
    assert (tmp_path / "check_results.csv").exists()
    assert (tmp_path / "check_summary.csv").exists()
    assert (tmp_path / "errors_by_file.csv").exists()
    assert (tmp_path / "file_error_overview.csv").exists()
    assert (tmp_path / "bundle_manifest.csv").exists()
    assert (tmp_path / "bundle_manifest.json").exists()
    assert "input/sample.log" in (tmp_path / "bundle_manifest.csv").read_text(encoding="utf-8")
    assert (tmp_path / "unknown_codes.csv").read_text(encoding="utf-8").splitlines() == ["code,count", "999,1"]
    error_events = (tmp_path / "error_events.csv").read_text(encoding="utf-8")
    assert "technical_error" in error_events
    assert "unknown" in error_events
    assert "success" not in error_events
    assert (tmp_path / "technical_error_events.csv").read_text(encoding="utf-8").count("technical_error") == 1
    assert "payment_start_resp_parse_failed" in (tmp_path / "diagnostics.csv").read_text(encoding="utf-8")

from core.models import DiagnosticLine
from reports.csv_report import write_csv_reports
from tests.test_counters import make_event
from analytics.counters import analyze_events


def test_writes_extended_csv_reports(tmp_path):
    events = [make_event(999, None, "4.4.12")]
    result = analyze_events(events)
    diagnostics = [DiagnosticLine("sample.log", 2, "payment_start_resp_parse_failed", "raw")]

    write_csv_reports(events, result, tmp_path, diagnostics=diagnostics)

    assert (tmp_path / "parsed_events.csv").exists()
    assert (tmp_path / "summary_by_reader_type.csv").exists()
    assert (tmp_path / "summary_by_duration_bucket.csv").exists()
    assert (tmp_path / "known_codes.csv").exists()
    assert (tmp_path / "file_diagnostics.csv").exists()
    assert (tmp_path / "comparison_by_bm_version.csv").exists()
    assert (tmp_path / "matrix_bm_version_by_code.csv").exists()
    assert (tmp_path / "matrix_bm_version_by_classification.csv").exists()
    assert (tmp_path / "unknown_codes.csv").read_text(encoding="utf-8").splitlines() == ["code,count", "999,1"]
    assert "payment_start_resp_parse_failed" in (tmp_path / "diagnostics.csv").read_text(encoding="utf-8")

from core.models import AnalysisResult, PipelineStats
from reports.console_report import render_console_summary


def test_console_summary_has_structured_sections():
    result = AnalysisResult(
        total=4,
        success_count=1,
        success_percent=25.0,
        decline_count=0,
        decline_percent=0.0,
        technical_error_count=2,
        technical_error_percent=50.0,
        unknown_count=1,
        unknown_percent=25.0,
        by_code={3: 2, 0: 1, 55: 1},
        by_bm_version={"4.4.12": 3, "4.4.6": 1},
        p90_ms=412.0,
        p95_ms=600.0,
    )
    stats = PipelineStats(
        scanned_lines=10,
        malformed_payment_lines=1,
        extracted_files=2,
    )

    summary = render_console_summary(result, stats=stats)

    assert "=== Pipeline ===" in summary
    assert "=== Result ===" in summary
    assert "=== By Code ===" in summary
    assert "=== By BM version ===" in summary
    assert "=== Unknown codes ===" in summary
    assert "• 55: 1" in summary

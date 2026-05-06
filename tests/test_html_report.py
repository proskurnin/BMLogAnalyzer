from core.models import PipelineStats
from core.version import __version__
from analytics.counters import analyze_events
from reports.html_report import write_html_report
from tests.test_counters import make_event


def test_writes_html_report_with_sources_errors_and_checks(tmp_path):
    events = [
        make_event(0, 100, "4.4.12"),
        make_event(3, 412, "4.4.12", message="Ошибка чтения карты"),
    ]
    result = analyze_events(events)
    stats = PipelineStats(
        scanned_lines=2,
        malformed_payment_lines=0,
        extracted_files=0,
        input_files=["input/sample.log"],
        analyzed_files=["input/sample.log"],
    )

    write_html_report(events, result, tmp_path / "analysis_report.html", stats=stats)

    html = (tmp_path / "analysis_report.html").read_text(encoding="utf-8")
    assert "Факт из логов" in html
    assert f"version {__version__}" in html
    assert "Коротко" in html
    assert "Состав архива" in html
    assert "Статусы BM" in html
    assert "Главные сигналы" in html
    assert "<details" in html
    assert "input/sample.log" in html
    assert "Ошибка чтения карты" in html
    assert "technical_error_code_3" in html
    assert "Гипотеза" in html

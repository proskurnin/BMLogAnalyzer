from datetime import datetime
from dataclasses import replace

from analytics.counters import analyze_events
from core.models import ArchiveInventoryRow, PipelineStats
from core.version import __version__
from reports.html_report import write_html_report
from tests.test_counters import make_event


def test_writes_html_report_with_archive_inventory_chart(tmp_path):
    stats = PipelineStats(
        scanned_lines=10,
        malformed_payment_lines=0,
        extracted_files=4,
        input_files=["input/archive.zip"],
        analyzed_files=["input/bm.log", "input/reader.log", "input/system.log", "input/other.log"],
        archive_inventory=[
            ArchiveInventoryRow(
                archive="input/archive.zip",
                category="BM rotate",
                count=7,
                files=["bm/logs/bm/a.log", "bm/logs/bm/b.log"],
            ),
            ArchiveInventoryRow(
                archive="input/archive.zip",
                category="Stopper stdout",
                count=2,
                files=["bm/logs/stopper-std/s1.log", "bm/logs/stopper-std/s2.log"],
            ),
            ArchiveInventoryRow(
                archive="input/archive.zip",
                category="Reader logs",
                count=1,
                files=["logs/reader/reader.log"],
            ),
            ArchiveInventoryRow(
                archive="input/archive.zip",
                category="Reader firmware binary",
                count=1,
                files=["reader-1.44.6518.bin.P.signed"],
            ),
            ArchiveInventoryRow(
                archive="input/archive.zip",
                category="Service config",
                count=1,
                files=["bm/bm.service"],
            ),
            ArchiveInventoryRow(
                archive="input/archive.zip",
                category="Other",
                count=1,
                files=["bm/stopper.service"],
            ),
        ],
    )

    events = [
        make_event(0, 100, "4.4.12", message="OK", payment_type=2, auth_type=0),
        make_event(3, 412, "4.4.12", message="Ошибка чтения карты"),
    ]
    events[0] = replace(events[0], timestamp=datetime(2026, 5, 4, 10, 0))
    events[1] = replace(events[1], timestamp=datetime(2026, 5, 5, 14, 0))
    result = analyze_events(events)
    write_html_report(events, result, tmp_path / "analysis_report.html", stats=stats)

    html = (tmp_path / "analysis_report.html").read_text(encoding="utf-8")
    assert "BM Log Analyzer" in html
    assert f"version {__version__}" in html
    assert "Log-файлы" in html
    assert "Прочие файлы" in html
    assert "BM-статусы" in html
    assert "Группировка статусов" in html
    assert "BM сведения" in html
    assert "Версии БМ" in html
    assert "Перевозчики" in html
    assert "Ридеры" in html
    assert "Даты" in html
    assert "BM logs" in html
    assert "Stopper logs" in html
    assert "Reader logs" in html
    assert "class=\"bar-chart\"" in html
    assert "class=\"bar-row\"" in html
    assert "class=\"metric metric--button\"" in html
    assert "class=\"bm-meta-card bm-meta-card--button\"" in html
    assert "class=\"status-table\"" in html
    assert "class=\"status-table status-table--grouped\"" in html
    assert "class=\"status-table status-table--diagnostic\"" in html
    assert "status-row--success" in html
    assert "status-row--clickable" in html
    assert "id=\"log-modal\"" in html
    assert "id=\"modal-body\"" in html
    assert "class=\"collapsible\"" in html
    assert "class=\"collapsible-body\"" in html
    assert "class=\"line-chart\"" in html
    assert "class=\"chart-legend\"" in html
    assert "Динамика по датам" in html
    assert "data-label=\"BM logs\"" in html
    assert "data-label=\"Прошивки ридеров\"" in html
    assert "data-label=\"Конфиги\"" in html
    assert "data-kind=\"metric\"" in html
    assert "data-format=\"records\"" in html
    assert "data-kind=\"status\"" in html
    assert "data-kind=\"group\"" in html
    assert "data-kind=\"meta\"" in html
    assert "class=\"modal-files\"" in html
    assert "class=\"modal-lines\"" in html
    assert "class=\"modal-toolbar\"" in html
    assert "data-filter-group-reset=" in html
    assert "Успех" in html
    assert "Ошибки" in html
    assert "Отказы" in html
    assert "Не классифицировано" in html

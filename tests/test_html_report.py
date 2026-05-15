from datetime import datetime
from dataclasses import replace
import json

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
        input_files=["input/2007201.zip"],
        analyzed_files=["input/bm.log", "input/reader.log", "input/system.log", "input/other.log"],
        archive_inventory=[
            ArchiveInventoryRow(
                archive="input/2007201.zip",
                category="BM rotate",
                count=7,
                files=["bm/logs/bm/a.log", "bm/logs/bm/b.log"],
            ),
            ArchiveInventoryRow(
                archive="input/2007201.zip",
                category="Stopper stdout",
                count=2,
                files=["bm/logs/stopper-std/s1.log", "bm/logs/stopper-std/s2.log"],
            ),
            ArchiveInventoryRow(
                archive="input/2007201.zip",
                category="Reader logs",
                count=1,
                files=["logs/reader/reader.log"],
            ),
            ArchiveInventoryRow(
                archive="input/2007201.zip",
                category="Reader firmware binary",
                count=1,
                files=["reader-1.44.6518.bin.P.signed"],
            ),
            ArchiveInventoryRow(
                archive="input/2007201.zip",
                category="Service config",
                count=1,
                files=["bm/bm.service"],
            ),
            ArchiveInventoryRow(
                archive="input/2007201.zip",
                category="Other",
                count=1,
                files=["bm/stopper.service"],
            ),
        ],
    )

    events = [
        make_event(17, 100, "4.4.12", message="Нет карты", payment_type=2, auth_type=0),
        make_event(17, 120, "4.4.12", message="Нет карты", payment_type=2, auth_type=0),
        make_event(3, 412, "4.4.12", message="Ошибка чтения карты"),
        make_event(3, 412, "4.4.12", message="Ошибка чтения карты"),
        make_event(17, 100, "4.4.7", message="Нет карты", payment_type=2, auth_type=0),
        make_event(3, 412, "4.4.7", message="Ошибка чтения карты"),
    ]
    events[0] = replace(events[0], source_file="_workdir/extracted/2007201.zip/bm/logs/bm/a.log", timestamp=datetime(2026, 4, 11, 10, 0))
    events[1] = replace(events[1], source_file="_workdir/extracted/2007201.zip/bm/logs/bm/b.log", timestamp=datetime(2026, 5, 5, 11, 0))
    events[2] = replace(events[2], source_file="_workdir/extracted/2007201.zip/bm/logs/bm/c.log", timestamp=datetime(2026, 4, 20, 12, 0))
    events[3] = replace(events[3], source_file="_workdir/extracted/2007201.zip/bm/logs/bm/d.log", timestamp=datetime(2026, 5, 5, 13, 0))
    events[4] = replace(events[4], source_file="_workdir/extracted/2007201.zip/bm/logs/bm/e.log", timestamp=datetime(2026, 5, 6, 9, 0))
    events[5] = replace(events[5], source_file="_workdir/extracted/2007201.zip/bm/logs/bm/f.log", timestamp=datetime(2026, 5, 12, 18, 0))
    events[0] = replace(events[0], raw_line="2026-04-11 PaymentStart, resp: {Code:17 Message:Нет карты}")
    events[1] = replace(events[1], raw_line="2026-05-05 PaymentStart, resp: {Code:17 Message:Нет карты}")
    events[2] = replace(events[2], raw_line="2026-04-20 PaymentStart, resp: {Code:3 Message:Ошибка чтения карты}")
    events[3] = replace(events[3], raw_line="2026-05-05 PaymentStart, resp: {Code:3 Message:Ошибка чтения карты}")
    events[4] = replace(events[4], raw_line="2026-05-06 PaymentStart, resp: {Code:17 Message:Нет карты}")
    events[5] = replace(events[5], raw_line="2026-05-12 PaymentStart, resp: {Code:3 Message:Ошибка чтения карты}")
    events[2] = replace(events[2], reader_firmware="1.44.6518", raw_line="2026-04-20 PaymentStart, resp: {Code:3 Message:Ошибка чтения карты ReaderVersion:1.44.6518}")
    result = analyze_events(events)
    write_html_report(events, result, tmp_path / "analysis_report.html", stats=stats)

    html = (tmp_path / "analysis_report.html").read_text(encoding="utf-8")
    manifest = (tmp_path / "analysis_report.json").read_text(encoding="utf-8")
    assert "BM Log Analyzer" in html
    assert f"отчёт создан в версии сервиса {__version__}" in html
    assert "Log-файлы" in html
    assert "Прочие файлы" in html
    assert "BM-статусы" in html
    assert "Подозрительно" in html
    assert "Проверки" in html
    assert "Сработало правил" in html
    assert "status-table--checks" in html
    assert "Почему подозрительно" in html
    assert "collapsible--suspicious" in html
    assert "status-table--suspicious" in html
    assert "Группировка статусов" in html
    assert "BM сведения" in html
    assert "Версии БМ" in html
    assert "Перевозчики" in html
    assert "Ридеры" in html
    assert "Версии ПО ридеров" in html
    assert "1.44.6518" in html
    assert "reader_firmwares" in html
    assert "Даты" in html
    assert "id=\"bm-filter-root\"" in html
    assert "BM logs" in html
    assert "Stopper logs" in html
    assert "Reader logs" in html
    assert "class=\"bar-chart\"" in html
    assert "class=\"bar-row\"" in html
    assert "class=\"metric metric--button\"" in html
    assert "class=\"bm-meta-card bm-meta-card--button\"" in html
    assert "data-kind=\"meta\"" in html
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
    assert "data-meta-kind=\"versions\"" in html
    assert "class=\"modal-files\"" in html
    assert "class=\"modal-lines\"" in html
    assert "line-highlight" in html
    assert "class=\"filter-panel\"" in html
    assert "filter-option-grid" in html
    assert "data-meta-kind=" in html
    assert "Успех" in html
    assert "Ошибки" in html
    assert "Отказ, истек таймаут" in html
    assert "Отказы" in html
    assert "Не классифицировано" in html
    assert "Аналитика по валидаторам" in html
    assert "2007201" in html
    assert "Версия 4.4.12" in html
    assert "Всего 4 транзакций" in html
    assert "Версия 4.4.7" in html
    assert "Всего 2 транзакций" in html
    assert "Отказ, нет карты в поле" in html
    assert "Отказ, ошибка чтения карты" in html
    assert '"report_type": "analysis_report"' in manifest
    assert '"schema_version": "bm-log-analyzer.analysis-report.v1"' in manifest
    assert '"stable_fields": [' in manifest
    assert '"stable_sections": [' in manifest
    assert '"counts": {' in manifest
    assert '"sections": [' in manifest
    assert '"log_files"' in manifest
    assert '"validator_sections"' in manifest
    assert '"suspicious"' in manifest
    assert '"suspicious_lines"' in manifest
    assert '"validation_checks"' in manifest
    ai_context = json.loads((tmp_path / "analysis_report.ai_context.json").read_text(encoding="utf-8"))
    assert ai_context["schema_version"] == "bm-log-analyzer.ai-context.v1"
    assert ai_context["summary"]["events"] == 6


def test_html_report_hides_empty_sections(tmp_path):
    stats = PipelineStats(scanned_lines=1, malformed_payment_lines=0, extracted_files=0)
    event = replace(
        make_event(0, 100, "4.4.12", message="OK", payment_type=2, auth_type=0),
        raw_line="PaymentStart, resp: {Code:0 Message:OK} error: no error",
    )
    result = analyze_events([event])

    write_html_report([event], result, tmp_path / "analysis_report.html", stats=stats)

    html = (tmp_path / "analysis_report.html").read_text(encoding="utf-8")
    manifest = json.loads((tmp_path / "analysis_report.json").read_text(encoding="utf-8"))
    assert "Log-файлы" not in html
    assert "<strong>Прочие файлы</strong>" not in html
    assert "Подозрительно" not in html
    assert "Подозрительных строк не найдено." not in html
    assert "Проверки" not in html
    assert 'id="bm-unclassified-root"><details' not in html
    assert 'id="bm-filter-root"' not in html
    assert "log_files" not in manifest["stable_sections"]
    assert "other_files" not in manifest["stable_sections"]
    assert "suspicious" not in manifest["stable_sections"]
    assert "validation_checks" not in manifest["stable_sections"]
    assert "unclassified_diagnostics" not in manifest["stable_sections"]

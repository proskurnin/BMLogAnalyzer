from datetime import datetime
from dataclasses import replace
import json

from analytics.counters import analyze_events
from core.models import ArchiveInventoryRow, InputSourceSummary, LogFileInventory, PipelineStats
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
        log_inventory=[
            LogFileInventory(
                source_file="_workdir/extracted/2007201.zip/bm/logs/bm/a.log",
                log_type="bm",
                detection_method="content",
                evidence="content:payment_start",
                bm_versions=["4.4.12"],
            ),
            LogFileInventory(
                source_file="_workdir/extracted/2007201.zip/logs/reader/reader.log",
                log_type="reader",
                detection_method="content",
                evidence="content:reader_firmware",
                reader_firmware_versions=["1.44.6518"],
            )
        ],
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
    events[2] = replace(events[2], raw_line="2026-04-20 PaymentStart, resp: {Code:3 Message:Ошибка чтения карты}")
    result = analyze_events(events)
    write_html_report(events, result, tmp_path / "analysis_report.html", stats=stats)

    html = (tmp_path / "analysis_report.html").read_text(encoding="utf-8")
    manifest = (tmp_path / "analysis_report.json").read_text(encoding="utf-8")
    assert "BM Log Analyzer" in html
    assert f"отчёт создан в версии сервиса {__version__}" in html
    assert "Он содержит логи следующих типов:" in html
    assert "Прочие файлы" in html
    assert "BM-статусы" in html
    assert "Подозрительно" in html
    assert "Проверки" in html
    assert "Сработало правил: 1 из 5" in html
    assert "Не сработали" in html
    assert "В разобранных событиях нет PaymentStart resp с Code:16." in html
    assert "Сценарии из протокола взаимодействия" in html
    assert "Разделы" in html
    assert "Цитаты" in html
    assert "Таймаут по умолчанию 3 секунды." in html
    assert "status-table--checks" in html
    assert "status-table--bm" in html
    assert '<colgroup><col style="width:68%"><col style="width:16%"><col style="width:16%"></colgroup>' in html
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
    assert "class=\"status-table status-table--bm\"" in html
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
    assert "source_sections" in manifest
    assert "source_quotes" in manifest
    assert "validation_check_catalog" in manifest
    assert "data-format=\"records\"" in html
    assert "Источник данных: БМ." in html
    assert '"section_sources": {' in manifest
    assert '"pipeline_steps": [' in manifest
    assert '"extraction_archives": [' in manifest
    assert "data-kind=\"status\"" in html
    assert "data-kind=\"group\"" in html
    assert "data-meta-kind=\"versions\"" in html
    assert "class=\"modal-files\"" in html
    assert "class=\"modal-lines\"" in html
    assert "line-highlight" in html
    assert "class=\"filter-panel\"" in html
    assert "filter-option-grid" in html
    assert "function selectedEvents()" in html
    assert "function eventMatchesFilters(event, exceptGroup = '')" in html
    assert "const events = selectedEvents();" in html
    assert "--filter-hover: #fff1a8" in html
    assert "--filter-active-border: #0b2f6b" in html
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
    assert '"validation_check_catalog"' in manifest
    assert '"validation_checks"' in manifest
    report_data = json.loads(html.split('<script id="report-data" type="application/json">', 1)[1].split("</script>", 1)[0])
    first_event = report_data["events"][0]
    assert first_event["date"] == "2026-04-11"
    assert first_event["version"] == "4.4.12"
    assert first_event["carriers"] == ["НБС"]
    assert first_event["reader"] == "ОТИ"
    ai_context = json.loads((tmp_path / "analysis_report.ai_context.json").read_text(encoding="utf-8"))
    assert ai_context["schema_version"] == "bm-log-analyzer.ai-context.v1"
    assert ai_context["summary"]["events"] == 6
    assert "protocol_scenario_results" in ai_context
    assert ai_context["input_sources"] == []


def test_html_report_maps_mmv2_package_to_mcd2_carrier(tmp_path):
    event = replace(
        make_event(0, 250, "1.1.7", message="OK"),
        package="mmv2-x86_64-1.1.7",
        carrier="mmv2",
        platform="x86_64",
        bm_type=None,
        reader_type=None,
        raw_line="2026-04-29 PaymentStart, resp: {Code:0 Message:OK} p: mmv2-x86_64-1.1.7",
    )
    result = analyze_events([event])

    write_html_report([event], result, tmp_path / "analysis_report.html")

    html = (tmp_path / "analysis_report.html").read_text(encoding="utf-8")
    manifest = json.loads((tmp_path / "analysis_report.json").read_text(encoding="utf-8"))
    assert "МЦД-2" in html
    assert "1.1.7" in html
    assert "mmv2" in html
    assert "modal-item--clickable" in html
    assert "data-evidence-label" in html
    assert "data-evidence-terms" in html
    assert "modal-item-tip" in html
    assert "Строки лога, из которых получено значение" in html
    assert manifest["counts"]["events"] == 1


def test_html_report_does_not_detect_carrier_from_message_text(tmp_path):
    event = replace(
        make_event(4, 250, "4.5.13", message="Карта в стоп листе. Оплатите долг в приложении «Метро Москвы»"),
        package="mgt_askp_9-oti-4.5.13",
        carrier="mgt_askp_9",
        raw_line=(
            "PaymentStart, resp: {Code:4 MessageRus:Карта в стоп листе. "
            "MessageEng:Card in stop-list. Pay the debt in Moscow Metro app "
            "BmSign:mgt_askp_9-oti-4.5.13.aes}"
        ),
    )
    result = analyze_events([event])

    write_html_report([event], result, tmp_path / "analysis_report.html")

    html = (tmp_path / "analysis_report.html").read_text(encoding="utf-8")
    report_data = json.loads(html.split('<script id="report-data" type="application/json">', 1)[1].split("</script>", 1)[0])
    assert report_data["events"][0]["carriers"] == ["АСКП"]


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
    assert "Проверки" in html
    assert "Сработало правил: 0 из 5" in html
    assert "Сработавших строк нет." in html
    assert 'id="bm-unclassified-root"><details' not in html
    assert 'id="bm-filter-root"' not in html
    assert "log_files" not in manifest["stable_sections"]
    assert "other_files" not in manifest["stable_sections"]
    assert "suspicious" not in manifest["stable_sections"]
    assert "validation_checks" in manifest["stable_sections"]
    assert "unclassified_diagnostics" not in manifest["stable_sections"]
    assert manifest["section_sources"]["bm_statuses"]["status"] == "available"


def test_writes_upload_composition_in_report_header_and_manifest(tmp_path):
    stats = PipelineStats(
        scanned_lines=2,
        malformed_payment_lines=0,
        extracted_files=2,
        input_files=["input/13-07-2026.zip"],
        analyzed_files=[
            "_workdir/extracted/13-07-2026.zip/bm/a.log",
            "_workdir/extracted/nested.zip/validator/start.log",
        ],
        input_source_summaries=[
            InputSourceSummary(
                source_file="input/13-07-2026.zip",
                input_kind="archive",
                log_types=["bm", "stopper", "oti_reader_library", "validator_app"],
                log_type_labels=["БМ", "ПО стоппера", "библиотеки ридера ОТИ", "ПО валидатора"],
                log_type_counts={"bm": 1, "stopper": 1, "oti_reader_library": 1, "validator_app": 1},
                log_type_evidence={
                    "bm": ["_workdir/extracted/13-07-2026.zip/bm/a.log: content:PaymentStart"],
                    "validator_app": ["_workdir/extracted/nested.zip/validator/start.log: content:validator_app"],
                },
                analyzed_files=[
                    "_workdir/extracted/13-07-2026.zip/bm/a.log",
                    "_workdir/extracted/nested.zip/validator/start.log",
                ],
                extracted_files=[
                    "_workdir/extracted/13-07-2026.zip/bm/a.log",
                    "_workdir/extracted/nested.zip/validator/start.log",
                ],
                archive_file_count=3,
                log_file_count=2,
                other_file_count=1,
                extracted_file_count=2,
                analyzed_file_count=2,
                skipped_file_count=1,
                skipped_reasons={"прочие файлы в архиве": 1},
            )
        ],
    )
    result = analyze_events([])

    write_html_report([], result, tmp_path / "analysis_report.html", stats=stats)

    html = (tmp_path / "analysis_report.html").read_text(encoding="utf-8")
    manifest = json.loads((tmp_path / "analysis_report.json").read_text(encoding="utf-8"))
    ai_context = json.loads((tmp_path / "analysis_report.ai_context.json").read_text(encoding="utf-8"))
    expected_text = "Загружен архив 13-07-2026.zip. Распознано типов логов: 4."
    assert "Состав загрузки" in html
    assert expected_text in html
    assert "Он содержит логи следующих типов:" in html
    assert "БМ" in html
    assert "ПО стоппера" in html
    assert "библиотеки ридера ОТИ" in html
    assert "ПО валидатора" in html
    assert "Полнота обработки" in html
    assert "Распознанные типы логов" in html
    assert '<details class="collapsible upload-composition-part upload-composition-details">' in html
    assert "Разделы отчёта и источники" in html
    assert "Файлов в источнике" in html
    assert "Проанализировано" in html
    assert "Покрытие log-файлов" in html
    assert "полностью" in html
    assert "2/2 (100.00%)" in html
    assert "прочие файлы в архиве: 1" in html
    assert "content:PaymentStart" in html
    assert "content:validator_app" in html
    assert "Скорость загрузки устройства" in html
    assert "BM-статусы" in html
    assert "доступен" in html
    assert "upload_composition" in manifest["stable_sections"]
    assert manifest["upload_composition"][0]["summary_text"] == expected_text
    assert manifest["upload_composition"][0]["log_types"] == ["bm", "stopper", "oti_reader_library", "validator_app"]
    assert manifest["upload_composition"][0]["log_type_counts"] == {
        "bm": 1,
        "stopper": 1,
        "oti_reader_library": 1,
        "validator_app": 1,
    }
    assert manifest["upload_composition"][0]["log_type_evidence"]["bm"] == [
        "_workdir/extracted/13-07-2026.zip/bm/a.log: content:PaymentStart"
    ]
    assert manifest["upload_composition"][0]["archive_file_count"] == 3
    assert manifest["upload_composition"][0]["log_file_count"] == 2
    assert manifest["upload_composition"][0]["other_file_count"] == 1
    assert manifest["upload_composition"][0]["extracted_file_count"] == 2
    assert manifest["upload_composition"][0]["analyzed_file_count"] == 2
    assert manifest["upload_composition"][0]["skipped_file_count"] == 1
    assert manifest["upload_composition"][0]["skipped_reasons"] == {"прочие файлы в архиве": 1}
    assert manifest["section_sources"]["upload_composition"]["data_source"] == "загруженные файлы"
    assert ai_context["input_sources"][0]["source_file"] == "input/13-07-2026.zip"
    assert ai_context["input_sources"][0]["log_type_counts"] == {
        "bm": 1,
        "stopper": 1,
        "oti_reader_library": 1,
        "validator_app": 1,
    }


def test_html_report_shows_non_emv_card_status_and_zero_row_toggle(tmp_path):
    stats = PipelineStats(scanned_lines=1, malformed_payment_lines=0, extracted_files=0)
    non_emv_event = replace(
        make_event(6, 100, "4.4.12", message="NON_EMV_CARD"),
        raw_line="2026-04-29 PaymentStart, resp: {Code:6 Message:NON_EMV_CARD}",
    )
    ok_event = replace(
        make_event(0, 100, "4.4.12", message="OK", payment_type=2, auth_type=0),
        raw_line="2026-04-29 PaymentStart, resp: {Code:0 Message:OK} error: no error",
    )
    result = analyze_events([non_emv_event, ok_event])

    write_html_report([non_emv_event, ok_event], result, tmp_path / "analysis_report.html", stats=stats)

    html = (tmp_path / "analysis_report.html").read_text(encoding="utf-8")
    assert "Скрыть строки с нулём" in html
    assert 'id="bm-status-hide-zero"' in html
    assert "bm-status-zero-toggle__switch" in html
    assert "bm-status-zero-toggle__thumb" in html
    assert "localStorage" in html
    assert 'data-count="${count}"' in html
    assert html.count("bm-status-hide-zero") >= 2
    assert html.count("Скрыть строки с нулём") >= 2
    assert "NON_EMV_CARD" in html
    assert html.index("<td>Отказ, ошибка ODA/CDA</td>") < html.index("<td>NON_EMV_CARD</td>")
    assert 'data-count="0"' in html
    assert "line-highlight" in html

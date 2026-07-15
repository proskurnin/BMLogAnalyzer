import json

from core.pipeline import run_analysis
from reports.html_report import write_html_report


def test_pipeline_builds_nbs_startup_report(tmp_path):
    input_dir = tmp_path / "input"
    extracted_dir = tmp_path / "extracted"
    input_dir.mkdir()
    (input_dir / "tt").mkdir()
    (input_dir / "tt" / "2026-07-15-10-50-21-158.log").write_text(
        "\n".join(
            [
                "[2026.07.15 10:50:21.158] {1160452024} T Log started",
                "[2026.07.15 10:50:29.356] {1986537256} T TicketProcessor: режим работы: MODE::VALIDATE",
                "[2026.07.15 10:50:39.628] {4236387826} D References_nbs_slm: начинается загрузка стоп-листов",
                "[2026.07.15 10:50:39.926] {4236387826} D References_nbs_slm: загрузка стоп-листов завершена за 0.3 сек.",
                "[2026.07.15 10:51:31.053] {2388773343} D bm::Connection: Send Commands::info with timeout: 5000",
                "[2026.07.15 10:51:31.900] {2388773343} T bm::Connection: Send Commands::info failed",
                "[2026.07.15 10:51:32.100] {2388773343} D InfoWithTimeout: timeout",
                "[2026.07.15 10:51:32.425] {2388773343} D reader status              : 0",
                "                             bm status                  : 64",
                "[2026.07.15 10:51:35.914] {2388773343} D ServiceBank: getInfo: QR data:",
                "[2026.07.15 10:52:31.091] {2388773343} D bm::Connection: Send Commands::info with timeout: 5000",
                "[2026.07.15 10:52:35.619] {2388773343} D reader status              : 0",
                "                             bm status                  : 0",
                "[2026.07.15 10:52:37.479] {2388773343} D ServiceBank: getInfo: QR data: https://transport.mos.ru/qr?abc",
            ]
        ),
        encoding="utf-8",
    )
    (input_dir / "logs" / "stopper").mkdir(parents=True)
    (input_dir / "logs" / "stopper" / "stopper-rotate.log").write_text(
        "\n".join(
            [
                'time="2026-07-15 10:51:00.000" level=info msg="readerConfiguration: ReaderConfiguration, req"',
                'time="2026-07-15 10:51:10.000" level=trace msg="UpdaterJobOnlyLists: work with db not allowed, skip"',
            ]
        ),
        encoding="utf-8",
    )

    events, result, stats = run_analysis(input_dir, extracted_dir=extracted_dir)

    assert len(stats.nbs_startup_reports) == 1
    report = stats.nbs_startup_reports[0]
    assert report.reader_type == "TT"
    assert report.mode_validate_to_qr_seconds == 128.123
    assert report.first_info_at.strftime("%H:%M:%S") == "10:51:31"
    assert report.first_ready_status_at.strftime("%H:%M:%S") == "10:52:35"
    assert report.ready_status_seen is True
    assert report.info_failure_count == 1
    assert report.info_timeout_count == 1
    assert report.stopper_load_marker_count == 0
    assert report.stopper_reader_configuration_count == 1
    assert report.stopper_skip_count == 1
    assert report.stoplist_search_max_ms == 300.0
    segments = {item.title: item.duration_seconds for item in report.segments}
    assert segments["Пауза НБС до первого Info"] == 61.697
    assert segments["MODE::VALIDATE до первого QR"] == 128.123
    assert segments["Первый статус 0/0 до первого QR"] == 1.86

    report_path = tmp_path / "report.html"
    write_html_report(events, result, report_path, stats=stats)
    html = report_path.read_text(encoding="utf-8")
    manifest = json.loads(report_path.with_suffix(".json").read_text(encoding="utf-8"))
    assert "Выход НБС в работу после открытия смены" in html
    assert "MODE::VALIDATE до первого QR" in html
    assert "Первый статус 0/0 до первого QR" in html
    assert "2 мин 08,123 сек" in html
    assert "nbs-startup-text-1" in html
    assert "Скопировать текстовый отчёт" in html
    assert "Фактические интервалы выхода НБС в работу и evidence-строки" in html
    assert "В окне MODE::VALIDATE -&gt; первый QR найдено признаков загрузки стоп-листов: 0." in html
    assert "Send Commands::info failed до первого QR: 1." in html
    assert "InfoWithTimeout до первого QR: 1." in html
    assert "nbs_startup" in manifest["stable_fields"]
    assert "nbs_startup" in manifest["stable_sections"]
    assert manifest["counts"]["nbs_startup_reports"] == 1
    assert manifest["nbs_startup"][0]["mode_validate_to_qr_seconds"] == 128.123
    assert manifest["nbs_startup"][0]["ready_status_seen"] is True
    assert manifest["nbs_startup"][0]["info_failure_count"] == 1
    assert manifest["nbs_startup"][0]["info_timeout_count"] == 1


def test_nbs_startup_report_counts_stopper_load_markers(tmp_path):
    input_dir = tmp_path / "input"
    extracted_dir = tmp_path / "extracted"
    input_dir.mkdir()
    (input_dir / "2026-07-15-11-19-55-443.log").write_text(
        "\n".join(
            [
                "[2026.07.15 11:19:55.443] {1160452024} T Log started",
                "[2026.07.15 11:19:56.443] {1160452024} D TicketProcessor: используется считыватель UNO:/dev/oti",
                "[2026.07.15 11:20:52.249] {1986537256} T TicketProcessor: режим работы: MODE::VALIDATE",
                "[2026.07.15 11:20:52.976] {2388773343} D bm::Connection: Send Commands::info with timeout: 5000",
                "[2026.07.15 11:21:01.384] {2388773343} D reader status : 0; bm status : 0",
                "[2026.07.15 11:21:02.058] {2388773343} D ServiceBank: getInfo: QR data: https://transport.mos.ru/qr?abc",
            ]
        ),
        encoding="utf-8",
    )
    (input_dir / "logs" / "stopper").mkdir(parents=True)
    (input_dir / "logs" / "stopper" / "stopper-rotate.log").write_text(
        'time="2026-07-15 11:21:00.000" level=info msg="UpdateByDiffApply: apply diff"\n',
        encoding="utf-8",
    )

    _, _, stats = run_analysis(input_dir, extracted_dir=extracted_dir)

    assert len(stats.nbs_startup_reports) == 1
    assert stats.nbs_startup_reports[0].reader_type == "OTI"
    assert stats.nbs_startup_reports[0].stopper_load_marker_count == 1

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
                "[2026.07.15 10:50:40.926] {4236387826} D StopListDb: поиск карты: 400 мс",
                "[2026.07.15 10:50:41.926] {4236387826} D References_nbs_slm: загрузка стоп-листов завершена за 3.6 сек.",
                "[2026.07.15 10:51:31.053] {2388773343} D bm::Connection: Send Commands::info with timeout: 5000",
                "[2026.07.15 10:51:31.900] {2388773343} T bm::Connection: Send Commands::info failed",
                "[2026.07.15 10:51:32.100] {2388773343} D InfoWithTimeout: timeout",
                "[2026.07.15 10:51:32.425] {2388773343} D reader status              : 0",
                "                             bm status                  : 64",
                "[2026.07.15 10:51:35.914] {2388773343} D ServiceBank: getInfo: QR data:",
                "[2026.07.15 10:52:31.091] {2388773343} D bm::Connection: Send Commands::info with timeout: 5000",
                "[2026.07.15 10:52:35.619] {2388773343} D reader status              : 0",
                "                             bm status                  : 0",
                "                             bm number                  : 2113001847384",
                "[2026.07.15 10:52:37.479] {2388773343} D ServiceBank: getInfo: QR data: https://transport.mos.ru/qr?abc",
            ]
        ),
        encoding="utf-8",
    )
    (input_dir / "logs" / "bm").mkdir(parents=True)
    (input_dir / "logs" / "bm" / "bm-rotate.log").write_text(
        "\n".join(
            [
                'time="2026-07-15 10:52:32.000" level=info msg="Info, req: status: st: 0, ist: 0 req: &{TmSerialNumber:1847384} tid: abc, rid: 2113001847384"',
                'time="2026-07-15 10:52:33.500" level=info msg="Info, resp: 1500ms, {ReaderStatus:0 BmStatus:0 BmNumber:2113001847384 QrData:https://transport.mos.ru/qr?abc} tid: abc, rid: 2113001847384"',
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
    assert report.stoplist_search_max_ms == 3600.0
    assert report.stoplist_search_stats.count == 3
    assert report.stoplist_search_stats.min_ms == 300.0
    assert report.stoplist_search_stats.max_ms == 3600.0
    assert report.stoplist_search_stats.median_ms == 400.0
    assert len(report.stoplist_search_stats.outlier_evidence) == 1
    assert report.problem_candidate is True
    assert report.classification == "problem"
    assert "BM Info сопоставлен с устройством" in report.classification_reasons
    assert report.device_ids["BmNumber"] == ["2113001847384"]
    assert report.bm_info_correlation is not None
    assert report.bm_info_correlation.status == "matched"
    assert report.bm_info_correlation.device_identity == "2113001847384"
    assert report.bm_info_correlation.bm_req_to_resp_seconds == 1.5
    assert report.bm_info_correlation.bm_resp_to_qr_seconds == 3.979
    assert report.bm_info_correlation.bm_info_duration_ms == 1500.0
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
    assert "BM Info корреляция: сопоставлен." in html
    assert "StopListDb/References_nbs_slm count/min/max/avg/median/p90/p95" in html
    assert "nbs_startup" in manifest["stable_fields"]
    assert "nbs_startup" in manifest["stable_sections"]
    assert manifest["counts"]["nbs_startup_reports"] == 1
    assert manifest["nbs_startup"][0]["mode_validate_to_qr_seconds"] == 128.123
    assert manifest["nbs_startup"][0]["classification"] == "problem"
    assert manifest["nbs_startup"][0]["session_phase"] == "after_log_started"
    assert manifest["nbs_startup"][0]["problem_candidate"] is True
    assert manifest["nbs_startup"][0]["device_ids"]["BmNumber"] == ["2113001847384"]
    assert manifest["nbs_startup"][0]["bm_info_correlation"]["status"] == "matched"
    assert manifest["nbs_startup"][0]["bm_info_correlation"]["bm_resp_to_qr_seconds"] == 3.979
    assert manifest["nbs_startup"][0]["stoplist_search_stats"]["count"] == 3
    assert manifest["nbs_startup"][0]["stoplist_search_stats"]["max_ms"] == 3600.0
    assert manifest["nbs_startup"][0]["qr_state_change_count"] == 1
    assert manifest["nbs_startup"][0]["ready_status_seen"] is True
    assert manifest["nbs_startup"][0]["info_failure_count"] == 1
    assert manifest["nbs_startup"][0]["info_timeout_count"] == 1
    assert "QR пропал" not in html


def test_nbs_startup_report_counts_stopper_load_markers(tmp_path):
    input_dir = tmp_path / "input"
    extracted_dir = tmp_path / "extracted"
    input_dir.mkdir()
    (input_dir / "2026-07-15-11-19-55-443.log").write_text(
        "\n".join(
            [
                "[2026.07.15 11:19:55.443] {1160452024} T Log started",
                "[2026.07.15 11:20:00.443] {1160452024} D TicketProcessor: режим работы: MODE::SESSION_CLOSED",
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
    assert stats.nbs_startup_reports[0].session_phase == "after_session_closed"
    assert stats.nbs_startup_reports[0].problem_candidate is False
    assert stats.nbs_startup_reports[0].classification == "excluded"
    assert "marker-нагрузки stopper" in stats.nbs_startup_reports[0].exclusion_reasons[0]


def test_nbs_startup_forbids_ambiguous_bm_info_correlation(tmp_path):
    input_dir = tmp_path / "input"
    extracted_dir = tmp_path / "extracted"
    input_dir.mkdir()
    (input_dir / "2026-07-15-12-00-00-000.log").write_text(
        "\n".join(
            [
                "[2026.07.15 12:00:00.000] {1} T Log started",
                "[2026.07.15 12:00:01.000] {2} T TicketProcessor: режим работы: MODE::VALIDATE",
                "[2026.07.15 12:00:02.000] {3} D bm::Connection: Send Commands::info with timeout: 5000",
                "[2026.07.15 12:00:07.000] {4} D reader status : 0; bm status : 0",
                "[2026.07.15 12:00:10.000] {5} D ServiceBank: getInfo: QR data: https://transport.mos.ru/qr?abc",
            ]
        ),
        encoding="utf-8",
    )
    (input_dir / "logs" / "bm").mkdir(parents=True)
    (input_dir / "logs" / "bm" / "bm-rotate.log").write_text(
        "\n".join(
            [
                'time="2026-07-15 12:00:03.000" level=info msg="Info, req: tid: a1, rid: 1001"',
                'time="2026-07-15 12:00:04.000" level=info msg="Info, resp: 1000ms, {BmNumber:1001 QrData:x} tid: a1, rid: 1001"',
                'time="2026-07-15 12:00:05.000" level=info msg="Info, req: tid: b2, rid: 2002"',
                'time="2026-07-15 12:00:06.000" level=info msg="Info, resp: 1000ms, {BmNumber:2002 QrData:x} tid: b2, rid: 2002"',
            ]
        ),
        encoding="utf-8",
    )

    _, _, stats = run_analysis(input_dir, extracted_dir=extracted_dir)

    assert len(stats.nbs_startup_reports) == 1
    correlation = stats.nbs_startup_reports[0].bm_info_correlation
    assert correlation is not None
    assert correlation.status == "ambiguous"
    assert correlation.bm_resp_to_qr_seconds is None
    assert correlation.candidate_count == 4
    assert stats.nbs_startup_reports[0].problem_candidate is False
    assert stats.nbs_startup_reports[0].classification == "excluded"
    assert "BM Info не сопоставлен" in stats.nbs_startup_reports[0].exclusion_reasons[-1]

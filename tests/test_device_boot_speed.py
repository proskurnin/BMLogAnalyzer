import json

from core.pipeline import run_analysis
from reports.html_report import write_html_report


def test_pipeline_builds_device_boot_speed_report(tmp_path):
    input_dir = tmp_path / "input"
    extracted_dir = tmp_path / "extracted"
    input_dir.mkdir()
    (input_dir / "a_validator.log").write_text(
        "\n".join(
            [
                "[2026-Jul-13 14:18:26.636522] [VALIDATOR] STARTED",
                "[2026-Jul-13 14:18:26.637000] version_major: 1",
                "[2026-Jul-13 14:18:26.638000] version_middle: 13",
                "[2026-Jul-13 14:18:26.639000] version_minor: 53",
                "[2026-Jul-13 14:18:26.640000] version_build: 0",
                "[2026-Jul-13 14:18:26.641000] serial: 59757",
                "[2026-Jul-13 14:18:26.642000] route: 1469",
                "[2026-Jul-13 14:18:26.650000] reader type: OTI",
                "[14:18:26.703695] ACTIVATE REFERENCES",
                "[14:18:32.346324] End LOAD DEVICE SETTINGS: OK",
                "[14:18:32.470673] Can't open and connect socket",
                "[14:18:42.542874] connect: OK",
                "[14:18:46.017063] Open reader SUCCESS",
                "[14:18:46.266258] End start reader",
                "[14:18:46.266537] Init QR",
                "[14:18:58.273869] Open QR failed",
                "[14:19:03.276020] Open QR failed",
                "[14:19:08.482585] QR NOT FOUND",
                "[14:19:08.528843] /validator/bm_modules/17/bm.sh stop",
                "[14:19:21.715082] [choose_and_start_bm]",
                "[14:19:33.871541] found for route: 1469 bm type: 17",
                "[14:19:33.873867] START BM AND WAIT 30 seconds!",
                "[14:19:33.874002] start BM: /validator/bm_modules/17/bm.sh start",
                "[14:20:51.375781] START COMPLETED!",
                "[14:20:51.377149] [error] send error: 1",
                "[14:20:52.406207] Info response",
                "Reader status: 0",
                "Bm status: 64",
                "Bm version: 4.5.13",
                "[14:20:52.407112] current protocol: 2",
                "[14:20:52.407231] Stop reader",
                "[14:20:52.917525] End stop reader",
                "[14:20:57.640391] Info response",
                "Reader status: 0",
                "Bm status: 64",
                "[14:20:57.641179] [updateConfiguration] Started",
                "[14:20:59.641423] Send Commands::updateConfiguration",
                "[14:21:01.647277] Send Commands::updateConfiguration",
                "[14:21:01.652634] [updateConfiguration] result: 1",
                "[14:21:01.853140] [bmInfoRequest] Start",
                "[14:21:01.909764] Info response",
                "Reader status: 0",
                "Bm status: 0",
            ]
        ),
        encoding="utf-8",
    )
    (input_dir / "z_bm.log").write_text(
        "\n".join(
            [
                'time="2026-07-13 14:20:25.601" level=info msg="listening TCP requests on :5888"',
                'time="2026-07-13 14:20:59.730" level=info msg="UpdateConfiguration: Stage1"',
                'time="2026-07-13 14:21:01.650" level=info msg="aes: UpdateSuccess: true"',
                'time="2026-07-13 14:21:01.650" level=info msg="UpdateConfiguration: serve.ConfigurationStatusWork"',
            ]
        ),
        encoding="utf-8",
    )

    events, result, stats = run_analysis(input_dir, extracted_dir=extracted_dir)

    assert result.total == 0
    assert len(stats.device_boot_reports) == 1
    report = stats.device_boot_reports[0]
    assert report.validator_serial == "59757"
    assert report.route == "1469"
    assert report.validator_version == "1.13.53.0"
    assert report.bm_version == "4.5.13"
    assert report.reader_type == "OTI"
    assert report.total_seconds == 155.273
    assert report.segments[0].duration_seconds == 5.71
    assert report.segments[4].duration_seconds == 51.727
    assert report.segments[9].duration_seconds == 0.257
    assert {item.log_type for item in stats.log_inventory} == {"bm", "validator_app"}

    write_html_report(events, result, tmp_path / "analysis_report.html", stats=stats)
    html = (tmp_path / "analysis_report.html").read_text(encoding="utf-8")
    manifest = json.loads((tmp_path / "analysis_report.json").read_text(encoding="utf-8"))
    assert "Скорость загрузки устройства" in html
    assert "АСКП_59757. Запуск 13.07.2026 в 14:18:26" in html
    assert "АСКП. Справочники и настройки" in html
    assert "АСКП и БМ. Контрольный Info 0/0" in html
    assert "UpdateSuccess: true" in html
    assert "155,273 секунды" in html
    assert "device_boot_speed" in manifest["stable_sections"]
    assert manifest["device_boot_speed"][0]["validator_serial"] == "59757"

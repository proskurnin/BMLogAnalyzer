import json

from core.pipeline import run_analysis
from reports.html_report import write_html_report


def test_pipeline_builds_validator_info_chain_report_and_links_boot(tmp_path):
    input_dir = tmp_path / "input"
    extracted_dir = tmp_path / "extracted"
    input_dir.mkdir()
    (input_dir / "a_boot.log").write_text(
        "\n".join(
            [
                "[2026-Jul-13 15:58:10.000000] [VALIDATOR] STARTED",
                "[2026-Jul-13 15:58:10.001000] serial: 59757",
                "[15:58:20.000000] START COMPLETED!",
                "[16:00:20.000000] Info response",
                "Reader status: 0",
                "Bm status: 0",
            ]
        ),
        encoding="utf-8",
    )
    (input_dir / "Workstation.ValidatorNT" / "59757-2026-07-13-15-59-27-157.log").parent.mkdir()
    (input_dir / "Workstation.ValidatorNT" / "59757-2026-07-13-15-59-27-157.log").write_text(
        "\n".join(
            [
                "[2026.07.13 15:59:44.594] {2388773343} D bm::Connection: Send Commands::info with timeout: 5000",
                "[2026.07.13 15:59:44.599] {2388773343} T bm::Connection: Connection endpoint: 127.0.0.1:5888",
                "[2026.07.13 15:59:44.632] {2388773343} T bm::Connection: Connection succeed",
                "[2026.07.13 15:59:44.636] {2388773343} D bm::Connection: Write buffer: 000000a6",
                "[2026.07.13 15:59:48.842] {2388773343} T bm::Connection: Writting succeed",
                "[2026.07.13 15:59:49.342] {2388773343} T bm::Connection: Send Commands::info succeed",
            ]
        ),
        encoding="utf-8",
    )

    events, result, stats = run_analysis(input_dir, extracted_dir=extracted_dir)

    assert result.total == 0
    assert len(stats.device_boot_reports) == 1
    assert len(stats.validator_info_chain_reports) == 1
    report = stats.validator_info_chain_reports[0]
    assert report.duration_seconds == 4.748
    assert report.endpoint == "127.0.0.1:5888"
    assert report.thread_id == "2388773343"
    assert report.linked_boot_title == "АСКП_59757. Запуск 13.07.2026 в 15:58:10"

    write_html_report(events, result, tmp_path / "analysis_report.html", stats=stats)
    html = (tmp_path / "analysis_report.html").read_text(encoding="utf-8")
    manifest = json.loads((tmp_path / "analysis_report.json").read_text(encoding="utf-8"))

    assert "Цепочки Info ПО валидатора" in html
    assert "Медленных цепочек Info: 1" in html
    assert "127.0.0.1:5888" in html
    assert "validator_info_chains" in manifest["stable_sections"]
    assert manifest["validator_info_chains"][0]["duration_seconds"] == 4.748
    assert manifest["validator_info_chains"][0]["linked_boot_title"] == "АСКП_59757. Запуск 13.07.2026 в 15:58:10"


def test_validator_info_chain_does_not_link_ambiguous_boot_reports(tmp_path):
    input_dir = tmp_path / "input"
    extracted_dir = tmp_path / "extracted"
    input_dir.mkdir()
    (input_dir / "a_boot_1.log").write_text(
        "\n".join(
            [
                "[2026-Jul-13 15:58:10.000000] [VALIDATOR] STARTED",
                "[2026-Jul-13 15:58:10.001000] serial: 11111",
                "[16:00:20.000000] Info response",
                "Reader status: 0",
                "Bm status: 0",
            ]
        ),
        encoding="utf-8",
    )
    (input_dir / "b_boot_2.log").write_text(
        "\n".join(
            [
                "[2026-Jul-13 15:58:20.000000] [VALIDATOR] STARTED",
                "[2026-Jul-13 15:58:20.001000] serial: 22222",
                "[16:00:30.000000] Info response",
                "Reader status: 0",
                "Bm status: 0",
            ]
        ),
        encoding="utf-8",
    )
    (input_dir / "Workstation.ValidatorNT.log").write_text(
        "\n".join(
            [
                "[2026.07.13 15:59:44.000] {2388773343} D bm::Connection: Send Commands::info with timeout: 5000",
                "[2026.07.13 15:59:44.100] {2388773343} T bm::Connection: Connection endpoint: 127.0.0.1:5888",
                "[2026.07.13 15:59:44.200] {2388773343} T bm::Connection: Connection succeed",
                "[2026.07.13 15:59:44.300] {2388773343} D bm::Connection: Write buffer: 000000a6",
                "[2026.07.13 15:59:48.000] {2388773343} T bm::Connection: Writting succeed",
                "[2026.07.13 15:59:48.100] {2388773343} T bm::Connection: Send Commands::info failed",
            ]
        ),
        encoding="utf-8",
    )

    _, _, stats = run_analysis(input_dir, extracted_dir=extracted_dir)

    assert len(stats.validator_info_chain_reports) == 1
    assert stats.validator_info_chain_reports[0].linked_boot_title is None

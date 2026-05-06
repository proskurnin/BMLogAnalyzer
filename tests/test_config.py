from core.config import load_app_config


def test_loads_paths_and_report_flags_from_config(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "\n".join(
            [
                "input_path: ./input",
                "reports_dir: ./reports",
                "extracted_dir: ./extracted",
                "reports:",
                "  parsed_events: false",
                "  analysis_report_html: false",
            ]
        ),
        encoding="utf-8",
    )

    config = load_app_config(config_path)

    assert config.input_path == "./input"
    assert config.reports_dir == "./reports"
    assert config.extracted_dir == "./extracted"
    assert not config.report_config.enabled("parsed_events")
    assert not config.report_config.enabled("analysis_report_html")
    assert config.report_config.enabled("summary_by_code")

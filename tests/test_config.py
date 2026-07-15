from core.config import load_app_config
from web.settings import load_settings


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
                "device_boot_diagnostics:",
                "  long_qr_seconds: 12.5",
                "  frequent_bm_stop_count: 3",
                "  slow_info_chain_seconds: 4.5",
                "  info_chain_duration_ratio: 1.7",
                "  version_duration_ratio: 1.5",
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
    assert config.device_boot_diagnostics.long_qr_seconds == 12.5
    assert config.device_boot_diagnostics.frequent_bm_stop_count == 3
    assert config.device_boot_diagnostics.slow_info_chain_seconds == 4.5
    assert config.device_boot_diagnostics.info_chain_duration_ratio == 1.7
    assert config.device_boot_diagnostics.version_duration_ratio == 1.5


def test_load_settings_uses_explicit_auth_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("BM_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("BM_AUTH_DIR", str(tmp_path / "shared-auth"))

    settings = load_settings()

    assert settings.data_dir == tmp_path / "data"
    assert settings.auth_dir == tmp_path / "shared-auth"


def test_load_settings_defaults_auth_dir_to_data_dir_auth(tmp_path, monkeypatch):
    monkeypatch.setenv("BM_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.delenv("BM_AUTH_DIR", raising=False)

    settings = load_settings()

    assert settings.auth_dir == tmp_path / "data" / "auth"

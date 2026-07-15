from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


DEFAULT_REPORTS: tuple[str, ...] = (
    "report_metadata",
    "parsed_events",
    "summary_by_code",
    "summary_by_message",
    "summary_by_bm_version",
    "summary_by_reader_type",
    "summary_by_reader_firmware",
    "summary_by_classification",
    "summary_by_duration_bucket",
    "known_codes",
    "unknown_codes",
    "diagnostics",
    "file_diagnostics",
    "bundle_manifest",
    "bundle_manifest_json",
    "archive_inventory",
    "summary_by_archive_category",
    "log_inventory",
    "summary_by_log_type",
    "bm_log_versions",
    "bm_status_summary",
    "reader_models",
    "reader_firmware_versions",
    "reader_firmware_timeline",
    "summary_reader_firmware_timeline",
    "reader_error_summary",
    "system_error_summary",
    "other_logs",
    "error_events",
    "technical_error_events",
    "errors_by_file",
    "file_error_overview",
    "repeat_attempts_after_failure",
    "summary_repeat_attempts_after_failure",
    "read_error_repeat_outcomes",
    "summary_read_error_repeat_outcomes",
    "timeout_repeat_outcomes",
    "summary_timeout_repeat_outcomes",
    "no_card_repeat_outcomes",
    "summary_no_card_repeat_outcomes",
    "card_check_markers",
    "summary_card_check_markers",
    "oda_cda_repeat_outcomes",
    "summary_oda_cda_repeat_outcomes",
    "card_identity_markers",
    "summary_card_identity_markers",
    "card_fingerprint_events",
    "read_error_card_history",
    "summary_read_error_card_history",
    "timeout_card_history",
    "summary_timeout_card_history",
    "no_card_card_history",
    "summary_no_card_card_history",
    "check_results",
    "check_summary",
    "protocol_scenario_results",
    "protocol_scenario_summary",
    "analysis_report_html",
    "comparison_by_bm_version",
    "comparison_by_reader_type",
    "matrix_bm_version_by_code",
    "matrix_reader_type_by_code",
    "matrix_bm_version_by_classification",
    "matrix_reader_type_by_classification",
)


@dataclass(frozen=True)
class ReportConfig:
    reports: dict[str, bool] = field(default_factory=lambda: {name: True for name in DEFAULT_REPORTS})

    def enabled(self, name: str) -> bool:
        return self.reports.get(name, True)

    def enabled_count(self) -> int:
        return sum(1 for enabled in self.reports.values() if enabled)


@dataclass(frozen=True)
class DeviceBootDiagnosticsConfig:
    long_qr_seconds: float = 10.0
    frequent_bm_stop_count: int = 2
    fixed_wait_min_seconds: float = 25.0
    fixed_wait_max_seconds: float = 35.0
    long_bm_start_seconds: float = 30.0
    long_first_info_seconds: float = 10.0
    version_duration_ratio: float = 1.25


@dataclass(frozen=True)
class AppConfig:
    input_path: str = "./_workdir/input"
    reports_dir: str = "./_workdir/reports"
    extracted_dir: str = "./_workdir/extracted"
    report_config: ReportConfig = field(default_factory=ReportConfig)
    device_boot_diagnostics: DeviceBootDiagnosticsConfig = field(default_factory=DeviceBootDiagnosticsConfig)


def load_app_config(path: Path | str) -> AppConfig:
    config_path = Path(path)
    if not config_path.exists():
        return AppConfig()

    data = _parse_simple_yaml(config_path.read_text(encoding="utf-8"))
    reports = {name: True for name in DEFAULT_REPORTS}
    reports.update({key: bool(value) for key, value in data.get("reports", {}).items()})
    diagnostics = _device_boot_diagnostics_config(data.get("device_boot_diagnostics", {}))
    return AppConfig(
        input_path=str(data.get("input_path", AppConfig.input_path)),
        reports_dir=str(data.get("reports_dir", AppConfig.reports_dir)),
        extracted_dir=str(data.get("extracted_dir", AppConfig.extracted_dir)),
        report_config=ReportConfig(reports=reports),
        device_boot_diagnostics=diagnostics,
    )


def _device_boot_diagnostics_config(value: object) -> DeviceBootDiagnosticsConfig:
    if not isinstance(value, dict):
        return DeviceBootDiagnosticsConfig()
    defaults = DeviceBootDiagnosticsConfig()
    return DeviceBootDiagnosticsConfig(
        long_qr_seconds=_float_value(value.get("long_qr_seconds"), defaults.long_qr_seconds),
        frequent_bm_stop_count=_int_value(value.get("frequent_bm_stop_count"), defaults.frequent_bm_stop_count),
        fixed_wait_min_seconds=_float_value(value.get("fixed_wait_min_seconds"), defaults.fixed_wait_min_seconds),
        fixed_wait_max_seconds=_float_value(value.get("fixed_wait_max_seconds"), defaults.fixed_wait_max_seconds),
        long_bm_start_seconds=_float_value(value.get("long_bm_start_seconds"), defaults.long_bm_start_seconds),
        long_first_info_seconds=_float_value(value.get("long_first_info_seconds"), defaults.long_first_info_seconds),
        version_duration_ratio=_float_value(value.get("version_duration_ratio"), defaults.version_duration_ratio),
    )


def _float_value(value: object, default: float) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _int_value(value: object, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_simple_yaml(text: str) -> dict[str, object]:
    result: dict[str, object] = {}
    current_section: str | None = None
    for raw_line in text.splitlines():
        line = raw_line.split("#", maxsplit=1)[0].rstrip()
        if not line.strip():
            continue
        if not raw_line.startswith((" ", "\t")):
            key, value = _split_key_value(line)
            if value is None:
                result[key] = {}
                current_section = key
            else:
                result[key] = _parse_scalar(value)
                current_section = None
            continue
        if current_section is None:
            continue
        key, value = _split_key_value(line.strip())
        section = result.setdefault(current_section, {})
        if not isinstance(section, dict):
            continue
        section[key] = _parse_scalar(value or "")
    return result


def _split_key_value(line: str) -> tuple[str, str | None]:
    if ":" not in line:
        return line.strip(), None
    key, value = line.split(":", maxsplit=1)
    value = value.strip()
    return key.strip(), value if value else None


def _parse_scalar(value: str) -> object:
    normalized = value.strip().strip('"').strip("'")
    if normalized.lower() in {"true", "yes", "on"}:
        return True
    if normalized.lower() in {"false", "no", "off"}:
        return False
    return normalized

from core.models import InputSourceSummary, LogFileInventory, PipelineStats
from reports.section_registry import build_section_sources


def test_section_sources_mark_available_partial_and_static_sections():
    stats = PipelineStats(
        scanned_lines=0,
        malformed_payment_lines=0,
        extracted_files=0,
        log_inventory=[
            LogFileInventory(
                source_file="bm.log",
                log_type="bm",
                detection_method="content",
                evidence="PaymentStart resp",
            )
        ],
    )

    section_sources = build_section_sources(
        stats,
        [],
        ["bm_statuses", "device_boot_speed", "log_files"],
    )

    assert section_sources["bm_statuses"]["status"] == "available"
    assert section_sources["bm_statuses"]["matched_log_type_labels"] == ["БМ"]
    assert section_sources["device_boot_speed"]["status"] == "partial"
    assert section_sources["device_boot_speed"]["missing_log_type_labels"] == ["ПО валидатора"]
    assert section_sources["log_files"]["status"] == "not_required"
    assert section_sources["log_files"]["note"] == "Источник данных: структура архива."


def test_section_sources_use_upload_composition_when_inventory_is_empty():
    stats = PipelineStats(
        scanned_lines=0,
        malformed_payment_lines=0,
        extracted_files=0,
        input_source_summaries=[
            InputSourceSummary(
                source_file="input/13-07-2026.zip",
                input_kind="archive",
                log_types=["bm", "validator_app"],
            )
        ],
    )

    section_sources = build_section_sources(stats, [], ["device_boot_speed"])

    assert section_sources["device_boot_speed"]["status"] == "available"
    assert section_sources["device_boot_speed"]["required_log_type_labels"] == ["ПО валидатора", "БМ"]

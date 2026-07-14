from analytics.log_inventory import (
    LogInventoryCollector,
    bm_log_date_range,
    bm_log_version_counts,
    error_status_counts_by_type,
    log_type_counts,
    reader_firmware_counts,
    reader_model_counts,
)


def test_collects_log_inventory_by_type_versions_dates_and_reader_metadata():
    collector = LogInventoryCollector()
    collector.observe_line(
        "bm/a.log",
        "2026-04-30 10:00:00.000 PaymentStart, resp: {Code:0 Message:OK} p: mgt_nbs-oti-4.4.12",
    )
    collector.observe_line("reader/r.log", "2026-05-01 reader model=RDR-1 ReaderVersion:1.2.3 error: bad frame")
    collector.observe_line("system/syslog.log", "2026-05-02 kernel: timeout waiting for service")
    collector.observe_line("logs/stopper/stopper-rotate.log", "2026-05-03 p: stopper-arm7_32-4.5.13 readerConfiguration: OK")
    collector.observe_line("lib/liboti_reader.log", "2026-05-04 oti_reader_library open device failed")
    collector.observe_line("misc/app.log", "2026-05-03 hello")

    inventory = collector.finalize()

    assert log_type_counts(inventory) == {
        "bm": 1,
        "oti_reader_library": 1,
        "other": 1,
        "reader": 1,
        "stopper": 1,
        "system": 1,
    }
    assert bm_log_version_counts(inventory) == {"4.4.12": 1}
    assert bm_log_date_range(inventory) == "2026-04-30"
    assert reader_model_counts(inventory) == {"RDR-1": 1}
    assert reader_firmware_counts(inventory) == {("RDR-1", "1.2.3"): 1}
    assert error_status_counts_by_type(inventory, "reader") == {"error": 1}
    assert error_status_counts_by_type(inventory, "system") == {"timeout": 1}
    assert error_status_counts_by_type(inventory, "oti_reader_library") == {"failure": 1}


def test_bm_markers_win_over_generic_stopper_content():
    collector = LogInventoryCollector()
    source = "logs/bm/bm-rotate.log"
    collector.observe_line(
        source,
        "2026-04-30 PaymentStart, resp: {Code:0 Message:OK} p: mgt_nbs-oti-4.4.12",
    )
    collector.observe_line(source, "readerConfiguration: OK")

    inventory = collector.finalize()

    assert len(inventory) == 1
    assert inventory[0].log_type == "bm"
    assert "content:stopper" in inventory[0].evidence


def test_temp_directory_name_with_bm_does_not_create_bm_path_hint():
    collector = LogInventoryCollector()
    collector.observe_line("/private/tmp/bmlog-work/validator/app.log", "2026-05-03 hello")

    inventory = collector.finalize()

    assert len(inventory) == 1
    assert inventory[0].log_type == "other"
    assert "path:bm" not in inventory[0].evidence


def test_explicit_bm_path_wins_over_validator_content():
    collector = LogInventoryCollector()
    collector.observe_line("logs/bm-std/bm.current.log", "[VALIDATOR] STARTED")
    collector.observe_line("logs/bm-std/bm.current.log", "START COMPLETED")

    inventory = collector.finalize()

    assert len(inventory) == 1
    assert inventory[0].log_type == "bm"
    assert "content:validator_app" in inventory[0].evidence
    assert "path:bm" in inventory[0].evidence


def test_bm_token_in_file_name_creates_bm_path_hint():
    collector = LogInventoryCollector()
    collector.observe_line("input/z_bm.log", "2026-05-03 hello")

    inventory = collector.finalize()

    assert len(inventory) == 1
    assert inventory[0].log_type == "bm"
    assert "path:bm" in inventory[0].evidence


def test_validator_markers_detect_validator_log_without_validator_path():
    collector = LogInventoryCollector()
    collector.observe_line("input/startup.log", "[14:18:32.346324] End LOAD DEVICE SETTINGS: OK")
    collector.observe_line("input/startup.log", "[14:21:01.853140] [bmInfoRequest] Start")

    inventory = collector.finalize()

    assert len(inventory) == 1
    assert inventory[0].log_type == "validator_app"
    assert "content:validator_app" in inventory[0].evidence


def test_system_markers_detect_system_log_without_system_path():
    collector = LogInventoryCollector()
    collector.observe_line("input/messages.log", "2026-07-13 systemd[1]: Started nginx.service")
    collector.observe_line("input/messages.log", "2026-07-13 kernel: audit: apparmor denied")

    inventory = collector.finalize()

    assert len(inventory) == 1
    assert inventory[0].log_type == "system"
    assert "content:system" in inventory[0].evidence

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
    collector.observe_line("misc/app.log", "2026-05-03 hello")

    inventory = collector.finalize()

    assert log_type_counts(inventory) == {"bm": 1, "other": 1, "reader": 1, "system": 1}
    assert bm_log_version_counts(inventory) == {"4.4.12": 1}
    assert bm_log_date_range(inventory) == "2026-04-30"
    assert reader_model_counts(inventory) == {"RDR-1": 1}
    assert reader_firmware_counts(inventory) == {("RDR-1", "1.2.3"): 1}
    assert error_status_counts_by_type(inventory, "reader") == {"error": 1}
    assert error_status_counts_by_type(inventory, "system") == {"timeout": 1}

from dataclasses import replace
from datetime import datetime

from analytics.reader_firmware_timeline import (
    reader_firmware_timeline_rows,
    reader_firmware_timeline_summary_rows,
)
from core.models import ArchiveInventoryRow, LogFileInventory
from tests.test_counters import make_event


def test_reader_firmware_timeline_counts_firmware_changes_within_source_file():
    first = replace(
        make_event(0, reader_type="TT"),
        source_file="bm/a.log",
        line_number=10,
        timestamp=datetime(2026, 5, 1, 10, 0, 0),
        reader_firmware="1.2.3",
        raw_line="PaymentStart resp firmware:1.2.3",
    )
    second = replace(
        make_event(0, reader_type="TT"),
        source_file="bm/a.log",
        line_number=20,
        timestamp=datetime(2026, 5, 1, 10, 1, 0),
        reader_firmware="1.2.4",
        raw_line="PaymentStart resp firmware:1.2.4",
    )

    rows = reader_firmware_timeline_rows([second, first])
    summary = {
        row["metric"]: row
        for row in reader_firmware_timeline_summary_rows([first, second], [], [])
    }

    assert [row["reader_firmware"] for row in rows] == ["1.2.3", "1.2.4"]
    assert summary["payment_events_with_reader_firmware"]["value"] == 2
    assert summary["distinct_reader_firmware_versions"]["value"] == 2
    assert summary["firmware_change_points"]["value"] == 1
    assert summary["files_with_multiple_firmware_versions"]["value"] == 1


def test_reader_firmware_summary_states_when_runtime_firmware_is_absent():
    inventory = [
        LogFileInventory(
            source_file="reader/r.log",
            log_type="reader",
            detection_method="content_or_path_rules",
            evidence="path:reader",
        )
    ]
    archive_inventory = [
        ArchiveInventoryRow(
            archive="input/a.zip",
            category="Reader firmware binary",
            count=3,
        )
    ]

    summary = {
        row["metric"]: row
        for row in reader_firmware_timeline_summary_rows(
            [make_event(0)],
            inventory,
            archive_inventory,
        )
    }

    assert summary["payment_events_with_reader_firmware"]["value"] == 0
    assert summary["reader_log_files"]["value"] == 1
    assert summary["reader_firmware_binary_files"]["value"] == 3
    assert "not found" in summary["fact_from_logs"]["message"]

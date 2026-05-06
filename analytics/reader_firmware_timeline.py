from __future__ import annotations

from collections import Counter, defaultdict

from core.models import ArchiveInventoryRow, LogFileInventory, PaymentEvent


def reader_firmware_timeline_rows(events: list[PaymentEvent]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for event in sorted(events, key=lambda item: (item.source_file, _timestamp_key(item), item.line_number)):
        if not event.reader_firmware:
            continue
        rows.append(
            {
                "source_file": event.source_file,
                "line_number": event.line_number,
                "timestamp": event.timestamp.isoformat(sep=" ") if event.timestamp else "",
                "reader_type": event.reader_type or "",
                "reader_firmware": event.reader_firmware,
                "bm_version": event.bm_version or "",
                "code": event.code if event.code is not None else "",
                "message": event.message or "",
                "raw_line": event.raw_line,
            }
        )
    return rows


def reader_firmware_timeline_summary_rows(
    events: list[PaymentEvent],
    inventory: list[LogFileInventory],
    archive_inventory: list[ArchiveInventoryRow],
) -> list[dict[str, object]]:
    events_with_firmware = [event for event in events if event.reader_firmware]
    firmwares = sorted({event.reader_firmware for event in events_with_firmware if event.reader_firmware})
    firmware_counts = Counter(event.reader_firmware for event in events_with_firmware)
    source_versions: dict[str, set[str]] = defaultdict(set)
    for event in events_with_firmware:
        source_versions[event.source_file].add(event.reader_firmware or "")

    reader_log_files = [item for item in inventory if item.log_type == "reader"]
    inventory_files_with_firmware = [item for item in inventory if item.reader_firmware_versions]
    reader_firmware_binary_count = sum(
        row.count for row in archive_inventory if row.category == "Reader firmware binary"
    )

    change_points = _firmware_change_points(events_with_firmware)
    summary: list[dict[str, object]] = [
        {
            "metric": "payment_events_analyzed",
            "value": len(events),
            "message": "parsed PaymentStart resp events analyzed",
        },
        {
            "metric": "payment_events_with_reader_firmware",
            "value": len(events_with_firmware),
            "message": "PaymentStart resp events with explicit reader firmware value",
        },
        {
            "metric": "distinct_reader_firmware_versions",
            "value": len(firmwares),
            "message": ";".join(firmwares),
        },
        {
            "metric": "firmware_change_points",
            "value": change_points,
            "message": "adjacent firmware value changes in chronological order within each source file",
        },
        {
            "metric": "files_with_multiple_firmware_versions",
            "value": sum(1 for versions in source_versions.values() if len(versions) > 1),
            "message": "source files where more than one explicit firmware version was found",
        },
        {
            "metric": "reader_log_files",
            "value": len(reader_log_files),
            "message": "files classified as reader logs by content/path inventory",
        },
        {
            "metric": "inventory_files_with_reader_firmware",
            "value": len(inventory_files_with_firmware),
            "message": "scanned files where reader firmware markers were found",
        },
        {
            "metric": "reader_firmware_binary_files",
            "value": reader_firmware_binary_count,
            "message": "firmware binary files found in archive inventory; binaries do not prove runtime firmware on reader",
        },
    ]
    if not events_with_firmware:
        summary.append(
            {
                "metric": "fact_from_logs",
                "value": 0,
                "message": "reader firmware values were not found in parsed PaymentStart resp events",
            }
        )
    for firmware, count in sorted(firmware_counts.items()):
        summary.append({"metric": f"reader_firmware_{firmware}", "value": count, "message": ""})
    return summary


def _firmware_change_points(events: list[PaymentEvent]) -> int:
    changes = 0
    grouped: dict[str, list[PaymentEvent]] = defaultdict(list)
    for event in events:
        grouped[event.source_file].append(event)
    for source_events in grouped.values():
        previous: str | None = None
        for event in sorted(source_events, key=lambda item: (_timestamp_key(item), item.line_number)):
            current = event.reader_firmware
            if previous is not None and current != previous:
                changes += 1
            previous = current
    return changes


def _timestamp_key(event: PaymentEvent) -> str:
    return event.timestamp.isoformat() if event.timestamp else ""

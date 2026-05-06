from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field

from core.models import LogFileInventory
from parsers.reader_parser import parse_reader_firmware
from parsers.timestamp_parser import parse_timestamp
from parsers.version_parser import parse_package

DATE_RE = re.compile(r"\b(?P<date>20\d{2}-\d{2}-\d{2}|20\d{2}\.\d{2}\.\d{2}|20\d{2}/\d{2}/\d{2})\b")
READER_MODEL_RE = re.compile(
    r"\b(?:reader[_ -]?model|model|ридер|reader)\s*[:= ]+\s*(?P<model>[A-Za-z0-9_.-]{2,})",
    re.IGNORECASE,
)
ERROR_RE = re.compile(r"\b(error|exception|fail(?:ed|ure)?|timeout)\b|ошибк|таймаут", re.IGNORECASE)
TIMEOUT_RE = re.compile(r"\btimeout\b|таймаут", re.IGNORECASE)
EXCEPTION_RE = re.compile(r"\bexception\b", re.IGNORECASE)
FAIL_RE = re.compile(r"\bfail(?:ed|ure)?\b", re.IGNORECASE)


@dataclass
class _InventoryBuilder:
    source_file: str
    path_hints: set[str] = field(default_factory=set)
    content_hints: set[str] = field(default_factory=set)
    dates: set[str] = field(default_factory=set)
    bm_versions: set[str] = field(default_factory=set)
    reader_models: set[str] = field(default_factory=set)
    reader_firmware_versions: set[str] = field(default_factory=set)
    error_status_counts: Counter[str] = field(default_factory=Counter)


class LogInventoryCollector:
    def __init__(self) -> None:
        self._items: dict[str, _InventoryBuilder] = {}

    def observe_line(self, source_file: str, line: str) -> None:
        item = self._items.setdefault(source_file, _InventoryBuilder(source_file=source_file))
        _observe_path(item)
        _observe_date(item, line)
        _observe_bm(item, line)
        _observe_reader(item, line)
        _observe_system(item, line)
        _observe_error(item, line)

    def finalize(self) -> list[LogFileInventory]:
        return [
            _build_inventory(item)
            for item in sorted(self._items.values(), key=lambda value: value.source_file)
        ]


def log_type_counts(inventory: list[LogFileInventory]) -> dict[str, int]:
    counts = Counter(item.log_type for item in inventory)
    return dict(sorted(counts.items()))


def bm_log_version_counts(inventory: list[LogFileInventory]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for item in inventory:
        if item.log_type != "bm":
            continue
        versions = item.bm_versions or ["missing"]
        for version in versions:
            counts[version] += 1
    return dict(sorted(counts.items()))


def bm_log_date_range(inventory: list[LogFileInventory]) -> str:
    dates = sorted({date for item in inventory if item.log_type == "bm" for date in item.dates})
    if not dates:
        return "missing"
    if len(dates) == 1:
        return dates[0]
    return f"{dates[0]}-{dates[-1]}"


def reader_model_counts(inventory: list[LogFileInventory]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for item in inventory:
        if item.log_type != "reader":
            continue
        models = item.reader_models or ["missing"]
        for model in models:
            counts[model] += 1
    return dict(sorted(counts.items()))


def reader_firmware_counts(inventory: list[LogFileInventory]) -> dict[tuple[str, str], int]:
    counts: Counter[tuple[str, str]] = Counter()
    for item in inventory:
        if item.log_type != "reader":
            continue
        models = item.reader_models or ["missing"]
        firmwares = item.reader_firmware_versions or ["missing"]
        for model in models:
            for firmware in firmwares:
                counts[(model, firmware)] += 1
    return dict(sorted(counts.items()))


def error_status_counts_by_type(inventory: list[LogFileInventory], log_type: str) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for item in inventory:
        if item.log_type != log_type:
            continue
        counts.update(item.error_status_counts)
    return dict(counts.most_common())


def other_log_descriptions(inventory: list[LogFileInventory]) -> list[dict[str, str]]:
    rows = []
    for item in inventory:
        if item.log_type != "other":
            continue
        rows.append(
            {
                "source_file": item.source_file,
                "description": "unclassified log file",
                "evidence": item.evidence,
            }
        )
    return rows


def _observe_path(item: _InventoryBuilder) -> None:
    path = item.source_file.lower()
    if "reader" in path or "ридер" in path:
        item.path_hints.add("path:reader")
    if "system" in path or "syslog" in path or "journal" in path:
        item.path_hints.add("path:system")
    if "bm" in path or "mgt_nbs" in path:
        item.path_hints.add("path:bm")


def _observe_date(item: _InventoryBuilder, line: str) -> None:
    match = DATE_RE.search(line)
    if not match:
        return
    raw = match.group("date").replace(".", "-").replace("/", "-")
    timestamp = parse_timestamp(f"{raw} 00:00:00")
    item.dates.add(timestamp.date().isoformat() if timestamp else raw)


def _observe_bm(item: _InventoryBuilder, line: str) -> None:
    package = parse_package(line)
    if package:
        item.content_hints.add("content:mgt_nbs_package")
        item.bm_versions.add(package.bm_version)
    if "PaymentStart" in line:
        item.content_hints.add("content:PaymentStart")


def _observe_reader(item: _InventoryBuilder, line: str) -> None:
    firmware = parse_reader_firmware(line)
    if firmware:
        item.content_hints.add("content:reader_firmware")
        item.reader_firmware_versions.add(firmware)
    model_match = READER_MODEL_RE.search(line)
    if model_match:
        item.content_hints.add("content:reader_model")
        item.reader_models.add(model_match.group("model"))


def _observe_system(item: _InventoryBuilder, line: str) -> None:
    lowered = line.lower()
    if "systemd" in lowered or "kernel:" in lowered or "service" in lowered:
        item.content_hints.add("content:system")


def _observe_error(item: _InventoryBuilder, line: str) -> None:
    if not ERROR_RE.search(line):
        return
    item.error_status_counts[_error_status(line)] += 1


def _error_status(line: str) -> str:
    if TIMEOUT_RE.search(line):
        return "timeout"
    if EXCEPTION_RE.search(line):
        return "exception"
    if FAIL_RE.search(line):
        return "failure"
    return "error"


def _build_inventory(item: _InventoryBuilder) -> LogFileInventory:
    log_type = _detect_log_type(item)
    evidence_items = sorted([*item.content_hints, *item.path_hints])
    return LogFileInventory(
        source_file=item.source_file,
        log_type=log_type,
        detection_method="content_or_path_rules" if evidence_items else "unclassified",
        evidence=", ".join(evidence_items) if evidence_items else "no known markers",
        dates=sorted(item.dates),
        bm_versions=sorted(item.bm_versions),
        reader_models=sorted(item.reader_models),
        reader_firmware_versions=sorted(item.reader_firmware_versions),
        error_status_counts=dict(item.error_status_counts.most_common()),
    )


def _detect_log_type(item: _InventoryBuilder) -> str:
    hints = item.content_hints | item.path_hints
    if "content:mgt_nbs_package" in hints or "content:PaymentStart" in hints or "path:bm" in hints:
        return "bm"
    if "content:reader_firmware" in hints or "content:reader_model" in hints or "path:reader" in hints:
        return "reader"
    if "content:system" in hints or "path:system" in hints:
        return "system"
    return "other"

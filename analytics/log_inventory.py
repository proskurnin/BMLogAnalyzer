from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass, field

from core.models import LogFileInventory
from parsers.reader_parser import parse_reader_firmware
from parsers.version_parser import parse_package

DATE_RE = re.compile(r"\b(?P<date>20\d{2}-\d{2}-\d{2}|20\d{2}\.\d{2}\.\d{2}|20\d{2}/\d{2}/\d{2})\b")
READER_MODEL_RE = re.compile(
    r"\b(?:reader[_ -]?model|model|ридер|reader)\s*[:= ]+\s*(?P<model>[A-Za-z0-9_.-]{2,})",
    re.IGNORECASE,
)
VALIDATOR_RE = re.compile(
    r"\[VALIDATOR\] STARTED|choose_and_start_bm|START COMPLETED|START BM AND WAIT|"
    r"LOAD DEVICE SETTINGS|Open reader SUCCESS|End start reader|Init QR|QR NOT FOUND|"
    r"updateConfiguration|bmInfoRequest|current protocol:|/validator/",
    re.IGNORECASE,
)
OTI_READER_LIBRARY_RE = re.compile(
    r"\b(?:liboti|oti[_ -]?reader|oti[_ -]?reader[_ -]?library|reader[_ -]?oti)\b",
    re.IGNORECASE,
)


@dataclass
class _InventoryBuilder:
    source_file: str
    path_observed: bool = False
    path_hints: set[str] = field(default_factory=set)
    content_hints: set[str] = field(default_factory=set)
    dates: set[str] = field(default_factory=set)
    bm_versions: set[str] = field(default_factory=set)
    reader_models: set[str] = field(default_factory=set)
    reader_firmware_versions: set[str] = field(default_factory=set)
    error_status_counts: Counter[str] = field(default_factory=Counter)
    evidence_samples: dict[str, list[str]] = field(default_factory=dict)


class LogInventoryCollector:
    def __init__(self) -> None:
        self._items: dict[str, _InventoryBuilder] = {}

    def observe_line(self, source_file: str, line: str) -> None:
        item = self._items.setdefault(source_file, _InventoryBuilder(source_file=source_file))
        lowered = line.lower()
        package = parse_package(line) if ("mgt" in lowered or "stopper-" in lowered) else None
        _observe_path(item)
        _observe_date(item, line)
        _observe_bm(item, line, package)
        _observe_stopper(item, line, lowered, package)
        _observe_validator(item, line, lowered)
        _observe_oti_reader_library(item, line, lowered)
        _observe_reader(item, line, lowered)
        _observe_system(item, lowered)
        _observe_error(item, line, lowered)

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
    if item.path_observed:
        return
    item.path_observed = True
    path = item.source_file.lower()
    path_parts = re.split(r"[/\\]+", path)
    path_name = path_parts[-1] if path_parts else path
    if any(part == "stopper" for part in path_parts) or path_name.startswith("stopper"):
        item.path_hints.add("path:stopper")
    if "reader" in path or "ридер" in path:
        item.path_hints.add("path:reader")
    if OTI_READER_LIBRARY_RE.search(path) or ("oti" in path and "lib" in path):
        item.path_hints.add("path:oti_reader_library")
    if "system" in path or "syslog" in path or "journal" in path:
        item.path_hints.add("path:system")
    if (
        any(part in {"bm", "bm-std"} for part in path_parts)
        or path_name.startswith(("bm.", "bm-", "bm_"))
        or re.search(r"(^|[-_.])bm($|[-_.])", path_name) is not None
        or "mgt_nbs" in path
        or "mgt_askp" in path
    ):
        item.path_hints.add("path:bm")


def _observe_date(item: _InventoryBuilder, line: str) -> None:
    if "20" not in line:
        return
    match = DATE_RE.search(line)
    if not match:
        return
    raw = match.group("date").replace(".", "-").replace("/", "-")
    item.dates.add(raw)


def _observe_bm(item: _InventoryBuilder, line: str, package) -> None:
    if package and package.carrier.startswith(("mgt_", "mgt")):
        item.content_hints.add("content:bm_package")
        item.bm_versions.add(package.bm_version)
        _add_evidence_sample(item, f"bm_version:{package.bm_version}", line)
        _add_evidence_sample(item, f"carrier:{package.carrier}", line)
    if "PaymentStart" in line:
        item.content_hints.add("content:PaymentStart")


def _observe_stopper(item: _InventoryBuilder, line: str, lowered: str, package) -> None:
    if package and package.carrier == "stopper":
        item.content_hints.add("content:stopper_package")
        _add_evidence_sample(item, f"stopper_version:{package.bm_version}", line)
    if "platform:platform_stopper" in lowered or "p: stopper-" in lowered or "readerconfiguration:" in lowered:
        item.content_hints.add("content:stopper")
        _add_evidence_sample(item, "stopper", line)


def _observe_validator(item: _InventoryBuilder, line: str, lowered: str) -> None:
    if (
        "validator" not in lowered
        and "choose_and_start_bm" not in lowered
        and "start completed" not in lowered
        and "start bm and wait" not in lowered
        and "load device settings" not in lowered
        and "open reader success" not in lowered
        and "end start reader" not in lowered
        and "init qr" not in lowered
        and "qr not found" not in lowered
        and "updateconfiguration" not in lowered
        and "bminforequest" not in lowered
        and "current protocol:" not in lowered
    ):
        return
    if not VALIDATOR_RE.search(line):
        return
    item.content_hints.add("content:validator_app")
    _add_evidence_sample(item, "validator_app", line)


def _observe_oti_reader_library(item: _InventoryBuilder, line: str, lowered: str) -> None:
    if "oti" not in lowered:
        return
    if not OTI_READER_LIBRARY_RE.search(line):
        return
    item.content_hints.add("content:oti_reader_library")
    _add_evidence_sample(item, "oti_reader_library", line)


def _observe_reader(item: _InventoryBuilder, line: str, lowered: str) -> None:
    if "reader" not in lowered and "firmware" not in lowered and "fw" not in lowered and "model" not in lowered and "ридер" not in lowered:
        return
    if "firmware" in lowered or "fw" in lowered or "readerversion" in lowered or "reader_version" in lowered or "reader version" in lowered:
        firmware = parse_reader_firmware(line)
        if firmware:
            item.content_hints.add("content:reader_firmware")
            item.reader_firmware_versions.add(firmware)
            _add_evidence_sample(item, f"reader_firmware:{firmware}", line)
    if "model" in lowered or "ридер" in lowered or "reader model" in lowered:
        model_match = READER_MODEL_RE.search(line)
    else:
        model_match = None
    if model_match:
        item.content_hints.add("content:reader_model")
        model = model_match.group("model")
        item.reader_models.add(model)
        _add_evidence_sample(item, f"reader_model:{model}", line)


def _observe_system(item: _InventoryBuilder, lowered: str) -> None:
    if (
        "system" not in lowered
        and "kernel:" not in lowered
        and "service" not in lowered
        and "systemd" not in lowered
        and "journal" not in lowered
        and "nginx" not in lowered
        and "networkmanager" not in lowered
        and "audit:" not in lowered
        and "cron" not in lowered
    ):
        return
    if (
        "systemd" in lowered
        or "kernel:" in lowered
        or "service" in lowered
        or "journal" in lowered
        or "nginx" in lowered
        or "networkmanager" in lowered
        or "audit:" in lowered
        or "cron" in lowered
    ):
        item.content_hints.add("content:system")


def _observe_error(item: _InventoryBuilder, line: str, lowered: str) -> None:
    if (
        "error" not in lowered
        and "exception" not in lowered
        and "fail" not in lowered
        and "timeout" not in lowered
        and "ошибк" not in lowered
        and "таймаут" not in lowered
    ):
        return
    item.error_status_counts[_error_status(lowered)] += 1


def _error_status(lowered: str) -> str:
    if "timeout" in lowered or "таймаут" in lowered:
        return "timeout"
    if "exception" in lowered:
        return "exception"
    if "fail" in lowered:
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
        evidence_samples={key: values[:5] for key, values in sorted(item.evidence_samples.items())},
    )


def _detect_log_type(item: _InventoryBuilder) -> str:
    hints = item.content_hints | item.path_hints
    if "content:stopper_package" in hints or "path:stopper" in hints:
        return "stopper"
    if "content:bm_package" in hints or "content:mgt_nbs_package" in hints or "path:bm" in hints:
        return "bm"
    if "content:validator_app" in hints:
        return "validator_app"
    if "content:oti_reader_library" in hints or "path:oti_reader_library" in hints:
        return "oti_reader_library"
    if "content:PaymentStart" in hints:
        return "bm"
    if "content:stopper" in hints:
        return "stopper"
    if "content:reader_firmware" in hints or "content:reader_model" in hints or "path:reader" in hints:
        return "reader"
    if "content:system" in hints or "path:system" in hints:
        return "system"
    return "other"


def _add_evidence_sample(item: _InventoryBuilder, key: str, line: str) -> None:
    samples = item.evidence_samples.setdefault(key, [])
    if len(samples) >= 5:
        return
    value = line.rstrip("\n")
    if value and value not in samples:
        samples.append(value)

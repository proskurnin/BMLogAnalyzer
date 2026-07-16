from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from core.models import DeviceBootEvidence, DeviceBootReport, PaymentEvent


@dataclass(frozen=True)
class DeviceProfileEvidence:
    source_file: str
    line_number: int | None
    timestamp: datetime | None
    label: str
    raw_line: str


@dataclass(frozen=True)
class DeviceProfile:
    device_id: str
    carrier: str | None
    carrier_source: str
    reader_type: str | None
    reader_source: str
    validator_versions: list[str] = field(default_factory=list)
    bm_versions: list[str] = field(default_factory=list)
    routes: list[str] = field(default_factory=list)
    package_reader_types: list[str] = field(default_factory=list)
    source_files: list[str] = field(default_factory=list)
    boot_report_count: int = 0
    payment_event_count: int = 0
    evidence: list[DeviceProfileEvidence] = field(default_factory=list)


def build_device_profiles(
    events: list[PaymentEvent],
    device_boot_reports: list[DeviceBootReport] | None,
) -> list[DeviceProfile]:
    boot_reports = device_boot_reports or []
    boot_serials = {report.validator_serial for report in boot_reports if report.validator_serial}
    groups: dict[str, dict[str, object]] = {}

    for report in boot_reports:
        device_id = report.validator_serial or _device_id_from_sources(report.source_files) or f"boot:{report.title}"
        group = groups.setdefault(device_id, _empty_group(device_id))
        group["boot_reports"].append(report)
        group["source_files"].update(report.source_files)
        if report.validator_version:
            group["validator_versions"].add(report.validator_version)
        if report.bm_version:
            group["bm_versions"].add(report.bm_version)
        if report.route:
            group["routes"].add(report.route)
        if report.reader_type:
            group["physical_readers"].add(report.reader_type)
        carrier = _carrier_from_boot_report(report)
        if carrier:
            group["physical_carriers"].add(carrier)
        group["evidence"].extend(_device_boot_evidence(report))

    for event in events:
        device_id = _event_device_id(event, boot_serials)
        if not device_id or device_id not in groups:
            continue
        group = groups.setdefault(device_id, _empty_group(device_id))
        group["events"].append(event)
        group["source_files"].add(event.source_file)
        if event.bm_version:
            group["bm_versions"].add(event.bm_version)
        if package_reader := _event_package_reader_type(event):
            group["package_readers"].add(package_reader)
        group["event_evidence"].append(
            DeviceProfileEvidence(
                source_file=event.source_file,
                line_number=event.line_number,
                timestamp=event.timestamp,
                label="PaymentStart resp",
                raw_line=event.raw_line,
            )
        )

    return [_finalize_group(group) for _, group in sorted(groups.items(), key=lambda item: _device_sort_key(item[0]))]


def _empty_group(device_id: str) -> dict[str, object]:
    return {
        "device_id": device_id,
        "boot_reports": [],
        "events": [],
        "physical_carriers": set(),
        "physical_readers": set(),
        "package_readers": set(),
        "validator_versions": set(),
        "bm_versions": set(),
        "routes": set(),
        "source_files": set(),
        "evidence": [],
        "event_evidence": [],
    }


def _finalize_group(group: dict[str, object]) -> DeviceProfile:
    carriers = sorted(group["physical_carriers"])
    readers = sorted(group["physical_readers"])
    package_readers = sorted(group["package_readers"])
    evidence = list(group["evidence"])
    if not evidence:
        evidence = list(group["event_evidence"])[:3]
    return DeviceProfile(
        device_id=str(group["device_id"]),
        carrier=carriers[0] if len(carriers) == 1 else None,
        carrier_source=_source_status(carriers, "device_boot"),
        reader_type=readers[0] if len(readers) == 1 else None,
        reader_source=_source_status(readers, "device_boot"),
        validator_versions=sorted(group["validator_versions"]),
        bm_versions=sorted(group["bm_versions"]),
        routes=sorted(group["routes"]),
        package_reader_types=package_readers,
        source_files=sorted(group["source_files"]),
        boot_report_count=len(group["boot_reports"]),
        payment_event_count=len(group["events"]),
        evidence=evidence[:10],
    )


def _source_status(values: list[str], source: str) -> str:
    if len(values) == 1:
        return source
    if len(values) > 1:
        return "conflict"
    return "missing"


def _device_boot_evidence(report: DeviceBootReport) -> list[DeviceProfileEvidence]:
    rows: list[DeviceProfileEvidence] = []
    for evidence in _all_device_boot_evidence(report):
        if _is_device_profile_evidence(evidence):
            rows.append(_device_profile_evidence(evidence))
    if rows:
        return rows
    return [_device_profile_evidence(evidence) for evidence in _all_device_boot_evidence(report)[:3]]


def _all_device_boot_evidence(report: DeviceBootReport) -> list[DeviceBootEvidence]:
    rows: list[DeviceBootEvidence] = []
    seen: set[tuple[str, int, str]] = set()
    for segment in report.segments:
        for evidence in segment.evidence:
            identity = (evidence.source_file, evidence.line_number, evidence.raw_line)
            if identity in seen:
                continue
            seen.add(identity)
            rows.append(evidence)
    return rows


def _is_device_profile_evidence(evidence: DeviceBootEvidence) -> bool:
    label = evidence.label.lower()
    line = evidence.raw_line.lower()
    return (
        "validator started" in label
        or label in {"serial", "route", "reader type"}
        or "serial" in line
        or "reader type" in line
        or "[reader]" in line
    )


def _device_profile_evidence(evidence: DeviceBootEvidence) -> DeviceProfileEvidence:
    return DeviceProfileEvidence(
        source_file=evidence.source_file,
        line_number=evidence.line_number,
        timestamp=evidence.timestamp,
        label=evidence.label,
        raw_line=evidence.raw_line,
    )


def _carrier_from_boot_report(report: DeviceBootReport) -> str:
    text = " ".join([report.title, *report.source_files]).casefold()
    if "аскп" in text or "askp" in text:
        return "АСКП"
    return ""


def _event_package_reader_type(event: PaymentEvent) -> str:
    platform = (event.platform or "").lower()
    if platform in {"oti", "tt"}:
        return platform.upper()
    match = re.search(r"\b[A-Za-z0-9_]+-(oti|tt)-\d", event.package or "", re.IGNORECASE)
    return match.group(1).upper() if match else ""


def _event_device_id(event: PaymentEvent, boot_serials: set[str]) -> str:
    raw = event.raw_line or ""
    for pattern in (
        r"\b(?:serial_number|serialNumber)\\?\"?\s*[:=]\s*\\?\"?(?P<value>\d{5,})",
        r"\b(?:BmNumber|TmSerialNumber)\s*:\s*(?P<value>\d{5,})",
        r"\breader_id\s*:\s*\\?\"(?P<value>\d{5,})\\?\"",
    ):
        match = re.search(pattern, raw)
        if match:
            return match.group("value")

    source_device_id = _device_id_from_sources([event.source_file])
    rid_match = re.search(r"\brid\s*:\s*(?P<value>\d{5,})", raw)
    if rid_match:
        rid = rid_match.group("value")
        for serial in boot_serials:
            if rid.endswith(serial):
                return serial
        if source_device_id and rid.endswith(source_device_id):
            return source_device_id
        return rid

    trx_match = re.search(r"\b(?:BmTrxId|trxid)\s*:\s*(?P<value>\d{5,})", raw)
    if trx_match:
        trx_id = trx_match.group("value")
        for serial in boot_serials:
            if trx_id.startswith(serial):
                return serial
    if len(boot_serials) == 1:
        return next(iter(boot_serials))
    if source_device_id:
        return source_device_id
    return ""


def _device_id_from_sources(source_files: list[str]) -> str:
    for source_file in source_files:
        for part in Path(source_file).parts:
            match = re.fullmatch(r"(?P<value>\d{5,})_logs(?:\.zip)?", part)
            if match:
                return match.group("value")
    return ""


def _device_sort_key(device_id: str) -> tuple[int, str]:
    return (0 if device_id.isdigit() else 1, device_id)

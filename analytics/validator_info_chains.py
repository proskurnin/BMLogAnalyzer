from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime

from core.models import DeviceBootReport, ValidatorInfoChainEvidence, ValidatorInfoChainReport

FULL_TS_RE = re.compile(r"\[(?P<value>20\d{2}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?)\]")
ISO_TS_RE = re.compile(r"\[(?P<value>20\d{2}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?)\]")
SHORT_TS_RE = re.compile(r"\[(?P<value>\d{2}:\d{2}:\d{2}(?:\.\d+)?)\]")
FILENAME_DATE_RE = re.compile(r"(20\d{2})-(\d{2})-(\d{2})")
THREAD_RE = re.compile(r"\{(?P<value>\d{4,})\}")
ENDPOINT_RE = re.compile(r"Connection endpoint:\s*(?P<value>\S+)")

INFO_CHAIN_NEEDLES = (
    "Send Commands::info",
    "Connection endpoint",
    "Connection succeed",
    "Write buffer",
    "Writting succeed",
    "Writing succeed",
)


@dataclass
class _InfoChain:
    source_file: str
    thread_id: str | None
    evidence: list[ValidatorInfoChainEvidence] = field(default_factory=list)
    endpoint: str | None = None


class ValidatorInfoChainCollector:
    def __init__(self, *, slow_threshold_seconds: float = 3.0) -> None:
        self.slow_threshold_seconds = slow_threshold_seconds
        self._active: dict[tuple[str, str | None], _InfoChain] = {}
        self._chains: list[_InfoChain] = []

    def observe_line(self, source_file: str, line_number: int, line: str) -> None:
        if not any(needle in line for needle in INFO_CHAIN_NEEDLES):
            return
        timestamp = _parse_timestamp(line, _date_from_source_file(source_file))
        thread_id = _thread_id(line)
        key = (source_file, thread_id)
        if "Send Commands::info" in line and "with timeout" in line:
            chain = _InfoChain(source_file=source_file, thread_id=thread_id)
            chain.evidence.append(_evidence(source_file, line_number, timestamp, "Send Commands::info", line))
            self._active[key] = chain
            return
        chain = self._active.get(key)
        if chain is None:
            return
        label = _line_label(line)
        evidence = _evidence(source_file, line_number, timestamp, label, line)
        chain.evidence.append(evidence)
        if label == "Connection endpoint":
            endpoint_match = ENDPOINT_RE.search(line)
            if endpoint_match:
                chain.endpoint = endpoint_match.group("value")
        if label in {"Send Commands::info succeed", "Send Commands::info failed"}:
            self._chains.append(chain)
            self._active.pop(key, None)

    def finalize(self, *, boot_reports: list[DeviceBootReport] | None = None) -> list[ValidatorInfoChainReport]:
        reports: list[ValidatorInfoChainReport] = []
        seen: set[tuple[str | None, str | None, str | None, str | None, str | None]] = set()
        for chain in self._chains:
            started_at = chain.evidence[0].timestamp if chain.evidence else None
            finished_at = chain.evidence[-1].timestamp if chain.evidence else None
            duration = _duration_seconds(started_at, finished_at)
            if duration is None or duration < self.slow_threshold_seconds:
                continue
            first = chain.evidence[0]
            last = chain.evidence[-1]
            identity = (
                started_at.isoformat(sep=" ") if started_at else None,
                finished_at.isoformat(sep=" ") if finished_at else None,
                chain.thread_id,
                first.raw_line,
                last.raw_line,
            )
            if identity in seen:
                continue
            seen.add(identity)
            linked_boot = _linked_boot_report(started_at, first.source_file, boot_reports or [])
            reports.append(
                ValidatorInfoChainReport(
                    started_at=started_at,
                    finished_at=finished_at,
                    duration_seconds=duration,
                    source_file=chain.source_file,
                    thread_id=chain.thread_id,
                    endpoint=chain.endpoint,
                    linked_boot_title=linked_boot.title if linked_boot else None,
                    linked_boot_started_at=linked_boot.started_at if linked_boot else None,
                    evidence=chain.evidence,
                )
            )
        return sorted(
            reports,
            key=lambda item: (
                item.duration_seconds or 0,
                item.started_at or datetime.min,
                item.source_file,
            ),
            reverse=True,
        )


def _linked_boot_report(
    timestamp: datetime | None,
    source_file: str,
    boot_reports: list[DeviceBootReport],
) -> DeviceBootReport | None:
    if timestamp is None:
        return None
    candidates = [
        report
        for report in boot_reports
        if report.started_at is not None
        and report.finished_at is not None
        and report.started_at <= timestamp <= report.finished_at
    ]
    if len(candidates) != 1:
        return None
    candidate = candidates[0]
    if source_file in candidate.source_files:
        return candidate
    if candidate.validator_serial and candidate.validator_serial in source_file:
        return candidate
    return None


def _evidence(
    source_file: str,
    line_number: int,
    timestamp: datetime | None,
    label: str,
    line: str,
) -> ValidatorInfoChainEvidence:
    return ValidatorInfoChainEvidence(
        source_file=source_file,
        line_number=line_number,
        timestamp=timestamp,
        label=label,
        raw_line=line.rstrip("\n"),
    )


def _line_label(line: str) -> str:
    if "Send Commands::info succeed" in line:
        return "Send Commands::info succeed"
    if "Send Commands::info failed" in line:
        return "Send Commands::info failed"
    if "Connection endpoint" in line:
        return "Connection endpoint"
    if "Connection succeed" in line:
        return "Connection succeed"
    if "Write buffer" in line:
        return "Write buffer"
    if "Writting succeed" in line or "Writing succeed" in line:
        return "Writting succeed"
    return "Info chain"


def _thread_id(line: str) -> str | None:
    match = THREAD_RE.search(line)
    return match.group("value") if match else None


def _parse_timestamp(line: str, fallback_date: date | None) -> datetime | None:
    if match := FULL_TS_RE.search(line):
        return _parse_datetime(match.group("value"), "%Y.%m.%d %H:%M:%S.%f", "%Y.%m.%d %H:%M:%S")
    if match := ISO_TS_RE.search(line):
        return _parse_datetime(match.group("value"), "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S")
    if fallback_date and (match := SHORT_TS_RE.search(line)):
        parsed_time = _parse_datetime(match.group("value"), "%H:%M:%S.%f", "%H:%M:%S")
        if parsed_time:
            return datetime.combine(fallback_date, parsed_time.time())
    return None


def _parse_datetime(value: str, *formats: str) -> datetime | None:
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _date_from_source_file(source_file: str) -> date | None:
    if match := FILENAME_DATE_RE.search(source_file):
        return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    return None


def _duration_seconds(started_at: datetime | None, finished_at: datetime | None) -> float | None:
    if started_at is None or finished_at is None:
        return None
    duration = (finished_at - started_at).total_seconds()
    if duration < 0:
        return None
    return round(duration, 3)

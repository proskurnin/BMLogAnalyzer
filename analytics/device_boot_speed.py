from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from core.models import DeviceBootEvidence, DeviceBootReport, DeviceBootSegment

VALIDATOR_TS_RE = re.compile(
    r"\[(?P<date>20\d{2}-[A-Za-z]{3}-\d{2}) (?P<time>\d{2}:\d{2}:\d{2}(?:\.\d+)?)\]"
)
SHORT_TS_RE = re.compile(r"\[(?P<time>\d{2}:\d{2}:\d{2}(?:\.\d+)?)\]")
BM_TS_RE = re.compile(r'time="(?P<value>20\d{2}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?)"')

VERSION_RE = re.compile(r"\bversion_(?P<part>major|middle|minor|build)\D+(?P<value>\d+)", re.IGNORECASE)
SERIAL_RE = re.compile(r"\bserial\D+(?P<value>\d{3,})\b", re.IGNORECASE)
ROUTE_RE = re.compile(r"\broute\D+(?P<value>\d+)\b", re.IGNORECASE)
READER_TYPE_RE = re.compile(r"\breader\s+type\D+(?P<value>OTI|TT)\b", re.IGNORECASE)
BM_VERSION_RE = re.compile(r"\bBm version:\s*(?P<value>[\d.]+)|\bBM Version:\s*(?P<value2>[\d.]+)", re.IGNORECASE)
BM_STATUS_RE = re.compile(r"\bBm status:?\s*(?P<validator>\d+)|\bBmStatus:(?P<bm>\d+)", re.IGNORECASE)
READER_STATUS_RE = re.compile(r"\bReader status:?\s*(?P<validator>\d+)|\bReaderStatus:(?P<bm>\d+)", re.IGNORECASE)
VALIDATOR_MARKERS = (
    ("activate_references", "ACTIVATE REFERENCES", "ACTIVATE REFERENCES"),
    ("settings_ok", "End LOAD DEVICE SETTINGS: OK", "End LOAD DEVICE SETTINGS: OK"),
    ("socket_error", "Can't open and connect socket", "Can't open and connect socket"),
    ("socket_connected", "connect: OK", "connect: OK"),
    ("reader_open_success", "Open reader SUCCESS", "Open reader SUCCESS"),
    ("reader_start_end", "End start reader", "End start reader"),
    ("qr_init", "Init QR", "Init QR"),
    ("qr_not_found", "QR NOT FOUND", "QR NOT FOUND"),
    ("choose_bm", "[choose_and_start_bm]", "choose_and_start_bm"),
    ("bm_found_for_route", "found for route:", "found for route"),
    ("start_bm_wait", "START BM AND WAIT", "START BM AND WAIT"),
    ("start_bm", "start BM:", "start BM"),
    ("start_completed", "START COMPLETED", "START COMPLETED"),
    ("send_error", "send error:", "send error"),
    ("current_protocol", "current protocol:", "current protocol"),
    ("stop_reader", "Stop reader", "Stop reader"),
    ("end_stop_reader", "End stop reader", "End stop reader"),
    ("update_started", "[updateConfiguration] Started", "updateConfiguration Started"),
    ("update_send", "Send Commands::updateConfiguration", "Send updateConfiguration"),
    ("update_result", "[updateConfiguration] result:", "updateConfiguration result"),
    ("bm_info_request", "[bmInfoRequest] Start", "bmInfoRequest Start"),
)
BM_MARKERS = (
    ("bm_tcp_listen", "listening TCP requests", "listening TCP requests"),
    ("bm_update_stage1", "UpdateConfiguration: Stage1", "UpdateConfiguration Stage1"),
    ("bm_update_success", "UpdateSuccess: true", "UpdateSuccess true"),
    ("bm_update_failed", "UpdateSuccess: false", "UpdateSuccess false"),
    ("bm_configuration_work", "ConfigurationStatusWork", "ConfigurationStatusWork"),
)
BOOT_RELEVANT_NEEDLES = tuple(
    needle
    for _, needle, _ in (*VALIDATOR_MARKERS, *BM_MARKERS)
) + (
    "[VALIDATOR] STARTED",
    "Open QR failed",
    "/bm.sh stop",
    "Info response",
    "Info, resp:",
    "Bm version:",
    "BM Version:",
    "version_",
    "serial",
    "route",
    "reader type",
    "Reader status",
    "ReaderStatus",
    "Bm status",
    "BmStatus",
)
BOOT_RELEVANT_LOWER_NEEDLES = (
    "bm version:",
    "version_",
    "serial",
    "route",
    "reader type",
    "reader status",
    "readerstatus",
    "bm status",
    "bmstatus",
)
BOOT_PREFILTER_NEEDLES = (
    "[",
    "BM",
    "Bm",
    "bm",
    "Reader",
    "reader",
    "Info",
    "info",
    "Update",
    "Configuration",
    "listen",
    "route",
    "serial",
    "version_",
    "QR",
    "socket",
    "connect",
    "start",
    "START",
    "Stop",
)


@dataclass
class _BootSession:
    source_file: str
    base_date: date | None = None
    version_parts: dict[str, str] = field(default_factory=dict)
    serial: str | None = None
    route: str | None = None
    reader_type: str | None = None
    bm_version: str | None = None
    events: dict[str, DeviceBootEvidence] = field(default_factory=dict)
    repeated: dict[str, list[DeviceBootEvidence]] = field(default_factory=dict)
    source_files: set[str] = field(default_factory=set)
    pending_reader_status: str | None = None
    pending_bm_status: str | None = None


class DeviceBootSpeedCollector:
    def __init__(self) -> None:
        self._sessions: list[_BootSession] = []
        self._current: _BootSession | None = None
        self._pending_info: DeviceBootEvidence | None = None
        self._global_events: dict[str, list[DeviceBootEvidence]] = {}
        self._global_bm_versions: list[tuple[DeviceBootEvidence, str]] = []

    def observe_line(self, source_file: str, line_number: int, line: str) -> None:
        if self._current is None and "[VALIDATOR] STARTED" not in line:
            return
        if self._current is not None and "stopper" in source_file.lower():
            return
        if self._current is not None and not _is_boot_relevant_line(line):
            return

        timestamp = _parse_timestamp(line, self._current.base_date if self._current else None)
        if "[VALIDATOR] STARTED" in line:
            session = _BootSession(source_file=source_file, base_date=timestamp.date() if timestamp else None)
            self._sessions.append(session)
            self._current = session
            self._record("validator_started", source_file, line_number, timestamp, line, "VALIDATOR STARTED")
            return

        session = self._current
        if session is None:
            return
        session.source_files.add(source_file)
        if timestamp and session.base_date is None:
            session.base_date = timestamp.date()

        _observe_metadata(session, line)
        self._observe_validator_marker(source_file, line_number, timestamp, line)
        self._observe_bm_marker(source_file, line_number, timestamp, line)
        self._observe_info_context(source_file, line_number, timestamp, line)

    def finalize(self) -> list[DeviceBootReport]:
        reports = []
        for session in self._sessions:
            if "validator_started" not in session.events:
                continue
            self._attach_global_bm_events(session)
            segments = _build_segments(session)
            finished = _final_finished_at(session)
            started = session.events["validator_started"].timestamp
            total = _duration_seconds(started, finished)
            reports.append(
                DeviceBootReport(
                    title=_report_title(session, started),
                    validator_serial=session.serial,
                    route=session.route,
                    validator_version=_validator_version(session),
                    bm_version=session.bm_version,
                    reader_type=session.reader_type,
                    started_at=started,
                    finished_at=finished,
                    total_seconds=total,
                    segments=segments,
                    summary=_summary_lines(session, segments, total),
                    source_files=sorted(session.source_files or {session.source_file}),
                )
            )
        return reports

    def _observe_validator_marker(self, source_file: str, line_number: int, timestamp: datetime | None, line: str) -> None:
        for key, needle, label in VALIDATOR_MARKERS:
            if needle in line:
                repeated = key in {"bm_stop", "qr_failed", "update_send"}
                self._record(key, source_file, line_number, timestamp, line, label, repeated=repeated)
        if "Open QR failed" in line:
            self._record("qr_failed", source_file, line_number, timestamp, line, "Open QR failed", repeated=True)
        if "/bm.sh stop" in line:
            self._record("bm_stop", source_file, line_number, timestamp, line, "bm.sh stop", repeated=True)
        if "Info response" in line:
            evidence = self._record("info_response", source_file, line_number, timestamp, line, "Info response", repeated=True)
            self._pending_info = evidence
            self._current.pending_reader_status = None
            self._current.pending_bm_status = None

    def _observe_bm_marker(self, source_file: str, line_number: int, timestamp: datetime | None, line: str) -> None:
        for key, needle, label in BM_MARKERS:
            if needle in line:
                self._record_global(key, source_file, line_number, timestamp, line, label)
        if "Info, resp:" in line:
            self._record_global("bm_info_resp", source_file, line_number, timestamp, line, "BM Info resp")
            if _line_has_statuses(line, reader_status="0", bm_status="0"):
                self._record_global("bm_info_status_0_0", source_file, line_number, timestamp, line, "BM Info resp 0/0")
            elif _line_has_statuses(line, bm_status="64"):
                self._record_global("bm_info_status_64", source_file, line_number, timestamp, line, "BM Info resp 64")
        version_match = BM_VERSION_RE.search(line)
        if version_match:
            version = version_match.group("value") or version_match.group("value2")
            evidence = DeviceBootEvidence(source_file, line_number, timestamp, "BM Version", line.rstrip("\n"))
            self._global_bm_versions.append((evidence, version))

    def _observe_info_context(self, source_file: str, line_number: int, timestamp: datetime | None, line: str) -> None:
        if self._current is None:
            return
        if 'time="' in line:
            return
        if version_match := BM_VERSION_RE.search(line):
            self._current.bm_version = version_match.group("value") or version_match.group("value2")
        evidence = DeviceBootEvidence(source_file, line_number, timestamp, "Info detail", line.rstrip("\n"))
        _apply_statuses_from_line(self._current, self._pending_info or evidence, line)

    def _record(
        self,
        key: str,
        source_file: str,
        line_number: int,
        timestamp: datetime | None,
        line: str,
        label: str,
        *,
        repeated: bool = False,
    ) -> DeviceBootEvidence:
        session = self._current
        if session is None:
            raise RuntimeError("device boot session is not initialized")
        evidence = DeviceBootEvidence(source_file, line_number, timestamp, label, line.rstrip("\n"))
        session.source_files.add(source_file)
        if repeated:
            session.repeated.setdefault(key, []).append(evidence)
        session.events.setdefault(key, evidence)
        return evidence

    def _record_global(
        self,
        key: str,
        source_file: str,
        line_number: int,
        timestamp: datetime | None,
        line: str,
        label: str,
    ) -> DeviceBootEvidence:
        evidence = DeviceBootEvidence(source_file, line_number, timestamp, label, line.rstrip("\n"))
        self._global_events.setdefault(key, []).append(evidence)
        return evidence

    def _attach_global_bm_events(self, session: _BootSession) -> None:
        _attach_first_global(session, self._global_events, "bm_tcp_listen", "start_bm", "start_completed")
        for key in ("bm_update_stage1", "bm_update_failed", "bm_update_success", "bm_configuration_work"):
            _attach_first_global(session, self._global_events, key, "update_started", "update_result")
        _attach_first_global_after(session, self._global_events, "bm_info_status_0_0", "update_result", max_seconds=30)
        if session.bm_version is None:
            start = _event_time(session, "start_bm")
            end = _event_time(session, "info_status_0_0")
            for evidence, version in self._global_bm_versions:
                if _timestamp_in_window(evidence.timestamp, start, end):
                    session.bm_version = version
                    session.source_files.add(evidence.source_file)
                    break


def _observe_metadata(session: _BootSession, line: str) -> None:
    if match := VERSION_RE.search(line):
        session.version_parts[match.group("part").lower()] = match.group("value")
    if match := SERIAL_RE.search(line):
        session.serial = session.serial or match.group("value")
    if match := ROUTE_RE.search(line):
        session.route = session.route or match.group("value")
    if match := READER_TYPE_RE.search(line):
        session.reader_type = match.group("value").upper()


def _is_boot_relevant_line(line: str) -> bool:
    if not any(needle in line for needle in BOOT_PREFILTER_NEEDLES):
        return False
    if any(needle in line for needle in BOOT_RELEVANT_NEEDLES):
        return True
    lowered = line.lower()
    return any(needle in lowered for needle in BOOT_RELEVANT_LOWER_NEEDLES)


def _attach_first_global(
    session: _BootSession,
    global_events: dict[str, list[DeviceBootEvidence]],
    key: str,
    start_key: str,
    end_key: str,
) -> None:
    start = _event_time(session, start_key)
    end = _event_time(session, end_key)
    for evidence in _sorted_evidence(global_events.get(key, [])):
        if not _timestamp_in_window(evidence.timestamp, start, end):
            continue
        session.events.setdefault(key, evidence)
        session.source_files.add(evidence.source_file)
        return


def _attach_first_global_after(
    session: _BootSession,
    global_events: dict[str, list[DeviceBootEvidence]],
    key: str,
    start_key: str,
    *,
    max_seconds: float,
) -> None:
    start = _event_time(session, start_key)
    if start is None:
        return
    end = start + timedelta(seconds=max_seconds)
    for evidence in _sorted_evidence(global_events.get(key, [])):
        if not _timestamp_in_window(evidence.timestamp, start, end):
            continue
        session.events.setdefault("info_status_0_0", evidence)
        session.source_files.add(evidence.source_file)
        return


def _event_time(session: _BootSession, key: str) -> datetime | None:
    event = session.events.get(key)
    return event.timestamp if event else None


def _sorted_evidence(items: list[DeviceBootEvidence]) -> list[DeviceBootEvidence]:
    return sorted(items, key=lambda item: (item.timestamp or datetime.max, item.source_file, item.line_number))


def _timestamp_in_window(value: datetime | None, start: datetime | None, end: datetime | None) -> bool:
    if value is None:
        return False
    if start is not None and value < start:
        return False
    if end is not None and value > end:
        return False
    return True


def _line_has_statuses(line: str, *, reader_status: str | None = None, bm_status: str | None = None) -> bool:
    reader_match = READER_STATUS_RE.search(line)
    bm_match = BM_STATUS_RE.search(line)
    if reader_status is not None:
        if not reader_match:
            return False
        actual_reader = reader_match.group("validator") or reader_match.group("bm")
        if actual_reader != reader_status:
            return False
    if bm_status is not None:
        if not bm_match:
            return False
        actual_bm = bm_match.group("validator") or bm_match.group("bm")
        if actual_bm != bm_status:
            return False
    return True


def _apply_statuses_from_line(session: _BootSession | None, info_evidence: DeviceBootEvidence, line: str) -> None:
    if session is None:
        return
    reader_match = READER_STATUS_RE.search(line)
    bm_match = BM_STATUS_RE.search(line)
    if not reader_match and not bm_match:
        return
    reader_status = reader_match.group("validator") or reader_match.group("bm") if reader_match else None
    bm_status = bm_match.group("validator") or bm_match.group("bm") if bm_match else None
    if reader_status is not None:
        session.pending_reader_status = reader_status
    if bm_status is not None:
        session.pending_bm_status = bm_status
    if bm_status == "64":
        session.events.setdefault("info_status_64", info_evidence)
    if session.pending_reader_status == "0" and session.pending_bm_status == "0":
        session.events["info_status_0_0"] = info_evidence


def _build_segments(session: _BootSession) -> list[DeviceBootSegment]:
    return [
        _segment(
            session,
            "АСКП. Справочники и настройки",
            "Активация справочников и загрузка настроек устройства.",
            "validator_started",
            "settings_ok",
            ["activate_references", "settings_ok"],
        ),
        _segment(
            session,
            f"АСКП. Сеть и ридер {session.reader_type or ''}".strip(),
            "Подключение socket и старт ридера.",
            "settings_ok",
            "reader_start_end",
            ["socket_error", "socket_connected", "reader_open_success", "reader_start_end"],
        ),
        _segment(
            session,
            "АСКП. Поиск QR-ридера",
            "Поиск QR-ридера до события QR NOT FOUND.",
            "qr_init",
            "qr_not_found",
            ["qr_init", "qr_failed", "qr_not_found"],
        ),
        _segment(
            session,
            "АСКП. Остановка вариантов и выбор БМ",
            "Остановка вариантов BM-модулей и выбор BM по маршруту.",
            "qr_not_found",
            "start_bm",
            ["bm_stop", "choose_bm", "bm_found_for_route", "start_bm_wait", "start_bm"],
        ),
        _segment(
            session,
            "АСКП/systemd. Запуск БМ",
            "Время от команды запуска BM до открытия TCP-порта BM.",
            "start_bm",
            "bm_tcp_listen",
            ["start_bm_wait", "start_bm", "bm_tcp_listen"],
        ),
        _segment(
            session,
            "АСКП ждёт, БМ запускается параллельно",
            "Ожидание АСКП после запуска BM; BM может открыть порт раньше завершения ожидания.",
            "bm_tcp_listen",
            "start_completed",
            ["start_bm_wait", "bm_tcp_listen", "start_completed"],
        ),
        _segment(
            session,
            "АСКП и БМ. Первый Info",
            "Первый обмен Info после START COMPLETED.",
            "start_completed",
            "current_protocol",
            ["send_error", "info_status_64", "current_protocol"],
        ),
        _segment(
            session,
            "АСКП и БМ. Подготовка UpdateConfiguration",
            "Остановка ридера и повторная проверка статуса BM перед UpdateConfiguration.",
            "current_protocol",
            "update_started",
            ["stop_reader", "end_stop_reader", "info_status_64", "update_started"],
        ),
        _segment(
            session,
            "АСКП и БМ. UpdateConfiguration",
            "Выполнение UpdateConfiguration до подтверждения результата.",
            "update_started",
            "update_result",
            ["update_started", "update_send", "bm_update_stage1", "bm_update_failed", "bm_update_success", "bm_configuration_work", "update_result"],
        ),
        _segment(
            session,
            "АСКП и БМ. Контрольный Info 0/0",
            "Контрольный Info с Reader status 0 и Bm status 0.",
            "update_result",
            "info_status_0_0",
            ["bm_info_request", "info_status_0_0"],
        ),
    ]


def _segment(
    session: _BootSession,
    title: str,
    description: str,
    start_key: str,
    end_key: str,
    evidence_keys: list[str],
) -> DeviceBootSegment:
    start = session.events.get(start_key)
    end = session.events.get(end_key)
    evidence = _evidence_for_keys(session, evidence_keys)
    return DeviceBootSegment(
        title=title,
        description=description,
        started_at=start.timestamp if start else None,
        finished_at=end.timestamp if end else None,
        duration_seconds=_duration_seconds(start.timestamp if start else None, end.timestamp if end else None),
        evidence=evidence,
    )


def _evidence_for_keys(session: _BootSession, keys: list[str]) -> list[DeviceBootEvidence]:
    rows: list[DeviceBootEvidence] = []
    seen: set[tuple[str, int, str]] = set()
    for key in keys:
        candidates = session.repeated.get(key) or ([session.events[key]] if key in session.events else [])
        if key in {"bm_stop", "qr_failed", "update_send"}:
            candidates = _first_last(candidates, limit=4)
        for item in candidates:
            identity = (item.source_file, item.line_number, item.raw_line)
            if identity in seen:
                continue
            seen.add(identity)
            rows.append(item)
    return sorted(rows, key=lambda item: (item.timestamp or datetime.max, item.source_file, item.line_number))


def _first_last(items: list[DeviceBootEvidence], *, limit: int) -> list[DeviceBootEvidence]:
    if len(items) <= limit:
        return items
    head = limit // 2
    tail = limit - head
    return [*items[:head], *items[-tail:]]


def _final_finished_at(session: _BootSession) -> datetime | None:
    if item := session.events.get("info_status_0_0"):
        return item.timestamp
    if item := session.events.get("update_result"):
        return item.timestamp
    if item := session.events.get("start_completed"):
        return item.timestamp
    return None


def _summary_lines(session: _BootSession, segments: list[DeviceBootSegment], total: float | None) -> list[str]:
    lines = []
    before_bm = _duration_between(session, "validator_started", "start_bm")
    bm_start = _duration_between(session, "start_bm", "bm_tcp_listen")
    fixed_wait = _duration_between(session, "bm_tcp_listen", "start_completed")
    after_start = _duration_between(session, "start_completed", "info_status_0_0")
    for label, value in (
        ("работа АСКП до команды запуска БМ", before_bm),
        ("ожидание запуска BM до открытия TCP-порта", bm_start),
        ("ожидание АСКП после открытия TCP-порта BM", fixed_wait),
        ("обмен АСКП с БМ до статуса 0/0", after_start),
        ("общее время", total),
    ):
        if value is not None:
            lines.append(f"{_format_seconds(value)} — {label}.")
    missing = [segment.title for segment in segments if segment.duration_seconds is None]
    if missing:
        lines.append("Не все этапы рассчитаны: не найдены обе границы для " + "; ".join(missing) + ".")
    return lines


def _duration_between(session: _BootSession, start_key: str, end_key: str) -> float | None:
    start = session.events.get(start_key)
    end = session.events.get(end_key)
    return _duration_seconds(start.timestamp if start else None, end.timestamp if end else None)


def _duration_seconds(started_at: datetime | None, finished_at: datetime | None) -> float | None:
    if started_at is None or finished_at is None:
        return None
    return round((finished_at - started_at).total_seconds(), 3)


def _validator_version(session: _BootSession) -> str | None:
    parts = [session.version_parts.get(part) for part in ("major", "middle", "minor", "build")]
    if all(part is not None for part in parts):
        return ".".join(str(part) for part in parts)
    return None


def _report_title(session: _BootSession, started: datetime | None) -> str:
    serial = session.serial or "unknown"
    if started:
        return f"АСКП_{serial}. Запуск {started:%d.%m.%Y} в {started:%H:%M:%S}"
    return f"АСКП_{serial}. Запуск"


def _parse_timestamp(line: str, base_date: date | None) -> datetime | None:
    if match := BM_TS_RE.search(line):
        return _parse_datetime(match.group("value"), "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S")
    if match := VALIDATOR_TS_RE.search(line):
        return _parse_datetime(
            f"{match.group('date')} {match.group('time')}",
            "%Y-%b-%d %H:%M:%S.%f",
            "%Y-%b-%d %H:%M:%S",
        )
    if base_date and (match := SHORT_TS_RE.search(line)):
        parsed_time = _parse_datetime(match.group("time"), "%H:%M:%S.%f", "%H:%M:%S")
        if parsed_time:
            return datetime.combine(base_date, parsed_time.time())
    return None


def _parse_datetime(value: str, *formats: str) -> datetime | None:
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _format_seconds(value: float) -> str:
    minutes = int(value // 60)
    seconds = value - minutes * 60
    seconds_text = f"{seconds:06.3f}".replace(".", ",")
    return f"{minutes} мин {seconds_text} сек"

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime, time
from pathlib import Path

from core.models import CardReadingComponent, CardReadingEvidence, CardReadingReport

FULL_TS_RE = re.compile(r"\[(?P<value>20\d{2}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?)\]")
ISO_TS_RE = re.compile(r"\[(?P<value>20\d{2}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?)\]")
SHORT_TS_RE = re.compile(r"\[(?P<value>\d{2}:\d{2}:\d{2}(?:\.\d+)?)\]")
BM_TS_RE = re.compile(r'time="(?P<value>20\d{2}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?)"')
FILENAME_DATE_RE = re.compile(r"(20\d{2})-(\d{2})-(\d{2})")
THREAD_RE = re.compile(r"\{(?P<value>\d{4,})\}")
CARD_START_RE = re.compile(r"TicketProcessor:\s*чтение карты\s+(?P<card>\d+)")
PAYMENT_START_CODE_RE = re.compile(r"\bCode:(?P<code>\d+)")
AUTH_TYPE_RE = re.compile(r"\bAuthType:(?P<auth>\d+)")
CONFIRM_CODE_RE = re.compile(r"PaymentConfirm,\s*resp:.*?\bCode:(?P<code>\d+)")
RESP_TOTAL_RE = re.compile(r"PaymentStart,\s*resp:\s*(?P<value>\d+(?:\.\d+)?)(?P<unit>ms|s)\b")
RESP_READER_RE = re.compile(r"\breader=(?P<value>\d+(?:\.\d+)?)(?P<unit>ms|s)\b")
NBS_VERSION_RE = re.compile(r"\bVersion:\s*(?P<value>\d+\.\d+\.\d+\.\d+(?:\+[A-Za-z0-9]+)?)")
BM_VERSION_RE = re.compile(r"\bp:\s*[\w-]+-(?P<version>\d+\.\d+\.\d+)\b")
READER_DEVICE_RE = re.compile(r"reader/device\s*=.*?/dev/(?P<reader>oti|tt)\b", re.IGNORECASE)
READER_SERIAL_RE = re.compile(r"DeviceReaderSerial:.*?/dev/(?P<reader>oti|tt)\b", re.IGNORECASE)

CARD_RELEVANT_NEEDLES = (
    "TicketProcessor: чтение карты",
    "TicketReader::read",
    "Failed authentication",
    "Successful authentication",
    "TicketReader: Read",
    "Loaded services",
    "Validator: карта прочитана",
    "Validator: валидация завершена",
    "статус онлайн-валидации",
    "Send Commands::paymentStart",
    "PaymentStart, req",
    "PaymentStart, resp",
    "PaymentConfirm, req",
    "PaymentConfirm, resp",
    "Send Commands::paymentConfirm",
    "StatusPassOnline",
    "Открытие смены",
    "Открытие - успешно",
    "reader/device",
    "DeviceReaderSerial",
)
BM_RELEVANT_NEEDLES = (
    "PaymentStart, req",
    "PaymentStart, resp",
    "PaymentConfirm, req",
    "PaymentConfirm, resp",
)


@dataclass
class _CardSession:
    source_file: str
    started_at: datetime | None
    card_id: str | None
    reader_type: str | None
    thread_id: str | None
    evidence: list[CardReadingEvidence] = field(default_factory=list)
    source_files: set[str] = field(default_factory=set)
    finished_at: datetime | None = None
    shift_opening: bool = False
    payment_start_send_at: datetime | None = None
    payment_start_succeed_at: datetime | None = None
    payment_confirm_req_at: datetime | None = None
    payment_confirm_resp_at: datetime | None = None
    payment_confirm_succeed_at: datetime | None = None


@dataclass(frozen=True)
class _BmEvent:
    kind: str
    evidence: CardReadingEvidence
    reader_type: str | None
    payment_start_duration: float | None = None
    reader_duration: float | None = None
    payment_start_code: int | None = None
    auth_type: int | None = None
    payment_confirm_code: int | None = None
    bm_version: str | None = None


class CardReadingCollector:
    def __init__(self, *, slow_threshold_seconds: float = 3.0) -> None:
        self.slow_threshold_seconds = slow_threshold_seconds
        self._active_by_file: dict[str, _CardSession] = {}
        self._sessions: list[_CardSession] = []
        self._bm_events: list[_BmEvent] = []
        self._reader_by_file: dict[str, str] = {}
        self.nbs_versions: set[str] = set()
        self.bm_versions: set[str] = set()

    def observe_line(self, source_file: str, line_number: int, line: str) -> None:
        if not _maybe_relevant(line):
            return
        timestamp = _parse_timestamp(line, _date_from_source_file(source_file))
        reader_from_line = _reader_type_from_line(line)
        if reader_from_line:
            self._reader_by_file[source_file] = reader_from_line
        version_match = NBS_VERSION_RE.search(line)
        if version_match:
            self.nbs_versions.add(version_match.group("value"))
        bm_version_match = BM_VERSION_RE.search(line)
        if bm_version_match:
            self.bm_versions.add(bm_version_match.group("version"))

        if any(needle in line for needle in BM_RELEVANT_NEEDLES):
            event = _bm_event(source_file, line_number, timestamp, line)
            if event:
                self._bm_events.append(event)
                if event.bm_version:
                    self.bm_versions.add(event.bm_version)

        start_match = CARD_START_RE.search(line)
        if start_match:
            self._close_unfinished(source_file)
            session = _CardSession(
                source_file=source_file,
                started_at=timestamp,
                card_id=start_match.group("card"),
                reader_type=_reader_type_from_source(source_file) or self._reader_by_file.get(source_file),
                thread_id=_thread_id(line),
            )
            session.source_files.add(source_file)
            session.evidence.append(_evidence(source_file, line_number, timestamp, "Начало чтения карты", line))
            self._active_by_file[source_file] = session
            return

        session = self._active_by_file.get(source_file)
        if session is None or not _is_session_relevant_line(line):
            return

        label = _line_label(line)
        session.evidence.append(_evidence(source_file, line_number, timestamp, label, line))
        session.source_files.add(source_file)
        if "Открытие смены" in line or "Открытие - успешно" in line:
            session.shift_opening = True
        if "Send Commands::paymentStart" in line and "succeed" not in line:
            session.payment_start_send_at = session.payment_start_send_at or timestamp
        if "Send Commands::paymentStart succeed" in line:
            session.payment_start_succeed_at = timestamp
        if "PaymentConfirm, req" in line:
            session.payment_confirm_req_at = session.payment_confirm_req_at or timestamp
        if "PaymentConfirm, resp" in line:
            session.payment_confirm_resp_at = timestamp
        if "Send Commands::paymentConfirm succeed" in line:
            session.payment_confirm_succeed_at = timestamp
        if "Validator: карта прочитана" in line and session.payment_start_send_at is None:
            session.finished_at = timestamp
            self._sessions.append(session)
            self._active_by_file.pop(source_file, None)
        elif "Validator: валидация завершена" in line:
            session.finished_at = timestamp
            self._sessions.append(session)
            self._active_by_file.pop(source_file, None)

    def finalize(self) -> list[CardReadingReport]:
        for source_file in list(self._active_by_file):
            self._close_unfinished(source_file)
        reports = []
        seen: set[tuple[str | None, str | None, str | None, str | None]] = set()
        for session in self._sessions:
            total = _duration_seconds(session.started_at, session.finished_at)
            if total is None or total <= self.slow_threshold_seconds:
                continue
            matched_bm = self._matched_bm_events(session)
            source_files = set(session.source_files)
            source_files.update(event.evidence.source_file for event in matched_bm)
            key = (
                session.reader_type,
                session.card_id,
                session.started_at.isoformat(sep=" ") if session.started_at else None,
                session.finished_at.isoformat(sep=" ") if session.finished_at else None,
            )
            if key in seen:
                continue
            seen.add(key)
            report = _build_report(session, matched_bm, total, sorted(source_files))
            reports.append(report)
        return sorted(
            reports,
            key=lambda item: (
                item.started_at or datetime.min,
                item.reader_type or "",
                item.card_id or "",
            ),
        )

    def _close_unfinished(self, source_file: str) -> None:
        session = self._active_by_file.pop(source_file, None)
        if session and session.finished_at:
            self._sessions.append(session)

    def _matched_bm_events(self, session: _CardSession) -> list[_BmEvent]:
        if session.started_at is None or session.finished_at is None:
            return []
        rows = []
        for event in self._bm_events:
            timestamp = event.evidence.timestamp
            if timestamp is None:
                continue
            if event.reader_type and session.reader_type and event.reader_type != session.reader_type:
                continue
            if session.started_at <= timestamp <= session.finished_at:
                rows.append(event)
        return rows


def _build_report(
    session: _CardSession,
    bm_events: list[_BmEvent],
    total: float,
    source_files: list[str],
) -> CardReadingReport:
    payment_start_resp = _first_event(bm_events, "payment_start_resp")
    payment_confirm_resp = _first_event(bm_events, "payment_confirm_resp")
    reader_seconds = payment_start_resp.reader_duration if payment_start_resp else None
    bm_seconds = _bm_without_reader_seconds(session, bm_events, reader_seconds)
    nbs_seconds = _nbs_seconds(total, bm_seconds, reader_seconds)
    evidence = list(session.evidence)
    evidence.extend(event.evidence for event in bm_events)
    evidence = sorted(evidence, key=lambda item: (item.timestamp or datetime.min, item.source_file, item.line_number))
    components = [
        CardReadingComponent(
            title="НБС",
            duration_seconds=nbs_seconds,
            description="Время обработки в ПО валидатора вне подтверждённого окна БМ и библиотеки ридера.",
            evidence=_component_evidence(evidence, ("Начало чтения карты", "Валидация завершена")),
        ),
        CardReadingComponent(
            title="БМ без библиотеки",
            duration_seconds=bm_seconds,
            description="Расчётное время обмена с БМ без reader-длительности из PaymentStart resp.",
            evidence=_component_evidence(evidence, ("Send paymentStart", "PaymentStart resp", "PaymentConfirm resp")),
        ),
        CardReadingComponent(
            title="ЛИБА vil_api/libcore",
            duration_seconds=reader_seconds,
            description="Значение reader=... из PaymentStart resp.",
            evidence=_component_evidence(evidence, ("PaymentStart resp",)),
        ),
    ]
    return CardReadingReport(
        reader_type=session.reader_type,
        card_id=session.card_id,
        started_at=session.started_at,
        finished_at=session.finished_at,
        total_seconds=total,
        result=_result_text(session, payment_start_resp, payment_confirm_resp),
        payment_start_code=payment_start_resp.payment_start_code if payment_start_resp else None,
        auth_type=payment_start_resp.auth_type if payment_start_resp else None,
        payment_confirm_code=payment_confirm_resp.payment_confirm_code if payment_confirm_resp else None,
        components=components,
        evidence=evidence,
        source_files=source_files,
    )


def _bm_without_reader_seconds(
    session: _CardSession,
    bm_events: list[_BmEvent],
    reader_seconds: float | None,
) -> float | None:
    start = session.payment_start_send_at or _event_timestamp(_first_event(bm_events, "payment_start_req"))
    finish = (
        session.payment_confirm_succeed_at
        or session.payment_confirm_resp_at
        or _event_timestamp(_first_event(bm_events, "payment_confirm_resp"))
        or session.payment_start_succeed_at
        or _event_timestamp(_first_event(bm_events, "payment_start_resp"))
    )
    window = _duration_seconds(start, finish)
    if window is None:
        return 0.0 if reader_seconds is None else None
    if reader_seconds is None:
        return round(window, 3)
    return max(0.0, round(window - reader_seconds, 3))


def _nbs_seconds(total: float, bm_seconds: float | None, reader_seconds: float | None) -> float | None:
    if bm_seconds is None and reader_seconds is None:
        return round(total, 3)
    return max(0.0, round(total - (bm_seconds or 0.0) - (reader_seconds or 0.0), 3))


def _result_text(
    session: _CardSession,
    payment_start_resp: _BmEvent | None,
    payment_confirm_resp: _BmEvent | None,
) -> str:
    if session.shift_opening and payment_start_resp is None:
        return "Открытие смены; PaymentStart не выполнялся"
    if payment_start_resp is None:
        return "PaymentStart не найден в окне чтения"
    code = payment_start_resp.payment_start_code
    auth = payment_start_resp.auth_type
    confirm_code = payment_confirm_resp.payment_confirm_code if payment_confirm_resp else None
    if code == 0 and confirm_code in {None, 0}:
        auth_text = f", AuthType {auth}" if auth is not None else ""
        return f"PaymentStart Code 0{auth_text}; проход разрешён"
    if code == 4:
        return "PaymentStart Code 4; карта в стоп-листе, проход запрещён"
    if confirm_code is not None:
        return f"PaymentStart Code {code}; PaymentConfirm Code {confirm_code}"
    return f"PaymentStart Code {code}"


def _bm_event(source_file: str, line_number: int, timestamp: datetime | None, line: str) -> _BmEvent | None:
    if "PaymentStart, req" in line:
        return _BmEvent("payment_start_req", _evidence(source_file, line_number, timestamp, "PaymentStart req", line), _reader_type_from_source(source_file))
    if "PaymentStart, resp" in line:
        return _BmEvent(
            "payment_start_resp",
            _evidence(source_file, line_number, timestamp, "PaymentStart resp", line),
            _reader_type_from_source(source_file),
            payment_start_duration=_parse_duration_match(RESP_TOTAL_RE.search(line)),
            reader_duration=_parse_duration_match(RESP_READER_RE.search(line)),
            payment_start_code=_int_match(PAYMENT_START_CODE_RE.search(line), "code"),
            auth_type=_int_match(AUTH_TYPE_RE.search(line), "auth"),
            bm_version=_str_match(BM_VERSION_RE.search(line), "version"),
        )
    if "PaymentConfirm, req" in line:
        return _BmEvent("payment_confirm_req", _evidence(source_file, line_number, timestamp, "PaymentConfirm req", line), _reader_type_from_source(source_file))
    if "PaymentConfirm, resp" in line:
        return _BmEvent(
            "payment_confirm_resp",
            _evidence(source_file, line_number, timestamp, "PaymentConfirm resp", line),
            _reader_type_from_source(source_file),
            payment_confirm_code=_int_match(CONFIRM_CODE_RE.search(line), "code"),
        )
    return None


def _parse_timestamp(line: str, fallback_date: date | None) -> datetime | None:
    bm_match = BM_TS_RE.search(line)
    if bm_match:
        return _parse_datetime(bm_match.group("value"), ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"))
    for regex, formats in (
        (FULL_TS_RE, ("%Y.%m.%d %H:%M:%S.%f", "%Y.%m.%d %H:%M:%S")),
        (ISO_TS_RE, ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S")),
    ):
        match = regex.search(line)
        if match:
            return _parse_datetime(match.group("value"), formats)
    short_match = SHORT_TS_RE.search(line)
    if short_match and fallback_date:
        parsed_time = _parse_time(short_match.group("value"))
        if parsed_time:
            return datetime.combine(fallback_date, parsed_time)
    return None


def _parse_datetime(value: str, formats: tuple[str, ...]) -> datetime | None:
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _parse_time(value: str) -> time | None:
    for fmt in ("%H:%M:%S.%f", "%H:%M:%S"):
        try:
            return datetime.strptime(value, fmt).time()
        except ValueError:
            continue
    return None


def _date_from_source_file(source_file: str) -> date | None:
    match = FILENAME_DATE_RE.search(source_file)
    if not match:
        return None
    try:
        return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    except ValueError:
        return None


def _duration_seconds(started_at: datetime | None, finished_at: datetime | None) -> float | None:
    if started_at is None or finished_at is None:
        return None
    seconds = (finished_at - started_at).total_seconds()
    if seconds < 0:
        return None
    return round(seconds, 3)


def _parse_duration_match(match: re.Match[str] | None) -> float | None:
    if not match:
        return None
    value = float(match.group("value"))
    if match.group("unit") == "ms":
        value = value / 1000
    return round(value, 3)


def _reader_type_from_source(source_file: str) -> str | None:
    parts = {part.lower() for part in Path(source_file.replace("!", "/")).parts}
    if "oti" in parts:
        return "OTI"
    if "tt" in parts:
        return "TT"
    lowered = source_file.lower()
    if "mgt_nbs-oti-" in lowered:
        return "OTI"
    if "mgt_nbs-tt-" in lowered:
        return "TT"
    return None


def _reader_type_from_line(line: str) -> str | None:
    for regex in (READER_DEVICE_RE, READER_SERIAL_RE):
        match = regex.search(line)
        if match:
            return match.group("reader").upper()
    return None


def _maybe_relevant(line: str) -> bool:
    return any(needle in line for needle in CARD_RELEVANT_NEEDLES) or bool(NBS_VERSION_RE.search(line))


def _is_session_relevant_line(line: str) -> bool:
    return any(needle in line for needle in CARD_RELEVANT_NEEDLES)


def _line_label(line: str) -> str:
    if "TicketReader::read" in line:
        return "TicketReader read"
    if "Failed authentication" in line:
        return "Неуспешная авторизация сектора"
    if "Successful authentication" in line:
        return "Успешная авторизация сектора"
    if "TicketReader: Read" in line:
        return "Физическое чтение завершено"
    if "Loaded services" in line:
        return "Loaded services"
    if "Validator: карта прочитана" in line:
        return "Карта прочитана"
    if "Validator: валидация завершена" in line:
        return "Валидация завершена"
    if "статус онлайн-валидации" in line:
        return "Онлайн-валидация"
    if "Send Commands::paymentStart succeed" in line:
        return "Send paymentStart succeed"
    if "Send Commands::paymentStart" in line:
        return "Send paymentStart"
    if "PaymentConfirm, req" in line:
        return "PaymentConfirm req"
    if "PaymentConfirm, resp" in line:
        return "PaymentConfirm resp"
    if "Открытие смены" in line:
        return "Открытие смены"
    if "Открытие - успешно" in line:
        return "Открытие смены успешно"
    if "StatusPassOnline" in line:
        return "PassOnline status"
    return "Evidence"


def _evidence(source_file: str, line_number: int, timestamp: datetime | None, label: str, line: str) -> CardReadingEvidence:
    return CardReadingEvidence(source_file, line_number, timestamp, label, line.rstrip("\n"))


def _thread_id(line: str) -> str | None:
    match = THREAD_RE.search(line)
    return match.group("value") if match else None


def _first_event(events: list[_BmEvent], kind: str) -> _BmEvent | None:
    for event in events:
        if event.kind == kind:
            return event
    return None


def _event_timestamp(event: _BmEvent | None) -> datetime | None:
    return event.evidence.timestamp if event else None


def _component_evidence(evidence: list[CardReadingEvidence], labels: tuple[str, ...]) -> list[CardReadingEvidence]:
    rows = [item for item in evidence if any(label in item.label for label in labels)]
    return rows[:6]


def _int_match(match: re.Match[str] | None, group: str) -> int | None:
    if not match:
        return None
    return int(match.group(group))


def _str_match(match: re.Match[str] | None, group: str) -> str | None:
    if not match:
        return None
    return match.group(group)

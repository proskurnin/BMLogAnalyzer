from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from statistics import mean, median

from core.models import (
    NbsStartupBmInfoCorrelation,
    NbsStartupEvidence,
    NbsStartupReport,
    NbsStartupSegment,
    NbsStartupStoplistStats,
)

FULL_TS_RE = re.compile(r"\[(?P<value>20\d{2}\.\d{2}\.\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?)\]")
ISO_TS_RE = re.compile(r"\[(?P<value>20\d{2}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?)\]")
SHORT_TS_RE = re.compile(r"\[(?P<value>\d{2}:\d{2}:\d{2}(?:\.\d+)?)\]")
BM_TS_RE = re.compile(r'time="(?P<value>20\d{2}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?)"')
FILENAME_DATE_RE = re.compile(r"(20\d{2})-(\d{2})-(\d{2})")
STOPLIST_DB_MS_RE = re.compile(r"StopListDb:.*?:\s*(?P<value>\d+(?:[.,]\d+)?)\s*мс", re.IGNORECASE)
REFERENCES_SECONDS_RE = re.compile(r"References_nbs_slm:.*?завершена за\s*(?P<value>\d+(?:[.,]\d+)?)\s*сек", re.IGNORECASE)
READER_STATUS_0_RE = re.compile(r"reader\s+status\s*:?\s*0", re.IGNORECASE)
BM_STATUS_RE = re.compile(r"bm\s+status\s*:?\s*(?P<value>\d+)", re.IGNORECASE)
BM_INFO_DURATION_RE = re.compile(r"Info,\s*resp:\s*(?P<value>\d+(?:[.,]\d+)?)(?P<unit>ms|s)\b", re.IGNORECASE)
TID_RE = re.compile(r"\btid:\s*(?P<value>[0-9a-fA-F]+)")
DEVICE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("rid", re.compile(r"\brid:\s*(?P<value>\d+)", re.IGNORECASE)),
    ("BmNumber", re.compile(r"\bBmNumber:\s*(?P<value>\d+)|\bbm\s+number\s*:?\s*(?P<value2>\d+)", re.IGNORECASE)),
    ("TmSerialNumber", re.compile(r"\bTmSerialNumber:\s*(?P<value>\d+)|\btm\s+serial\s+number\s*:?\s*(?P<value2>\d+)", re.IGNORECASE)),
    ("reader_id", re.compile(r"\breader(?:\s+id|_id)\s*:?\s*(?P<value>\d+)", re.IGNORECASE)),
)

VALIDATOR_NEEDLES = (
    "Log started",
    "MODE::SESSION_CLOSED",
    "MODE::VALIDATE",
    "Send Commands::info",
    "reader status",
    "bm status",
    "ServiceBank: getInfo: QR data",
    "InfoWithTimeout",
    "Bm Info:",
    "Info, req:",
    "Info, resp:",
    "TmSerialNumber",
    "BmNumber",
    "tm serial number",
    "bm number",
    "rid:",
    "qrData",
    "Send Commands::settime",
    "Send Commands::updateConfiguration",
    "StopListDb",
    "References_nbs_slm",
    "используется считыватель",
    "Производитель:",
)
STOPPER_NEEDLES = (
    "Downloading",
    "BeginLargeWrite",
    "UpdateByDiffApply",
    "UpdatePan",
    "UpdatePar",
    "readerConfiguration",
    "UpdaterJobOnlyLists: work with db not allowed, skip",
)
STOPPER_LOAD_MARKERS = ("Downloading", "BeginLargeWrite", "UpdateByDiffApply", "UpdatePan", "UpdatePar")
PROBLEM_MODE_VALIDATE_TO_QR_SECONDS = 9.0


@dataclass
class _BmInfoEvent:
    kind: str
    source_file: str
    line_number: int
    timestamp: datetime | None
    evidence: NbsStartupEvidence
    device_ids: dict[str, set[str]] = field(default_factory=dict)
    tid: str | None = None
    duration_ms: float | None = None


@dataclass
class _StartupSession:
    source_file: str
    started_at: datetime | None
    reader_type: str | None = None
    device_ids: dict[str, set[str]] = field(default_factory=dict)
    evidence: list[NbsStartupEvidence] = field(default_factory=list)
    mode_session_closed: NbsStartupEvidence | None = None
    mode_validate: NbsStartupEvidence | None = None
    first_info: NbsStartupEvidence | None = None
    first_ready_status: NbsStartupEvidence | None = None
    pending_reader_status_0: NbsStartupEvidence | None = None
    bm_info_block_timestamp: datetime | None = None
    first_qr: NbsStartupEvidence | None = None
    info_failed: list[NbsStartupEvidence] = field(default_factory=list)
    info_timeout: list[NbsStartupEvidence] = field(default_factory=list)
    stoplist_searches: list[tuple[float, NbsStartupEvidence]] = field(default_factory=list)
    qr_state_events: list[NbsStartupEvidence] = field(default_factory=list)


class NbsStartupCollector:
    def __init__(self, *, slow_mode_validate_to_qr_seconds: float = 3.0) -> None:
        self.slow_mode_validate_to_qr_seconds = slow_mode_validate_to_qr_seconds
        self._active_by_file: dict[str, _StartupSession] = {}
        self._sessions: list[_StartupSession] = []
        self._stopper_events: list[NbsStartupEvidence] = []
        self._bm_info_events: list[_BmInfoEvent] = []

    def observe_line(self, source_file: str, line_number: int, line: str) -> None:
        if not any(needle in line for needle in (*VALIDATOR_NEEDLES, *STOPPER_NEEDLES)):
            return
        timestamp = _parse_timestamp(line, _date_from_source_file(source_file))
        if "Info, req:" in line or "Info, resp:" in line:
            self._observe_bm_info(source_file, line_number, timestamp, line)
        if any(needle in line for needle in STOPPER_NEEDLES):
            self._observe_stopper(source_file, line_number, timestamp, line)
        if not any(needle in line for needle in VALIDATOR_NEEDLES):
            return
        self._observe_validator(source_file, line_number, timestamp, line)

    def finalize(self) -> list[NbsStartupReport]:
        reports_by_window: dict[tuple[str | None, str | None], NbsStartupReport] = {}
        for session in self._sessions:
            if session.mode_validate is None or session.first_qr is None:
                continue
            duration = _duration_seconds(session.mode_validate.timestamp, session.first_qr.timestamp)
            if duration is None or duration < self.slow_mode_validate_to_qr_seconds:
                continue
            key = (
                session.mode_validate.timestamp.isoformat(sep=" ") if session.mode_validate.timestamp else None,
                session.first_qr.timestamp.isoformat(sep=" ") if session.first_qr.timestamp else None,
            )
            report = self._build_report(session, duration)
            existing = reports_by_window.get(key)
            if existing is None or (existing.reader_type is None and report.reader_type is not None):
                reports_by_window[key] = report
        reports = list(reports_by_window.values())
        return sorted(
            reports,
            key=lambda item: (
                item.mode_validate_to_qr_seconds or 0,
                item.mode_validate_at or datetime.min,
                item.source_file,
            ),
            reverse=True,
        )

    def _observe_stopper(self, source_file: str, line_number: int, timestamp: datetime | None, line: str) -> None:
        label = _stopper_label(line)
        if label is None:
            return
        self._stopper_events.append(_evidence(source_file, line_number, timestamp, label, line))

    def _observe_bm_info(self, source_file: str, line_number: int, timestamp: datetime | None, line: str) -> None:
        kind = "req" if "Info, req:" in line else "resp"
        label = "BM Info req" if kind == "req" else "BM Info resp"
        evidence = _evidence(source_file, line_number, timestamp, label, line)
        self._bm_info_events.append(
            _BmInfoEvent(
                kind=kind,
                source_file=source_file,
                line_number=line_number,
                timestamp=timestamp,
                evidence=evidence,
                device_ids=_device_ids_from_line(line),
                tid=_tid_from_line(line),
                duration_ms=_bm_info_duration_ms(line),
            )
        )

    def _observe_validator(self, source_file: str, line_number: int, timestamp: datetime | None, line: str) -> None:
        if "Log started" in line:
            self._close_active(source_file)
            evidence = _evidence(source_file, line_number, timestamp, "Log started", line)
            self._active_by_file[source_file] = _StartupSession(
                source_file=source_file,
                started_at=timestamp,
                evidence=[evidence],
            )
            return
        session = self._active_by_file.get(source_file)
        if session is None:
            return
        _merge_device_ids(session.device_ids, _device_ids_from_line(line))
        if "MODE::SESSION_CLOSED" in line:
            evidence = _evidence(source_file, line_number, timestamp, "MODE::SESSION_CLOSED", line)
            session.mode_session_closed = evidence
            session.evidence.append(evidence)
            return
        if reader_type := _reader_type_from_line(line):
            session.reader_type = session.reader_type or reader_type
            session.evidence.append(_evidence(source_file, line_number, timestamp, f"reader type {reader_type}", line))
            return
        if "MODE::VALIDATE" in line:
            evidence = _evidence(source_file, line_number, timestamp, "MODE::VALIDATE", line)
            session.mode_validate = evidence
            session.evidence.append(evidence)
            return
        if session.mode_validate is None:
            self._observe_stoplist_search(session, source_file, line_number, timestamp, line)
            return
        if "Send Commands::info" in line and "with timeout" in line:
            evidence = _evidence(source_file, line_number, timestamp, "Send Commands::info", line)
            session.first_info = session.first_info or evidence
            session.evidence.append(evidence)
            return
        if qr_evidence := _qr_state_evidence(source_file, line_number, timestamp, line):
            session.qr_state_events.append(qr_evidence)
            session.evidence.append(qr_evidence)
            if qr_evidence.label != "QR data":
                return
        if "Send Commands::info failed" in line:
            evidence = _evidence(source_file, line_number, timestamp, "Send Commands::info failed", line)
            session.info_failed.append(evidence)
            session.evidence.append(evidence)
            return
        if "InfoWithTimeout" in line:
            evidence = _evidence(source_file, line_number, timestamp, "InfoWithTimeout", line)
            session.info_timeout.append(evidence)
            session.evidence.append(evidence)
            return
        if "Bm Info:" in line:
            session.bm_info_block_timestamp = timestamp
            session.evidence.append(_evidence(source_file, line_number, timestamp, "Bm Info", line))
            return
        if _line_has_ready_status(line):
            evidence = _evidence(source_file, line_number, timestamp, "reader/bm status 0/0", line)
            session.first_ready_status = session.first_ready_status or evidence
            session.evidence.append(evidence)
            return
        if _line_has_reader_status_0(line):
            evidence = _evidence(
                source_file,
                line_number,
                timestamp or session.bm_info_block_timestamp,
                "reader status 0",
                line,
            )
            session.pending_reader_status_0 = evidence
            session.evidence.append(evidence)
            return
        bm_status = _bm_status_value(line)
        if bm_status is not None:
            if bm_status == 0 and session.pending_reader_status_0 is not None:
                evidence = _evidence(
                    source_file,
                    line_number,
                    timestamp or session.pending_reader_status_0.timestamp or session.bm_info_block_timestamp,
                    "reader/bm status 0/0",
                    line,
                )
                session.first_ready_status = session.first_ready_status or evidence
                session.evidence.append(evidence)
            session.pending_reader_status_0 = None
            return
        if "ServiceBank: getInfo: QR data" in line:
            evidence = _evidence(source_file, line_number, timestamp, "QR data", line)
            if _line_has_non_empty_qr(line):
                session.first_qr = evidence
                session.evidence.append(evidence)
                self._sessions.append(session)
                self._active_by_file.pop(source_file, None)
            else:
                session.evidence.append(evidence)
            return
        self._observe_stoplist_search(session, source_file, line_number, timestamp, line)

    def _observe_stoplist_search(
        self,
        session: _StartupSession,
        source_file: str,
        line_number: int,
        timestamp: datetime | None,
        line: str,
    ) -> None:
        duration_ms = _stoplist_search_duration_ms(line)
        if duration_ms is None:
            return
        evidence = _evidence(source_file, line_number, timestamp, "StopListDb/References_nbs_slm", line)
        session.stoplist_searches.append((duration_ms, evidence))
        session.evidence.append(evidence)

    def _close_active(self, source_file: str) -> None:
        session = self._active_by_file.pop(source_file, None)
        if session and session.first_qr:
            self._sessions.append(session)

    def _build_report(self, session: _StartupSession, duration: float) -> NbsStartupReport:
        started_at = session.started_at
        mode_validate_at = session.mode_validate.timestamp if session.mode_validate else None
        first_qr_at = session.first_qr.timestamp if session.first_qr else None
        stopper_window = _events_in_window(self._stopper_events, mode_validate_at, first_qr_at)
        stopper_load = [item for item in stopper_window if item.label == "stopper load marker"]
        stopper_reader = [item for item in stopper_window if item.label == "readerConfiguration"]
        stopper_skip = [item for item in stopper_window if item.label == "UpdaterJobOnlyLists skip"]
        max_search = max((value for value, _ in session.stoplist_searches), default=None)
        max_search_evidence = [
            evidence
            for value, evidence in session.stoplist_searches
            if max_search is not None and value == max_search
        ][:3]
        bm_info_correlation = _build_bm_info_correlation(session, self._bm_info_events)
        classification, is_problem, classification_reasons, exclusion_reasons = _classify_report(
            duration,
            len(stopper_load),
            _session_phase(session),
            bm_info_correlation,
        )
        return NbsStartupReport(
            title=_report_title(session),
            reader_type=_reader_type_from_session(session),
            started_at=started_at,
            mode_validate_at=mode_validate_at,
            first_info_at=session.first_info.timestamp if session.first_info else None,
            first_ready_status_at=session.first_ready_status.timestamp if session.first_ready_status else None,
            first_qr_at=first_qr_at,
            mode_validate_to_qr_seconds=duration,
            source_file=session.source_file,
            device_ids=_device_ids_payload(session.device_ids),
            session_phase=_session_phase(session),
            segments=_build_segments(session),
            evidence=_dedupe_evidence(session.evidence),
            problem_candidate=is_problem,
            classification=classification,
            classification_reasons=classification_reasons,
            exclusion_reasons=exclusion_reasons,
            ready_status_seen=session.first_ready_status is not None,
            info_failure_count=len(session.info_failed),
            info_timeout_count=len(session.info_timeout),
            bm_info_correlation=bm_info_correlation,
            stopper_load_marker_count=len(stopper_load),
            stopper_reader_configuration_count=len(stopper_reader),
            stopper_skip_count=len(stopper_skip),
            stopper_evidence=_first_last([*stopper_load, *stopper_reader, *stopper_skip], limit=8),
            stoplist_search_max_ms=max_search,
            stoplist_search_evidence=max_search_evidence,
            stoplist_search_stats=_stoplist_stats(session.stoplist_searches),
            qr_state_evidence=_dedupe_evidence(session.qr_state_events),
            qr_state_change_count=sum(1 for item in session.qr_state_events if item.label != "QR data"),
        )


def _build_segments(session: _StartupSession) -> list[NbsStartupSegment]:
    return [
        _segment(
            "Открытие смены до MODE::VALIDATE",
            "Время от старта лога или MODE::SESSION_CLOSED до перехода НБС в MODE::VALIDATE.",
            session.mode_session_closed or (session.evidence[0] if session.evidence else None),
            session.mode_validate,
        ),
        _segment(
            "Пауза НБС до первого Info",
            "Интервал от MODE::VALIDATE до первой отправки Send Commands::info.",
            session.mode_validate,
            session.first_info,
        ),
        _segment(
            "MODE::VALIDATE до первого QR",
            "Интервал от MODE::VALIDATE до первого непустого ServiceBank: getInfo: QR data.",
            session.mode_validate,
            session.first_qr,
        ),
        _segment(
            "Первый статус 0/0 до первого QR",
            "Интервал от первого найденного reader/bm status 0/0 до первого непустого QR в НБС.",
            session.first_ready_status,
            session.first_qr,
        ),
    ]


def _segment(
    title: str,
    description: str,
    start: NbsStartupEvidence | None,
    end: NbsStartupEvidence | None,
) -> NbsStartupSegment:
    evidence = [item for item in (start, end) if item is not None]
    return NbsStartupSegment(
        title=title,
        description=description,
        started_at=start.timestamp if start else None,
        finished_at=end.timestamp if end else None,
        duration_seconds=_duration_seconds(start.timestamp if start else None, end.timestamp if end else None),
        evidence=evidence,
    )


def _build_bm_info_correlation(
    session: _StartupSession,
    bm_events: list[_BmInfoEvent],
) -> NbsStartupBmInfoCorrelation:
    if session.first_info is None or session.first_qr is None:
        return NbsStartupBmInfoCorrelation(
            status="missing_nbs_window",
            reason="Нет первого Send Commands::info или первого QR для окна корреляции.",
        )
    started_at = session.first_info.timestamp
    finished_at = session.first_qr.timestamp
    if started_at is None or finished_at is None:
        return NbsStartupBmInfoCorrelation(
            status="missing_timestamp",
            reason="Нет timestamp у первого Info или QR.",
        )
    window_events = [
        event
        for event in bm_events
        if event.timestamp is not None and started_at <= event.timestamp <= finished_at
    ]
    if not window_events:
        return NbsStartupBmInfoCorrelation(
            status="not_found",
            reason="В окне первого Info -> QR не найдены BM Info req/resp.",
        )

    session_ids = _device_ids_payload(session.device_ids)
    matched_by_device = [event for event in window_events if _device_ids_match(session.device_ids, event.device_ids)]
    identity, identity_source = _correlation_identity(session.device_ids, matched_by_device or window_events)
    if session_ids:
        if not matched_by_device:
            return NbsStartupBmInfoCorrelation(
                status="not_found_for_device",
                reason="BM Info найден в окне, но device id не совпал с NBS-сценарием.",
                device_identity=identity,
                device_identity_source=identity_source,
                candidate_count=len(window_events),
            )
        candidates = matched_by_device
        status_source = "device_id"
    else:
        unique_identities = _unique_event_identities(window_events)
        if len(unique_identities) == 1:
            candidates = window_events
            identity, identity_source = unique_identities[0]
            status_source = "single_device"
        else:
            return NbsStartupBmInfoCorrelation(
                status="ambiguous",
                reason="BM Info не сопоставлен: найдено несколько кандидатов без device id NBS-сценария.",
                candidate_count=len(window_events),
            )

    req, resp = _first_bm_req_resp_pair(candidates)
    if req is None and resp is None:
        return NbsStartupBmInfoCorrelation(
            status="not_found",
            reason="В окне нет пары BM Info req/resp после фильтрации кандидатов.",
            device_identity=identity,
            device_identity_source=status_source,
            candidate_count=len(candidates),
        )
    evidence = [event.evidence for event in (req, resp) if event is not None]
    return NbsStartupBmInfoCorrelation(
        status="matched",
        reason="BM Info сопоставлен по device id." if status_source == "device_id" else "BM Info сопоставлен, потому что в окне найдено одно устройство.",
        device_identity=identity,
        device_identity_source=identity_source or status_source,
        send_info_at=started_at,
        bm_info_req_at=req.timestamp if req else None,
        bm_info_resp_at=resp.timestamp if resp else None,
        qr_at=finished_at,
        send_info_to_bm_req_seconds=_duration_seconds(started_at, req.timestamp if req else None),
        bm_req_to_resp_seconds=_duration_seconds(req.timestamp if req else None, resp.timestamp if resp else None),
        bm_resp_to_qr_seconds=_duration_seconds(resp.timestamp if resp else None, finished_at),
        bm_info_duration_ms=resp.duration_ms if resp else None,
        candidate_count=len(candidates),
        evidence=evidence,
    )


def _first_bm_req_resp_pair(events: list[_BmInfoEvent]) -> tuple[_BmInfoEvent | None, _BmInfoEvent | None]:
    ordered = sorted(events, key=lambda item: (item.timestamp or datetime.min, item.line_number))
    req_by_tid = {event.tid: event for event in ordered if event.kind == "req" and event.tid}
    first_req = next((event for event in ordered if event.kind == "req"), None)
    for event in ordered:
        if event.kind != "resp":
            continue
        if event.tid and event.tid in req_by_tid:
            return req_by_tid[event.tid], event
        return first_req, event
    return first_req, None


def _stoplist_stats(items: list[tuple[float, NbsStartupEvidence]]) -> NbsStartupStoplistStats:
    values = sorted(value for value, _ in items)
    if not values:
        return NbsStartupStoplistStats()
    threshold = _outlier_threshold(values)
    outliers = [
        evidence
        for value, evidence in items
        if threshold is not None and value >= threshold
    ][:5]
    return NbsStartupStoplistStats(
        count=len(values),
        min_ms=round(values[0], 3),
        max_ms=round(values[-1], 3),
        average_ms=round(mean(values), 3),
        median_ms=round(median(values), 3),
        p90_ms=round(_percentile(values, 0.90), 3),
        p95_ms=round(_percentile(values, 0.95), 3),
        outlier_threshold_ms=threshold,
        outlier_evidence=outliers,
    )


def _percentile(values: list[float], percentile: float) -> float:
    if len(values) == 1:
        return values[0]
    index = (len(values) - 1) * percentile
    lower = int(index)
    upper = min(lower + 1, len(values) - 1)
    if lower == upper:
        return values[lower]
    fraction = index - lower
    return values[lower] * (1 - fraction) + values[upper] * fraction


def _outlier_threshold(values: list[float]) -> float | None:
    if len(values) < 3:
        return None
    med = median(values)
    threshold = max(med * 3, med + 1000)
    return round(threshold, 3)


def _classify_report(
    duration: float,
    stopper_load_count: int,
    session_phase: str,
    bm_info_correlation: NbsStartupBmInfoCorrelation,
) -> tuple[str, bool, list[str], list[str]]:
    reasons: list[str] = []
    exclusions: list[str] = []
    if duration >= PROBLEM_MODE_VALIDATE_TO_QR_SECONDS:
        reasons.append(f"MODE::VALIDATE -> QR >= {PROBLEM_MODE_VALIDATE_TO_QR_SECONDS:.0f} сек")
    else:
        exclusions.append(f"MODE::VALIDATE -> QR < {PROBLEM_MODE_VALIDATE_TO_QR_SECONDS:.0f} сек")
    if session_phase in {"after_log_started", "after_session_closed"}:
        reasons.append("сценарий найден после Log started или MODE::SESSION_CLOSED")
    else:
        exclusions.append("нет фактической границы Log started или MODE::SESSION_CLOSED")
    if stopper_load_count == 0:
        reasons.append("в окне не найдены marker-нагрузки stopper")
    else:
        exclusions.append("в окне есть marker-нагрузки stopper")
    if bm_info_correlation.status == "matched":
        reasons.append("BM Info сопоставлен с устройством")
    else:
        exclusions.append(f"BM Info не сопоставлен с устройством: {bm_info_correlation.status}")
    is_problem = not exclusions
    if is_problem:
        return "problem", True, reasons, exclusions
    if duration < PROBLEM_MODE_VALIDATE_TO_QR_SECONDS:
        return "context_short", False, reasons, exclusions
    return "excluded", False, reasons, exclusions


def _session_phase(session: _StartupSession) -> str:
    if session.mode_session_closed is not None:
        return "after_session_closed"
    if session.started_at is not None:
        return "after_log_started"
    return "unknown"


def _events_in_window(
    items: list[NbsStartupEvidence],
    started_at: datetime | None,
    finished_at: datetime | None,
) -> list[NbsStartupEvidence]:
    if started_at is None or finished_at is None:
        return []
    return [
        item
        for item in items
        if item.timestamp is not None and started_at <= item.timestamp <= finished_at
    ]


def _dedupe_evidence(items: list[NbsStartupEvidence]) -> list[NbsStartupEvidence]:
    seen: set[tuple[str, int, str]] = set()
    rows: list[NbsStartupEvidence] = []
    for item in sorted(items, key=lambda value: (value.timestamp or datetime.min, value.source_file, value.line_number)):
        identity = (item.source_file, item.line_number, item.raw_line)
        if identity in seen:
            continue
        seen.add(identity)
        rows.append(item)
    return rows


def _first_last(items: list[NbsStartupEvidence], *, limit: int) -> list[NbsStartupEvidence]:
    rows = _dedupe_evidence(items)
    if len(rows) <= limit:
        return rows
    head = limit // 2
    tail = limit - head
    return [*rows[:head], *rows[-tail:]]


def _stopper_label(line: str) -> str | None:
    if any(marker in line for marker in STOPPER_LOAD_MARKERS):
        return "stopper load marker"
    if "readerConfiguration" in line:
        return "readerConfiguration"
    if "UpdaterJobOnlyLists: work with db not allowed, skip" in line:
        return "UpdaterJobOnlyLists skip"
    return None


def _stoplist_search_duration_ms(line: str) -> float | None:
    if match := STOPLIST_DB_MS_RE.search(line):
        return _number(match.group("value"))
    if match := REFERENCES_SECONDS_RE.search(line):
        seconds = _number(match.group("value"))
        return seconds * 1000 if seconds is not None else None
    return None


def _line_has_ready_status(line: str) -> bool:
    return _line_has_reader_status_0(line) and _bm_status_value(line) == 0


def _line_has_reader_status_0(line: str) -> bool:
    return bool(READER_STATUS_0_RE.search(line))


def _bm_status_value(line: str) -> int | None:
    if match := BM_STATUS_RE.search(line):
        return int(match.group("value"))
    return None


def _line_has_non_empty_qr(line: str) -> bool:
    return "ServiceBank: getInfo: QR data:" in line and "http" in line


def _report_title(session: _StartupSession) -> str:
    reader = _reader_type_from_session(session) or "не найдено"
    if session.mode_validate and session.mode_validate.timestamp:
        return f"{reader} | MODE::VALIDATE {session.mode_validate.timestamp:%d.%m.%Y в %H:%M:%S}"
    return f"{reader} | Выход НБС в работу"


def _reader_type_from_session(session: _StartupSession) -> str | None:
    return session.reader_type or _reader_type_from_source(session.source_file)


def _reader_type_from_source(source_file: str) -> str | None:
    lowered = source_file.lower()
    if "/oti/" in lowered or "\\oti\\" in lowered:
        return "OTI"
    if "/tt/" in lowered or "\\tt\\" in lowered:
        return "TT"
    return None


def _reader_type_from_line(line: str) -> str | None:
    lowered = line.lower()
    if "используется считыватель" not in lowered and "производитель:" not in lowered:
        return None
    if "/dev/oti" in lowered or "uno:/dev/oti" in lowered or "производитель: oti" in lowered:
        return "OTI"
    if "/dev/termt" in lowered or "termt:" in lowered or "производитель: tt" in lowered:
        return "TT"
    return None


def _device_ids_from_line(line: str) -> dict[str, set[str]]:
    result: dict[str, set[str]] = {}
    for key, pattern in DEVICE_PATTERNS:
        for match in pattern.finditer(line):
            value = match.groupdict().get("value") or match.groupdict().get("value2")
            if value:
                result.setdefault(key, set()).add(value)
    return result


def _merge_device_ids(target: dict[str, set[str]], source: dict[str, set[str]]) -> None:
    for key, values in source.items():
        target.setdefault(key, set()).update(values)


def _device_ids_payload(device_ids: dict[str, set[str]]) -> dict[str, list[str]]:
    return {key: sorted(values) for key, values in sorted(device_ids.items()) if values}


def _device_ids_match(left: dict[str, set[str]], right: dict[str, set[str]]) -> bool:
    if not left or not right:
        return False
    for left_key, left_values in left.items():
        for right_key, right_values in right.items():
            for left_value in left_values:
                for right_value in right_values:
                    if _device_id_values_match(left_key, left_value, right_key, right_value):
                        return True
    return False


def _device_id_values_match(left_key: str, left_value: str, right_key: str, right_value: str) -> bool:
    if left_key == right_key and left_value == right_value:
        return True
    serial_keys = {"BmNumber", "TmSerialNumber", "rid", "reader_id"}
    if left_key in serial_keys and right_key in serial_keys:
        return left_value.endswith(right_value) or right_value.endswith(left_value)
    return False


def _correlation_identity(
    session_ids: dict[str, set[str]],
    events: list[_BmInfoEvent],
) -> tuple[str | None, str | None]:
    for key in ("rid", "BmNumber", "TmSerialNumber", "reader_id"):
        if values := session_ids.get(key):
            return sorted(values)[0], key
    identities = _unique_event_identities(events)
    return identities[0] if len(identities) == 1 else (None, None)


def _unique_event_identities(events: list[_BmInfoEvent]) -> list[tuple[str, str]]:
    identities: set[tuple[str, str]] = set()
    for event in events:
        for key in ("rid", "BmNumber", "TmSerialNumber", "reader_id"):
            values = event.device_ids.get(key, set())
            if values:
                value = sorted(values)[0]
                identities.add((value, key))
                break
    return sorted(identities)


def _tid_from_line(line: str) -> str | None:
    if match := TID_RE.search(line):
        return match.group("value")
    return None


def _bm_info_duration_ms(line: str) -> float | None:
    if not (match := BM_INFO_DURATION_RE.search(line)):
        return None
    value = _number(match.group("value"))
    if value is None:
        return None
    return round(value * 1000, 3) if match.group("unit").lower() == "s" else round(value, 3)


def _qr_state_evidence(
    source_file: str,
    line_number: int,
    timestamp: datetime | None,
    line: str,
) -> NbsStartupEvidence | None:
    lowered = line.lower()
    if "servicebank: getinfo: qr data" in lowered:
        label = "QR data" if "http" in lowered else "QR data empty"
        return _evidence(source_file, line_number, timestamp, label, line)
    if "qrdata" in lowered:
        return _evidence(source_file, line_number, timestamp, "qrData marker", line)
    if "send commands::updateconfiguration" in lowered:
        return _evidence(source_file, line_number, timestamp, "updateConfiguration command", line)
    if "send commands::settime" in lowered:
        return _evidence(source_file, line_number, timestamp, "settime command", line)
    if "qr" in lowered and any(marker in lowered for marker in ("clear", "reset", "empty", "очист", "сброс")):
        return _evidence(source_file, line_number, timestamp, "QR state change marker", line)
    return None


def _evidence(
    source_file: str,
    line_number: int,
    timestamp: datetime | None,
    label: str,
    line: str,
) -> NbsStartupEvidence:
    return NbsStartupEvidence(
        source_file=source_file,
        line_number=line_number,
        timestamp=timestamp,
        label=label,
        raw_line=line.rstrip("\n"),
    )


def _parse_timestamp(line: str, fallback_date: date | None) -> datetime | None:
    if match := BM_TS_RE.search(line):
        return _parse_datetime(match.group("value"), "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S")
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


def _number(value: str) -> float | None:
    try:
        return float(value.replace(",", "."))
    except ValueError:
        return None

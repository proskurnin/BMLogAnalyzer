from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from core.models import DeviceBootDiagnostic, DeviceBootEvidence, DeviceBootReport, DeviceBootSegment


@dataclass(frozen=True)
class DeviceBootDiagnosticThresholds:
    long_qr_seconds: float = 10.0
    frequent_bm_stop_count: int = 2
    fixed_wait_min_seconds: float = 25.0
    fixed_wait_max_seconds: float = 35.0
    long_bm_start_seconds: float = 30.0
    long_first_info_seconds: float = 10.0
    version_duration_ratio: float = 1.25


def diagnose_device_boot(
    reports: list[DeviceBootReport],
    *,
    thresholds: DeviceBootDiagnosticThresholds | None = None,
) -> dict[str, list[DeviceBootDiagnostic]]:
    active_thresholds = thresholds or DeviceBootDiagnosticThresholds()
    diagnostics_by_report = {
        _report_key(report): diagnose_device_boot_report(report, thresholds=active_thresholds)
        for report in reports
    }
    _append_old_validator_version_diagnostics(reports, diagnostics_by_report, active_thresholds)
    return diagnostics_by_report


def diagnose_device_boot_report(
    report: DeviceBootReport,
    *,
    thresholds: DeviceBootDiagnosticThresholds | None = None,
) -> list[DeviceBootDiagnostic]:
    active_thresholds = thresholds or DeviceBootDiagnosticThresholds()
    diagnostics: list[DeviceBootDiagnostic] = []
    diagnostics.extend(_long_qr_search(report, active_thresholds))
    diagnostics.extend(_frequent_bm_stops(report, active_thresholds))
    diagnostics.extend(_fixed_bm_wait(report, active_thresholds))
    diagnostics.extend(_long_bm_start(report, active_thresholds))
    diagnostics.extend(_long_first_info(report, active_thresholds))
    diagnostics.extend(_update_configuration_when_ready(report))
    return diagnostics


def _long_qr_search(
    report: DeviceBootReport,
    thresholds: DeviceBootDiagnosticThresholds,
) -> list[DeviceBootDiagnostic]:
    segment = _segment_by_title(report, "Поиск QR-ридера")
    if not segment or segment.duration_seconds is None or segment.duration_seconds < thresholds.long_qr_seconds:
        return []
    failed_count = _evidence_count(segment.evidence, "Open QR failed")
    fact = (
        f"QR-ридер искался {_format_seconds(segment.duration_seconds)}"
        f"; Open QR failed: {failed_count}."
    )
    return [
        DeviceBootDiagnostic(
            diagnostic_id="long_qr_search",
            title="Долгий поиск QR-ридера",
            severity="warning",
            fact=fact,
            what_to_check="Проверить, почему устройство ищет QR-ридер и можно ли сократить этот этап, если QR отсутствует.",
            started_at=segment.started_at,
            finished_at=segment.finished_at,
            duration_seconds=segment.duration_seconds,
            count=failed_count,
            evidence=segment.evidence,
        )
    ]


def _frequent_bm_stops(
    report: DeviceBootReport,
    thresholds: DeviceBootDiagnosticThresholds,
) -> list[DeviceBootDiagnostic]:
    segment = _segment_by_title(report, "Остановка вариантов")
    if not segment:
        return []
    stop_evidence = [item for item in segment.evidence if "bm.sh stop" in item.raw_line]
    if len(stop_evidence) < thresholds.frequent_bm_stop_count:
        return []
    return [
        DeviceBootDiagnostic(
            diagnostic_id="frequent_bm_stops",
            title="Повторные остановки БМ",
            severity="warning",
            fact=f"Перед запуском БМ найдены команды bm.sh stop: {len(stop_evidence)}.",
            what_to_check="Проверить, нужны ли повторные остановки BM-модулей перед выбором и запуском фактического БМ.",
            started_at=_first_timestamp(stop_evidence),
            finished_at=_last_timestamp(stop_evidence),
            count=len(stop_evidence),
            evidence=stop_evidence,
        )
    ]


def _fixed_bm_wait(
    report: DeviceBootReport,
    thresholds: DeviceBootDiagnosticThresholds,
) -> list[DeviceBootDiagnostic]:
    segment = _segment_by_title(report, "АСКП ждёт")
    if not segment or segment.duration_seconds is None:
        return []
    if not thresholds.fixed_wait_min_seconds <= segment.duration_seconds <= thresholds.fixed_wait_max_seconds:
        return []
    return [
        DeviceBootDiagnostic(
            diagnostic_id="fixed_bm_wait",
            title="Фиксированное ожидание после запуска БМ",
            severity="info",
            fact=f"После открытия TCP-порта БМ АСКП продолжал ждать {_format_seconds(segment.duration_seconds)}.",
            what_to_check="Проверить, можно ли заменить фиксированное ожидание на ожидание фактической готовности БМ.",
            started_at=segment.started_at,
            finished_at=segment.finished_at,
            duration_seconds=segment.duration_seconds,
            evidence=segment.evidence,
        )
    ]


def _long_bm_start(
    report: DeviceBootReport,
    thresholds: DeviceBootDiagnosticThresholds,
) -> list[DeviceBootDiagnostic]:
    segment = _segment_by_title(report, "Запуск БМ")
    if not segment or segment.duration_seconds is None or segment.duration_seconds < thresholds.long_bm_start_seconds:
        return []
    hypothesis = ""
    if not _has_systemd_or_network_evidence(segment.evidence):
        hypothesis = "Возможна задержка systemd или сети, но прямых systemd/network evidence-строк в этом интервале не найдено."
    return [
        DeviceBootDiagnostic(
            diagnostic_id="long_bm_start",
            title="Долгий запуск БМ",
            severity="warning",
            fact=f"Интервал запуска БМ занял {_format_seconds(segment.duration_seconds)}.",
            hypothesis=hypothesis,
            what_to_check="Проверить логи systemd/network-online и активность процесса БМ в этом временном окне.",
            started_at=segment.started_at,
            finished_at=segment.finished_at,
            duration_seconds=segment.duration_seconds,
            evidence=segment.evidence,
        )
    ]


def _long_first_info(
    report: DeviceBootReport,
    thresholds: DeviceBootDiagnosticThresholds,
) -> list[DeviceBootDiagnostic]:
    segment = _segment_by_title(report, "Первый Info")
    if not segment or segment.duration_seconds is None or segment.duration_seconds < thresholds.long_first_info_seconds:
        return []
    return [
        DeviceBootDiagnostic(
            diagnostic_id="long_first_info",
            title="Долгая пауза до первого Info",
            severity="warning",
            fact=f"После START COMPLETED первый Info/протокол был получен через {_format_seconds(segment.duration_seconds)}.",
            what_to_check="Проверить активность ПО валидатора и обмен с БМ после START COMPLETED.",
            started_at=segment.started_at,
            finished_at=segment.finished_at,
            duration_seconds=segment.duration_seconds,
            evidence=segment.evidence,
        )
    ]


def _update_configuration_when_ready(report: DeviceBootReport) -> list[DeviceBootDiagnostic]:
    update_segment = _segment_by_title(report, "UpdateConfiguration")
    if not update_segment:
        return []
    ready_evidence = _ready_status_evidence_before_update(update_segment.evidence)
    if not ready_evidence:
        return []
    update_evidence = [
        item
        for item in update_segment.evidence
        if "updateconfiguration" in item.raw_line.lower() or "Send updateConfiguration" in item.label
    ]
    if not update_evidence:
        return []
    evidence = [*ready_evidence, *update_evidence[:3]]
    return [
        DeviceBootDiagnostic(
            diagnostic_id="update_configuration_when_ready",
            title="UpdateConfiguration при готовом БМ",
            severity="info",
            fact="Перед UpdateConfiguration найден Info со статусами reader status 0 и bm status 0.",
            what_to_check="Проверить, нужен ли UpdateConfiguration, если БМ уже вернул готовые статусы 0/0.",
            started_at=_first_timestamp(evidence),
            finished_at=_last_timestamp(evidence),
            evidence=evidence,
        )
    ]


def _append_old_validator_version_diagnostics(
    reports: list[DeviceBootReport],
    diagnostics_by_report: dict[str, list[DeviceBootDiagnostic]],
    thresholds: DeviceBootDiagnosticThresholds,
) -> None:
    comparable = [
        report
        for report in reports
        if report.validator_version and report.total_seconds is not None
    ]
    versions = {_version_tuple(report.validator_version or "") for report in comparable}
    versions.discard(())
    if len(versions) < 2:
        return
    latest_version = max(versions)
    latest_reports = [
        report
        for report in comparable
        if _version_tuple(report.validator_version or "") == latest_version
    ]
    latest_durations = [report.total_seconds for report in latest_reports if report.total_seconds is not None]
    if not latest_durations:
        return
    latest_avg = sum(latest_durations) / len(latest_durations)
    for report in comparable:
        version = _version_tuple(report.validator_version or "")
        if not version or version >= latest_version:
            continue
        duration = report.total_seconds
        if duration is None or duration < latest_avg * thresholds.version_duration_ratio:
            continue
        diagnostics_by_report.setdefault(_report_key(report), []).append(
            DeviceBootDiagnostic(
                diagnostic_id="old_validator_version",
                title="Старая версия ПО валидатора",
                severity="info",
                fact=(
                    f"Запуск выполнен на версии {report.validator_version}; "
                    f"в этом наборе также есть более новая версия {_format_version_tuple(latest_version)}."
                ),
                what_to_check="Проверить отличия версии ПО валидатора и возможность обновления, если это подтверждено владельцем ПО.",
                duration_seconds=duration,
                evidence=_first_evidence(report),
            )
        )


def _ready_status_evidence_before_update(evidence: list[DeviceBootEvidence]) -> list[DeviceBootEvidence]:
    ready_rows: list[DeviceBootEvidence] = []
    pending_reader_ready: DeviceBootEvidence | None = None
    for item in evidence:
        raw = item.raw_line.lower()
        if "updateconfiguration" in raw:
            break
        if _line_has_ready_reader_status(raw):
            pending_reader_ready = item
        if _line_has_ready_bm_status(raw):
            if pending_reader_ready is not None:
                ready_rows = [pending_reader_ready, item] if pending_reader_ready != item else [item]
            elif _line_has_ready_reader_status(raw):
                ready_rows = [item]
        if ready_rows:
            return ready_rows
    return []


def _line_has_ready_reader_status(raw: str) -> bool:
    return (
        "reader status" in raw
        and (": 0" in raw or ":0" in raw)
    ) or "readerstatus:0" in raw


def _line_has_ready_bm_status(raw: str) -> bool:
    return (
        "bm status" in raw
        and (": 0" in raw or ":0" in raw)
    ) or "bmstatus:0" in raw


def _segment_by_title(report: DeviceBootReport, needle: str) -> DeviceBootSegment | None:
    needle_lower = needle.lower()
    for segment in report.segments:
        if needle_lower in segment.title.lower():
            return segment
    return None


def _evidence_count(evidence: list[DeviceBootEvidence], text: str) -> int:
    text_lower = text.lower()
    return sum(1 for item in evidence if text_lower in item.raw_line.lower())


def _has_systemd_or_network_evidence(evidence: list[DeviceBootEvidence]) -> bool:
    needles = ("systemd", "network-online", "networkmanager", "kernel:")
    return any(any(needle in item.raw_line.lower() for needle in needles) for item in evidence)


def _first_timestamp(evidence: list[DeviceBootEvidence]) -> datetime | None:
    values = [item.timestamp for item in evidence if item.timestamp is not None]
    return min(values) if values else None


def _last_timestamp(evidence: list[DeviceBootEvidence]) -> datetime | None:
    values = [item.timestamp for item in evidence if item.timestamp is not None]
    return max(values) if values else None


def _report_key(report: DeviceBootReport) -> str:
    started = report.started_at.isoformat(sep=" ") if report.started_at else ""
    serial = report.validator_serial or "unknown"
    return f"{serial}|{started}"


def _format_seconds(value: float) -> str:
    minutes = int(value // 60)
    seconds = value - minutes * 60
    if minutes:
        return f"{minutes} мин {seconds:06.3f} сек".replace(".", ",")
    return f"{seconds:.3f} сек".replace(".", ",")


def _version_tuple(value: str) -> tuple[int, ...]:
    parts = []
    for part in value.split("."):
        if not part.isdigit():
            break
        parts.append(int(part))
    return tuple(parts)


def _format_version_tuple(value: tuple[int, ...]) -> str:
    return ".".join(str(part) for part in value)


def _first_evidence(report: DeviceBootReport) -> list[DeviceBootEvidence]:
    for segment in report.segments:
        if segment.evidence:
            return [segment.evidence[0]]
    return []

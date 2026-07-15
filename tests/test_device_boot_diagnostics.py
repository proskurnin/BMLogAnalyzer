from datetime import datetime, timedelta

from dataclasses import replace

from analytics.device_boot_diagnostics import DeviceBootDiagnosticThresholds, diagnose_device_boot, diagnose_device_boot_report
from core.models import DeviceBootEvidence, DeviceBootReport, DeviceBootSegment


def _evidence(label: str, seconds: float, raw_line: str) -> DeviceBootEvidence:
    return DeviceBootEvidence(
        source_file="validator.log",
        line_number=int(seconds * 10) + 1,
        timestamp=datetime(2026, 7, 13, 12, 0, 0) + timedelta(seconds=seconds),
        label=label,
        raw_line=raw_line,
    )


def _segment(title: str, start: float, end: float, evidence: list[DeviceBootEvidence]) -> DeviceBootSegment:
    started_at = datetime(2026, 7, 13, 12, 0, 0) + timedelta(seconds=start)
    finished_at = datetime(2026, 7, 13, 12, 0, 0) + timedelta(seconds=end)
    return DeviceBootSegment(
        title=title,
        description=title,
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=round(end - start, 3),
        evidence=evidence,
    )


def _report(segments: list[DeviceBootSegment]) -> DeviceBootReport:
    return DeviceBootReport(
        title="АСКП_59757. Запуск",
        validator_serial="59757",
        route="1469",
        validator_version="1.13.53.0",
        bm_version="4.5.13",
        reader_type="OTI",
        started_at=datetime(2026, 7, 13, 12, 0, 0),
        finished_at=datetime(2026, 7, 13, 12, 3, 0),
        total_seconds=180.0,
        segments=segments,
        source_files=["validator.log"],
    )


def test_device_boot_diagnostics_find_evidence_backed_rules():
    report = _report(
        [
            _segment(
                "АСКП. Поиск QR-ридера",
                20,
                43,
                [
                    _evidence("Init QR", 20, "Init QR"),
                    _evidence("Open QR failed", 31, "Open QR failed"),
                    _evidence("Open QR failed", 38, "Open QR failed"),
                    _evidence("QR NOT FOUND", 43, "QR NOT FOUND"),
                ],
            ),
            _segment(
                "АСКП. Остановка вариантов и выбор БМ",
                43,
                60,
                [
                    _evidence("bm.sh stop", 44, "/validator/bm_modules/1/bm.sh stop"),
                    _evidence("bm.sh stop", 50, "/validator/bm_modules/2/bm.sh stop"),
                ],
            ),
            _segment(
                "АСКП/systemd. Запуск БМ",
                60,
                112,
                [
                    _evidence("start BM", 60, "start BM: /validator/bm_modules/17/bm.sh start"),
                    _evidence("listening TCP requests", 112, "listening TCP requests on :5888"),
                ],
            ),
            _segment(
                "АСКП ждёт, БМ запускается параллельно",
                112,
                140,
                [
                    _evidence("listening TCP requests", 112, "listening TCP requests on :5888"),
                    _evidence("START COMPLETED", 140, "START COMPLETED!"),
                ],
            ),
            _segment(
                "АСКП и БМ. Первый Info",
                140,
                158,
                [
                    _evidence("send error", 141, "send error: 1"),
                    _evidence("current protocol", 158, "current protocol: 2"),
                ],
            ),
            _segment(
                "АСКП и БМ. Подготовка UpdateConfiguration",
                158,
                164,
                [
                    _evidence("Info detail", 159, "Reader status: 0"),
                    _evidence("Info detail", 160, "Bm status: 0"),
                    _evidence("updateConfiguration Started", 164, "[updateConfiguration] Started"),
                ],
            ),
        ]
    )

    diagnostics = diagnose_device_boot_report(report)
    ids = [item.diagnostic_id for item in diagnostics]

    assert ids == [
        "long_qr_search",
        "frequent_bm_stops",
        "fixed_bm_wait",
        "long_bm_start",
        "long_first_info",
        "update_configuration_when_ready",
    ]
    assert diagnostics[0].fact == "QR-ридер искался 23,000 сек; Open QR failed: 2."
    assert diagnostics[1].count == 2
    assert diagnostics[3].hypothesis.startswith("Возможна задержка systemd")
    assert diagnostics[3].what_to_check
    assert diagnostics[4].evidence[0].raw_line == "send error: 1"
    assert diagnostics[5].fact == "Перед UpdateConfiguration найден Info со статусами reader status 0 и bm status 0."


def test_device_boot_diagnostics_do_not_invent_missing_facts():
    report = _report(
        [
            DeviceBootSegment(
                title="АСКП. Поиск QR-ридера",
                description="missing boundaries",
                started_at=None,
                finished_at=None,
                duration_seconds=None,
                evidence=[_evidence("Init QR", 20, "Init QR")],
            )
        ]
    )

    diagnostics = diagnose_device_boot_report(
        report,
        thresholds=DeviceBootDiagnosticThresholds(long_qr_seconds=1),
    )

    assert diagnostics == []


def test_device_boot_diagnostics_compare_old_validator_versions():
    old_report = replace(
        _report([]),
        validator_version="1.35.0.453",
        total_seconds=130.0,
        started_at=datetime(2026, 7, 13, 12, 0, 0),
        segments=[
            _segment(
                "АСКП. Справочники и настройки",
                0,
                5,
                [_evidence("version", 0, "version_major: 1")],
            )
        ],
    )
    new_report = replace(
        _report([]),
        validator_version="1.35.6.523",
        total_seconds=80.0,
        started_at=datetime(2026, 7, 13, 13, 0, 0),
    )

    diagnostics = diagnose_device_boot(
        [old_report, new_report],
        thresholds=DeviceBootDiagnosticThresholds(version_duration_ratio=1.25),
    )
    old_key = "59757|2026-07-13 12:00:00"
    new_key = "59757|2026-07-13 13:00:00"

    assert [item.diagnostic_id for item in diagnostics[old_key]] == ["old_validator_version"]
    assert diagnostics[old_key][0].fact.startswith("Запуск выполнен на версии 1.35.0.453")
    assert diagnostics[new_key] == []


def test_device_boot_diagnostics_find_slow_info_chain():
    report = _report(
        [
            _segment(
                "АСКП и БМ. Цепочка Info 1",
                10,
                10.5,
                [
                    _evidence("Send Commands::info", 10, "bm::Connection: Send Commands::info with timeout: 5000"),
                    _evidence("Connection endpoint", 10.1, "bm::Connection: Connection endpoint"),
                    _evidence("Connection succeed", 10.2, "bm::Connection: Connection succeed"),
                    _evidence("Write buffer", 10.3, "bm::Connection: Write buffer"),
                    _evidence("Writting succeed", 10.5, "bm::Connection: Writting succeed"),
                ],
            ),
            _segment(
                "АСКП и БМ. Цепочка Info 2",
                20,
                24.2,
                [
                    _evidence("Send Commands::info", 20, "bm::Connection: Send Commands::info with timeout: 5000"),
                    _evidence("Connection endpoint", 21, "bm::Connection: Connection endpoint"),
                    _evidence("Connection succeed", 22, "bm::Connection: Connection succeed"),
                    _evidence("Write buffer", 23, "bm::Connection: Write buffer"),
                    _evidence("Writting succeed", 24.2, "bm::Connection: Writting succeed"),
                ],
            ),
        ]
    )

    diagnostics = diagnose_device_boot_report(
        report,
        thresholds=DeviceBootDiagnosticThresholds(slow_info_chain_seconds=3, info_chain_duration_ratio=2),
    )

    assert [item.diagnostic_id for item in diagnostics] == ["slow_info_chain"]
    assert diagnostics[0].fact == (
        "Полная цепочка Info заняла 4,200 сек; самая быстрая полная цепочка Info в этом запуске: 0,500 сек."
    )
    assert diagnostics[0].evidence[0].raw_line == "bm::Connection: Send Commands::info with timeout: 5000"

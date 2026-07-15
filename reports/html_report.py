from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from html import escape
from pathlib import Path

from analytics.archive_inventory import bm_log_count, explicit_reader_log_count, explicit_system_log_count
from analytics.bm_statuses import UNCLASSIFIED_STATUS, bm_status_summary_rows, classify_bm_status
from analytics.ai_context import build_ai_context
from analytics.carrier_directory import carrier_markers_for_text, carrier_names_for_text, load_carrier_rules
from analytics.check_cases import load_check_cases, run_builtin_checks
from analytics.protocol_scenarios import load_protocol_scenarios, run_protocol_scenarios
from analytics.suspicious import suspicious_line_payloads
from core.contracts import REPORT_MANIFEST_SCHEMA_VERSION
from core.models import (
    AnalysisResult,
    ArchiveInventoryRow,
    CardReadingComponent,
    CardReadingEvidence,
    CardReadingReport,
    CheckCase,
    CheckResult,
    DeviceBootEvidence,
    DeviceBootReport,
    DeviceBootSegment,
    InputSourceSummary,
    PaymentEvent,
    PipelineStats,
)
from core.version import __version__
from reports.section_registry import build_section_sources

LOG_GROUP_SPECS: list[tuple[str, set[str]]] = [
    ("BM logs", {"BM rotate", "BM stdout"}),
    ("Stopper logs", {"Stopper rotate", "Stopper stdout"}),
    ("VIL logs", {"VIL logs"}),
    ("Validator app logs", {"Validator app logs"}),
    ("Reader logs", {"Reader logs"}),
    ("System logs", {"System logs"}),
    ("Unclassified log files", {"Other log-like"}),
]
LOG_CATEGORIES: set[str] = {category for _, categories in LOG_GROUP_SPECS for category in categories}
LOG_TYPE_LABELS: dict[str, str] = {
    "bm": "БМ",
    "stopper": "ПО стоппера",
    "reader": "ридера",
    "oti_reader_library": "библиотеки ридера ОТИ",
    "validator_app": "ПО валидатора",
    "system": "операционной системы",
    "other": "неопределённые",
}
LOG_TYPE_ORDER: dict[str, int] = {
    "bm": 0,
    "stopper": 1,
    "reader": 2,
    "oti_reader_library": 3,
    "validator_app": 4,
    "system": 5,
    "other": 99,
}


def write_html_report(
    events: list[PaymentEvent],
    result: AnalysisResult,
    path: Path | str,
    *,
    stats: PipelineStats | None = None,
) -> None:
    report_path = Path(path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(render_html_report(events, result, stats=stats), encoding="utf-8")
    manifest_path = report_path.with_suffix(".json")
    manifest_path.write_text(
        json.dumps(render_html_report_manifest(events, result, stats=stats), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    ai_context_path = report_path.with_suffix(".ai_context.json")
    ai_context_path.write_text(
        json.dumps(build_ai_context(events, result, stats=stats), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _json_script(data: object) -> str:
    return json.dumps(data, ensure_ascii=False).replace("</", "<\\/")


def render_html_report(
    events: list[PaymentEvent],
    result: AnalysisResult,
    *,
    stats: PipelineStats | None = None,
) -> str:
    archive_inventory = stats.archive_inventory if stats else []
    log_groups, log_total = _build_log_groups(archive_inventory)
    other_groups, other_total = _build_other_groups(archive_inventory)
    input_source_summaries = stats.input_source_summaries if stats else []
    bm_summary_count = _recognized_log_type_count(input_source_summaries, {"bm"}) if input_source_summaries else bm_log_count(archive_inventory)
    reader_summary_count = (
        _recognized_log_type_count(input_source_summaries, {"reader", "oti_reader_library"})
        if input_source_summaries
        else explicit_reader_log_count(archive_inventory)
    )
    system_summary_count = (
        _recognized_log_type_count(input_source_summaries, {"system"})
        if input_source_summaries
        else explicit_system_log_count(archive_inventory)
    )
    recognized_log_groups, recognized_log_total = _build_recognized_log_groups(input_source_summaries)
    visible_log_groups = recognized_log_groups if recognized_log_total else log_groups
    visible_log_total = recognized_log_total if recognized_log_total else log_total
    bm_summary_groups = (
        _recognized_log_type_groups(input_source_summaries, {"bm"})
        if input_source_summaries
        else _build_metric_groups(archive_inventory, {"BM rotate", "BM stdout"})
    )
    reader_summary_groups = (
        _recognized_log_type_groups(input_source_summaries, {"reader", "oti_reader_library"})
        if input_source_summaries
        else _build_metric_groups(archive_inventory, {"Reader logs"})
    )
    system_summary_groups = (
        _recognized_log_type_groups(input_source_summaries, {"system"})
        if input_source_summaries
        else _build_metric_groups(archive_inventory, {"System logs"})
    )
    archive_count = len(stats.input_files) if stats else 0
    bm_versions = _bm_versions(events)
    archive_names = _archive_name_set(stats.input_files if stats else [])
    device_boot_reports = stats.device_boot_reports if stats else []
    card_reading_reports = stats.card_reading_reports if stats else []
    report_carriers = _report_carriers_from_device_boot(device_boot_reports)
    reader_device_profiles = _reader_device_profiles(events, device_boot_reports)
    event_reader_overrides = _event_reader_overrides(reader_device_profiles)
    bm_version_records = _bm_version_records(events, archive_names)
    bm_carrier_records = _bm_carrier_records(events, archive_names, device_boot_reports)
    bm_carriers = _bm_carriers(events, device_boot_reports)
    bm_reader_records = _bm_reader_records(events, archive_names, device_boot_reports)
    reader_types = _reader_types(events, device_boot_reports)
    log_inventory = stats.log_inventory if stats else []
    reader_firmware_records = _reader_firmware_records(events, archive_names, log_inventory)
    reader_firmwares = _reader_firmwares(events, log_inventory)
    bm_period = _bm_period(events)
    bm_date_records = _bm_date_records(events, archive_names)
    bm_group_rows = _bm_status_groups(events)
    bm_group_payloads = _bm_group_payloads(events, archive_names)
    validator_sections = _validator_analytics(events, archive_names)
    suspicious_rows = suspicious_line_payloads(events)
    check_cases = [check for check in load_check_cases() if check.enabled]
    check_results = run_builtin_checks(events, checks=check_cases)
    protocol_scenarios = [scenario for scenario in load_protocol_scenarios() if scenario.enabled]
    protocol_results = run_protocol_scenarios(events, scenarios=protocol_scenarios)
    date_chart = _bm_date_chart(events)
    unclassified_diag = _unclassified_diagnostics(events)
    section_sources = build_section_sources(stats, events)
    report_data = _report_data(
        events,
        archive_names,
        log_groups=log_groups,
        other_groups=other_groups,
        bm_version_records=bm_version_records,
        bm_carrier_records=bm_carrier_records,
        bm_reader_records=bm_reader_records,
        reader_firmware_records=reader_firmware_records,
        bm_date_records=bm_date_records,
        validator_sections=validator_sections,
        protocol_results=protocol_results,
        report_carriers=report_carriers,
        event_reader_overrides=event_reader_overrides,
    )

    bm_meta_cards = _bm_meta_cards(
        versions=bm_versions,
        version_records=bm_version_records,
        carriers=bm_carriers,
        carrier_records=bm_carrier_records,
        readers=reader_types,
        reader_records=bm_reader_records,
        reader_firmwares=reader_firmwares,
        reader_firmware_records=reader_firmware_records,
        period=bm_period,
        date_records=bm_date_records,
    )
    summary_cards = _summary_cards(
        archive_count=archive_count,
        log_total=log_total,
        other_total=other_total,
        bm_version_count=_bm_version_count(events),
        bm_version_records=bm_version_records,
        bm_log_count=bm_summary_count,
        reader_log_count=reader_summary_count,
        system_log_count=system_summary_count,
        archive_records=_archive_records(stats.input_files if stats else []),
        log_groups=log_groups,
        other_groups=other_groups,
        bm_groups=bm_summary_groups,
        reader_groups=reader_summary_groups,
        system_groups=system_summary_groups,
    )
    show_filters = _has_filter_combinations(
        bm_version_records,
        bm_carrier_records,
        bm_reader_records,
        bm_date_records,
    )
    body_parts = [
        "<!doctype html>",
        '<html lang="ru">',
        "<head>",
        '<meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1">',
        "<title>BM Log Analyzer</title>",
        f"<style>{_css()}</style>",
        "</head>",
        "<body>",
        "<main>",
        '<header class="header">',
        "<div>",
        "<h1>BM Log Analyzer</h1>",
        f'<span class="version">отчёт создан в версии сервиса {escape(__version__)}</span>',
        "</div>",
        "</header>",
        _upload_composition_section(
            input_source_summaries,
            visible_log_groups,
            visible_log_total,
            other_groups,
            other_total,
            section_sources,
            _section_source_text(section_sources, "upload_composition"),
            _section_source_text(section_sources, "log_files"),
            _section_source_text(section_sources, "other_files"),
        ),
        '<section class="section">',
        f'<div class="summary-grid">{summary_cards}</div>',
        "</section>",
        '<section class="section section--bm-meta">',
        '<div class="section-title">',
        "<h2>BM сведения</h2>",
        _section_source_note(section_sources, "bm_meta"),
        "</div>",
        _bm_meta_grid(bm_meta_cards),
    ]
    if show_filters:
        body_parts.extend(
            [
                '<div id="active-filters" class="active-filters"></div>',
                '<div id="bm-filter-root" class="bm-filter-root"></div>',
            ]
        )
    body_parts.append("</section>")
    body_parts.extend(
        [
            f'<script id="report-data" type="application/json">{_json_script(report_data)}</script>',
            _check_results_section(check_results, check_cases, events, _section_source_text(section_sources, "validation_checks")),
            _suspicious_section(suspicious_rows, _section_source_text(section_sources, "suspicious")) if suspicious_rows else "",
            _protocol_scenario_results_section(
                protocol_results,
                protocol_scenarios,
                _section_source_text(section_sources, "protocol_scenarios"),
            ),
            _device_boot_speed_section(device_boot_reports, _section_source_text(section_sources, "device_boot_speed")),
            _card_reading_speed_section(
                card_reading_reports,
                _section_source_text(section_sources, "card_reading_speed"),
            ),
            _bm_status_section(
                events,
                bm_group_rows,
                bm_group_payloads,
                date_chart,
                unclassified_diag,
                archive_names,
                _section_source_text(section_sources, "bm_statuses"),
            ),
            _validator_section(validator_sections, _section_source_text(section_sources, "validator_analytics")),
            _modal(),
            _script(),
            "</main>",
            "</body>",
            "</html>",
        ]
    )

    return "\n".join(part for part in body_parts if part)


def render_html_report_manifest(
    events: list[PaymentEvent],
    result: AnalysisResult,
    *,
    stats: PipelineStats | None = None,
) -> dict[str, object]:
    archive_inventory = stats.archive_inventory if stats else []
    log_groups, log_total = _build_log_groups(archive_inventory)
    other_groups, other_total = _build_other_groups(archive_inventory)
    input_source_summaries = stats.input_source_summaries if stats else []
    bm_summary_count = _recognized_log_type_count(input_source_summaries, {"bm"}) if input_source_summaries else bm_log_count(archive_inventory)
    reader_summary_count = (
        _recognized_log_type_count(input_source_summaries, {"reader", "oti_reader_library"})
        if input_source_summaries
        else explicit_reader_log_count(archive_inventory)
    )
    system_summary_count = (
        _recognized_log_type_count(input_source_summaries, {"system"})
        if input_source_summaries
        else explicit_system_log_count(archive_inventory)
    )
    summary_rows = bm_status_summary_rows(events)
    grouped_rows = _bm_status_groups(events)
    archive_names = _archive_name_set(stats.input_files if stats else [])
    validator_sections = _validator_analytics(events, archive_names)
    device_boot_reports = stats.device_boot_reports if stats else []
    card_reading_reports = stats.card_reading_reports if stats else []
    suspicious_rows = suspicious_line_payloads(events)
    check_cases = [check for check in load_check_cases() if check.enabled]
    check_results = run_builtin_checks(events, checks=check_cases)
    protocol_scenarios = [scenario for scenario in load_protocol_scenarios() if scenario.enabled]
    protocol_results = run_protocol_scenarios(events, scenarios=protocol_scenarios)
    stable_sections = [
        "summary",
        "bm_meta",
        "bm_statuses",
        "grouped_statuses",
        "date_dynamics",
        "validator_analytics",
    ]
    if log_total:
        stable_sections.insert(2, "log_files")
    if input_source_summaries:
        stable_sections.insert(1, "upload_composition")
    if other_total:
        insert_at = stable_sections.index("log_files") + 1 if log_total else stable_sections.index("bm_meta") + 1
        stable_sections.insert(insert_at, "other_files")
    if suspicious_rows:
        insert_at = stable_sections.index("bm_statuses")
        stable_sections.insert(insert_at, "suspicious")
    if check_cases:
        insert_at = stable_sections.index("bm_statuses")
        stable_sections.insert(insert_at, "validation_checks")
    if protocol_scenarios:
        insert_at = stable_sections.index("bm_statuses")
        stable_sections.insert(insert_at, "protocol_scenarios")
    if device_boot_reports:
        insert_at = stable_sections.index("bm_statuses")
        stable_sections.insert(insert_at, "device_boot_speed")
    if card_reading_reports:
        insert_at = stable_sections.index("bm_statuses")
        stable_sections.insert(insert_at, "card_reading_speed")
    if unclassified_total := sum(int(row.get("count", 0)) for row in _unclassified_diagnostics(events)):
        stable_sections.insert(-1, "unclassified_diagnostics")
    section_sources = build_section_sources(stats, events, stable_sections)
    return {
        "schema_version": REPORT_MANIFEST_SCHEMA_VERSION,
        "report_type": "analysis_report",
        "report_title": "BM Log Analyzer",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "version": __version__,
        "stable_fields": [
            "schema_version",
            "report_type",
            "report_title",
            "generated_at",
            "version",
            "counts",
            "sections",
            "status_groups",
            "grouped_statuses",
            "log_groups",
            "other_groups",
            "upload_composition",
            "validator_sections",
            "suspicious_lines",
            "validation_check_catalog",
            "validation_checks",
            "protocol_scenarios",
            "protocol_scenario_results",
            "device_boot_speed",
            "card_reading_speed",
            "section_sources",
            "pipeline_steps",
            "extraction_archives",
        ],
        "stable_sections": stable_sections,
        "counts": {
            "archives": len(stats.input_files) if stats else 0,
            "log_files": log_total,
            "other_files": other_total,
            "bm_logs": bm_summary_count,
            "reader_logs": reader_summary_count,
            "system_logs": system_summary_count,
            "events": result.total,
            "success": result.success_count,
            "decline": result.decline_count,
            "technical_error": result.technical_error_count,
            "unknown": result.unknown_count,
            "suspicious": len(suspicious_rows),
            "validation_check_catalog": len(check_cases),
            "validation_checks": len(check_results),
            "protocol_scenarios": len(protocol_results),
            "device_boot_reports": len(device_boot_reports),
            "card_reading_reports": len(card_reading_reports),
            "input_sources": len(input_source_summaries),
        },
        "sections": stable_sections,
        "status_groups": [str(row["status"]) for row in summary_rows],
        "grouped_statuses": [str(row["label"]) for row in grouped_rows],
        "log_groups": [str(group["label"]) for group in log_groups],
        "other_groups": [str(group["label"]) for group in other_groups],
        "upload_composition": [_input_source_summary_payload(item) for item in input_source_summaries],
        "validator_sections": [str(item["validator"]) for item in validator_sections],
        "suspicious_lines": suspicious_rows,
        "validation_check_catalog": [_check_case_payload(item) for item in check_cases],
        "validation_checks": [_check_result_payload(item) for item in check_results],
        "protocol_scenario_results": [_protocol_scenario_result_payload(item) for item in protocol_results],
        "device_boot_speed": [_device_boot_report_payload(item) for item in device_boot_reports],
        "card_reading_speed": [_card_reading_report_payload(item) for item in card_reading_reports],
        "section_sources": section_sources,
        "pipeline_steps": [_pipeline_step_payload(item) for item in (stats.steps if stats else [])],
        "extraction_archives": [_extraction_archive_payload(item) for item in (stats.extraction_archive_stats if stats else [])],
    }


def _pipeline_step_payload(step) -> dict[str, object]:
    return {
        "name": step.name,
        "status": step.status,
        "duration_ms": round(float(step.duration_ms), 3),
        "errors": step.errors,
        "details": dict(step.details),
    }


def _extraction_archive_payload(item) -> dict[str, object]:
    return {
        "source_archive": item.source_archive,
        "origin_archive": item.origin_archive,
        "archive_type": item.archive_type,
        "status": item.status,
        "duration_ms": round(float(item.duration_ms), 3),
        "extracted_files": item.extracted_files,
        "skipped_files": item.skipped_files,
        "size_bytes": item.size_bytes,
        "cache_status": item.cache_status,
    }


def _section_source_text(section_sources: dict[str, dict[str, object]], section_id: str) -> str:
    payload = section_sources.get(section_id) or {}
    return str(payload.get("note") or "")


def _section_source_note(section_sources: dict[str, dict[str, object]], section_id: str) -> str:
    note = _section_source_text(section_sources, section_id)
    if not note:
        return ""
    return f'<p class="section-source">{escape(note)}</p>'


def _metric_card(
    label: str,
    value: object,
    payload: list[dict[str, object]],
    *,
    kind: str,
    payload_format: str = "files",
) -> str:
    return (
        f'<button type="button" class="metric metric--button" '
        f'data-kind="{escape(kind)}" data-format="{escape(payload_format)}" data-label="{escape(label)}" '
        f'data-payload="{escape(json.dumps(payload, ensure_ascii=False))}">'
        f"<span>{escape(label)}</span>"
        f"<strong>{escape(str(value))}</strong>"
        "</button>"
    )


def _bm_meta_grid(cards_html: str) -> str:
    return f'<div class="bm-meta-grid">{cards_html}</div>'


def _upload_composition_section(
    input_items: list[InputSourceSummary],
    log_groups: list[dict[str, object]],
    log_total: int,
    other_groups: list[dict[str, object]],
    other_total: int,
    section_sources: dict[str, dict[str, object]],
    upload_source_note: str = "",
    log_source_note: str = "",
    other_source_note: str = "",
) -> str:
    if not input_items and log_total == 0 and other_total == 0:
        return ""
    summary_text = _upload_composition_summary_text(input_items, log_total, other_total)
    input_rows = "".join(f"<li>{escape(_input_source_summary_text(item))}</li>" for item in input_items)
    input_parts = ['<div class="upload-composition-summary">']
    if input_rows:
        input_parts.append(f"<ul>{input_rows}</ul>")
    elif summary_text:
        input_parts.append(f"<p>{escape(summary_text)}</p>")
    input_parts.append("</div>")
    input_block = "".join(input_parts)
    diagnostics_block = _upload_processing_diagnostics(input_items)
    log_type_detection_block = _upload_log_type_detection(input_items)
    section_source_block = _section_source_matrix(section_sources)
    log_block = ""
    if log_total:
        log_block = "\n".join(
            [
                '<section class="upload-composition-part">',
                "<h3>Он содержит логи следующих типов:</h3>",
                f"<p class=\"muted\">Кликабельная сводка по распознанным типам логов. {escape(log_source_note)}</p>",
                f'<div id="log-files-root">{_bar_chart(log_groups, log_total)}</div>',
                "</section>",
            ]
        )
    other_block = ""
    if other_total:
        other_block = "\n".join(
            [
                '<section class="upload-composition-part">',
                "<h3>Прочие файлы</h3>",
                f"<p class=\"muted\">Файлы, которые не относятся к логам. {escape(other_source_note)}</p>",
                f'<div id="other-files-root">{_bar_chart(other_groups, other_total)}</div>',
                "</section>",
            ]
        )
    return "\n".join(
        [
            '<details class="collapsible collapsible--upload-composition" open>',
            "<summary>",
            "<span>",
            "<strong>Состав загрузки</strong>",
            f"<em>{escape(summary_text)} {escape(upload_source_note)}</em>",
            "</span>",
            "</summary>",
            '<div class="collapsible-body">',
            input_block,
            log_block,
            diagnostics_block,
            log_type_detection_block,
            section_source_block,
            other_block,
            "</div>",
            "</details>",
        ]
    )


def _upload_composition_summary_text(items: list[InputSourceSummary], log_total: int, other_total: int) -> str:
    if items:
        base = f"Источников загрузки: {len(items)}."
    else:
        base = "Состав загрузки рассчитан по структуре архива."
    details = []
    if log_total:
        details.append(f"Распознанных log-файлов: {log_total}.")
    if other_total:
        details.append(f"Прочих файлов: {other_total}.")
    return " ".join([base, *details])


def _upload_processing_diagnostics(items: list[InputSourceSummary]) -> str:
    if not items:
        return ""
    rows = []
    for item in items:
        reasons = _format_skipped_reasons(item.skipped_reasons)
        status, status_class = _processing_status(item)
        coverage = _format_processing_coverage(item)
        rows.append(
            "<tr>"
            f"<td>{escape(Path(item.source_file).name)}</td>"
            f"<td>{escape(_input_kind_label(item.input_kind))}</td>"
            f'<td><span class="source-status source-status--{escape(status_class)}">{escape(status)}</span></td>'
            f"<td>{item.archive_file_count}</td>"
            f"<td>{item.log_file_count}</td>"
            f"<td>{item.other_file_count}</td>"
            f"<td>{item.extracted_file_count}</td>"
            f"<td>{item.analyzed_file_count}</td>"
            f"<td>{escape(coverage)}</td>"
            f"<td>{item.skipped_file_count}</td>"
            f"<td>{escape(reasons)}</td>"
            "</tr>"
        )
    return "\n".join(
        [
            '<section class="upload-composition-part">',
            "<h3>Полнота обработки</h3>",
            '<div class="table-wrap">',
            '<table class="status-table status-table--upload-diagnostics">',
            "<thead class=\"status-table-head\"><tr>"
            "<th>Источник</th><th>Тип</th><th>Статус</th><th>Файлов в источнике</th><th>Log-файлов</th>"
            "<th>Прочих</th><th>Извлечено</th><th>Проанализировано</th><th>Покрытие log-файлов</th><th>Пропущено</th><th>Причины</th>"
            "</tr></thead>",
            f"<tbody>{''.join(rows)}</tbody>",
            "</table>",
            "</div>",
            "</section>",
        ]
    )


def _upload_log_type_detection(items: list[InputSourceSummary]) -> str:
    rows = []
    for item in items:
        for log_type in item.log_types:
            evidence = item.log_type_evidence.get(log_type, [])
            evidence_html = "<br>".join(f"<code>{escape(line)}</code>" for line in evidence[:3])
            if not evidence_html:
                evidence_html = '<span class="muted">evidence не найден</span>'
            rows.append(
                "<tr>"
                f"<td>{escape(Path(item.source_file).name)}</td>"
                f"<td>{escape(_log_type_label(item, log_type))}</td>"
                f"<td>{item.log_type_counts.get(log_type, 0)}</td>"
                f"<td>{evidence_html}</td>"
                "</tr>"
            )
    if not rows:
        return ""
    return "\n".join(
        [
            '<details class="collapsible upload-composition-part upload-composition-details">',
            "<summary>",
            "<span>",
            "<strong>Распознанные типы логов</strong>",
            "<em>Evidence по правилам классификации файлов.</em>",
            "</span>",
            "</summary>",
            '<div class="collapsible-body">',
            '<div class="table-wrap">',
            '<table class="status-table status-table--log-type-detection">',
            '<colgroup><col style="width:20%"><col style="width:18%"><col style="width:10%"><col style="width:52%"></colgroup>',
            '<thead class="status-table-head"><tr><th>Источник</th><th>Тип лога</th><th>Файлов</th><th>Evidence</th></tr></thead>',
            f"<tbody>{''.join(rows)}</tbody>",
            "</table>",
            "</div>",
            "</div>",
            "</details>",
        ]
    )


def _log_type_label(item: InputSourceSummary, log_type: str) -> str:
    labels_by_type = dict(zip(item.log_types, item.log_type_labels, strict=False))
    return labels_by_type.get(log_type, log_type)


def _section_source_matrix(section_sources: dict[str, dict[str, object]]) -> str:
    rows = []
    for section_id in _section_source_matrix_order():
        source = section_sources.get(section_id)
        if not source:
            continue
        required = [str(item) for item in source.get("required_log_type_labels", [])]
        if not required:
            continue
        matched = [str(item) for item in source.get("matched_log_type_labels", [])]
        missing = [str(item) for item in source.get("missing_log_type_labels", [])]
        status = str(source.get("status", ""))
        rows.append(
            '<tr class="status-row">'
            f"<td>{escape(str(source.get('title') or section_id))}</td>"
            f"<td>{escape(', '.join(required) or 'не требуется')}</td>"
            f"<td>{escape(', '.join(matched) or 'нет')}</td>"
            f"<td>{escape(', '.join(missing) or 'нет')}</td>"
            f'<td><span class="source-status source-status--{escape(status)}">{escape(_section_source_status_label(status))}</span></td>'
            "</tr>"
        )
    if not rows:
        return ""
    return "\n".join(
        [
            '<details class="collapsible upload-composition-part upload-composition-details">',
            "<summary>",
            "<span>",
            "<strong>Разделы отчёта и источники</strong>",
            "<em>Какие типы логов нужны разделам отчёта.</em>",
            "</span>",
            "</summary>",
            '<div class="collapsible-body">',
            '<div class="table-wrap">',
            '<table class="status-table status-table--section-sources">',
            "<thead class=\"status-table-head\"><tr>"
            "<th>Раздел</th><th>Нужны логи</th><th>Найдены</th><th>Не найдены</th><th>Статус</th>"
            "</tr></thead>",
            f"<tbody>{''.join(rows)}</tbody>",
            "</table>",
            "</div>",
            "</div>",
            "</details>",
        ]
    )


def _section_source_matrix_order() -> list[str]:
    return [
        "bm_meta",
        "validation_checks",
        "suspicious",
        "protocol_scenarios",
        "device_boot_speed",
        "bm_statuses",
        "grouped_statuses",
        "date_dynamics",
        "unclassified_diagnostics",
        "validator_analytics",
    ]


def _section_source_status_label(status: str) -> str:
    if status == "available":
        return "доступен"
    if status == "partial":
        return "частично"
    if status == "missing":
        return "нет данных"
    return status or "не требуется"


def _format_skipped_reasons(reasons: dict[str, int]) -> str:
    if not reasons:
        return "нет"
    return "; ".join(f"{label}: {count}" for label, count in sorted(reasons.items()))


def _processing_status(item: InputSourceSummary) -> tuple[str, str]:
    if item.log_file_count == 0 and item.analyzed_file_count == 0:
        return "нет log-файлов", "not_required"
    if item.log_file_count and item.analyzed_file_count >= item.log_file_count:
        return "полностью", "available"
    if item.analyzed_file_count:
        return "частично", "partial"
    return "нет анализа", "missing"


def _format_processing_coverage(item: InputSourceSummary) -> str:
    if item.log_file_count <= 0:
        return "не требуется"
    percent = item.analyzed_file_count / item.log_file_count * 100
    return f"{item.analyzed_file_count}/{item.log_file_count} ({percent:.2f}%)"


def _input_kind_label(input_kind: str) -> str:
    if input_kind == "archive":
        return "архив"
    if input_kind == "log_file":
        return "лог-файл"
    return input_kind


def _input_source_summary_text(item: InputSourceSummary) -> str:
    name = Path(item.source_file).name
    labels = _known_log_type_labels(item)
    if item.input_kind == "archive":
        if labels:
            return f"Загружен архив {name}. Распознано типов логов: {len(labels)}."
        return f"Загружен архив {name}. Распознанные типы логов не найдены."

    if labels:
        if len(labels) == 1:
            return f"Загружен лог {labels[0]} {name}."
        return f"Загружен файл {name}. Распознано типов логов: {len(labels)}."
    return f"Загружен файл {name}. Тип лога не определён."


def _known_log_type_labels(item: InputSourceSummary) -> list[str]:
    return [
        label
        for log_type, label in zip(item.log_types, item.log_type_labels, strict=False)
        if log_type != "other"
    ]


def _input_source_summary_payload(item: InputSourceSummary) -> dict[str, object]:
    return {
        "source_file": item.source_file,
        "source_name": Path(item.source_file).name,
        "input_kind": item.input_kind,
        "log_types": item.log_types,
        "log_type_labels": item.log_type_labels,
        "log_type_counts": item.log_type_counts,
        "log_type_evidence": item.log_type_evidence,
        "analyzed_files": item.analyzed_files,
        "extracted_files": item.extracted_files,
        "evidence": item.evidence,
        "summary_text": _input_source_summary_text(item),
        "archive_file_count": item.archive_file_count,
        "log_file_count": item.log_file_count,
        "other_file_count": item.other_file_count,
        "extracted_file_count": item.extracted_file_count,
        "analyzed_file_count": item.analyzed_file_count,
        "skipped_file_count": item.skipped_file_count,
        "skipped_reasons": item.skipped_reasons,
    }


def _bm_meta_cards(
    *,
    versions: str,
    version_records: list[dict[str, object]],
    carriers: str,
    carrier_records: list[dict[str, object]],
    readers: str,
    reader_records: list[dict[str, object]],
    reader_firmwares: str,
    reader_firmware_records: list[dict[str, object]],
    period: str,
    date_records: list[dict[str, object]],
) -> str:
    items = [
        ("Версии БМ", versions or "missing", version_records, "versions"),
        ("Перевозчики", carriers or "missing", carrier_records, "carriers"),
        ("Ридеры", readers or "missing", reader_records, "readers"),
        ("Версии ПО ридеров", reader_firmwares or "missing", reader_firmware_records, "reader_firmwares"),
        ("Даты", period or "missing", date_records, "dates"),
    ]
    return "".join(
        _bm_meta_card(label, value, payload, meta_kind=focus_group)
        for label, value, payload, focus_group in items
    )


def _bm_meta_card(
    label: str,
    value: object,
    payload: list[dict[str, object]],
    *,
    meta_kind: str,
) -> str:
    return (
        f'<button type="button" class="bm-meta-card bm-meta-card--button" '
        f'data-kind="meta" data-meta-kind="{escape(meta_kind)}" data-label="{escape(label)}" '
        f'data-payload="{escape(json.dumps(payload, ensure_ascii=False))}">'
        f"<span>{escape(label)}</span>"
        f"<strong>{escape(str(value))}</strong>"
        "</button>"
    )


def _bar_chart(groups: list[dict[str, object]], total: int) -> str:
    if total == 0:
        return (
            '<div class="bar-chart bar-chart--empty">'
            '<div class="muted">В архиве не найдено файлов для группировки.</div>'
            "</div>"
        )

    palette = ["#2764a3", "#137752", "#a15c06", "#7c3aed", "#d14343", "#475467", "#0f766e", "#5b5bd6", "#6b7280"]
    rows: list[str] = []
    for index, group in enumerate(groups):
        label = str(group["label"])
        count = int(group["count"])
        size_bytes = int(group["size_bytes"])
        payload = group["payload"]
        if count <= 0:
            continue
        percent = count / total * 100
        color = palette[index % len(palette)]
        rows.append(
            "<button type=\"button\" class=\"bar-row\" "
            f'data-kind="files" data-label="{escape(label)}" data-payload="{escape(json.dumps(payload, ensure_ascii=False))}">'
            f"<div class=\"bar-head\"><span>{escape(label)}</span><strong>{count} ({percent:.2f}%) { _format_size(size_bytes) }</strong></div>"
            f"<div class=\"bar-track\"><div class=\"bar-fill\" style=\"width:{min(percent, 100):.2f}%;background:{color};\"></div></div>"
            "</button>"
        )
    return f'<div class="bar-chart">{"".join(rows)}</div>'


def _build_metric_groups(
    archive_inventory: list[ArchiveInventoryRow],
    categories: set[str],
) -> list[dict[str, object]]:
    payload, _, _ = _aggregate_archive_categories(archive_inventory, categories)
    return payload


def _build_log_groups(archive_inventory: list[ArchiveInventoryRow]) -> tuple[list[dict[str, object]], int]:
    rows: list[dict[str, object]] = []
    total = 0
    for label, categories in LOG_GROUP_SPECS:
        payload, count, size_bytes = _aggregate_archive_categories(archive_inventory, categories)
        if count:
            rows.append({"label": label, "count": count, "size_bytes": size_bytes, "payload": payload})
            total += count
    return rows, total


def _build_recognized_log_groups(items: list[InputSourceSummary]) -> tuple[list[dict[str, object]], int]:
    log_types = sorted(
        {log_type for item in items for log_type in item.log_types if log_type != "other"},
        key=lambda value: (LOG_TYPE_ORDER.get(value, 50), value),
    )
    rows = [_recognized_log_type_group(items, {log_type}, label=_log_type_display_label(log_type)) for log_type in log_types]
    rows = [row for row in rows if int(row["count"]) > 0]
    return rows, sum(int(row["count"]) for row in rows)


def _recognized_log_type_groups(items: list[InputSourceSummary], log_types: set[str]) -> list[dict[str, object]]:
    row = _recognized_log_type_group(items, log_types, label=_joined_log_type_label(log_types))
    return [row] if int(row["count"]) > 0 else []


def _recognized_log_type_group(items: list[InputSourceSummary], log_types: set[str], *, label: str) -> dict[str, object]:
    archives = []
    total = 0
    for item in items:
        count = sum(_recognized_log_type_item_count(item, log_type) for log_type in log_types)
        if count <= 0:
            continue
        files = []
        for log_type in log_types:
            files.extend(_source_files_from_evidence(item.log_type_evidence.get(log_type, [])))
        archives.append(
            {
                "archive": Path(item.source_file).name,
                "count": count,
                "size_bytes": 0,
                "files": sorted(set(files)),
            }
        )
        total += count
    return {"label": label, "count": total, "size_bytes": 0, "payload": archives}


def _recognized_log_type_item_count(item: InputSourceSummary, log_type: str) -> int:
    count = int(item.log_type_counts.get(log_type, 0))
    if count > 0:
        return count
    if log_type not in item.log_types:
        return 0
    evidence_files = _source_files_from_evidence(item.log_type_evidence.get(log_type, []))
    if evidence_files:
        return len(set(evidence_files))
    return 1


def _recognized_log_type_count(items: list[InputSourceSummary], log_types: set[str]) -> int:
    return sum(_recognized_log_type_item_count(item, log_type) for item in items for log_type in log_types)


def _source_files_from_evidence(evidence_rows: list[str]) -> list[str]:
    files = []
    for row in evidence_rows:
        source_file = row.split(": ", 1)[0].strip()
        if source_file:
            files.append(source_file)
    return files


def _log_type_display_label(log_type: str) -> str:
    return LOG_TYPE_LABELS.get(log_type, log_type)


def _joined_log_type_label(log_types: set[str]) -> str:
    labels = [_log_type_display_label(log_type) for log_type in sorted(log_types, key=lambda value: (LOG_TYPE_ORDER.get(value, 50), value))]
    return ", ".join(labels)


def _build_other_groups(archive_inventory: list[ArchiveInventoryRow]) -> tuple[list[dict[str, object]], int]:
    groups: dict[str, dict[str, object]] = {}
    total = 0
    for row in archive_inventory:
        if row.category in LOG_CATEGORIES:
            continue
        file_entries = row.file_sizes.items() if row.file_sizes else ((file_name, 0) for file_name in row.files)
        for file_name, size_bytes in file_entries:
            if _is_archive_container_file(str(file_name)):
                continue
            label = _classify_other_file(file_name)
            archive_name = Path(row.archive).name
            group = groups.setdefault(label, {"label": label, "count": 0, "size_bytes": 0, "archives": defaultdict(lambda: {"archive": "", "count": 0, "size_bytes": 0, "files": []})})
            archive_group = group["archives"][archive_name]
            archive_group["archive"] = archive_name
            archive_group["count"] += 1
            archive_group["size_bytes"] += int(size_bytes)
            archive_group["files"].append(file_name)
            group["count"] += 1
            group["size_bytes"] += int(size_bytes)
            total += 1

    rows: list[dict[str, object]] = []
    for label in _other_category_order(groups):
        group = groups[label]
        archives = []
        for archive_name in sorted(group["archives"]):
            archive_group = group["archives"][archive_name]
            archives.append(
                {
                    "archive": archive_group["archive"],
                    "count": archive_group["count"],
                    "size_bytes": archive_group["size_bytes"],
                    "files": sorted({str(file_name) for file_name in archive_group["files"] if file_name}),
                }
            )
        rows.append(
            {
                "label": label,
                "count": group["count"],
                "size_bytes": group["size_bytes"],
                "payload": archives,
            }
        )
    return rows, total


def _is_archive_container_file(file_name: str) -> bool:
    name = file_name.lower()
    return (
        name.endswith(".zip")
        or name.endswith(".rar")
        or name.endswith(".tar.gz")
        or name.endswith(".tgz")
    )


def _summary_cards(
    *,
    archive_count: int,
    log_total: int,
    other_total: int,
    bm_version_count: int,
    bm_version_records: list[dict[str, object]],
    bm_log_count: int,
    reader_log_count: int,
    system_log_count: int,
    archive_records: list[dict[str, object]],
    log_groups: list[dict[str, object]],
    other_groups: list[dict[str, object]],
    bm_groups: list[dict[str, object]],
    reader_groups: list[dict[str, object]],
    system_groups: list[dict[str, object]],
) -> str:
    cards = [
        _metric_card("Архивов", archive_count, archive_records, kind="metric", payload_format="files"),
        _metric_card("Log-файлов", log_total, log_groups, kind="metric", payload_format="files"),
        _metric_card("Прочих файлов", other_total, other_groups, kind="metric", payload_format="files"),
        _metric_card("BM версий", bm_version_count, bm_version_records, kind="metric", payload_format="records"),
        _metric_card("BM logs", bm_log_count, bm_groups, kind="metric", payload_format="files"),
        _metric_card("Reader logs", reader_log_count, reader_groups, kind="metric", payload_format="files"),
        _metric_card("System logs", system_log_count, system_groups, kind="metric", payload_format="files"),
    ]
    return "".join(cards)


def _suspicious_section(rows: list[dict[str, object]], source_note: str = "") -> str:
    if not rows:
        return ""
    rendered_rows = []
    for row in rows:
        source = str(row.get("source_file") or "")
        line_number = str(row.get("line_number") or "")
        timestamp = str(row.get("timestamp") or "")
        code = str(row.get("code") or "")
        reason = str(row.get("reason") or "")
        raw_line = str(row.get("raw_line") or "")
        location = f"{source}:{line_number}" if line_number else source
        rendered_rows.append(
            "<tr class=\"status-row status-row--suspicious\">"
            f"<td><strong>{escape(location)}</strong><br><span class=\"muted\">{escape(timestamp)}</span></td>"
            f"<td>{escape(code)}</td>"
            f"<td>{escape(reason)}</td>"
            f"<td><code>{escape(raw_line)}</code></td>"
            "</tr>"
        )
    table = (
        '<div class="table-wrap suspicious-table-wrap">'
        '<table class="status-table status-table--suspicious">'
        '<thead class="status-table-head"><tr><th>Источник</th><th>Код</th><th>Почему подозрительно</th><th>Строка лога</th></tr></thead>'
        f"<tbody>{''.join(rendered_rows)}</tbody>"
        "</table>"
        "</div>"
    )
    return "\n".join(
        [
            '<details class="collapsible collapsible--suspicious">',
            "<summary>",
            "<span>",
            "<strong>Подозрительно</strong>",
            f"<em>Найдено строк: {len(rows)}. Baseline строится по успешным PaymentStart resp с Code:0. {escape(source_note)}</em>",
            "</span>",
            "</summary>",
            '<div class="collapsible-body">',
            table,
            "</div>",
            "</details>",
        ]
    )


def _check_results_section(
    results: list[CheckResult],
    checks: list[CheckCase],
    events: list[PaymentEvent],
    source_note: str = "",
) -> str:
    if not checks:
        return ""
    matched_check_ids = {result.check_id for result in results}
    active_check_count = len(checks)
    unmatched_checks = [check for check in checks if check.check_id not in matched_check_ids]
    severity_order = {"critical": 0, "warning": 1, "info": 2}

    def sort_key(result: CheckResult) -> tuple[int, str, str, str]:
        return (
            severity_order.get(result.severity, 99),
            result.source_file,
            str(result.line_number or 0).zfill(8),
            result.title,
        )

    sorted_results = sorted(results, key=sort_key)
    rows = []
    for result in sorted_results:
        location = result.source_file
        if result.line_number is not None:
            location = f"{location}:{result.line_number}"
        timestamp = result.timestamp.isoformat(sep=" ") if result.timestamp else ""
        severity_class = f"status-row--check-{result.severity}"
        rows.append(
            f'<tr class="status-row status-row--check {severity_class}">'
            f"<td><strong>{escape(location)}</strong><br><span class=\"muted\">{escape(timestamp)}</span></td>"
            f"<td>{escape(result.severity)}</td>"
            f"<td><strong>{escape(result.title)}</strong><br><span class=\"muted\">{escape(result.check_id)}</span></td>"
            f"<td>{escape(result.evidence)}</td>"
            f"<td><code>{escape(result.raw_line)}</code></td>"
            "</tr>"
        )
    severity_counts: dict[str, int] = defaultdict(int)
    for result in sorted_results:
        severity_counts[result.severity] += 1
    summary_order = ("critical", "warning", "info")
    summary = ", ".join(f"{severity}: {severity_counts[severity]}" for severity in summary_order if severity_counts.get(severity))
    unmatched_rows = []
    for check in unmatched_checks:
        unmatched_rows.append(
            '<tr class="status-row status-row--check-muted">'
            f"<td>{escape(check.severity)}</td>"
            f"<td><strong>{escape(check.title)}</strong><br><span class=\"muted\">{escape(check.check_id)}</span></td>"
            f"<td>{escape(_unmatched_check_reason(check, events))}</td>"
            "</tr>"
        )
    unmatched_block = ""
    if unmatched_rows:
        unmatched_block = "\n".join(
            [
                '<h3 class="checks-subtitle">Не сработали</h3>',
                '<div class="table-wrap checks-table-wrap">',
                '<table class="status-table status-table--checks">',
                '<thead class="status-table-head"><tr><th>Severity</th><th>Проверка</th><th>Почему не сработала</th></tr></thead>',
                f"<tbody>{''.join(unmatched_rows)}</tbody>",
                "</table>",
                "</div>",
            ]
        )
    matched_block = "\n".join(
        [
            '<div class="table-wrap checks-table-wrap">',
            '<table class="status-table status-table--checks">',
            '<colgroup><col style="width:18%"><col style="width:11%"><col style="width:19%"><col style="width:12%"><col style="width:40%"></colgroup>',
            '<thead class="status-table-head"><tr><th>Источник</th><th>Severity</th><th>Проверка</th><th>Evidence</th><th>Строка лога</th></tr></thead>',
            f"<tbody>{''.join(rows)}</tbody>",
            "</table>",
            "</div>",
        ]
    )
    if not rows:
        matched_block = '<p class="muted checks-empty">Сработавших строк нет.</p>'
    summary_text = escape(summary) if summary else "нет сработавших правил"
    return "\n".join(
        [
            '<details class="collapsible collapsible--checks">',
            "<summary>",
            "<span>",
            "<strong>Проверки</strong>",
            f"<em>Сработало правил: {len(matched_check_ids)} из {active_check_count}. {summary_text}. {escape(source_note)}</em>",
            "</span>",
            "</summary>",
            '<div class="collapsible-body">',
            matched_block,
            unmatched_block,
            "</div>",
            "</details>",
        ]
    )


def _protocol_scenario_results_section(results, scenarios, source_note: str = "") -> str:
    if not scenarios:
        return ""
    matched_ids = {result.scenario_id for result in results}
    unmatched = [scenario for scenario in scenarios if scenario.scenario_id not in matched_ids]
    rows = []
    for result in sorted(results, key=lambda item: (item.source_file, item.line_number or 0, item.scenario_id)):
        location = result.source_file
        if result.line_number is not None:
            location = f"{location}:{result.line_number}"
        timestamp = result.timestamp.isoformat(sep=" ") if result.timestamp else ""
        status_class = "status-row--check" if result.status == "matched" else "status-row--check-muted"
        source_note = result.source_document or "нет источника"
        source_sections_text = " / ".join(result.source_sections) if result.source_sections else result.source_section.replace("\n", " / ")
        source_quote_text = " / ".join(result.source_quotes) if result.source_quotes else result.source_quote.replace("\n", " / ")
        rows.append(
            f'<tr class="status-row {status_class}">'
            f"<td><strong>{escape(location)}</strong><br><span class=\"muted\">{escape(timestamp)}</span></td>"
            f"<td>{escape(source_note)}</td>"
            f"<td>{escape(source_sections_text or '—')}</td>"
            f"<td>{escape(source_quote_text or '—')}</td>"
            f"<td>{escape(result.status)}</td>"
            f"<td><strong>{escape(result.title)}</strong><br><span class=\"muted\">{escape(result.scenario_id)}</span></td>"
            f"<td>{escape(result.evidence)}</td>"
            f"<td><code>{escape(result.raw_line)}</code></td>"
            "</tr>"
        )
    matched_block = ""
    if rows:
        matched_block = "\n".join(
            [
                '<div class="table-wrap checks-table-wrap">',
                '<table class="status-table status-table--checks">',
                '<colgroup><col style="width:12%"><col style="width:16%"><col style="width:13%"><col style="width:13%"><col style="width:10%"><col style="width:16%"><col style="width:10%"><col style="width:10%"></colgroup>',
                '<thead class="status-table-head"><tr><th>Источник лога</th><th>Источник</th><th>Разделы</th><th>Цитаты</th><th>Статус</th><th>Сценарий</th><th>Evidence</th><th>Строка лога</th></tr></thead>',
                f"<tbody>{''.join(rows)}</tbody>",
                "</table>",
                "</div>",
            ]
        )
    else:
        matched_block = '<p class="muted checks-empty">Совпадений сценариев нет.</p>'

    unmatched_rows = []
    for scenario in unmatched:
        source_note = scenario.source_document or "нет источника"
        source_sections_text = " / ".join(scenario.source_sections) if scenario.source_sections else scenario.source_section.replace("\n", " / ")
        source_quote_text = " / ".join(scenario.source_quotes) if scenario.source_quotes else scenario.source_quote.replace("\n", " / ")
        unmatched_rows.append(
            '<tr class="status-row status-row--check-muted">'
            f"<td>{escape(scenario.title)}</td>"
            f"<td>{escape(scenario.scenario_id)}</td>"
            f"<td>{escape(source_note)}</td>"
            f"<td>{escape(source_sections_text or '—')}</td>"
            f"<td>{escape(source_quote_text or '—')}</td>"
            f"<td>{escape(scenario.description or 'Нет описания')}</td>"
            "</tr>"
        )
    unmatched_block = ""
    if unmatched_rows:
        unmatched_block = "\n".join(
            [
                '<h3 class="checks-subtitle">Не сработали</h3>',
                '<div class="table-wrap checks-table-wrap">',
                '<table class="status-table status-table--checks">',
                '<colgroup><col style="width:18%"><col style="width:12%"><col style="width:16%"><col style="width:16%"><col style="width:18%"><col style="width:20%"></colgroup>',
                '<thead class="status-table-head"><tr><th>Сценарий</th><th>ID</th><th>Источник</th><th>Разделы</th><th>Цитаты</th><th>Описание</th></tr></thead>',
                f"<tbody>{''.join(unmatched_rows)}</tbody>",
                "</table>",
                "</div>",
            ]
        )
    return "\n".join(
        [
            '<details class="collapsible collapsible--checks">',
            "<summary>",
            "<span>",
            "<strong>Сценарии из протокола взаимодействия</strong>",
            f"<em>Активных сценариев: {len(scenarios)}. Сработало: {len(matched_ids)}. {escape(source_note)}</em>",
            "</span>",
            "</summary>",
            '<div class="collapsible-body">',
            matched_block,
            unmatched_block,
            "</div>",
            "</details>",
        ]
    )


def _device_boot_speed_section(reports: list[DeviceBootReport], source_note: str = "") -> str:
    if not reports:
        return ""
    rendered_reports = []
    for index, report in enumerate(reports, start=1):
        rendered_reports.append(
            "\n".join(
                [
                    '<details class="collapsible device-boot-report">',
                    "<summary>",
                    "<span>",
                    f"<strong>{escape(_device_boot_summary_title(report))}</strong>",
                    f"<em>{escape(_device_boot_source_summary(report))}</em>",
                    "</span>",
                    "</summary>",
                    '<div class="collapsible-body">',
                    _device_boot_report_head(report),
                    _device_boot_segment_table(report.segments),
                    _device_boot_text_report_block(report, index),
                    "</div>",
                    "</details>",
                ]
            )
        )
    summary = f"Найдено запусков: {len(reports)}."
    return "\n".join(
        [
            '<details class="collapsible collapsible--device-boot">',
            "<summary>",
            "<span>",
            "<strong>Скорость загрузки устройства</strong>",
            f"<em>{escape(summary)} Факты рассчитаны по строкам ПО валидатора и BM. {escape(source_note)}</em>",
            "</span>",
            "</summary>",
            '<div class="collapsible-body">',
            _device_boot_summary_cards(reports),
            _device_boot_overview_chart(reports),
            _device_boot_slowest_segments(reports),
            "".join(rendered_reports),
            "</div>",
            "</details>",
        ]
    )


def _device_boot_report_head(report: DeviceBootReport) -> str:
    facts = [
        ("АСКП", report.validator_version or "не найдено"),
        ("БМ", report.bm_version or "не найдено"),
        ("Ридер", report.reader_type or "не найдено"),
        ("Маршрут", report.route or "не найдено"),
        ("Всего", _format_duration(report.total_seconds)),
    ]
    cards = "".join(
        '<div class="device-boot-fact">'
        f"<span>{escape(label)}</span>"
        f"<strong>{escape(value)}</strong>"
        "</div>"
        for label, value in facts
    )
    sources = ", ".join(report.source_files)
    return "\n".join(
        [
            '<div class="device-boot-head">',
            f"<h3>{escape(report.title)}</h3>",
            f'<p class="muted">Источник: {escape(sources or "не найден")}</p>',
            f'<div class="device-boot-facts">{cards}</div>',
            _device_boot_segment_timeline(report.segments, report.total_seconds),
            "</div>",
        ]
    )


def _device_boot_summary_cards(reports: list[DeviceBootReport]) -> str:
    durations = sorted(report.total_seconds for report in reports if report.total_seconds is not None)
    if not durations:
        return ""
    avg = sum(durations) / len(durations)
    facts = [
        ("Запусков с временем", str(len(durations))),
        ("Минимум", _format_duration(durations[0])),
        ("Среднее", _format_duration(avg)),
        ("Максимум", _format_duration(durations[-1])),
    ]
    cards = "".join(
        '<div class="device-boot-fact">'
        f"<span>{escape(label)}</span>"
        f"<strong>{escape(value)}</strong>"
        "</div>"
        for label, value in facts
    )
    return f'<div class="device-boot-facts device-boot-facts--summary">{cards}</div>'


def _device_boot_overview_chart(reports: list[DeviceBootReport]) -> str:
    durations = [report.total_seconds for report in reports if report.total_seconds is not None]
    if not durations:
        return ""
    max_duration = max(durations) or 1
    rows = []
    for report in reports:
        duration = report.total_seconds
        width = 0.0 if duration is None else max(1.0, min(100.0, duration / max_duration * 100))
        rows.append(
            '<div class="device-boot-chart-row">'
            f'<span class="device-boot-chart-label">{escape(_device_boot_short_title(report))}</span>'
            '<div class="device-boot-chart-track">'
            f'<div class="device-boot-chart-fill" style="width:{width:.2f}%"></div>'
            "</div>"
            f'<strong>{escape(_format_duration(duration))}</strong>'
            "</div>"
        )
    return "\n".join(
        [
            '<section class="device-boot-chart">',
            "<h3>Время запусков</h3>",
            "".join(rows),
            "</section>",
        ]
    )


def _device_boot_slowest_segments(reports: list[DeviceBootReport], limit: int = 7) -> str:
    rows_data = []
    for report in reports:
        for segment in report.segments:
            if segment.duration_seconds is None:
                continue
            share = None
            if report.total_seconds and report.total_seconds > 0:
                share = segment.duration_seconds / report.total_seconds * 100
            rows_data.append((segment.duration_seconds, report, segment, share))
    if not rows_data:
        return ""
    rows = []
    for duration, report, segment, share in sorted(rows_data, key=lambda item: item[0], reverse=True)[:limit]:
        evidence = segment.evidence[0] if segment.evidence else None
        evidence_html = escape(_evidence_short_line(evidence)) if evidence else "evidence не найден"
        rows.append(
            '<tr class="status-row">'
            f"<td>{escape(_device_boot_short_title(report))}</td>"
            f"<td><strong>{escape(segment.title)}</strong><br><span class=\"muted\">{escape(segment.description)}</span></td>"
            f"<td>{escape(_format_duration(duration))}</td>"
            f"<td>{escape(_format_percent_value(share))}</td>"
            f"<td><code>{evidence_html}</code></td>"
            "</tr>"
        )
    return "\n".join(
        [
            '<section class="device-boot-chart">',
            "<h3>Самые долгие этапы</h3>",
            '<div class="table-wrap">',
            '<table class="status-table status-table--device-boot-slowest">',
            '<colgroup><col style="width:18%"><col style="width:30%"><col style="width:12%"><col style="width:12%"><col style="width:28%"></colgroup>',
            '<thead class="status-table-head"><tr><th>Запуск</th><th>Этап</th><th>Длительность</th><th>Доля запуска</th><th>Evidence</th></tr></thead>',
            f"<tbody>{''.join(rows)}</tbody>",
            "</table>",
            "</div>",
            "</section>",
        ]
    )


def _device_boot_segment_timeline(segments: list[DeviceBootSegment], total_seconds: float | None) -> str:
    timed_segments = [segment for segment in segments if segment.duration_seconds is not None]
    if not timed_segments or total_seconds is None or total_seconds <= 0:
        return ""
    chips = []
    for index, segment in enumerate(timed_segments, start=1):
        duration = segment.duration_seconds or 0
        width = max(1.0, min(100.0, duration / total_seconds * 100))
        chips.append(
            '<span class="device-boot-timeline-segment" '
            f'style="width:{width:.2f}%" title="{escape(segment.title)}: {escape(_format_duration(duration))}">'
            f"{index}"
            "</span>"
        )
    return "\n".join(
        [
            '<div class="device-boot-timeline" aria-label="Доли этапов запуска">',
            "".join(chips),
            "</div>",
        ]
    )


def _device_boot_summary_title(report: DeviceBootReport) -> str:
    serial = report.validator_serial or "не найдено"
    started_at = _format_datetime(report.started_at)
    return f"АСКП_{serial} | Запуск {started_at} | Время запуска: {_format_duration(report.total_seconds)}"


def _device_boot_short_title(report: DeviceBootReport) -> str:
    serial = report.validator_serial or "unknown"
    if report.started_at:
        return f"АСКП_{serial} {report.started_at:%d.%m %H:%M:%S}"
    return f"АСКП_{serial}"


def _device_boot_source_summary(report: DeviceBootReport) -> str:
    sources = ", ".join(report.source_files)
    return f"Источник: {sources or 'не найден'}"


def _device_boot_text_report_block(report: DeviceBootReport, index: int) -> str:
    text_id = f"device-boot-text-{index}"
    return "\n".join(
        [
            '<details class="collapsible device-boot-text-details">',
            "<summary>",
            "<span>",
            "<strong>Текстовый отчёт</strong>",
            "<em>Подробные этапы запуска и evidence-строки.</em>",
            "</span>",
            "</summary>",
            '<div class="collapsible-body">',
            f'<button type="button" class="copy-button" data-copy-target="{escape(text_id)}">Скопировать текстовый отчёт</button>',
            f'<pre class="device-boot-text" id="{escape(text_id)}">{escape(_device_boot_text_report(report))}</pre>',
            "</div>",
            "</details>",
        ]
    )


def _device_boot_segment_table(segments: list[DeviceBootSegment]) -> str:
    rows = []
    for index, segment in enumerate(segments, start=1):
        evidence = "<br>".join(
            f'<code>{escape(_evidence_short_line(item))}</code>'
            for item in segment.evidence[:8]
        )
        if not evidence:
            evidence = '<span class="muted">evidence не найден</span>'
        rows.append(
            '<tr class="status-row">'
            f"<td><strong>{index}. {escape(segment.title)}</strong><br><span class=\"muted\">{escape(segment.description)}</span></td>"
            f"<td>{escape(_format_time_range(segment.started_at, segment.finished_at))}</td>"
            f"<td>{escape(_format_duration(segment.duration_seconds))}</td>"
            f"<td>{evidence}</td>"
            "</tr>"
        )
    return "\n".join(
        [
            '<div class="table-wrap checks-table-wrap">',
            '<table class="status-table status-table--device-boot">',
            '<colgroup><col style="width:26%"><col style="width:16%"><col style="width:12%"><col style="width:46%"></colgroup>',
            '<thead class="status-table-head"><tr><th>Этап</th><th>Время</th><th>Длительность</th><th>Evidence</th></tr></thead>',
            f"<tbody>{''.join(rows)}</tbody>",
            "</table>",
            "</div>",
        ]
    )


def _device_boot_text_report(report: DeviceBootReport) -> str:
    lines = [
        report.title,
        "",
        f"АСКП — основной управляющий софт, версия {report.validator_version or 'не найдена'}.",
        f"БМ — банковский модуль, версия {report.bm_version or 'не найдена'}.",
        "",
        f"Начало: {_format_timestamp(report.started_at)}.",
        f"Конец: {_format_timestamp(report.finished_at)}.",
        f"Всего: {_format_duration(report.total_seconds)}.",
        "",
    ]
    for index, segment in enumerate(report.segments, start=1):
        lines.extend(
            [
                f"{index}. {segment.title}",
                "",
                f"Время: {_format_duration(segment.duration_seconds)}, {_format_time_range(segment.started_at, segment.finished_at)}.",
                f"На что ушло: {segment.description}",
                "Лог:",
            ]
        )
        if segment.evidence:
            lines.extend(_evidence_short_line(item) for item in segment.evidence)
        else:
            lines.append("evidence не найден")
        lines.append("")
    lines.append("Итог")
    lines.append("")
    lines.extend(report.summary or ["Недостаточно фактов для итоговых расчётов."])
    return "\n".join(lines).strip()


def _device_boot_report_payload(report: DeviceBootReport) -> dict[str, object]:
    return {
        "title": report.title,
        "validator_serial": report.validator_serial,
        "route": report.route,
        "validator_version": report.validator_version,
        "bm_version": report.bm_version,
        "reader_type": report.reader_type,
        "started_at": report.started_at.isoformat(sep=" ") if report.started_at else None,
        "finished_at": report.finished_at.isoformat(sep=" ") if report.finished_at else None,
        "total_seconds": report.total_seconds,
        "source_files": report.source_files,
        "summary": report.summary,
        "slowest_segments": [_device_boot_segment_payload(item) for item in _slowest_report_segments(report)],
        "segments": [_device_boot_segment_payload(item) for item in report.segments],
    }


def _slowest_report_segments(report: DeviceBootReport, limit: int = 3) -> list[DeviceBootSegment]:
    return sorted(
        [segment for segment in report.segments if segment.duration_seconds is not None],
        key=lambda segment: segment.duration_seconds or 0,
        reverse=True,
    )[:limit]


def _device_boot_segment_payload(segment: DeviceBootSegment) -> dict[str, object]:
    return {
        "title": segment.title,
        "description": segment.description,
        "started_at": segment.started_at.isoformat(sep=" ") if segment.started_at else None,
        "finished_at": segment.finished_at.isoformat(sep=" ") if segment.finished_at else None,
        "duration_seconds": segment.duration_seconds,
        "evidence": [_device_boot_evidence_payload(item) for item in segment.evidence],
    }


def _device_boot_evidence_payload(evidence: DeviceBootEvidence) -> dict[str, object]:
    return {
        "source_file": evidence.source_file,
        "line_number": evidence.line_number,
        "timestamp": evidence.timestamp.isoformat(sep=" ") if evidence.timestamp else None,
        "label": evidence.label,
        "raw_line": evidence.raw_line,
    }


def _card_reading_speed_section(reports: list[CardReadingReport], source_note: str = "") -> str:
    if not reports:
        return ""
    rendered_reports = []
    for index, report in enumerate(reports, start=1):
        rendered_reports.append(
            "\n".join(
                [
                    '<details class="collapsible card-reading-report">',
                    "<summary>",
                    "<span>",
                    f"<strong>{escape(_card_reading_summary_title(report))}</strong>",
                    f"<em>{escape(report.result)}. Источников: {len(report.source_files)}.</em>",
                    "</span>",
                    "</summary>",
                    '<div class="collapsible-body">',
                    _card_reading_report_head(report),
                    _card_reading_component_table(report.components),
                    _card_reading_evidence_table(report.evidence),
                    _card_reading_text_report_block(report, index),
                    "</div>",
                    "</details>",
                ]
            )
        )
    summary = f"Кейсов дольше 3 сек: {len(reports)}."
    return "\n".join(
        [
            '<details class="collapsible collapsible--card-reading">',
            "<summary>",
            "<span>",
            "<strong>Долгое чтение и валидация карт</strong>",
            f"<em>{escape(summary)} Факты рассчитаны по строкам ПО валидатора и BM. {escape(source_note)}</em>",
            "</span>",
            "</summary>",
            '<div class="collapsible-body">',
            _card_reading_summary_cards(reports),
            _card_reading_overview_table(reports),
            "".join(rendered_reports),
            "</div>",
            "</details>",
        ]
    )


def _card_reading_report_head(report: CardReadingReport) -> str:
    facts = [
        ("Ридер", report.reader_type or "не найдено"),
        ("Карта", report.card_id or "не найдено"),
        ("Начало", _format_datetime(report.started_at)),
        ("Всего", _format_duration(report.total_seconds)),
        ("Результат", report.result),
    ]
    cards = "".join(
        '<div class="device-boot-fact">'
        f"<span>{escape(label)}</span>"
        f"<strong>{escape(value)}</strong>"
        "</div>"
        for label, value in facts
    )
    sources = ", ".join(report.source_files)
    return "\n".join(
        [
            '<div class="device-boot-head">',
            f"<h3>{escape(_card_reading_summary_title(report))}</h3>",
            f'<p class="muted">Источник: {escape(sources or "не найден")}</p>',
            f'<div class="device-boot-facts">{cards}</div>',
            "</div>",
        ]
    )


def _card_reading_summary_cards(reports: list[CardReadingReport]) -> str:
    durations = sorted(report.total_seconds for report in reports if report.total_seconds is not None)
    if not durations:
        return ""
    reader_counts: dict[str, int] = defaultdict(int)
    for report in reports:
        reader_counts[report.reader_type or "не найдено"] += 1
    reader_text = ", ".join(f"{reader}: {count}" for reader, count in sorted(reader_counts.items()))
    facts = [
        ("Кейсов", str(len(reports))),
        ("По ридерам", reader_text),
        ("Максимум", _format_duration(durations[-1])),
        ("Среднее", _format_duration(sum(durations) / len(durations))),
    ]
    cards = "".join(
        '<div class="device-boot-fact">'
        f"<span>{escape(label)}</span>"
        f"<strong>{escape(value)}</strong>"
        "</div>"
        for label, value in facts
    )
    return f'<div class="device-boot-facts device-boot-facts--summary">{cards}</div>'


def _card_reading_overview_table(reports: list[CardReadingReport]) -> str:
    rows = []
    for report in sorted(reports, key=lambda item: item.total_seconds or 0, reverse=True)[:12]:
        rows.append(
            '<tr class="status-row">'
            f"<td>{escape(report.reader_type or 'не найдено')}</td>"
            f"<td>{escape(_format_datetime(report.started_at))}</td>"
            f"<td>{escape(report.card_id or 'не найдено')}</td>"
            f"<td>{escape(_format_duration(report.total_seconds))}</td>"
            f"<td>{escape(report.result)}</td>"
            "</tr>"
        )
    return "\n".join(
        [
            '<section class="device-boot-chart">',
            "<h3>Самые долгие кейсы</h3>",
            '<div class="table-wrap">',
            '<table class="status-table status-table--card-reading-overview">',
            '<colgroup><col style="width:10%"><col style="width:18%"><col style="width:20%"><col style="width:14%"><col style="width:38%"></colgroup>',
            '<thead class="status-table-head"><tr><th>Ридер</th><th>Начало</th><th>Карта</th><th>Время</th><th>Результат</th></tr></thead>',
            f"<tbody>{''.join(rows)}</tbody>",
            "</table>",
            "</div>",
            "</section>",
        ]
    )


def _card_reading_component_table(components: list[CardReadingComponent]) -> str:
    rows = []
    for component in components:
        evidence = "<br>".join(
            f"<code>{escape(_card_reading_evidence_short_line(item))}</code>"
            for item in component.evidence[:6]
        )
        if not evidence:
            evidence = '<span class="muted">evidence не найден</span>'
        rows.append(
            '<tr class="status-row">'
            f"<td><strong>{escape(component.title)}</strong><br><span class=\"muted\">{escape(component.description)}</span></td>"
            f"<td>{escape(_format_duration(component.duration_seconds))}</td>"
            f"<td>{evidence}</td>"
            "</tr>"
        )
    return "\n".join(
        [
            '<div class="table-wrap checks-table-wrap">',
            '<table class="status-table status-table--card-reading">',
            '<colgroup><col style="width:34%"><col style="width:14%"><col style="width:52%"></colgroup>',
            '<thead class="status-table-head"><tr><th>Компонент</th><th>Длительность</th><th>Evidence</th></tr></thead>',
            f"<tbody>{''.join(rows)}</tbody>",
            "</table>",
            "</div>",
        ]
    )


def _card_reading_evidence_table(evidence: list[CardReadingEvidence]) -> str:
    rows = []
    for item in evidence[:30]:
        rows.append(
            '<tr class="status-row">'
            f"<td>{escape(_format_timestamp(item.timestamp))}</td>"
            f"<td>{escape(item.label)}</td>"
            f"<td><code>{escape(_card_reading_evidence_short_line(item))}</code></td>"
            "</tr>"
        )
    if not rows:
        return ""
    return "\n".join(
        [
            '<div class="table-wrap checks-table-wrap">',
            '<table class="status-table status-table--card-reading-evidence">',
            '<colgroup><col style="width:12%"><col style="width:20%"><col style="width:68%"></colgroup>',
            '<thead class="status-table-head"><tr><th>Время</th><th>Факт</th><th>Строка</th></tr></thead>',
            f"<tbody>{''.join(rows)}</tbody>",
            "</table>",
            "</div>",
        ]
    )


def _card_reading_text_report_block(report: CardReadingReport, index: int) -> str:
    text_id = f"card-reading-text-{index}"
    return "\n".join(
        [
            '<details class="collapsible device-boot-text-details">',
            "<summary>",
            "<span>",
            "<strong>Текстовый отчёт</strong>",
            "<em>Компоненты длительности и evidence-строки.</em>",
            "</span>",
            "</summary>",
            '<div class="collapsible-body">',
            f'<button type="button" class="copy-button" data-copy-target="{escape(text_id)}">Скопировать текстовый отчёт</button>',
            f'<pre class="device-boot-text" id="{escape(text_id)}">{escape(_card_reading_text_report(report))}</pre>',
            "</div>",
            "</details>",
        ]
    )


def _card_reading_text_report(report: CardReadingReport) -> str:
    lines = [
        _card_reading_summary_title(report),
        "",
        f"Результат: {report.result}.",
        f"Начало: {_format_timestamp(report.started_at)}.",
        f"Конец: {_format_timestamp(report.finished_at)}.",
        f"Всего: {_format_duration(report.total_seconds)}.",
        "",
        "Компоненты:",
    ]
    for component in report.components:
        lines.append(f"- {component.title}: {_format_duration(component.duration_seconds)}")
    lines.extend(["", "Лог:"])
    lines.extend(_card_reading_evidence_short_line(item) for item in report.evidence)
    return "\n".join(lines).strip()


def _card_reading_report_payload(report: CardReadingReport) -> dict[str, object]:
    return {
        "reader_type": report.reader_type,
        "card_id": report.card_id,
        "started_at": report.started_at.isoformat(sep=" ") if report.started_at else None,
        "finished_at": report.finished_at.isoformat(sep=" ") if report.finished_at else None,
        "total_seconds": report.total_seconds,
        "result": report.result,
        "payment_start_code": report.payment_start_code,
        "auth_type": report.auth_type,
        "payment_confirm_code": report.payment_confirm_code,
        "source_files": report.source_files,
        "components": [_card_reading_component_payload(item) for item in report.components],
        "evidence": [_card_reading_evidence_payload(item) for item in report.evidence],
    }


def _card_reading_component_payload(component: CardReadingComponent) -> dict[str, object]:
    return {
        "title": component.title,
        "duration_seconds": component.duration_seconds,
        "description": component.description,
        "evidence": [_card_reading_evidence_payload(item) for item in component.evidence],
    }


def _card_reading_evidence_payload(evidence: CardReadingEvidence) -> dict[str, object]:
    return {
        "source_file": evidence.source_file,
        "line_number": evidence.line_number,
        "timestamp": evidence.timestamp.isoformat(sep=" ") if evidence.timestamp else None,
        "label": evidence.label,
        "raw_line": evidence.raw_line,
    }


def _card_reading_summary_title(report: CardReadingReport) -> str:
    reader = report.reader_type or "не найдено"
    card = report.card_id or "карта не найдена"
    return f"{reader} | Чтение {_format_datetime(report.started_at)} | Карта {card} | Время: {_format_duration(report.total_seconds)}"


def _card_reading_evidence_short_line(evidence: CardReadingEvidence) -> str:
    timestamp = _format_timestamp(evidence.timestamp)
    return f"{evidence.source_file}:{evidence.line_number} [{timestamp}] {evidence.raw_line}"


def _format_timestamp(value: datetime | None) -> str:
    if value is None:
        return "не найдено"
    return value.strftime("%H:%M:%S.%f").rstrip("0").rstrip(".")


def _format_datetime(value: datetime | None) -> str:
    if value is None:
        return "не найдено"
    return value.strftime("%d.%m.%Y в %H:%M:%S")


def _format_time_range(started_at: datetime | None, finished_at: datetime | None) -> str:
    if started_at is None or finished_at is None:
        return "не рассчитано"
    return f"{_format_timestamp(started_at)}-{_format_timestamp(finished_at)}"


def _format_duration(value: float | None) -> str:
    if value is None:
        return "не рассчитано"
    minutes = int(value // 60)
    seconds = value - minutes * 60
    seconds_text = f"{seconds:06.3f}".replace(".", ",")
    return f"{minutes} мин {seconds_text} сек"


def _format_percent_value(value: float | None) -> str:
    if value is None:
        return "не рассчитано"
    return f"{value:.2f}%"


def _evidence_short_line(evidence: DeviceBootEvidence) -> str:
    prefix = _format_timestamp(evidence.timestamp)
    if prefix == "не найдено":
        return evidence.raw_line
    return f"[{prefix}] {evidence.raw_line.split('] ', 1)[-1]}"


def _unmatched_check_reason(check: CheckCase, events: list[PaymentEvent]) -> str:
    if not events:
        return "Нет разобранных PaymentStart resp событий для проверки."
    if check.condition_type == "builtin":
        if check.check_id == "repeat_after_failure_3s":
            return "Не найден повторный PaymentStart resp в том же source log в интервале 0-3 секунды после non-success события."
        if check.check_id == "technical_error_code_3":
            return "В разобранных событиях нет PaymentStart resp с Code:3."
        if check.check_id == "timeout_code_16":
            return "В разобранных событиях нет PaymentStart resp с Code:16."
        if check.check_id == "many_declines_code_255":
            return "В разобранных событиях нет PaymentStart resp с Code:255."
        if check.check_id == "unknown_code_detected":
            return "В разобранных событиях нет кодов вне таблицы классификации."
        return "Условие встроенной проверки не найдено в разобранных событиях."
    if check.condition_type == "code":
        return f"В разобранных событиях нет PaymentStart resp с Code:{check.condition_value}."
    if check.condition_type == "message_contains":
        return f"В сообщениях и raw-строках нет фрагмента: {check.condition_value}."
    if check.condition_type == "duration_gt":
        return f"Нет событий с duration_ms больше {check.condition_value}."
    if check.condition_type == "repeat_within_seconds":
        return f"Не найден повторный PaymentStart resp в заданном интервале: {check.condition_value} секунд."
    return "Условие проверки не найдено в разобранных событиях."


def _check_result_payload(result: CheckResult) -> dict[str, object]:
    return {
        "check_id": result.check_id,
        "title": result.title,
        "severity": result.severity,
        "status": result.status,
        "source_file": result.source_file,
        "line_number": result.line_number,
        "timestamp": result.timestamp.isoformat(sep=" ") if result.timestamp else "",
        "code": result.code,
        "message": result.message or "",
        "evidence": result.evidence,
        "raw_line": result.raw_line,
    }


def _check_case_payload(check: CheckCase) -> dict[str, object]:
    return {
        "check_id": check.check_id,
        "title": check.title,
        "description": check.description,
        "severity": check.severity,
        "enabled": check.enabled,
        "version": check.version,
        "condition_type": check.condition_type,
        "condition_value": check.condition_value,
    }


def _protocol_scenario_result_payload(result) -> dict[str, object]:
    return {
        "scenario_id": result.scenario_id,
        "title": result.title,
        "status": result.status,
        "source_document": result.source_document,
        "source_section": result.source_section,
        "source_sections": result.source_sections,
        "source_quote": result.source_quote,
        "source_quotes": result.source_quotes,
        "source_file": result.source_file,
        "line_number": result.line_number,
        "timestamp": result.timestamp.isoformat(sep=" ") if result.timestamp else "",
        "evidence": result.evidence,
        "raw_line": result.raw_line,
        "matched_event_type": result.matched_event_type,
        "matched_code": result.matched_code if result.matched_code is not None else "",
    }


def _archive_records(input_files: list[str]) -> list[dict[str, object]]:
    records = []
    for name in sorted({str(file_name) for file_name in input_files if file_name}):
        records.append({"archive": Path(name).name, "count": 1, "size_bytes": 0, "files": [name]})
    return records


def _archive_name_set(input_files: list[str]) -> set[str]:
    return {Path(name).name for name in input_files if name}


def _has_filter_combinations(*record_groups: list[dict[str, object]]) -> bool:
    return any(len(group) > 1 for group in record_groups)


def _report_data(
    events: list[PaymentEvent],
    archive_names: set[str],
    *,
    log_groups: list[dict[str, object]],
    other_groups: list[dict[str, object]],
    bm_version_records: list[dict[str, object]],
    bm_carrier_records: list[dict[str, object]],
    bm_reader_records: list[dict[str, object]],
    reader_firmware_records: list[dict[str, object]],
    bm_date_records: list[dict[str, object]],
    validator_sections: list[dict[str, object]],
    protocol_results: list[object],
    report_carriers: list[str] | None = None,
    event_reader_overrides: dict[int, str] | None = None,
) -> dict[str, object]:
    event_rows = []
    carrier_rules = load_carrier_rules()
    carrier_override = list(report_carriers or [])
    reader_overrides = event_reader_overrides or {}
    reader_labels = {"OTI": "ОТИ", "TT": "ТТ"}
    for event in events:
        status = classify_bm_status(event)
        reader_type = reader_overrides[id(event)] if id(event) in reader_overrides else _event_reader_type(event)
        event_rows.append(
            {
                "archive": _archive_from_source(event.source_file, archive_names),
                "source_file": event.source_file,
                "line_number": event.line_number,
                "timestamp": event.timestamp.isoformat(sep=" ") if event.timestamp else "",
                "date": event.timestamp.date().isoformat() if event.timestamp else "",
                "version": event.bm_version or "",
                "carriers": carrier_override or carrier_names_for_text(_carrier_search_text(event), carrier_rules),
                "reader": reader_labels.get(reader_type, reader_type),
                "reader_firmware": event.reader_firmware or "",
                "status": status,
                "group": _bm_group_label(status),
                "code": event.code if event.code is not None else "",
                "message": event.message or "",
                "decision_terms": _bm_status_decision_terms(event),
                "raw_line": event.raw_line,
            }
        )
    return {
        "archives": sorted(archive_names),
        "log_groups": log_groups,
        "other_groups": other_groups,
        "meta": {
            "versions": bm_version_records,
            "carriers": bm_carrier_records,
            "readers": bm_reader_records,
            "reader_firmwares": reader_firmware_records,
            "dates": bm_date_records,
        },
        "validators": validator_sections,
        "protocol_scenarios": [_protocol_scenario_result_payload(item) for item in protocol_results],
        "events": event_rows,
    }


def _other_category_order(groups: dict[str, dict[str, object]]) -> list[str]:
    preferred = [
        "Прошивки ридеров",
        "Базы данных",
        "Конфиги",
        "Ключи",
        "Скрипты и бинарники",
        "Библиотеки",
        "Архивы",
        "Изображения",
        "Исходники",
        "Прочее",
    ]
    present = [label for label in preferred if label in groups]
    extras = sorted(label for label in groups if label not in preferred)
    return present + extras


def _aggregate_archive_categories(
    archive_inventory: list[ArchiveInventoryRow],
    categories: set[str],
) -> tuple[list[dict[str, object]], int, int]:
    grouped: dict[str, dict[str, object]] = {}
    count = 0
    size_bytes = 0
    for row in archive_inventory:
        if row.category not in categories:
            continue
        archive_name = Path(row.archive).name
        item = grouped.setdefault(archive_name, {"archive": archive_name, "count": 0, "size_bytes": 0, "files": []})
        item["count"] += row.count
        item["size_bytes"] += row.size_bytes
        files = row.files or list(row.file_sizes.keys()) or row.examples
        item["files"].extend(files)
        count += row.count
        size_bytes += row.size_bytes
    payload = [
        {
            "archive": grouped[archive_name]["archive"],
            "count": grouped[archive_name]["count"],
            "size_bytes": grouped[archive_name]["size_bytes"],
            "files": sorted({str(name) for name in grouped[archive_name]["files"] if name}),
        }
        for archive_name in sorted(grouped)
    ]
    return payload, count, size_bytes


def _bm_status_section(
    events: list[PaymentEvent],
    bm_group_rows: list[dict[str, object]],
    bm_group_payloads: dict[str, list[dict[str, object]]],
    date_chart: str,
    unclassified_diag: list[dict[str, object]],
    archive_names: set[str],
    source_note: str = "",
) -> str:
    summary_rows = bm_status_summary_rows(events)
    payloads = _status_payloads(events, archive_names)
    rows: list[str] = []
    for row in summary_rows:
        status = str(row["status"])
        count = int(row["count"])
        percent = float(row["percent"])
        clickable = count > 0
        classes = ["status-row"]
        if status.startswith("Успешный"):
            classes.append("status-row--success")
        if clickable:
            classes.append("status-row--clickable")
        data_attrs = ""
        if clickable:
            data = payloads.get(status, [])
            data_attrs = (
                f' data-kind="status" data-status="{escape(status)}" '
                f'data-payload="{escape(json.dumps(data, ensure_ascii=False))}" tabindex="0"'
            )
        rows.append(
            f'<tr class="{" ".join(classes)}" data-count="{count}"{data_attrs}>'
            f"<td>{escape(status)}</td>"
            f"<td>{count}</td>"
            f"<td>{percent:.2f}%</td>"
            "</tr>"
        )
    rows.append(
        '<tr class="status-row status-row--total">'
        "<td>ИТОГО</td>"
        f"<td>{len(events)}</td>"
        "<td>100.00%</td>"
        "</tr>"
    )
    table = (
        '<div class="bm-status-controls">'
        '<label class="bm-status-zero-toggle">'
        '<input type="checkbox" id="bm-status-hide-zero">'
        '<span class="bm-status-zero-toggle__switch" aria-hidden="true"><span class="bm-status-zero-toggle__thumb"></span></span>'
        '<span class="bm-status-zero-toggle__text">Скрыть строки с нулём</span>'
        '</label>'
        '</div>'
        '<div class="table-wrap bm-table-wrap">'
        '<table class="status-table status-table--bm">'
        '<colgroup><col style="width:68%"><col style="width:16%"><col style="width:16%"></colgroup>'
        '<thead class="status-table-head"><tr><th>Статус</th><th>Кол-во</th><th>%</th></tr></thead>'
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
        "</div>"
    )
    return "\n".join(
        [
            '<section class="section section--bm">',
            '<div class="section-title">',
            "<h2>BM-статусы</h2>",
            f"<p>Строки таблицы можно открыть по клику. {escape(source_note)}</p>",
            "</div>",
            '<div id="bm-status-table-root">' + table + "</div>",
            '<div class="section-title section-title--compact">',
            "<h3>Группировка статусов</h3>",
            "</div>",
            '<div id="bm-grouped-table-root">' + _grouped_status_table(bm_group_rows, bm_group_payloads) + "</div>",
            f'<div id="bm-date-chart-root">{date_chart}</div>',
            f'<div id="bm-unclassified-root">{_collapsible_unclassified_section(unclassified_diag)}</div>',
            "</section>",
        ]
    )


def _grouped_status_table(rows: list[dict[str, object]], payloads: dict[str, list[dict[str, object]]]) -> str:
    rendered_rows: list[str] = []
    for row in rows:
        label = str(row["label"])
        count = int(row["count"])
        percent = float(row["percent"])
        if label == "ИТОГО":
            rendered_rows.append(
                '<tr class="status-row status-row--total">'
                "<td>ИТОГО</td>"
                f"<td>{count}</td>"
                f"<td>{percent:.2f}%</td>"
                "</tr>"
            )
            continue
        classes = ["status-row", "status-row--group"]
        if label == "Успех":
            classes.append("status-row--success")
        if count > 0:
            classes.append("status-row--clickable")
        data_attrs = ""
        if count > 0:
            data_attrs = (
                f' data-kind="group" data-status="{escape(label)}" '
                f'data-payload="{escape(json.dumps(payloads.get(label, []), ensure_ascii=False))}" tabindex="0"'
            )
        rendered_rows.append(
            f'<tr class="{" ".join(classes)}"{data_attrs}>'
            f"<td>{escape(label)}</td>"
            f"<td>{count}</td>"
            f"<td>{percent:.2f}%</td>"
            "</tr>"
        )
    return (
        '<div class="table-wrap bm-table-wrap bm-table-wrap--compact">'
        '<table class="status-table status-table--grouped">'
        '<thead class="status-table-head"><tr><th>Группа</th><th>Кол-во</th><th>%</th></tr></thead>'
        f"<tbody>{''.join(rendered_rows)}</tbody>"
        "</table>"
        "</div>"
    )


def _bm_status_group_row(label: str, count: int, percent: float, success: bool) -> str:
    classes = ["status-row"]
    if success:
        classes.append("status-row--success")
    if label != "ИТОГО":
        classes.append("status-row--group")
    return (
        f'<tr class="{" ".join(classes)}">'
        f"<td>{escape(label)}</td>"
        f"<td>{count}</td>"
        f"<td>{percent:.2f}%</td>"
        "</tr>"
    )


def _bm_status_groups(events: list[PaymentEvent]) -> list[dict[str, object]]:
    summary = {row["status"]: int(row["count"]) for row in bm_status_summary_rows(events)}
    groups = [
        ("Успех", ["Успешный онлайн (БЕЗ МИР)", "Успешный онлайн МИР", "Успешный оффлайн"]),
        (
            "Ошибки",
            [
                "Отказ, ошибка чтения карты",
                "Отказ, нет карты в поле",
                "Отказ, ошибка ODA/CDA",
                "Отказ, коллизия",
            ],
        ),
        ("NON_EMV_CARD", ["NON_EMV_CARD"]),
        ("Отказ, истек таймаут", ["Отказ, истек таймаут"]),
        (
            "Отказы",
            [
                "Проход зафейлен (онлайн - не получили конфирм и зарегали как фейл)",
                "Отказ, повторное предъявление",
                "Отказ, карта в стоп листе",
                "Отказ, QR-код недействителен",
                "Отказ, операция отклонена",
            ],
        ),
        ("Не классифицировано", [UNCLASSIFIED_STATUS]),
    ]
    total = len(events)
    rows = [
        {
            "label": label,
            "count": sum(summary.get(status, 0) for status in statuses),
            "percent": _percent(sum(summary.get(status, 0) for status in statuses), total),
        }
        for label, statuses in groups
    ]
    rows.append({"label": "ИТОГО", "count": total, "percent": 100.0 if total else 0.0})
    return rows


def _bm_group_payloads(events: list[PaymentEvent], archive_names: set[str]) -> dict[str, list[dict[str, object]]]:
    summary = {
        "Успех": {"Успешный онлайн (БЕЗ МИР)", "Успешный онлайн МИР", "Успешный оффлайн"},
        "Ошибки": {"Отказ, ошибка чтения карты", "Отказ, нет карты в поле", "Отказ, ошибка ODA/CDA", "Отказ, коллизия"},
        "NON_EMV_CARD": {"NON_EMV_CARD"},
        "Отказ, истек таймаут": {"Отказ, истек таймаут"},
        "Отказы": {"Проход зафейлен (онлайн - не получили конфирм и зарегали как фейл)", "Отказ, повторное предъявление", "Отказ, карта в стоп листе", "Отказ, QR-код недействителен", "Отказ, операция отклонена"},
        "Не классифицировано": {UNCLASSIFIED_STATUS},
    }
    payloads: dict[str, list[dict[str, object]]] = {label: [] for label in summary}
    for event in events:
        status = classify_bm_status(event)
        for label, statuses in summary.items():
            if status in statuses:
                payloads[label].append(
                    {
                        "archive": _archive_from_source(event.source_file, archive_names),
                        "source_file": event.source_file,
                        "line_number": event.line_number,
                        "timestamp": event.timestamp.isoformat(sep=" ") if event.timestamp else "",
                        "code": event.code if event.code is not None else "",
                        "message": event.message or "",
                        "decision_terms": _bm_status_decision_terms(event),
                        "raw_line": event.raw_line,
                    }
                )
                break
    return payloads


def _bm_status_decision_terms(event: PaymentEvent) -> list[str]:
    text = " ".join(part for part in [event.message, event.raw_line] if part)
    lowered = text.lower()
    terms: list[str] = []
    code = event.code

    if code == 0:
        if "error: no error" in lowered:
            terms.append("error: no error")
        for term in ("ОДОБРЕНО", "ПРОХОДИТЕ", "Авторизация", "online", "offline", "mir", "мир"):
            if term.lower() in lowered:
                terms.append(term)
    elif code == 1:
        for term in ("Следующий проход", "повтор", "repeat"):
            if term.lower() in lowered:
                terms.append(term)
    elif code == 3:
        if "ошибка чтения карты" in lowered:
            terms.append("Ошибка чтения карты")
        elif "read" in lowered:
            terms.append("read")
    elif code == 4:
        if "стоп" in lowered:
            terms.append("стоп")
    elif code == 6:
        for term in ("одну карту", "коллизи"):
            if term in lowered:
                terms.append(term)
    elif code == 16:
        for term in ("истек таймаут", "timeout expired", "timeout"):
            if term in lowered:
                terms.append(term)
    elif code == 17:
        if "нет карты" in lowered:
            terms.append("Нет карты")
    elif code == 12:
        for term in ("qr-код недейств", "qr code invalid"):
            if term in lowered:
                terms.append("QR-КОД НЕДЕЙСТВИТЕЛЕН")
                break
    elif code in {14, 255}:
        if "операция отклонена" in lowered:
            terms.append("Операция отклонена")
    if "non_emv_card" in lowered:
        terms.append("NON_EMV_CARD")
    if "oda" in lowered:
        terms.append("ODA")
    if "cda" in lowered:
        terms.append("CDA")
    if "конфирм" in lowered or "confirm" in lowered:
        terms.append("конфирм")
    return list(dict.fromkeys(term for term in terms if term))


def _bm_group_label(status: str) -> str:
    if status.startswith("Успешный"):
        return "Успех"
    if status in {
        "Отказ, ошибка чтения карты",
        "Отказ, нет карты в поле",
        "Отказ, ошибка ODA/CDA",
        "Отказ, коллизия",
        "Отказ, истек таймаут",
    }:
        return "Ошибки"
    if status in {
        "Проход зафейлен (онлайн - не получили конфирм и зарегали как фейл)",
        "Отказ, повторное предъявление",
        "Отказ, карта в стоп листе",
        "Отказ, QR-код недействителен",
        "Отказ, операция отклонена",
    }:
        return "Отказы"
    return "Не классифицировано"


def _bm_selectable_record(
    kind: str,
    value: str,
    events: list[PaymentEvent],
    archive_names: set[str],
    *,
    markers: bool = False,
) -> dict[str, object]:
    archives_by_name: dict[str, dict[str, object]] = defaultdict(lambda: {"archive": "", "count": 0})
    evidence_rows = []
    for event in events:
        archive_name = _archive_from_source(event.source_file, archive_names)
        item = archives_by_name[archive_name]
        item["archive"] = archive_name
        item["count"] = int(item["count"]) + 1
        evidence_rows.append(_event_evidence_row(event, archive_name))
    record: dict[str, object] = {
        kind: value,
        "count": len(events),
        "archives": [archives_by_name[archive] for archive in sorted(archives_by_name)],
        "evidence": evidence_rows[:25],
    }
    if markers:
        record["markers"] = sorted({marker for event in events for marker in _carrier_markers(event)})
    return record


def _event_evidence_row(event: PaymentEvent, archive_name: str) -> dict[str, object]:
    return {
        "archive": archive_name,
        "source_file": event.source_file,
        "line_number": event.line_number,
        "timestamp": event.timestamp.isoformat(sep=" ") if event.timestamp else "",
        "raw_line": event.raw_line,
    }


def _inventory_evidence_rows(item: object, key: str, archive_name: str) -> list[dict[str, object]]:
    source_file = str(getattr(item, "source_file", ""))
    samples = (getattr(item, "evidence_samples", {}) or {}).get(key, [])
    if not samples:
        samples = [str(getattr(item, "evidence", ""))]
    return [
        {
            "archive": archive_name,
            "source_file": source_file,
            "line_number": "",
            "timestamp": "",
            "raw_line": sample,
        }
        for sample in samples[:25]
        if sample
    ]


def _bm_carrier_records(
    events: list[PaymentEvent],
    archive_names: set[str],
    device_boot_reports: list[DeviceBootReport] | None = None,
) -> list[dict[str, object]]:
    report_carriers = _report_carriers_from_device_boot(device_boot_reports)
    if report_carriers:
        return [
            _report_carrier_record(carrier, events, archive_names, device_boot_reports or [])
            for carrier in report_carriers
        ]

    records: list[dict[str, object]] = []
    rules = load_carrier_rules()
    for rule in rules:
        rule_events = [event for event in events if carrier_names_for_text(_carrier_search_text(event), [rule])]
        if rule_events:
            records.append(_bm_selectable_record("carrier", rule.name, rule_events, archive_names, markers=True))
    return records


def _report_carrier_record(
    carrier: str,
    events: list[PaymentEvent],
    archive_names: set[str],
    device_boot_reports: list[DeviceBootReport],
) -> dict[str, object]:
    archives_by_name: dict[str, dict[str, object]] = defaultdict(lambda: {"archive": "", "count": 0})
    for event in events:
        archive_name = _archive_from_source(event.source_file, archive_names)
        item = archives_by_name[archive_name]
        item["archive"] = archive_name
        item["count"] = int(item["count"]) + 1

    evidence_rows = _report_carrier_event_evidence(carrier, events, archive_names)
    if not evidence_rows:
        evidence_rows = _report_carrier_boot_evidence(carrier, device_boot_reports, archive_names)

    return {
        "carrier": carrier,
        "count": len(events) if events else len(device_boot_reports),
        "archives": [archives_by_name[archive] for archive in sorted(archives_by_name)],
        "evidence": evidence_rows[:25],
        "markers": _report_carrier_markers(carrier),
    }


def _report_carrier_event_evidence(
    carrier: str,
    events: list[PaymentEvent],
    archive_names: set[str],
) -> list[dict[str, object]]:
    if carrier != "АСКП":
        return []
    evidence_rows = []
    for event in events:
        if "askp" not in _carrier_search_text(event):
            continue
        evidence_rows.append(_event_evidence_row(event, _archive_from_source(event.source_file, archive_names)))
    return evidence_rows


def _report_carrier_boot_evidence(
    carrier: str,
    device_boot_reports: list[DeviceBootReport],
    archive_names: set[str],
) -> list[dict[str, object]]:
    evidence_rows = []
    for report in device_boot_reports:
        if carrier not in _report_carriers_from_device_boot([report]):
            continue
        for evidence in _all_device_boot_evidence(report):
            evidence_rows.append(
                {
                    "archive": _archive_from_source(evidence.source_file, archive_names),
                    "source_file": evidence.source_file,
                    "line_number": evidence.line_number,
                    "timestamp": evidence.timestamp.isoformat(sep=" ") if evidence.timestamp else "",
                    "raw_line": evidence.raw_line,
                }
            )
    return evidence_rows


def _all_device_boot_evidence(report: DeviceBootReport) -> list[DeviceBootEvidence]:
    return [evidence for segment in report.segments for evidence in segment.evidence]


def _report_carrier_markers(carrier: str) -> list[str]:
    if carrier == "АСКП":
        return ["АСКП", "mgt_askp"]
    return [carrier]


def _bm_reader_records(
    events: list[PaymentEvent],
    archive_names: set[str],
    device_boot_reports: list[DeviceBootReport] | None = None,
) -> list[dict[str, object]]:
    counts: dict[str, list[dict[str, object]]] = defaultdict(list)
    for profile in _reader_device_profiles(events, device_boot_reports):
        reader_type = str(profile.get("reader_type") or "")
        if reader_type:
            counts[reader_type].append(profile)
    return [
        _device_reader_record({"OTI": "ОТИ", "TT": "ТТ"}.get(reader, reader), profiles, archive_names)
        for reader, profiles in sorted(counts.items())
    ]


def _device_reader_record(
    reader: str,
    profiles: list[dict[str, object]],
    archive_names: set[str],
) -> dict[str, object]:
    archives_by_name: dict[str, dict[str, object]] = defaultdict(lambda: {"archive": "", "count": 0})
    evidence_rows = []
    count = 0
    for profile in profiles:
        profile_events = list(profile.get("events") or [])
        profile_reports = list(profile.get("reports") or [])
        count += len(profile_events) if profile_events else len(profile_reports)
        if profile_events:
            for event in profile_events:
                archive_name = _archive_from_source(event.source_file, archive_names)
                item = archives_by_name[archive_name]
                item["archive"] = archive_name
                item["count"] = int(item["count"]) + 1
        else:
            for report in profile_reports:
                source_file = report.source_files[0] if report.source_files else report.title
                archive_name = _archive_from_source(source_file, archive_names)
                item = archives_by_name[archive_name]
                item["archive"] = archive_name
                item["count"] = int(item["count"]) + 1
        evidence_rows.extend(_device_profile_reader_evidence(profile, archive_names))
    return {
        "reader": reader,
        "count": count,
        "archives": [archives_by_name[archive] for archive in sorted(archives_by_name)],
        "evidence": evidence_rows[:25],
    }


def _device_profile_reader_evidence(profile: dict[str, object], archive_names: set[str]) -> list[dict[str, object]]:
    rows = []
    for report in profile.get("reports") or []:
        for evidence in _reader_evidence_from_boot_report(report):
            rows.append(
                {
                    "archive": _archive_from_source(evidence.source_file, archive_names),
                    "source_file": evidence.source_file,
                    "line_number": evidence.line_number,
                    "timestamp": evidence.timestamp.isoformat(sep=" ") if evidence.timestamp else "",
                    "raw_line": evidence.raw_line,
                }
            )
    if rows:
        return rows
    for event in profile.get("events") or []:
        rows.append(_event_evidence_row(event, _archive_from_source(event.source_file, archive_names)))
    return rows


def _reader_evidence_from_boot_report(report: DeviceBootReport) -> list[DeviceBootEvidence]:
    rows = [
        evidence
        for segment in report.segments
        for evidence in segment.evidence
        if evidence.label in {"reader_open_success", "reader_start_end"} or "[READER]" in evidence.raw_line
    ]
    return rows[:3]


def _reader_firmware_records(
    events: list[PaymentEvent],
    archive_names: set[str],
    log_inventory: list[object] | None = None,
) -> list[dict[str, object]]:
    counts: dict[str, list[PaymentEvent]] = defaultdict(list)
    for event in events:
        if event.reader_firmware:
            counts[event.reader_firmware].append(event)
    records = [
        _bm_selectable_record("reader_firmware", firmware, firmware_events, archive_names)
        for firmware, firmware_events in sorted(counts.items())
    ]
    seen = {str(record["reader_firmware"]) for record in records}
    for item in log_inventory or []:
        firmwares = getattr(item, "reader_firmware_versions", []) or []
        for firmware in firmwares:
            if firmware in seen:
                continue
            archive_name = _archive_from_source(str(getattr(item, "source_file", "")), archive_names)
            records.append(
                {
                    "reader_firmware": firmware,
                    "count": 1,
                    "archives": [{"archive": archive_name, "count": 1}],
                    "evidence": _inventory_evidence_rows(item, f"reader_firmware:{firmware}", archive_name),
                }
            )
            seen.add(firmware)
    return sorted(records, key=lambda record: str(record["reader_firmware"]))


def _bm_date_records(events: list[PaymentEvent], archive_names: set[str]) -> list[dict[str, object]]:
    counts: dict[str, list[PaymentEvent]] = defaultdict(list)
    for event in events:
        if event.timestamp:
            counts[event.timestamp.date().isoformat()].append(event)
    return [_bm_selectable_record("date", date, date_events, archive_names) for date, date_events in sorted(counts.items())]


def _validator_analytics(events: list[PaymentEvent], archive_names: set[str]) -> list[dict[str, object]]:
    grouped: dict[str, dict[str, list[PaymentEvent]]] = defaultdict(lambda: defaultdict(list))
    for event in events:
        validator = _validator_from_source(event.source_file, archive_names)
        version = event.bm_version or "missing"
        grouped[validator][version].append(event)

    payload: list[dict[str, object]] = []
    for validator in sorted(grouped):
        versions = []
        for version in sorted(grouped[validator]):
            version_events = grouped[validator][version]
            total = len(version_events)
            date_range = _date_range_for_events(version_events)
            versions.append(
                {
                    "version": version,
                    "total": total,
                    "date_from": date_range[0],
                    "date_to": date_range[1],
                    "declines": [
                        {
                            "status": "Отказ, нет карты в поле",
                            "count": _count_events(version_events, {"Отказ, нет карты в поле"}),
                            "percent": _percent(_count_events(version_events, {"Отказ, нет карты в поле"}), total),
                        },
                        {
                            "status": "Отказ, ошибка чтения карты",
                            "count": _count_events(version_events, {"Отказ, ошибка чтения карты"}),
                            "percent": _percent(_count_events(version_events, {"Отказ, ошибка чтения карты"}), total),
                        },
                    ],
                }
            )
        payload.append({"validator": validator, "versions": versions})
    return payload


def _validator_from_source(source_file: str, archive_names: set[str]) -> str:
    path = Path(source_file)
    for part in path.parts:
        if part in archive_names:
            return Path(part).stem
    for part in path.parts:
        stem = Path(part).stem
        if stem and any(ch.isdigit() for ch in stem):
            return stem
    return path.stem or path.name or "missing"


def _count_events(events: list[PaymentEvent], statuses: set[str]) -> int:
    return sum(1 for event in events if classify_bm_status(event) in statuses)


def _date_range_for_events(events: list[PaymentEvent]) -> tuple[str, str]:
    timestamps = sorted(event.timestamp for event in events if event.timestamp)
    if not timestamps:
        return ("missing", "missing")
    start = timestamps[0].strftime("%d.%m.%Y")
    end = timestamps[-1].strftime("%d.%m.%Y")
    return (start, end)


def _bm_date_chart(events: list[PaymentEvent]) -> str:
    by_date = _bm_grouped_date_counts(events)
    if len(by_date) <= 1:
        return ""

    width = 960
    height = 280
    pad_left = 54
    pad_right = 24
    pad_top = 24
    pad_bottom = 56
    plot_width = width - pad_left - pad_right
    plot_height = height - pad_top - pad_bottom
    max_value = max(max(series.values()) for series in by_date.values()) if by_date else 0
    max_value = max(max_value, 1)
    dates = sorted(by_date)
    x_step = plot_width / max(len(dates) - 1, 1)
    series_specs = [
        ("Успех", "#137752"),
        ("Ошибки", "#d14343"),
        ("Отказы", "#a15c06"),
    ]
    paths: list[str] = []
    points: list[str] = []
    for label, color in series_specs:
        coords = []
        for index, date in enumerate(dates):
            value = by_date[date].get(label, 0)
            x = pad_left + (index * x_step if len(dates) > 1 else plot_width / 2)
            y = pad_top + plot_height - (value / max_value * plot_height)
            coords.append((x, y, value))
        if coords:
            d = " ".join([f"M {coords[0][0]:.1f} {coords[0][1]:.1f}"] + [f"L {x:.1f} {y:.1f}" for x, y, _ in coords[1:]])
            paths.append(f'<path d="{d}" fill="none" stroke="{color}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" />')
            for x, y, value in coords:
                points.append(
                    f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4.5" fill="{color}"><title>{escape(label)}: {value}</title></circle>'
                )
    axis_lines = (
        f'<line x1="{pad_left}" y1="{pad_top + plot_height}" x2="{width - pad_right}" y2="{pad_top + plot_height}" stroke="#cfd8e3" />'
        f'<line x1="{pad_left}" y1="{pad_top}" x2="{pad_left}" y2="{pad_top + plot_height}" stroke="#cfd8e3" />'
    )
    y_ticks = []
    for tick in range(0, max_value + 1, max(1, max_value // 4 or 1)):
        y = pad_top + plot_height - (tick / max_value * plot_height)
        y_ticks.append(
            f'<text x="{pad_left - 10}" y="{y + 4:.1f}" text-anchor="end">{tick}</text>'
        )
    x_labels = []
    for index, date in enumerate(dates):
        x = pad_left + (index * x_step if len(dates) > 1 else plot_width / 2)
        x_labels.append(
            f'<text x="{x:.1f}" y="{height - 20}" text-anchor="middle">{escape(date)}</text>'
        )
    return "\n".join(
        [
            '<div class="section-title section-title--compact">',
            "<h3>Динамика по датам</h3>",
            "</div>",
            '<div class="chart-card">',
            '<svg class="line-chart" viewBox="0 0 960 280" role="img" aria-label="Динамика статусов по датам">',
            f"<g class=\"axis\">{axis_lines}</g>",
            f"<g class=\"ticks\">{''.join(y_ticks)}</g>",
            f"<g class=\"series\">{''.join(paths)}{''.join(points)}</g>",
            f"<g class=\"labels\">{''.join(x_labels)}</g>",
            "</svg>",
            '<div class="chart-legend">',
            '<span><i style="background:#137752"></i>Успех</span>',
            '<span><i style="background:#d14343"></i>Ошибки</span>',
            '<span><i style="background:#a15c06"></i>Отказы</span>',
            "</div>",
            "</div>",
        ]
    )


def _bm_grouped_date_counts(events: list[PaymentEvent]) -> dict[str, dict[str, int]]:
    grouped: dict[str, dict[str, int]] = defaultdict(lambda: {"Успех": 0, "Ошибки": 0, "Отказы": 0})
    for event in events:
        if not event.timestamp:
            continue
        date = event.timestamp.date().isoformat()
        grouped[date]
        status = classify_bm_status(event)
        if status.startswith("Успешный"):
            grouped[date]["Успех"] += 1
        elif status in {
            "Отказ, ошибка чтения карты",
            "Отказ, нет карты в поле",
            "Отказ, ошибка ODA/CDA",
            "Отказ, коллизия",
        }:
            grouped[date]["Ошибки"] += 1
        elif status in {
            "Проход зафейлен (онлайн - не получили конфирм и зарегали как фейл)",
            "Отказ, повторное предъявление",
            "Отказ, карта в стоп листе",
        }:
            grouped[date]["Отказы"] += 1
    return dict(sorted(grouped.items()))


def _unclassified_diagnostics(events: list[PaymentEvent]) -> list[dict[str, object]]:
    groups: dict[tuple[str, str], dict[str, object]] = defaultdict(lambda: {"code": "", "message": "", "count": 0})
    for event in events:
        if classify_bm_status(event) != UNCLASSIFIED_STATUS:
            continue
        code = "" if event.code is None else str(event.code)
        message = event.message or ""
        key = (code, message)
        item = groups[key]
        item["code"] = code or "missing"
        item["message"] = message or "missing"
        item["count"] = int(item["count"]) + 1
    rows = sorted(groups.values(), key=lambda row: (-int(row["count"]), str(row["code"]), str(row["message"])))
    return rows


def _collapsible_unclassified_section(rows: list[dict[str, object]]) -> str:
    summary = sum(int(row["count"]) for row in rows)
    if summary == 0:
        return ""
    rendered_rows = "".join(
        f"<tr><td>{escape(str(row['code']))}</td><td>{escape(str(row['message']))}</td><td>{int(row['count'])}</td></tr>"
        for row in rows
    )
    table = (
        '<div class="table-wrap">'
        '<table class="status-table status-table--diagnostic">'
        '<thead class="status-table-head"><tr><th>Код</th><th>Сообщение</th><th>Кол-во</th></tr></thead>'
        f"<tbody>{rendered_rows or '<tr><td colspan=\"3\" class=\"muted\">Нет данных</td></tr>'}</tbody>"
        "</table>"
        "</div>"
    )
    return "\n".join(
        [
            '<details class="collapsible">',
            "<summary>",
            "<span>",
            f"<strong>Не классифицировано</strong>",
            f"<em>{summary} строк</em>",
            "</span>",
            "</summary>",
            '<div class="collapsible-body">',
            table,
            "</div>",
            "</details>",
        ]
    )


def _validator_section(groups: list[dict[str, object]], source_note: str = "") -> str:
    if not groups:
        content = '<p class="muted">Нет данных по валидаторам.</p>'
    else:
        blocks: list[str] = []
        for group in groups:
            validator = str(group.get("validator") or "missing")
            versions = group.get("versions") or []
            version_blocks: list[str] = []
            for version in versions:
                declines = version.get("declines") or []
                decline_rows = "".join(
                    f"<tr><td>{escape(str(row.get('status') or ''))}</td><td>{int(row.get('count') or 0)}</td><td>{float(row.get('percent') or 0):.2f}%</td></tr>"
                    for row in declines
                )
                date_from = escape(str(version.get("date_from") or "missing"))
                date_to = escape(str(version.get("date_to") or "missing"))
                version_blocks.append(
                    "\n".join(
                        [
                            '<div class="validator-version">',
                            '<div class="validator-version__head">',
                            f'<strong>Версия {escape(str(version.get("version") or "missing"))}</strong>',
                            f'<span>Всего {int(version.get("total") or 0)} транзакций</span>',
                            "</div>",
                            f'<div class="validator-version__meta"><span>с {date_from} по {date_to}</span></div>',
                            '<table class="status-table status-table--validator">',
                            '<thead class="status-table-head"><tr><th>Статус</th><th>Кол-во</th><th>%</th></tr></thead>',
                            f"<tbody>{decline_rows or '<tr><td colspan=\"3\" class=\"muted\">Нет данных</td></tr>'}</tbody>",
                            "</table>",
                            "</div>",
                        ]
                    )
                )
            blocks.append(
                "\n".join(
                    [
                        '<details class="collapsible collapsible--validator">',
                        "<summary>",
                        "<span>",
                        f"<strong>{escape(validator)}</strong>",
                        f"<em>{len(versions)} версий</em>",
                        "</span>",
                        "</summary>",
                        '<div class="collapsible-body">',
                        "".join(version_blocks) if version_blocks else '<p class="muted">Нет данных по версиям.</p>',
                        "</div>",
                        "</details>",
                    ]
                )
            )
        content = "".join(blocks)
    return "\n".join(
        [
            '<details class="collapsible collapsible--validators">',
            "<summary>",
            "<span>",
            "<strong>Аналитика по валидаторам</strong>",
            f"<em>Версии BM, даты и два ключевых отказа по каждому архиву. {escape(source_note)}</em>",
            "</span>",
            "</summary>",
            '<div class="collapsible-body">',
            content,
            "</div>",
            "</details>",
        ]
    )


def _percent(count: int, total: int) -> float:
    if total == 0:
        return 0.0
    return round((count / total) * 100, 2)


def _status_payloads(events: list[PaymentEvent], archive_names: set[str]) -> dict[str, list[dict[str, object]]]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for event in events:
        status = classify_bm_status(event)
        grouped[status].append(
            {
                "archive": _archive_from_source(event.source_file, archive_names),
                "source_file": event.source_file,
                "line_number": event.line_number,
                "timestamp": event.timestamp.isoformat(sep=" ") if event.timestamp else "",
                "code": event.code if event.code is not None else "",
                "message": event.message or "",
                "raw_line": event.raw_line,
            }
        )
    return grouped


def _bm_versions(events: list[PaymentEvent]) -> str:
    versions = sorted({event.bm_version for event in events if event.bm_version})
    return ", ".join(versions) if versions else "missing"


def _bm_version_records(events: list[PaymentEvent], archive_names: set[str]) -> list[dict[str, object]]:
    grouped: dict[str, list[PaymentEvent]] = defaultdict(list)
    for event in events:
        if event.bm_version:
            grouped[event.bm_version].append(event)
    return [_bm_selectable_record("version", version, grouped[version], archive_names) for version in sorted(grouped)]


def _bm_version_count(events: list[PaymentEvent]) -> int:
    return len({event.bm_version for event in events if event.bm_version})


def _bm_carriers(events: list[PaymentEvent], device_boot_reports: list[DeviceBootReport] | None = None) -> str:
    report_carriers = _report_carriers_from_device_boot(device_boot_reports)
    if report_carriers:
        return ", ".join(report_carriers)

    carriers: list[str] = []
    rules = load_carrier_rules()
    for event in events:
        carriers.extend(carrier_names_for_text(_carrier_search_text(event), rules))
    return ", ".join(dict.fromkeys(carriers)) if carriers else "missing"


def _reader_types(events: list[PaymentEvent], device_boot_reports: list[DeviceBootReport] | None = None) -> str:
    types = [
        str(profile["reader_type"])
        for profile in _reader_device_profiles(events, device_boot_reports)
        if profile.get("reader_type")
    ]
    mapping = {"OTI": "ОТИ", "TT": "ТТ"}
    return ", ".join(dict.fromkeys(mapping.get(reader_type, reader_type) for reader_type in types)) if types else "missing"


def _reader_firmwares(events: list[PaymentEvent], log_inventory: list[object] | None = None) -> str:
    firmwares = {event.reader_firmware for event in events if event.reader_firmware}
    for item in log_inventory or []:
        firmwares.update(getattr(item, "reader_firmware_versions", []) or [])
    firmwares = sorted(firmwares)
    return ", ".join(firmwares) if firmwares else "missing"


def _bm_period(events: list[PaymentEvent]) -> str:
    timestamps = sorted(event.timestamp for event in events if event.timestamp)
    if not timestamps:
        return "missing"
    start = timestamps[0]
    end = timestamps[-1]
    start_text = start.strftime("%d.%m.%Y")
    end_text = end.strftime("%d.%m.%Y")
    if start.date() == end.date():
        return start_text
    return f"{start_text} - {end_text}"


def _carrier_search_text(event: PaymentEvent) -> str:
    return " ".join(
        value.lower()
        for value in (event.carrier, event.package)
        if value
    )


def _reader_device_profiles(
    events: list[PaymentEvent],
    device_boot_reports: list[DeviceBootReport] | None,
) -> list[dict[str, object]]:
    boot_serials = {report.validator_serial for report in device_boot_reports or [] if report.validator_serial}
    profiles: dict[str, dict[str, object]] = {}
    for report in device_boot_reports or []:
        device_id = report.validator_serial or _device_id_from_sources(report.source_files) or f"boot:{report.title}"
        profile = profiles.setdefault(device_id, _empty_reader_profile(device_id))
        profile["reports"].append(report)
        if report.reader_type:
            profile["boot_readers"].add(report.reader_type)
    for event in events:
        device_id = _event_device_id(event, boot_serials)
        profile = profiles.setdefault(device_id, _empty_reader_profile(device_id))
        profile["events"].append(event)
        package_reader = _event_package_reader_type(event)
        if package_reader:
            profile["package_readers"].add(package_reader)
    for profile in profiles.values():
        boot_readers = profile["boot_readers"]
        package_readers = profile["package_readers"]
        if len(boot_readers) == 1:
            profile["reader_type"] = next(iter(boot_readers))
            profile["reader_source"] = "device_boot"
        elif not boot_readers and len(package_readers) == 1:
            profile["reader_type"] = next(iter(package_readers))
            profile["reader_source"] = "bm_package"
        else:
            profile["reader_type"] = ""
            profile["reader_source"] = "conflict" if boot_readers or len(package_readers) > 1 else "missing"
    return list(profiles.values())


def _empty_reader_profile(device_id: str) -> dict[str, object]:
    return {
        "device_id": device_id,
        "events": [],
        "reports": [],
        "boot_readers": set(),
        "package_readers": set(),
        "reader_type": "",
        "reader_source": "missing",
    }


def _event_reader_overrides(profiles: list[dict[str, object]]) -> dict[int, str]:
    overrides: dict[int, str] = {}
    for profile in profiles:
        reader_type = str(profile.get("reader_type") or "")
        for event in profile.get("events") or []:
            overrides[id(event)] = reader_type
    return overrides


def _event_reader_type(event: PaymentEvent) -> str:
    if event.reader_type:
        return event.reader_type
    return _event_package_reader_type(event)


def _event_package_reader_type(event: PaymentEvent) -> str:
    platform = (event.platform or "").lower()
    if platform in {"oti", "tt"}:
        return platform.upper()
    package = event.package or ""
    match = re.search(r"\b[A-Za-z0-9_]+-(oti|tt)-\d", package, re.IGNORECASE)
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
    return f"unknown:{event.source_file}"


def _device_id_from_sources(source_files: list[str]) -> str:
    for source_file in source_files:
        for part in Path(source_file).parts:
            match = re.fullmatch(r"(?P<value>\d{5,})_logs(?:\.zip)?", part)
            if match:
                return match.group("value")
    return ""


def _report_carriers_from_device_boot(device_boot_reports: list[DeviceBootReport] | None) -> list[str]:
    carriers: list[str] = []
    for report in device_boot_reports or []:
        text = " ".join([report.title, *report.source_files]).casefold()
        if "аскп" in text or "askp" in text:
            carriers.append("АСКП")
    return list(dict.fromkeys(carriers))


def _carrier_markers(event: PaymentEvent) -> set[str]:
    return carrier_markers_for_text(_carrier_search_text(event))


def _archive_from_source(source_file: str, archive_names: set[str]) -> str:
    path = Path(source_file)
    for part in path.parts:
        if part in archive_names:
            return part
    return path.name


def _classify_other_file(path: str) -> str:
    lower = path.lower()
    name = Path(lower).name

    if _is_reader_firmware(lower):
        return "Прошивки ридеров"
    if _is_database(lower):
        return "Базы данных"
    if _is_config(lower):
        return "Конфиги"
    if _is_key(lower):
        return "Ключи"
    if _is_library(lower):
        return "Библиотеки"
    if _is_archive(lower):
        return "Архивы"
    if _is_image(lower):
        return "Изображения"
    if _is_source(lower):
        return "Исходники"
    if _is_executable(lower, name):
        return "Скрипты и бинарники"
    return "Прочее"


def _is_reader_firmware(path: str) -> bool:
    return bool(re.search(r"/reader-[\d.]+\.bin(?:\.[^/]*)*$", path) or re.search(r"\breader-[\d.]+\.bin(?:\.[^/]*)*$", path))


def _is_database(path: str) -> bool:
    return bool(
        re.search(r"\.(?:db|sqlite3?|mdb)(?:-[a-z]+)?$", path)
        or path.endswith(".db-shm")
        or path.endswith(".db-wal")
        or "/db/" in path
    )


def _is_config(path: str) -> bool:
    return bool(
        path.endswith((".yml", ".yaml", ".json", ".cfg", ".conf", ".ini", ".service", ".txt", ".xml", ".properties"))
        or Path(path).name in {"ver", "config", "settings"}
        or "config" in Path(path).name
    )


def _is_key(path: str) -> bool:
    return bool(path.endswith((".pem", ".key", ".pub", ".crt", ".cer")) or "key" in Path(path).name.lower())


def _is_library(path: str) -> bool:
    return bool(
        path.endswith((".so", ".so.1", ".so.2", ".dll", ".dylib", ".a"))
        or "/lib/" in path
        or Path(path).name.lower().startswith("lib")
    )


def _is_archive(path: str) -> bool:
    return bool(path.endswith((".zip", ".rar", ".tar", ".tgz", ".tar.gz", ".gz")))


def _is_image(path: str) -> bool:
    return bool(path.endswith((".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")))


def _is_source(path: str) -> bool:
    return bool(path.endswith((".sh", ".py", ".cpp", ".c", ".hpp", ".h", ".js", ".ts")) or Path(path).name in {"bm", "stopper", "rotator", "paycon", "bmtest"})


def _is_executable(path: str, name: str) -> bool:
    return bool(_is_source(path) or (not Path(path).suffix and name not in {"ver"}))


def _format_size(size_bytes: int) -> str:
    mb = size_bytes / (1024 * 1024)
    text = f"{mb:.1f}".rstrip("0").rstrip(".")
    return f"{text} MB"


def _modal() -> str:
    return """
<div class="modal-backdrop" id="log-modal" hidden>
  <div class="modal" role="dialog" aria-modal="true" aria-labelledby="modal-title">
    <div class="modal-head">
      <div>
        <h2 id="modal-title">Данные</h2>
        <p id="modal-subtitle" class="muted"></p>
      </div>
      <button type="button" class="modal-close" id="modal-close" aria-label="Закрыть">×</button>
    </div>
    <div id="modal-body" class="modal-body"></div>
  </div>
</div>
""".strip()


def _script() -> str:
    return r"""
<script>
(function() {
  const REPORT = JSON.parse(document.getElementById('report-data').textContent || '{}');
  const modal = document.getElementById('log-modal');
  const title = document.getElementById('modal-title');
  const subtitle = document.getElementById('modal-subtitle');
  const body = document.getElementById('modal-body');
  const closeBtn = document.getElementById('modal-close');
  const logFilesRoot = document.getElementById('log-files-root');
  const otherFilesRoot = document.getElementById('other-files-root');
  const bmStatusTableRoot = document.getElementById('bm-status-table-root');
  const bmStatusZeroToggleKey = 'bm-status-hide-zero';
  const bmGroupedTableRoot = document.getElementById('bm-grouped-table-root');
  const bmDateChartRoot = document.getElementById('bm-date-chart-root');
  const bmUnclassifiedRoot = document.getElementById('bm-unclassified-root');
  const activeFiltersRoot = document.getElementById('active-filters');
  const bmFilterRoot = document.getElementById('bm-filter-root');
  let currentMetaState = null;
  const statusOrder = [
    'Успешный онлайн (БЕЗ МИР)',
    'Успешный онлайн МИР',
    'Успешный оффлайн',
    'Проход зафейлен (онлайн - не получили конфирм и зарегали как фейл)',
    'Отказ, повторное предъявление',
    'Отказ, ошибка чтения карты',
    'Отказ, карта в стоп листе',
    'Отказ, коллизия',
    'Отказ, ошибка ODA/CDA',
    'NON_EMV_CARD',
    'Отказ, QR-код недействителен',
    'Отказ, операция отклонена',
    'Отказ, истек таймаут',
    'Отказ, нет карты в поле'
  ];
  const successStatuses = new Set([
    'Успешный онлайн (БЕЗ МИР)',
    'Успешный онлайн МИР',
    'Успешный оффлайн'
  ]);
  const errorStatuses = new Set([
    'Отказ, ошибка чтения карты',
    'Отказ, нет карты в поле',
    'Отказ, ошибка ODA/CDA',
    'Отказ, коллизия',
    'Отказ, истек таймаут'
  ]);
  const declineStatuses = new Set([
    'Проход зафейлен (онлайн - не получили конфирм и зарегали как фейл)',
    'Отказ, повторное предъявление',
    'Отказ, карта в стоп листе',
    'Отказ, QR-код недействителен',
    'Отказ, операция отклонена'
  ]);
  const filterLabels = {
      versions: 'Версии БМ',
      carriers: 'Перевозчики',
      readers: 'Ридеры',
      reader_firmwares: 'Версии ПО ридеров',
      dates: 'Даты'
    };
  const filterGroupOrder = [
    { name: 'versions', label: 'Версии БМ' },
    { name: 'carriers', label: 'Перевозчики' },
    { name: 'readers', label: 'Ридеры' },
    { name: 'dates', label: 'Даты' }
  ];
  const filterState = {
    versions: new Set(),
    carriers: new Set(),
    readers: new Set(),
    dates: new Set()
  };
  const metaIndex = buildMetaIndex();

  function buildMetaIndex() {
    const meta = REPORT.meta || {};
    return {
      versions: new Map((meta.versions || []).map((item) => [String(item.version || ''), item])),
      carriers: new Map((meta.carriers || []).map((item) => [String(item.carrier || ''), item])),
      readers: new Map((meta.readers || []).map((item) => [String(item.reader || ''), item])),
      reader_firmwares: new Map((meta.reader_firmwares || []).map((item) => [String(item.reader_firmware || ''), item])),
      dates: new Map((meta.dates || []).map((item) => [String(item.date || ''), item]))
    };
  }

  function openModal(target) {
    const kind = target.dataset.kind || 'files';
    const format = target.dataset.format || 'files';
    const label = target.dataset.label || 'Данные';
    const metaKind = target.dataset.metaKind || '';
    const payload = JSON.parse(target.dataset.payload || '[]');
    title.textContent = label;
    subtitle.textContent = kind === 'status'
      ? 'Полные строки логов для выбранного статуса'
      : kind === 'group'
      ? 'Полные строки логов для выбранной группы'
      : kind === 'meta'
      ? 'Подробная информация по выбранному блоку'
      : format === 'records'
      ? 'Записи для выбранной сводки'
      : 'Файлы этого типа, сгруппированные по архивам';
    body.innerHTML = kind === 'status'
      ? renderStatusItems(payload)
      : kind === 'group'
      ? renderStatusItems(payload)
      : kind === 'meta'
      ? renderMetaDetails(label, metaKind, payload)
      : format === 'records'
      ? renderRecordItems(payload)
      : renderFileItems(payload);
    currentMetaState = kind === 'meta' ? { label, metaKind, payload } : null;
    modal.hidden = false;
    document.body.classList.add('modal-open');
    syncFilterButtonState();
  }

  function focusFilterGroup(groupName) {
    const target = document.getElementById(`filter-group-${groupName}`);
    if (!target) {
      return;
    }
    target.scrollIntoView({ behavior: 'smooth', block: 'center' });
    target.classList.add('filter-group--flash');
    window.setTimeout(() => target.classList.remove('filter-group--flash'), 900);
  }

  function renderFileItems(items) {
    if (!items || !items.length) {
      return '<p class="muted">Нет данных</p>';
    }
    return items.map((item) => {
      if (item && item.payload) {
        const nested = renderFileItems(item.payload);
        return `
        <div class="modal-item">
          <div class="modal-item-head">
            <strong>${escapeHtml(String(item.label || item.archive || 'Данные'))}</strong>
            <span>${escapeHtml(String(item.count || 0))} файлов · ${escapeHtml(formatSize(item.size_bytes || 0))}</span>
          </div>
          ${nested}
        </div>`;
      }
      const files = (item.files || []).map((value) => `<li>${escapeHtml(String(value))}</li>`).join('');
      return `
        <div class="modal-item">
          <div class="modal-item-head">
            <strong>${escapeHtml(String(item.archive))}</strong>
            <span>${escapeHtml(String(item.count))} файлов · ${escapeHtml(formatSize(item.size_bytes || 0))}</span>
          </div>
          ${files ? `<ul class="modal-files">${files}</ul>` : '<p class="muted">Файлы не перечислены</p>'}
        </div>`;
    }).join('');
  }

  function renderStatusItems(items) {
    if (!items || !items.length) {
      return '<p class="muted">Нет строк для этого статуса.</p>';
    }
    const groups = new Map();
    items.forEach((item) => {
      const key = item.source_file || 'missing';
      if (!groups.has(key)) {
        groups.set(key, []);
      }
      groups.get(key).push(item);
    });
    return [...groups.entries()].map(([source, lines]) => {
      const rows = lines.map((line) => `
        <li>
          <div class="modal-line-head">
            <span>${escapeHtml(String(source))}</span>
            <em>${escapeHtml(String(line.line_number || ''))}${line.timestamp ? ` · ${escapeHtml(String(line.timestamp))}` : ''}</em>
          </div>
          <code>${highlightLine(String(line.raw_line || ''), line.decision_terms || [])}</code>
        </li>`).join('');
      return `
        <div class="modal-item">
          <div class="modal-item-head">
            <strong>${escapeHtml(String(source))}</strong>
            <span>${escapeHtml(String(lines.length))} строк</span>
          </div>
          <ul class="modal-lines">${rows}</ul>
        </div>`;
    }).join('');
  }

  function renderRecordItems(items) {
    if (!items || !items.length) {
      return '<p class="muted">Нет данных</p>';
    }
    return items.map((item) => {
      const archives = (item.archives || []).map((archive) => `
        <li>
          <div class="modal-line-head">
            <span>${escapeHtml(String(archive.archive || ''))}</span>
            <em>${escapeHtml(String(archive.count || ''))} событий</em>
          </div>
        </li>`).join('');
      return `
        <div class="modal-item">
          <div class="modal-item-head">
            <strong>${escapeHtml(String(item.version || item.carrier || item.reader || item.reader_firmware || item.date || ''))}</strong>
            <span>${escapeHtml(String(item.count ?? 0))} событий</span>
          </div>
          ${archives ? `<ul class="modal-lines">${archives}</ul>` : '<p class="muted">Нет данных по архивам</p>'}
        </div>`;
    }).join('');
  }

  function highlightLine(rawLine, terms) {
    const text = escapeHtml(String(rawLine || ''));
    const uniqueTerms = [...new Set((terms || []).map((term) => String(term || '').trim()).filter(Boolean))]
      .sort((a, b) => b.length - a.length);
    let highlighted = text;
    uniqueTerms.forEach((term) => {
      const escapedTerm = escapeHtml(term);
      const pattern = new RegExp(escapeRegExp(escapedTerm), 'g');
      highlighted = highlighted.replace(pattern, `<mark class="line-highlight">$&</mark>`);
    });
    return highlighted;
  }

  function escapeRegExp(text) {
    return String(text).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }

  function renderMetaDetails(label, metaKind, items) {
    if (!items || !items.length) {
      return '<p class="muted">Нет данных</p>';
    }
    const normalizedKind = String(metaKind || '').toLowerCase();
    const total = items.reduce((sum, item) => sum + Number(item.count || 0), 0);
    const introMap = {
      versions: 'Найденные версии BM и архивы, где они встречаются.',
      carriers: 'Перевозчики, найденные в логах, и признаки, по которым они определены.',
      readers: 'Типы ридеров и архивы, в которых они встречаются.',
      reader_firmwares: 'Версии ПО ридеров из маркеров ReaderVersion, firmware или fw.',
      dates: 'Даты из логов и количество событий на каждой дате.'
    };
    const rendered = items.map((item) => {
      const titleValue = item.version || item.carrier || item.reader || item.reader_firmware || item.date || 'Данные';
      const count = Number(item.count || 0);
      const evidence = item.evidence || [];
      const evidencePayload = escapeHtml(JSON.stringify(evidence));
      const evidenceTerms = [String(titleValue), ...((item.markers || []).map((value) => String(value)))].filter(Boolean);
      const evidenceTermsPayload = escapeHtml(JSON.stringify([...new Set(evidenceTerms)]));
      const archiveRows = (item.archives || []).map((archive) => {
        return `<li>${escapeHtml(String(archive.archive || ''))} · ${escapeHtml(String(archive.count || 0))}</li>`;
      }).join('');
      const markerRows = item.markers && item.markers.length
        ? `<ul class="modal-files">${item.markers.map((value) => `<li>${escapeHtml(String(value))}</li>`).join('')}</ul>`
        : '';
      return `
        <div class="modal-item modal-item--clickable" role="button" tabindex="0" data-evidence-label="${escapeHtml(String(titleValue))}" data-evidence="${evidencePayload}" data-evidence-terms="${evidenceTermsPayload}">
          <div class="modal-item-head">
            <strong class="modal-item-value">${escapeHtml(String(titleValue))}</strong>
            <span>${escapeHtml(String(count))} событий</span>
          </div>
          <div class="modal-item-tip">Откройте, чтобы увидеть сниппеты логов и признак, по которому значение попало в карточку.</div>
          ${normalizedKind === 'carriers' && markerRows ? markerRows : ''}
          ${archiveRows ? `<ul class="modal-files">${archiveRows}</ul>` : '<p class="muted">Нет данных по архивам</p>'}
        </div>`;
    }).join('');
    return `
      <div class="modal-toolbar">
        <span class="muted">${escapeHtml(introMap[normalizedKind] || 'Подробная информация по выбранному блоку.')}</span>
        <span class="muted">Всего: ${escapeHtml(String(total))}</span>
      </div>
      ${rendered}`;
  }

  function renderEvidenceDetails(label, evidence, terms = []) {
    if (!evidence || !evidence.length) {
      return `
        <button type="button" class="modal-back" data-action="back-to-meta">Назад</button>
        <p class="muted">Строки-источники для этого значения не сохранены.</p>`;
    }
    const highlightTerms = [...new Set([String(label), ...(terms || []).map((term) => String(term || '').trim()).filter(Boolean)])];
    const rows = evidence.map((line) => `
      <li>
        <div class="modal-line-head">
          <span>${escapeHtml(String(line.source_file || line.archive || ''))}</span>
          <em>${escapeHtml(String(line.line_number || ''))}${line.timestamp ? ` · ${escapeHtml(String(line.timestamp))}` : ''}</em>
        </div>
        <code>${highlightLine(String(line.raw_line || ''), highlightTerms)}</code>
      </li>`).join('');
    return `
      <button type="button" class="modal-back" data-action="back-to-meta">Назад</button>
      <div class="modal-item">
        <div class="modal-item-head">
          <strong>${escapeHtml(String(label))}</strong>
          <span>${escapeHtml(String(evidence.length))} строк</span>
        </div>
        <ul class="modal-lines">${rows}</ul>
      </div>`;
  }

  function renderFilterPanel() {
    if (!bmFilterRoot) {
      return;
    }
    const isOpen = bmFilterRoot.querySelector('details')?.open ?? false;
    const sections = filterGroupOrder.map((group) => renderFilterGroup(group)).join('');
    bmFilterRoot.innerHTML = `
      <details class="filter-panel" ${isOpen ? 'open' : ''}>
        <summary class="filter-panel__summary">
          <span>
            <strong>Фильтры</strong>
            <em>Выбирайте значения здесь. Доступные варианты пересчитываются на лету.</em>
          </span>
          <b>${countActiveFilters()}</b>
        </summary>
        <div class="filter-panel__body">
          <div class="filter-panel__head">
            <div class="muted">Сейчас видно только совместимые варианты.</div>
            <button type="button" class="filter-clear" data-action="clear-all-filters">Сбросить все</button>
          </div>
          <div class="filter-panel__groups">${sections}</div>
        </div>
      </details>`;
  }

  function countActiveFilters() {
    return Object.values(filterState).reduce((sum, bucket) => sum + bucket.size, 0);
  }

  function renderFilterGroup(group) {
    const items = Array.from((metaIndex[group.name] || new Map()).values());
    const allowedKeys = allowedKeysForGroup(group.name);
    const selected = filterState[group.name];
    const visibleItems = items.filter((item) => {
      const key = String(item[groupKey(group.name)] || '');
      return selected.has(key) || allowedKeys === null || allowedKeys.has(key);
    });
    const options = visibleItems.map((item) => renderFilterOption(group.name, group.label, item)).join('');
    return `
      <section class="filter-group" id="filter-group-${escapeHtml(group.name)}">
        <div class="filter-group__head">
          <div>
            <h3>${escapeHtml(group.label)}</h3>
            <p>${escapeHtml(String(selected.size))} выбрано · ${escapeHtml(String(visibleItems.length))} доступно</p>
          </div>
          <button type="button" class="filter-group__clear" data-filter-group-reset="${escapeHtml(group.name)}">Сбросить</button>
        </div>
        <div class="filter-option-grid">${options || '<span class="muted">Нет доступных значений</span>'}</div>
      </section>`;
  }

  function renderFilterOption(groupName, groupLabel, item) {
    const key = String(item[groupKey(groupName)] || '');
    const selected = filterState[groupName].has(key);
    const allowedKeys = allowedKeysForGroup(groupName);
    const visible = selected || allowedKeys === null || allowedKeys.has(key);
    if (!visible) {
      return '';
    }
    const count = Number(item.count || 0);
    const label = key || 'missing';
    return `
      <button type="button" class="filter-option${selected ? ' filter-option--active' : ''}" data-filter-group="${escapeHtml(groupName)}" data-filter-key="${escapeHtml(key)}" aria-pressed="${selected ? 'true' : 'false'}">
        <span>${escapeHtml(label)}</span>
        <strong>${count}</strong>
      </button>`;
  }

  function metaGroupForLabel(label) {
    if (label === 'Версии БМ') {
      return { name: 'versions', key: 'version' };
    }
    if (label === 'Перевозчики') {
      return { name: 'carriers', key: 'carrier' };
    }
    if (label === 'Ридеры') {
      return { name: 'readers', key: 'reader' };
    }
    if (label === 'Даты') {
      return { name: 'dates', key: 'date' };
    }
    return { name: 'versions', key: 'version' };
  }

  function groupKey(groupName) {
    if (groupName === 'versions') return 'version';
    if (groupName === 'carriers') return 'carrier';
    if (groupName === 'readers') return 'reader';
    if (groupName === 'dates') return 'date';
    return 'version';
  }

  function closeModal() {
    modal.hidden = true;
    currentMetaState = null;
    document.body.classList.remove('modal-open');
  }

  function toggleFilter(group, key) {
    const bucket = filterState[group];
    if (!bucket) {
      return;
    }
    if (bucket.has(key)) {
      bucket.delete(key);
    } else {
      bucket.add(key);
    }
    renderAll();
    syncFilterButtonState();
  }

  function clearFilters() {
    Object.values(filterState).forEach((bucket) => bucket.clear());
    renderAll();
    syncFilterButtonState();
  }

  function clearGroup(group) {
    const bucket = filterState[group];
    if (!bucket) {
      return;
    }
    bucket.clear();
    renderAll();
    syncFilterButtonState();
  }

  function selectedEvents() {
    if (!hasActiveFilters()) {
      return REPORT.events || [];
    }
    return (REPORT.events || []).filter((event) => eventMatchesFilters(event));
  }

  function hasActiveFilters() {
    return Object.values(filterState).some((bucket) => bucket.size > 0);
  }

  function eventMatchesFilters(event, exceptGroup = '') {
    return Object.entries(filterState).every(([group, bucket]) => {
      if (group === exceptGroup || bucket.size === 0) {
        return true;
      }
      return eventFilterValues(event, group).some((value) => bucket.has(value));
    });
  }

  function eventFilterValues(event, group) {
    if (group === 'versions') {
      return [String(event.version || '')];
    }
    if (group === 'carriers') {
      return (event.carriers || []).map((value) => String(value || ''));
    }
    if (group === 'readers') {
      return [String(event.reader || '')];
    }
    if (group === 'dates') {
      return [String(event.date || String(event.timestamp || '').slice(0, 10) || '')];
    }
    return [];
  }

  function allowedKeysForGroup(groupName) {
    const hasOtherFilters = Object.entries(filterState).some(([name, bucket]) => name !== groupName && bucket.size > 0);
    if (!hasOtherFilters) {
      return null;
    }
    const allowed = new Set();
    (REPORT.events || []).forEach((event) => {
      if (!eventMatchesFilters(event, groupName)) {
        return;
      }
      eventFilterValues(event, groupName).forEach((value) => {
        if (value) {
          allowed.add(value);
        }
      });
    });
    return allowed;
  }

  function selectedArchives() {
    if (!hasActiveFilters()) {
      return null;
    }
    return new Set(selectedEvents().map((event) => String(event.archive || '')).filter(Boolean));
  }

  function renderActiveFilters() {
    if (!activeFiltersRoot) {
      return;
    }
    const chips = [];
    Object.entries(filterState).forEach(([group, bucket]) => {
      bucket.forEach((key) => {
        chips.push(`
          <button type="button" class="filter-chip" data-filter-group="${escapeHtml(group)}" data-filter-key="${escapeHtml(key)}">
            <span>${escapeHtml(filterLabels[group] || group)}:</span>
            <strong>${escapeHtml(key)}</strong>
            <i>×</i>
          </button>`);
      });
    });
    activeFiltersRoot.innerHTML = chips.length
      ? `<div class="active-filters__row">${chips.join('')}<button type="button" class="filter-clear" data-action="clear-active-filters">Сбросить</button></div>`
      : '<div class="active-filters__empty muted">Фильтры не выбраны</div>';
  }

  function filterArchiveGroups(groups, allowed) {
    return (groups || []).map((group) => {
      const payload = (group.payload || []).filter((entry) => allowed === null || allowed.has(String(entry.archive || '')));
      const count = payload.reduce((sum, entry) => sum + Number(entry.count || 0), 0);
      const sizeBytes = payload.reduce((sum, entry) => sum + Number(entry.size_bytes || 0), 0);
      return { ...group, payload, count, size_bytes: sizeBytes };
    }).filter((group) => group.count > 0);
  }

  function renderLogChart(groups, total) {
    if (!total) {
      return '<div class="bar-chart bar-chart--empty"><div class="muted">В архиве не найдено файлов для группировки.</div></div>';
    }
    const palette = ['#2764a3', '#137752', '#a15c06', '#7c3aed', '#d14343', '#475467', '#0f766e', '#5b5bd6', '#6b7280'];
    return `<div class="bar-chart">${groups.map((group, index) => {
      const percent = total ? (group.count / total) * 100 : 0;
      const color = palette[index % palette.length];
      return `
        <button type="button" class="bar-row" data-kind="files" data-label="${escapeHtml(String(group.label || ''))}" data-payload="${escapeHtml(JSON.stringify(group.payload || []))}">
          <div class="bar-head"><span>${escapeHtml(String(group.label || ''))}</span><strong>${group.count} (${percent.toFixed(2)}%) ${formatSize(group.size_bytes || 0)}</strong></div>
          <div class="bar-track"><div class="bar-fill" style="width:${Math.min(percent, 100).toFixed(2)}%;background:${color};"></div></div>
        </button>`;
    }).join('')}</div>`;
  }

  function renderLogFiles() {
    if (!logFilesRoot) {
      return;
    }
    const allowed = selectedArchives();
    const groups = filterArchiveGroups(REPORT.log_groups || [], allowed);
    const total = groups.reduce((sum, group) => sum + Number(group.count || 0), 0);
    logFilesRoot.innerHTML = renderLogChart(groups, total);
  }

  function renderOtherFiles() {
    if (!otherFilesRoot) {
      return;
    }
    const allowed = selectedArchives();
    const groups = filterArchiveGroups(REPORT.other_groups || [], allowed);
    const total = groups.reduce((sum, group) => sum + Number(group.count || 0), 0);
    otherFilesRoot.innerHTML = renderLogChart(groups, total);
  }

  function renderStatusSection() {
    const events = selectedEvents();
    if (bmStatusTableRoot) {
      bmStatusTableRoot.innerHTML = renderStatusTable(events);
    }
    if (bmGroupedTableRoot) {
      bmGroupedTableRoot.innerHTML = renderGroupedTable(events);
    }
    if (bmDateChartRoot) {
      bmDateChartRoot.innerHTML = renderDateChart(events);
    }
    if (bmUnclassifiedRoot) {
      bmUnclassifiedRoot.innerHTML = renderUnclassified(events);
    }
    syncStatusZeroRows();
  }

  function renderStatusTable(events) {
    const counts = countByStatus(events);
    const total = events.length;
    const rows = statusOrder.map((status) => renderStatusRow(status, counts.get(status) || 0, total, events)).join('');
    const unclassified = counts.get('Не классифицировано') || 0;
    return `
      <div class="bm-status-controls">
        <label class="bm-status-zero-toggle">
          <input type="checkbox" id="bm-status-hide-zero"${loadStatusZeroPreference() ? ' checked' : ''}>
          <span class="bm-status-zero-toggle__switch" aria-hidden="true"><span class="bm-status-zero-toggle__thumb"></span></span>
          <span class="bm-status-zero-toggle__text">Скрыть строки с нулём</span>
        </label>
      </div>
      <div class="table-wrap bm-table-wrap">
        <table class="status-table status-table--bm">
          <colgroup><col style="width:68%"><col style="width:16%"><col style="width:16%"></colgroup>
          <thead class="status-table-head"><tr><th>Статус</th><th>Кол-во</th><th>%</th></tr></thead>
          <tbody>
            ${rows}
            ${unclassified > 0 ? renderStatusRow('Не классифицировано', unclassified, total, events) : ''}
            <tr class="status-row status-row--total"><td>ИТОГО</td><td>${total}</td><td>100.00%</td></tr>
          </tbody>
        </table>
      </div>`;
  }

  function renderStatusRow(status, count, total, events = []) {
    const percent = total ? (count / total) * 100 : 0;
    const classes = ['status-row'];
    if (successStatuses.has(status)) {
      classes.push('status-row--success');
    }
    if (count > 0) {
      classes.push('status-row--clickable');
    }
    const payload = JSON.stringify(events.filter((event) => event.status === status));
    return `
      <tr class="${classes.join(' ')}" data-count="${count}"${count > 0 ? ` data-kind="status" data-status="${escapeHtml(status)}" data-payload="${escapeHtml(payload)}" tabindex="0"` : ''}>
        <td>${escapeHtml(status)}</td>
        <td>${count}</td>
        <td>${percent.toFixed(2)}%</td>
      </tr>`;
  }

  function countByStatus(events) {
    const counts = new Map(statusOrder.map((status) => [status, 0]));
    counts.set('Не классифицировано', 0);
    events.forEach((event) => {
      const status = String(event.status || 'Не классифицировано');
      counts.set(status, (counts.get(status) || 0) + 1);
    });
    return counts;
  }

  function renderGroupedTable(events) {
    const groups = groupByStatusGroup(events);
    const total = events.length;
    const unclassifiedRow = groups.unclassified > 0
      ? renderGroupedRow('Не классифицировано', groups.unclassified, total, false, events)
      : '';
    const rows = [
      renderGroupedRow('Успех', groups.success, total, true, events),
      renderGroupedRow('Ошибки', groups.errors, total, false, events),
      renderGroupedRow('Отказы', groups.declines, total, false, events),
      unclassifiedRow
    ].join('');
    return `
      <div class="table-wrap bm-table-wrap bm-table-wrap--compact">
        <table class="status-table status-table--grouped">
          <thead class="status-table-head"><tr><th>Группа</th><th>Кол-во</th><th>%</th></tr></thead>
          <tbody>
            ${rows}
            <tr class="status-row status-row--total"><td>ИТОГО</td><td>${total}</td><td>100.00%</td></tr>
          </tbody>
        </table>
      </div>`;
  }

  function renderGroupedRow(label, count, total, success, events = []) {
    const percent = total ? (count / total) * 100 : 0;
    const classes = ['status-row', 'status-row--group'];
    if (success) {
      classes.push('status-row--success');
    }
    if (count > 0) {
      classes.push('status-row--clickable');
    }
    const payload = JSON.stringify(events.filter((event) => groupLabel(event.status) === label));
    return `
      <tr class="${classes.join(' ')}"${count > 0 ? ` data-kind="group" data-status="${escapeHtml(label)}" data-payload="${escapeHtml(payload)}" tabindex="0"` : ''}>
        <td>${escapeHtml(label)}</td>
        <td>${count}</td>
        <td>${percent.toFixed(2)}%</td>
      </tr>`;
  }

  function groupByStatusGroup(events) {
    const result = { success: 0, errors: 0, non_emv: 0, declines: 0, unclassified: 0 };
    events.forEach((event) => {
      const group = groupLabel(event.status);
      if (group === 'Успех') {
        result.success += 1;
      } else if (group === 'Ошибки') {
        result.errors += 1;
      } else if (group === 'NON_EMV_CARD') {
        result.non_emv += 1;
      } else if (group === 'Отказы') {
        result.declines += 1;
      } else {
        result.unclassified += 1;
      }
    });
    return result;
  }

  function groupLabel(status) {
    if (successStatuses.has(status)) {
      return 'Успех';
    }
    if (status === 'Отказ, истек таймаут') {
      return 'Отказ, истек таймаут';
    }
    if (status === 'NON_EMV_CARD') {
      return 'NON_EMV_CARD';
    }
    if (errorStatuses.has(status)) {
      return 'Ошибки';
    }
    if (declineStatuses.has(status)) {
      return 'Отказы';
    }
    return 'Не классифицировано';
  }

  function renderDateChart(events) {
    const byDate = groupByDate(events);
    const dates = Object.keys(byDate).sort();
    if (dates.length <= 1) {
      return '';
    }
    const width = 960;
    const height = 280;
    const padLeft = 54;
    const padRight = 24;
    const padTop = 24;
    const padBottom = 56;
    const plotWidth = width - padLeft - padRight;
    const plotHeight = height - padTop - padBottom;
    const maxValue = Math.max(1, ...dates.map((date) => Math.max(byDate[date].success, byDate[date].errors, byDate[date].non_emv, byDate[date].declines)));
    const xStep = plotWidth / Math.max(dates.length - 1, 1);
    const seriesSpecs = [
      ['Успех', '#137752', 'success'],
      ['Ошибки', '#d14343', 'errors'],
      ['NON_EMV_CARD', '#7c3aed', 'non_emv'],
      ['Отказы', '#a15c06', 'declines']
    ];
    const paths = [];
    const points = [];
    seriesSpecs.forEach(([label, color, key]) => {
      const coords = dates.map((date, index) => {
        const value = byDate[date][key] || 0;
        const x = padLeft + (index * xStep);
        const y = padTop + plotHeight - (value / maxValue * plotHeight);
        return [x, y, value];
      });
      if (coords.length) {
        const d = `M ${coords[0][0].toFixed(1)} ${coords[0][1].toFixed(1)} ` + coords.slice(1).map(([x, y]) => `L ${x.toFixed(1)} ${y.toFixed(1)}`).join(' ');
        paths.push(`<path d="${d}" fill="none" stroke="${color}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" />`);
        coords.forEach(([x, y, value]) => {
          points.push(`<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="4.5" fill="${color}"><title>${escapeHtml(label)}: ${value}</title></circle>`);
        });
      }
    });
    const axisLines = `
      <line x1="${padLeft}" y1="${padTop + plotHeight}" x2="${width - padRight}" y2="${padTop + plotHeight}" stroke="#cfd8e3" />
      <line x1="${padLeft}" y1="${padTop}" x2="${padLeft}" y2="${padTop + plotHeight}" stroke="#cfd8e3" />`;
    const yTicks = [];
    for (let tick = 0; tick <= maxValue; tick += Math.max(1, Math.floor(maxValue / 4) || 1)) {
      const y = padTop + plotHeight - (tick / maxValue * plotHeight);
      yTicks.push(`<text x="${padLeft - 10}" y="${y + 4}" text-anchor="end">${tick}</text>`);
    }
    const xLabels = dates.map((date, index) => {
      const x = padLeft + (index * xStep);
      return `<text x="${x.toFixed(1)}" y="${height - 20}" text-anchor="middle">${escapeHtml(date)}</text>`;
    });
    return `
      <div class="section-title section-title--compact">
        <h3>Динамика по датам</h3>
      </div>
      <div class="chart-card">
        <svg class="line-chart" viewBox="0 0 960 280" role="img" aria-label="Динамика статусов по датам">
          <g class="axis">${axisLines}</g>
          <g class="ticks">${yTicks.join('')}</g>
          <g class="series">${paths.join('')}${points.join('')}</g>
          <g class="labels">${xLabels.join('')}</g>
        </svg>
        <div class="chart-legend">
          <span><i style="background:#137752"></i>Успех</span>
          <span><i style="background:#d14343"></i>Ошибки</span>
          <span><i style="background:#7c3aed"></i>NON_EMV_CARD</span>
          <span><i style="background:#a15c06"></i>Отказы</span>
        </div>
      </div>`;
  }

  function groupByDate(events) {
    const grouped = {};
    events.forEach((event) => {
      if (!event.timestamp) {
        return;
      }
      if (event.status === 'Отказ, истек таймаут') {
        return;
      }
      const date = String(event.timestamp).slice(0, 10);
      if (!grouped[date]) {
        grouped[date] = { success: 0, errors: 0, non_emv: 0, declines: 0 };
      }
      const group = groupLabel(event.status);
      if (group === 'Успех') {
        grouped[date].success += 1;
      } else if (group === 'Ошибки') {
        grouped[date].errors += 1;
      } else if (group === 'NON_EMV_CARD') {
        grouped[date].non_emv += 1;
      } else if (group === 'Отказы') {
        grouped[date].declines += 1;
      }
    });
    return grouped;
  }

  function syncStatusZeroRows() {
    if (!bmStatusTableRoot) {
      return;
    }
    const checkbox = bmStatusTableRoot.querySelector('#bm-status-hide-zero');
    const rows = bmStatusTableRoot.querySelectorAll('tbody tr[data-count]');
    if (!checkbox) {
      return;
    }
    const apply = () => {
      const hideZero = checkbox.checked;
      saveStatusZeroPreference(hideZero);
      rows.forEach((row) => {
        const count = Number(row.dataset.count || 0);
        const isTotal = row.classList.contains('status-row--total');
        row.hidden = hideZero && !isTotal && count === 0;
      });
    };
    if (!checkbox.dataset.bound) {
      checkbox.addEventListener('change', apply);
      checkbox.dataset.bound = 'true';
    }
    apply();
  }

  function loadStatusZeroPreference() {
    try {
      return window.localStorage.getItem(bmStatusZeroToggleKey) === '1';
    } catch (error) {
      return false;
    }
  }

  function saveStatusZeroPreference(value) {
    try {
      window.localStorage.setItem(bmStatusZeroToggleKey, value ? '1' : '0');
    } catch (error) {
      return;
    }
  }

  function renderUnclassified(events) {
    const rows = {};
    events.filter((event) => groupLabel(event.status) === 'Не классифицировано').forEach((event) => {
      const code = String(event.code || 'missing');
      const message = String(event.message || 'missing');
      const key = `${code}|||${message}`;
      if (!rows[key]) {
        rows[key] = { code, message, count: 0 };
      }
      rows[key].count += 1;
    });
    const entries = Object.values(rows).sort((a, b) => b.count - a.count || a.code.localeCompare(b.code) || a.message.localeCompare(b.message));
    const summary = entries.reduce((sum, row) => sum + row.count, 0);
    if (summary === 0) {
      return '';
    }
    const renderedRows = entries.map((row) => `<tr><td>${escapeHtml(row.code)}</td><td>${escapeHtml(row.message)}</td><td>${row.count}</td></tr>`).join('');
    const table = `
      <div class="table-wrap">
        <table class="status-table status-table--diagnostic">
          <thead class="status-table-head"><tr><th>Код</th><th>Сообщение</th><th>Кол-во</th></tr></thead>
          <tbody>${renderedRows || '<tr><td colspan="3" class="muted">Нет данных</td></tr>'}</tbody>
        </table>
      </div>`;
    return `
      <details class="collapsible">
        <summary>
          <span>
            <strong>Не классифицировано</strong>
            <em>${summary} строк</em>
          </span>
        </summary>
        <div class="collapsible-body">
          ${table}
        </div>
      </details>`;
  }

  function renderAll() {
    renderActiveFilters();
    renderFilterPanel();
    renderLogFiles();
    renderOtherFiles();
    renderStatusSection();
  }

  function syncFilterButtonState() {
    document.querySelectorAll('[data-filter-group]').forEach((element) => {
      const group = element.dataset.filterGroup;
      const key = element.dataset.filterKey || '';
      const active = filterState[group] && filterState[group].has(key);
      if (element.classList.contains('modal-select')) {
        element.classList.toggle('modal-select--active', active);
      }
      if (element.classList.contains('filter-chip')) {
        element.classList.toggle('filter-chip--active', active);
      }
      element.setAttribute('aria-pressed', active ? 'true' : 'false');
    });
  }

  function openModal(target) {
    const kind = target.dataset.kind || 'files';
    const format = target.dataset.format || 'files';
    const label = target.dataset.label || 'Данные';
    const metaKind = target.dataset.metaKind || '';
    const payload = JSON.parse(target.dataset.payload || '[]');
    title.textContent = label;
    subtitle.textContent = kind === 'status'
      ? 'Полные строки логов для выбранного статуса'
      : kind === 'group'
      ? 'Полные строки логов для выбранной группы'
      : kind === 'meta'
      ? 'Подробная информация по выбранному блоку'
      : format === 'records'
      ? 'Записи для выбранной сводки'
      : 'Файлы этого типа, сгруппированные по архивам';
    body.innerHTML = kind === 'status'
      ? renderStatusItems(payload)
      : kind === 'group'
      ? renderStatusItems(payload)
      : kind === 'meta'
      ? renderMetaDetails(label, metaKind, payload)
      : format === 'records'
      ? renderRecordItems(payload)
      : renderFileItems(payload);
    currentMetaState = kind === 'meta' ? { label, metaKind, payload } : null;
    modal.hidden = false;
    document.body.classList.add('modal-open');
    syncFilterButtonState();
  }

  function closeModal() {
    modal.hidden = true;
    currentMetaState = null;
    document.body.classList.remove('modal-open');
  }

  document.addEventListener('click', (event) => {
    const interactive = event.target.closest('[data-kind]');
    if (!interactive || interactive.closest('[data-filter-group]')) {
      return;
    }
    openModal(interactive);
  });
  document.addEventListener('keydown', (event) => {
    if (event.key !== 'Enter' && event.key !== ' ') {
      return;
    }
    const evidenceButton = event.target.closest('[data-evidence]');
    if (evidenceButton) {
      event.preventDefault();
      const label = evidenceButton.dataset.evidenceLabel || 'Данные';
      const evidence = JSON.parse(evidenceButton.dataset.evidence || '[]');
      const terms = JSON.parse(evidenceButton.dataset.evidenceTerms || '[]');
      title.textContent = label;
      subtitle.textContent = 'Строки лога, из которых получено значение';
      body.innerHTML = renderEvidenceDetails(label, evidence, terms);
      return;
    }
    const interactive = event.target.closest('[data-kind]');
    if (!interactive || interactive.closest('[data-filter-group]')) {
      return;
    }
    event.preventDefault();
    openModal(interactive);
  });
  document.addEventListener('click', (event) => {
    const evidenceButton = event.target.closest('[data-evidence]');
    if (evidenceButton) {
      event.preventDefault();
      event.stopPropagation();
      const label = evidenceButton.dataset.evidenceLabel || 'Данные';
      const evidence = JSON.parse(evidenceButton.dataset.evidence || '[]');
      const terms = JSON.parse(evidenceButton.dataset.evidenceTerms || '[]');
      title.textContent = label;
      subtitle.textContent = 'Строки лога, из которых получено значение';
      body.innerHTML = renderEvidenceDetails(label, evidence, terms);
      return;
    }
    const backToMeta = event.target.closest('[data-action="back-to-meta"]');
    if (backToMeta && currentMetaState) {
      event.preventDefault();
      event.stopPropagation();
      title.textContent = currentMetaState.label;
      subtitle.textContent = 'Подробная информация по выбранному блоку';
      body.innerHTML = renderMetaDetails(currentMetaState.label, currentMetaState.metaKind, currentMetaState.payload);
      syncFilterButtonState();
      return;
    }
    const clearAll = event.target.closest('[data-action="clear-all-filters"]');
    if (clearAll) {
      event.preventDefault();
      event.stopPropagation();
      clearFilters();
      return;
    }
    const clearActive = event.target.closest('[data-action="clear-active-filters"]');
    if (clearActive) {
      event.preventDefault();
      event.stopPropagation();
      clearFilters();
      return;
    }
    const copyButton = event.target.closest('[data-copy-target]');
    if (copyButton) {
      event.preventDefault();
      event.stopPropagation();
      copyTextBlock(copyButton);
      return;
    }
    const focusGroup = event.target.closest('[data-focus-group]');
    if (focusGroup) {
      event.preventDefault();
      focusFilterGroup(focusGroup.dataset.focusGroup || '');
      return;
    }
    const groupReset = event.target.closest('[data-filter-group-reset]');
    if (groupReset) {
      event.preventDefault();
      event.stopPropagation();
      clearGroup(groupReset.dataset.filterGroupReset || '');
      return;
    }
    const button = event.target.closest('[data-filter-group]');
    if (!button) {
      return;
    }
    event.preventDefault();
    event.stopPropagation();
    toggleFilter(button.dataset.filterGroup, button.dataset.filterKey || '');
  });
  closeBtn.addEventListener('click', closeModal);
  modal.addEventListener('click', (event) => {
    if (event.target === modal) {
      closeModal();
    }
  });
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && !modal.hidden) {
      closeModal();
    }
  });

  function escapeHtml(text) {
    return text
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;')
      .replaceAll("'", '&#39;');
  }

  function formatSize(bytes) {
    const mb = bytes / (1024 * 1024);
    return `${mb.toFixed(1).replace(/\.0$/, '')} MB`;
  }

  function copyTextBlock(button) {
    const targetId = button.dataset.copyTarget || '';
    const target = document.getElementById(targetId);
    const text = target ? target.textContent || '' : '';
    if (!text) {
      return;
    }
    const original = button.textContent;
    const done = () => {
      button.textContent = 'Скопировано';
      window.setTimeout(() => {
        button.textContent = original;
      }, 1600);
    };
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(done).catch(() => fallbackCopy(text, done));
      return;
    }
    fallbackCopy(text, done);
  }

  function fallbackCopy(text, done) {
    const field = document.createElement('textarea');
    field.value = text;
    field.setAttribute('readonly', '');
    field.style.position = 'fixed';
    field.style.top = '-9999px';
    document.body.appendChild(field);
    field.select();
    try {
      document.execCommand('copy');
      done();
    } finally {
      document.body.removeChild(field);
    }
  }

  renderAll();
})();
</script>
""".strip()


def _css() -> str:
    return """
:root { color-scheme: light; --bg: #eef2f6; --panel: #ffffff; --line: #d9e0e7; --text: #1f2933; --muted: #667085; --soft: #f6f8fb; --blue: #2764a3; --filter-hover: #fff1a8; --filter-hover-border: #d49a00; --filter-active-border: #0b2f6b; }
* { box-sizing: border-box; }
body { margin: 0; background: linear-gradient(180deg, #f6f8fb 0%, #eef2f6 100%); color: var(--text); font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
main { max-width: 1120px; margin: 0 auto; padding: 28px; }
h1, h2, p { margin: 0; }
.header { display: flex; align-items: flex-end; justify-content: space-between; gap: 18px; margin-bottom: 18px; padding: 16px 18px; border: 1px solid var(--line); border-radius: 14px; background: var(--panel); box-shadow: 0 8px 24px rgba(31, 41, 51, 0.04); }
.header p { color: var(--muted); font-size: 13px; }
.version { display: inline-block; margin-top: 6px; color: var(--muted); font-size: 13px; }
.header h1 { font-size: 30px; line-height: 1.1; }
.upload-composition-summary { margin-bottom: 14px; padding: 12px; border: 1px solid var(--line); border-radius: 8px; background: var(--soft); }
.upload-composition-summary ul { margin: 8px 0 0; padding-left: 18px; }
.upload-composition-part + .upload-composition-part { margin-top: 16px; }
.upload-composition-part h3 { margin: 0 0 4px; font-size: 15px; line-height: 1.2; }
.upload-composition-part p { margin-bottom: 10px; }
.status-table--log-type-detection code { display: inline-block; max-width: 100%; white-space: pre-wrap; overflow-wrap: anywhere; }
.source-status { display: inline-flex; align-items: center; border: 1px solid var(--line); border-radius: 999px; padding: 3px 8px; font-size: 12px; white-space: nowrap; background: var(--soft); }
.source-status--available { color: #137752; background: #eef8ee; border-color: #c9ddc9; }
.source-status--partial { color: #9a6700; background: #fff8e6; border-color: #ead7a0; }
.source-status--missing { color: #b42318; background: #fff1f0; border-color: #f2c4c0; }
.source-status--not_required { color: #4f5d6b; background: #f6f8fb; border-color: #d9e0e7; }
.header-badge { min-width: 130px; background: var(--soft); border: 1px solid var(--line); border-radius: 12px; padding: 12px; }
.header-badge span, .metric span, .bm-meta-card span { display: block; color: var(--muted); font-size: 12px; }
.header-badge strong, .metric strong, .bm-meta-card strong { display: block; margin-top: 4px; font-size: 18px; line-height: 1.2; }
.section { margin-top: 14px; background: var(--panel); border: 1px solid var(--line); border-radius: 14px; padding: 18px; box-shadow: 0 8px 24px rgba(31, 41, 51, 0.04); }
.section-title { display: flex; align-items: baseline; justify-content: space-between; gap: 16px; margin-bottom: 12px; }
.section-title h2 { font-size: 18px; line-height: 1.2; }
.section-title--compact { margin: 14px 0 10px; }
.section-title--compact h3 { font-size: 15px; line-height: 1.2; color: var(--muted); }
.section-title p, .muted { color: var(--muted); }
.section-source { font-size: 12px; text-align: right; }
.summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 10px; }
.metric, .bm-meta-card { background: var(--soft); border: 1px solid var(--line); border-radius: 12px; padding: 12px; }
.metric--button { width: 100%; color: inherit; text-align: left; cursor: pointer; appearance: none; font: inherit; }
.metric--button:hover { background: #f2f5f8; border-color: #dbe3ea; }
.metric--button:focus-visible { outline: 2px solid #9db9d6; outline-offset: 2px; }
.bm-meta-card--button { width: 100%; color: inherit; text-align: left; cursor: pointer; appearance: none; font: inherit; }
.bm-meta-card--button:hover { background: #f2f5f8; border-color: #dbe3ea; }
.bm-meta-card--button:focus-visible { outline: 2px solid #9db9d6; outline-offset: 2px; }
.bm-meta-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 10px; }
.bm-filter-root { margin-top: 14px; }
.filter-panel { border: 1px solid var(--line); border-radius: 14px; background: var(--soft); padding: 0; overflow: hidden; }
.filter-panel__summary { list-style: none; cursor: pointer; display: flex; justify-content: space-between; align-items: center; gap: 12px; padding: 14px; }
.filter-panel__summary::-webkit-details-marker { display: none; }
.filter-panel__summary strong { display: block; font-size: 16px; }
.filter-panel__summary em { display: block; margin-top: 2px; color: var(--muted); font-style: normal; font-size: 13px; }
.filter-panel__summary b { min-width: 34px; text-align: center; color: var(--blue); background: #e6f1ff; border-radius: 999px; padding: 4px 8px; font-size: 12px; }
.filter-panel__body { padding: 0 14px 14px; }
.filter-panel__head { display: flex; justify-content: space-between; align-items: center; gap: 12px; margin-bottom: 12px; }
.filter-panel__head strong { display: block; font-size: 16px; }
.filter-panel__head span { display: block; margin-top: 2px; font-size: 13px; }
.filter-panel__groups { display: grid; gap: 12px; }
.filter-group { background: var(--panel); border: 1px solid var(--line); border-radius: 12px; padding: 12px; }
.filter-group--flash { box-shadow: 0 0 0 2px rgba(39, 100, 163, 0.12); border-color: #9db9d6; }
.filter-group__head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; margin-bottom: 10px; }
.filter-group__head h3 { font-size: 14px; line-height: 1.2; margin: 0; }
.filter-group__head p { margin-top: 4px; color: var(--muted); font-size: 12px; }
.filter-group__clear { appearance: none; border: 1px solid var(--line); background: var(--soft); color: inherit; font: inherit; border-radius: 999px; padding: 6px 10px; cursor: pointer; white-space: nowrap; }
.filter-group__clear:hover { background: var(--filter-hover); border-color: var(--filter-hover-border); }
.filter-option-grid { display: flex; flex-wrap: wrap; gap: 8px; }
.filter-option { appearance: none; border: 1px solid var(--line); background: var(--soft); color: inherit; font: inherit; border-radius: 999px; padding: 7px 10px; cursor: pointer; display: inline-flex; align-items: center; gap: 8px; }
.filter-option span { font-size: 13px; }
.filter-option strong { color: var(--muted); font-size: 12px; }
.filter-option:hover { background: var(--filter-hover); border-color: var(--filter-hover-border); }
.filter-option--active { background: #dbeafe; border-color: var(--filter-active-border); }
.filter-option--active:hover { background: var(--filter-hover); border-color: var(--filter-active-border); }
.filter-option:focus-visible { outline: 2px solid #9db9d6; outline-offset: 2px; }
.active-filters { margin-top: 12px; }
.active-filters__row { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
.active-filters__empty { color: var(--muted); font-size: 13px; }
.filter-chip, .filter-clear, .modal-select { appearance: none; border: 1px solid var(--line); background: var(--soft); color: inherit; font: inherit; }
.filter-chip { display: inline-flex; align-items: center; gap: 6px; padding: 7px 10px; border-radius: 999px; cursor: pointer; }
.filter-chip--active { background: #e6f1ff; border-color: var(--filter-active-border); }
.filter-chip span { color: var(--muted); font-size: 12px; }
.filter-chip strong { font-size: 13px; font-weight: 600; }
.filter-chip i { font-style: normal; color: var(--muted); }
.filter-chip:hover, .filter-clear:hover { background: var(--filter-hover); border-color: var(--filter-hover-border); }
.filter-chip--active:hover { background: var(--filter-hover); border-color: var(--filter-active-border); }
.filter-chip:focus-visible, .filter-clear:focus-visible, .modal-select:focus-visible { outline: 2px solid #9db9d6; outline-offset: 2px; }
.filter-clear { padding: 7px 12px; border-radius: 999px; cursor: pointer; }
.modal-select { display: block; width: 100%; text-align: left; cursor: pointer; }
.modal-select:hover { background: var(--filter-hover); border-color: var(--filter-hover-border); }
.modal-select--active { background: #dbeafe; border-color: var(--filter-active-border); box-shadow: 0 0 0 1px rgba(39, 100, 163, 0.10) inset; }
.modal-select--active:hover { background: var(--filter-hover); border-color: var(--filter-active-border); }
.modal-toolbar { display: flex; justify-content: space-between; align-items: center; gap: 12px; margin-bottom: 10px; }
.modal-toolbar__clear { appearance: none; border: 1px solid var(--line); background: var(--soft); color: inherit; font: inherit; border-radius: 999px; padding: 6px 10px; cursor: pointer; }
.modal-toolbar__clear:hover { background: #f2f5f8; border-color: #dbe3ea; }
.bar-chart { display: grid; gap: 12px; }
.bar-row { display: grid; gap: 6px; width: 100%; padding: 10px 12px; border: 1px solid transparent; border-radius: 12px; background: transparent; text-align: left; cursor: pointer; transition: background-color .16s ease, border-color .16s ease, box-shadow .16s ease; }
.bar-row:hover { background: rgba(39, 100, 163, 0.025); border-color: #e1e8ef; box-shadow: 0 3px 10px rgba(31, 41, 51, 0.03); }
.bar-row:focus-visible { outline: 2px solid #9db9d6; outline-offset: 2px; }
.bar-head { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
.bar-head span { font-weight: 600; }
.bar-head strong { color: var(--muted); font-size: 12px; font-weight: 600; white-space: nowrap; }
.bar-track { height: 14px; background: #e8edf2; border-radius: 999px; overflow: hidden; }
.bar-fill { height: 100%; border-radius: inherit; min-width: 2px; }
.bar-chart--empty { padding: 4px 0; }
.section--bm .section-title { margin-bottom: 8px; }
.section--bm .section-title h2 { font-size: 17px; line-height: 1.2; }
.bm-status-controls { display: flex; justify-content: flex-end; margin: 6px 0 10px; }
.bm-table-wrap .status-table--bm { table-layout: fixed; }
.bm-table-wrap .status-table--bm th:nth-child(1),
.bm-table-wrap .status-table--bm td:nth-child(1) { overflow-wrap: anywhere; }
.bm-table-wrap .status-table--bm th:nth-child(2),
.bm-table-wrap .status-table--bm td:nth-child(2),
.bm-table-wrap .status-table--bm th:nth-child(3),
.bm-table-wrap .status-table--bm td:nth-child(3) { white-space: nowrap; width: 1%; }
.bm-status-zero-toggle {
  display: inline-flex;
  align-items: center;
  gap: 10px;
  color: var(--muted);
  font-size: 13px;
  user-select: none;
  cursor: pointer;
}
.bm-status-zero-toggle input {
  position: absolute;
  opacity: 0;
  pointer-events: none;
}
.bm-status-zero-toggle__switch {
  position: relative;
  width: 46px;
  height: 28px;
  border-radius: 999px;
  background: #d7dde5;
  border: 1px solid rgba(24, 33, 47, 0.12);
  box-shadow: inset 0 1px 2px rgba(24, 33, 47, 0.08);
  transition: background 160ms ease, border-color 160ms ease;
  flex: 0 0 auto;
}
.bm-status-zero-toggle__thumb {
  position: absolute;
  top: 2px;
  left: 2px;
  width: 22px;
  height: 22px;
  border-radius: 50%;
  background: #fff;
  box-shadow: 0 1px 3px rgba(24, 33, 47, 0.25);
  transition: transform 160ms ease;
}
.bm-status-zero-toggle input:checked + .bm-status-zero-toggle__switch {
  background: #2cc36b;
  border-color: #2aa95d;
}
.bm-status-zero-toggle input:checked + .bm-status-zero-toggle__switch .bm-status-zero-toggle__thumb {
  transform: translateX(18px);
}
.bm-status-zero-toggle input:focus-visible + .bm-status-zero-toggle__switch {
  outline: 2px solid rgba(36, 87, 166, 0.35);
  outline-offset: 2px;
}
.bm-status-zero-toggle__text { line-height: 1.3; }
.status-table { width: 100%; border-collapse: collapse; background: var(--panel); }
.status-table-head th { background: #e8f1e8; border-bottom: 1px solid #c9ddc9; font-weight: 700; color: #274027; }
.status-row td, .status-table th { padding: 9px 10px; border-bottom: 1px solid #e8edf2; text-align: left; vertical-align: top; }
.status-row--success td { background: #eef8ee; }
.status-row--total td { background: #f6f8fb; font-weight: 700; }
.status-row--clickable { cursor: pointer; }
.status-row--clickable:hover td { background: rgba(39, 100, 163, 0.04); }
.status-table--suspicious code { white-space: pre-wrap; overflow-wrap: anywhere; }
.status-row--suspicious td { background: #fffaf0; }
.suspicious-table-wrap { max-height: 560px; }
.status-table--checks code { white-space: pre-wrap; overflow-wrap: anywhere; }
.status-table--checks { table-layout: fixed; }
.status-table--checks th, .status-table--checks td { padding: 8px 9px; }
.status-table--checks td:nth-child(5), .status-table--checks th:nth-child(5) { white-space: pre-wrap; overflow-wrap: anywhere; }
.device-boot-report + .device-boot-report { margin-top: 12px; }
.collapsible--device-boot .device-boot-report { box-shadow: none; border-color: #d9e0e7; }
.device-boot-report summary strong { font-size: 15px; }
.device-boot-chart { margin-bottom: 14px; padding: 12px; border: 1px solid var(--line); border-radius: 8px; background: var(--soft); }
.device-boot-chart h3 { margin: 0 0 10px; font-size: 15px; line-height: 1.2; }
.device-boot-chart-row { display: grid; grid-template-columns: minmax(180px, 240px) minmax(120px, 1fr) minmax(112px, auto); gap: 10px; align-items: center; min-height: 28px; }
.device-boot-chart-row + .device-boot-chart-row { margin-top: 8px; }
.device-boot-chart-label { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: var(--text); font-size: 13px; }
.device-boot-chart-track { height: 14px; border-radius: 999px; background: #e8edf2; overflow: hidden; }
.device-boot-chart-fill { height: 100%; border-radius: inherit; background: #2764a3; min-width: 2px; }
.device-boot-chart-row strong { white-space: nowrap; font-size: 13px; color: var(--muted); text-align: right; }
.device-boot-head h3 { margin: 0 0 4px; font-size: 16px; }
.device-boot-facts { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 8px; margin: 12px 0; }
.device-boot-fact { background: var(--soft); border: 1px solid var(--line); border-radius: 8px; padding: 10px; }
.device-boot-fact span { display: block; color: var(--muted); font-size: 12px; }
.device-boot-fact strong { display: block; margin-top: 3px; font-size: 15px; line-height: 1.25; overflow-wrap: anywhere; }
.device-boot-timeline { display: flex; width: 100%; min-height: 22px; border-radius: 999px; overflow: hidden; background: #e8edf2; border: 1px solid var(--line); }
.device-boot-timeline-segment { display: inline-flex; align-items: center; justify-content: center; min-width: 18px; color: #fff; font-size: 11px; line-height: 1; background: #2764a3; border-right: 1px solid rgba(255,255,255,0.6); }
.device-boot-timeline-segment:nth-child(2n) { background: #137752; }
.device-boot-timeline-segment:nth-child(3n) { background: #a15c06; }
.device-boot-timeline-segment:nth-child(4n) { background: #7c3aed; }
.status-table--device-boot { table-layout: fixed; }
.status-table--device-boot code { white-space: pre-wrap; overflow-wrap: anywhere; }
.status-table--device-boot-slowest { table-layout: fixed; }
.status-table--device-boot-slowest code { white-space: pre-wrap; overflow-wrap: anywhere; }
.device-boot-text-details { margin-top: 14px; box-shadow: none; border-color: #d9e0e7; }
.device-boot-text-details summary strong { font-size: 15px; }
.device-boot-text { margin: 8px 0 0; padding: 14px; border: 1px solid var(--line); border-radius: 8px; background: #f8fafc; color: #1f2933; white-space: pre-wrap; overflow-wrap: anywhere; font: 12px/1.45 ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
.copy-button { appearance: none; border: 1px solid var(--line); background: var(--soft); color: inherit; font: inherit; border-radius: 8px; padding: 8px 10px; cursor: pointer; }
.copy-button:hover { background: #f2f5f8; border-color: #dbe3ea; }
.copy-button:focus-visible { outline: 2px solid #9db9d6; outline-offset: 2px; }
.status-row--check-critical td { color: #b42318; }
.status-row--check-warning td { color: #9a6700; }
.status-row--check-info td { color: #111827; }
.status-row--check td code { color: inherit; }
.status-row--check td { background: #f8fbff; }
.status-row--check-muted td { background: #fbfcfe; color: #4f5d6b; }
.checks-subtitle { margin: 16px 0 8px; font-size: 14px; line-height: 1.3; }
.checks-empty { margin: 0 0 10px; }
.checks-table-wrap { max-height: 560px; }
.status-table--grouped .status-row--group td { background: transparent; }
.status-table--grouped .status-row--success td { background: #eef8ee; }
.status-table--grouped .status-row--clickable:hover td { background: rgba(39, 100, 163, 0.04); }
.status-table--diagnostic td { vertical-align: top; }
.status-table--validator { margin-top: 12px; }
.status-table--validator .status-row td { background: #fff; }
.collapsible--validators { margin-top: 18px; }
.collapsible--validator { margin-top: 12px; border-color: #d9e0e7; box-shadow: none; }
.validator-version { padding: 12px 0 2px; border-top: 1px solid #e8edf2; }
.validator-version:first-child { border-top: 0; padding-top: 0; }
.validator-version__head { display: flex; justify-content: space-between; gap: 12px; align-items: baseline; }
.validator-version__head strong { font-size: 15px; }
.validator-version__head span { color: var(--muted); white-space: nowrap; }
.validator-version__meta { margin-top: 4px; color: var(--muted); font-size: 12px; }
.chart-card { border: 1px solid var(--line); border-radius: 12px; padding: 12px; background: var(--soft); }
.line-chart { width: 100%; height: auto; display: block; }
.line-chart text { fill: var(--muted); font-size: 12px; }
.chart-legend { display: flex; gap: 14px; flex-wrap: wrap; margin-top: 10px; color: var(--muted); font-size: 12px; }
.chart-legend span { display: inline-flex; align-items: center; gap: 6px; }
.chart-legend i { width: 10px; height: 10px; border-radius: 999px; display: inline-block; }
.collapsible { margin-top: 14px; background: var(--panel); border: 1px solid var(--line); border-radius: 14px; padding: 0; box-shadow: 0 8px 24px rgba(31, 41, 51, 0.04); overflow: hidden; }
.collapsible summary { list-style: none; cursor: pointer; display: flex; align-items: center; justify-content: space-between; gap: 16px; padding: 18px; }
.collapsible summary::-webkit-details-marker { display: none; }
.collapsible summary strong { font-size: 18px; }
.collapsible summary em { display: block; margin-top: 4px; color: var(--muted); font-style: normal; }
.collapsible summary > span { min-width: 0; }
.collapsible-body { padding: 0 18px 18px; }
.modal-backdrop { position: fixed; inset: 0; background: rgba(15, 23, 42, 0.45); display: grid; place-items: center; padding: 18px; z-index: 50; }
.modal-backdrop[hidden] { display: none !important; }
.modal { width: min(860px, 100%); max-height: min(80vh, 760px); overflow: auto; background: var(--panel); border-radius: 16px; border: 1px solid var(--line); box-shadow: 0 30px 80px rgba(15, 23, 42, 0.25); }
.modal-head { display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; padding: 16px 18px; border-bottom: 1px solid var(--line); }
.modal-head h2 { font-size: 18px; }
.modal-head p { margin-top: 4px; }
.modal-close { width: 34px; height: 34px; border: 0; background: var(--soft); color: var(--text); border-radius: 999px; cursor: pointer; font-size: 22px; line-height: 1; }
.modal-body { padding: 16px 18px 18px; display: grid; gap: 12px; }
.modal-item { border: 1px solid var(--line); border-radius: 12px; padding: 12px; background: var(--soft); }
.modal-item--clickable { cursor: pointer; transition: background-color .16s ease, border-color .16s ease, box-shadow .16s ease, transform .16s ease; outline: none; }
.modal-item--clickable:hover { background: #f4f8fc; border-color: #cfd8e3; box-shadow: 0 8px 18px rgba(31, 41, 51, 0.06); transform: translateY(-1px); }
.modal-item--clickable:active { background: #eaf2fb; border-color: #b9cbe0; box-shadow: 0 2px 8px rgba(31, 41, 51, 0.04); transform: translateY(0); }
.modal-item--clickable:focus-visible { outline: 2px solid #9db9d6; outline-offset: 2px; }
.modal-item-head { display: flex; justify-content: space-between; gap: 12px; align-items: baseline; }
.modal-item-head span { color: var(--muted); font-size: 12px; white-space: nowrap; }
.modal-item-value { color: var(--primary); font: inherit; font-weight: 700; }
.modal-item-tip { margin-top: 6px; color: var(--muted); font-size: 12px; line-height: 1.35; }
.modal-back { appearance: none; border: 1px solid #cfd8e3; background: #fff; color: var(--primary); border-radius: 6px; padding: 7px 10px; font-weight: 700; cursor: pointer; margin-bottom: 12px; }
.modal-back:hover { background: #f6f8fb; }
.modal-files, .modal-lines { margin: 10px 0 0; padding-left: 18px; color: var(--text); max-height: 44vh; overflow: auto; }
.modal-files li, .modal-lines li { margin-top: 4px; word-break: break-word; }
.modal-line-head { display: flex; justify-content: space-between; gap: 12px; align-items: baseline; margin-bottom: 4px; }
.modal-line-head span { font-weight: 600; }
.modal-line-head em { color: var(--muted); font-style: normal; font-size: 12px; white-space: nowrap; }
.modal-lines code { display: block; white-space: pre-wrap; word-break: break-word; background: #fff; border: 1px solid #e8edf2; border-radius: 8px; padding: 8px 10px; }
.line-highlight { background: #fff2a8; color: inherit; padding: 0 2px; border-radius: 3px; }
.modal-open { overflow: hidden; }
.notes { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; }
.notes h2 { margin-bottom: 8px; font-size: 16px; }
table { width: 100%; border-collapse: collapse; background: var(--panel); }
tr:last-child td { border-bottom: 0; }
.table-wrap { overflow-x: auto; border: 1px solid var(--line); border-radius: 8px; }
@media (max-width: 760px) {
  main { padding: 16px; }
  .header, .section-title, .modal-line-head { align-items: flex-start; flex-direction: column; }
  .device-boot-chart-row { grid-template-columns: 1fr; gap: 5px; }
  .device-boot-chart-row strong { text-align: left; }
  .notes { grid-template-columns: 1fr; }
}
""".strip()

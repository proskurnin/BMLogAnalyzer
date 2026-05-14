from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from html import escape
from pathlib import Path

from analytics.archive_inventory import bm_log_count, explicit_reader_log_count, explicit_system_log_count
from analytics.bm_statuses import UNCLASSIFIED_STATUS, bm_status_summary_rows, classify_bm_status
from analytics.suspicious import suspicious_line_payloads
from core.contracts import REPORT_MANIFEST_SCHEMA_VERSION
from core.models import AnalysisResult, ArchiveInventoryRow, PaymentEvent, PipelineStats
from core.version import __version__

LOG_GROUP_SPECS: list[tuple[str, set[str]]] = [
    ("BM logs", {"BM rotate", "BM stdout"}),
    ("Stopper logs", {"Stopper rotate", "Stopper stdout"}),
    ("VIL logs", {"VIL logs"}),
    ("Reader logs", {"Reader logs"}),
    ("System logs", {"System logs"}),
    ("Other log-like", {"Other log-like"}),
]
LOG_CATEGORIES: set[str] = {category for _, categories in LOG_GROUP_SPECS for category in categories}


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
    archive_count = len(stats.input_files) if stats else 0
    bm_versions = _bm_versions(events)
    archive_names = _archive_name_set(stats.input_files if stats else [])
    bm_version_records = _bm_version_records(events, archive_names)
    bm_carrier_records = _bm_carrier_records(events, archive_names)
    bm_carriers = _bm_carriers(events)
    bm_reader_records = _bm_reader_records(events, archive_names)
    reader_types = _reader_types(events)
    bm_period = _bm_period(events)
    bm_date_records = _bm_date_records(events, archive_names)
    bm_group_rows = _bm_status_groups(events)
    bm_group_payloads = _bm_group_payloads(events, archive_names)
    validator_sections = _validator_analytics(events, archive_names)
    suspicious_rows = suspicious_line_payloads(events)
    date_chart = _bm_date_chart(events)
    unclassified_diag = _unclassified_diagnostics(events)
    report_data = _report_data(
        events,
        archive_names,
        log_groups=log_groups,
        other_groups=other_groups,
        bm_version_records=bm_version_records,
        bm_carrier_records=bm_carrier_records,
        bm_reader_records=bm_reader_records,
        bm_date_records=bm_date_records,
        validator_sections=validator_sections,
    )

    bm_meta_cards = _bm_meta_cards(
        versions=bm_versions,
        version_records=bm_version_records,
        carriers=bm_carriers,
        carrier_records=bm_carrier_records,
        readers=reader_types,
        reader_records=bm_reader_records,
        period=bm_period,
        date_records=bm_date_records,
    )
    summary_cards = _summary_cards(
        archive_count=archive_count,
        log_total=log_total,
        other_total=other_total,
        bm_version_count=_bm_version_count(events),
        bm_version_records=bm_version_records,
        bm_log_count=bm_log_count(archive_inventory),
        reader_log_count=explicit_reader_log_count(archive_inventory),
        system_log_count=explicit_system_log_count(archive_inventory),
        archive_records=_archive_records(stats.input_files if stats else []),
        log_groups=log_groups,
        other_groups=other_groups,
        bm_groups=_build_metric_groups(archive_inventory, {"BM rotate", "BM stdout"}),
        reader_groups=_build_metric_groups(archive_inventory, {"Reader logs"}),
        system_groups=_build_metric_groups(archive_inventory, {"System logs"}),
    )

    return "\n".join(
        [
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
            f'<span class="version">version {escape(__version__)}</span>',
            "</div>",
            "</header>",
            '<section class="section">',
            f'<div class="summary-grid">{summary_cards}</div>',
            "</section>",
            '<section class="section section--bm-meta">',
            '<div class="section-title">',
            "<h2>BM сведения</h2>",
            "</div>",
            _bm_meta_grid(bm_meta_cards),
            '<div id="active-filters" class="active-filters"></div>',
            '<div id="bm-filter-root" class="bm-filter-root"></div>',
            "</section>",
            '<section class="section section--chart">',
            '<div class="section-title">',
            "<h2>Log-файлы</h2>",
            "<p>Кликабельная сводка по типам логов в архивах.</p>",
            "</div>",
            f'<div id="log-files-root">{_bar_chart(log_groups, log_total)}</div>',
            "</section>",
            _collapsible_other_section(other_groups, other_total),
            f'<script id="report-data" type="application/json">{_json_script(report_data)}</script>',
            _suspicious_section(suspicious_rows),
            _bm_status_section(events, bm_group_rows, bm_group_payloads, date_chart, unclassified_diag, archive_names),
            _validator_section(validator_sections),
            _modal(),
            _script(),
            "</main>",
            "</body>",
            "</html>",
        ]
    )


def render_html_report_manifest(
    events: list[PaymentEvent],
    result: AnalysisResult,
    *,
    stats: PipelineStats | None = None,
) -> dict[str, object]:
    archive_inventory = stats.archive_inventory if stats else []
    log_groups, log_total = _build_log_groups(archive_inventory)
    other_groups, other_total = _build_other_groups(archive_inventory)
    summary_rows = bm_status_summary_rows(events)
    grouped_rows = _bm_status_groups(events)
    archive_names = _archive_name_set(stats.input_files if stats else [])
    validator_sections = _validator_analytics(events, archive_names)
    suspicious_rows = suspicious_line_payloads(events)
    stable_sections = [
        "summary",
        "bm_meta",
        "log_files",
        "other_files",
        "suspicious",
        "bm_statuses",
        "grouped_statuses",
        "date_dynamics",
        "unclassified_diagnostics",
        "validator_analytics",
    ]
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
            "validator_sections",
            "suspicious_lines",
        ],
        "stable_sections": stable_sections,
        "counts": {
            "archives": len(stats.input_files) if stats else 0,
            "log_files": log_total,
            "other_files": other_total,
            "bm_logs": bm_log_count(archive_inventory),
            "reader_logs": explicit_reader_log_count(archive_inventory),
            "system_logs": explicit_system_log_count(archive_inventory),
            "events": result.total,
            "success": result.success_count,
            "decline": result.decline_count,
            "technical_error": result.technical_error_count,
            "unknown": result.unknown_count,
            "suspicious": len(suspicious_rows),
        },
        "sections": stable_sections,
        "status_groups": [str(row["status"]) for row in summary_rows],
        "grouped_statuses": [str(row["label"]) for row in grouped_rows],
        "log_groups": [str(group["label"]) for group in log_groups],
        "other_groups": [str(group["label"]) for group in other_groups],
        "validator_sections": [str(item["validator"]) for item in validator_sections],
        "suspicious_lines": suspicious_rows,
    }


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


def _bm_meta_cards(
    *,
    versions: str,
    version_records: list[dict[str, object]],
    carriers: str,
    carrier_records: list[dict[str, object]],
    readers: str,
    reader_records: list[dict[str, object]],
    period: str,
    date_records: list[dict[str, object]],
) -> str:
    items = [
        ("Версии БМ", versions or "missing", version_records, "versions"),
        ("Перевозчики", carriers or "missing", carrier_records, "carriers"),
        ("Ридеры", readers or "missing", reader_records, "readers"),
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


def _build_other_groups(archive_inventory: list[ArchiveInventoryRow]) -> tuple[list[dict[str, object]], int]:
    groups: dict[str, dict[str, object]] = {}
    total = 0
    for row in archive_inventory:
        if row.category in LOG_CATEGORIES:
            continue
        file_entries = row.file_sizes.items() if row.file_sizes else ((file_name, 0) for file_name in row.files)
        for file_name, size_bytes in file_entries:
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


def _collapsible_other_section(other_groups: list[dict[str, object]], other_total: int) -> str:
    return "\n".join(
        [
            '<details class="collapsible">',
            "<summary>",
            "<span>",
            "<strong>Прочие файлы</strong>",
            "<em>Файлы, которые не относятся к логам.</em>",
            "</span>",
            f"<strong>{other_total}</strong>",
            "</summary>",
            '<div class="collapsible-body" id="other-files-root">',
            _bar_chart(other_groups, other_total),
            "</div>",
            "</details>",
        ]
    )


def _suspicious_section(rows: list[dict[str, object]]) -> str:
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
    empty = '<tr><td colspan="4" class="muted">Подозрительных строк не найдено.</td></tr>'
    table = (
        '<div class="table-wrap suspicious-table-wrap">'
        '<table class="status-table status-table--suspicious">'
        '<thead class="status-table-head"><tr><th>Источник</th><th>Код</th><th>Почему подозрительно</th><th>Строка лога</th></tr></thead>'
        f"<tbody>{''.join(rendered_rows) if rendered_rows else empty}</tbody>"
        "</table>"
        "</div>"
    )
    return "\n".join(
        [
            '<details class="collapsible collapsible--suspicious">',
            "<summary>",
            "<span>",
            "<strong>Подозрительно</strong>",
            f"<em>Найдено строк: {len(rows)}. Baseline строится по успешным PaymentStart resp с Code:0.</em>",
            "</span>",
            "</summary>",
            '<div class="collapsible-body">',
            table,
            "</div>",
            "</details>",
        ]
    )


def _archive_records(input_files: list[str]) -> list[dict[str, object]]:
    records = []
    for name in sorted({str(file_name) for file_name in input_files if file_name}):
        records.append({"archive": Path(name).name, "count": 1, "size_bytes": 0, "files": [name]})
    return records


def _archive_name_set(input_files: list[str]) -> set[str]:
    return {Path(name).name for name in input_files if name}


def _report_data(
    events: list[PaymentEvent],
    archive_names: set[str],
    *,
    log_groups: list[dict[str, object]],
    other_groups: list[dict[str, object]],
    bm_version_records: list[dict[str, object]],
    bm_carrier_records: list[dict[str, object]],
    bm_reader_records: list[dict[str, object]],
    bm_date_records: list[dict[str, object]],
    validator_sections: list[dict[str, object]],
) -> dict[str, object]:
    event_rows = []
    for event in events:
        status = classify_bm_status(event)
        event_rows.append(
            {
                "archive": _archive_from_source(event.source_file, archive_names),
                "source_file": event.source_file,
                "line_number": event.line_number,
                "timestamp": event.timestamp.isoformat(sep=" ") if event.timestamp else "",
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
            "dates": bm_date_records,
        },
        "validators": validator_sections,
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
            f'<tr class="{" ".join(classes)}"{data_attrs}>'
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
        '<div class="table-wrap bm-table-wrap">'
        '<table class="status-table">'
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
            "<p>Строки таблицы можно открыть по клику.</p>",
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
    for event in events:
        archive_name = _archive_from_source(event.source_file, archive_names)
        item = archives_by_name[archive_name]
        item["archive"] = archive_name
        item["count"] = int(item["count"]) + 1
    record: dict[str, object] = {
        kind: value,
        "count": len(events),
        "archives": [archives_by_name[archive] for archive in sorted(archives_by_name)],
    }
    if markers:
        record["markers"] = sorted({marker for event in events for marker in _carrier_markers(event)})
    return record


def _bm_carrier_records(events: list[PaymentEvent], archive_names: set[str]) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    nbs_events = [event for event in events if _is_nbs_event(event)]
    askp_events = [event for event in events if _is_askp_event(event)]
    if nbs_events:
        records.append(_bm_selectable_record("carrier", "НБС", nbs_events, archive_names, markers=True))
    if askp_events:
        records.append(_bm_selectable_record("carrier", "АСКП", askp_events, archive_names, markers=True))
    return records


def _bm_reader_records(events: list[PaymentEvent], archive_names: set[str]) -> list[dict[str, object]]:
    counts: dict[str, list[PaymentEvent]] = defaultdict(list)
    for event in events:
        if event.reader_type:
            counts[event.reader_type].append(event)
    return [
        _bm_selectable_record("reader", {"OTI": "ОТИ", "TT": "ТТ"}.get(reader, reader), reader_events, archive_names)
        for reader, reader_events in sorted(counts.items())
    ]


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


def _validator_section(groups: list[dict[str, object]]) -> str:
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
            "<em>Версии BM, даты и два ключевых отказа по каждому архиву</em>",
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
    counts: dict[str, int] = defaultdict(int)
    archives_by_version: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for event in events:
        if event.bm_version:
            counts[event.bm_version] += 1
            archive_name = _archive_from_source(event.source_file, archive_names)
            archives_by_version[event.bm_version][archive_name] += 1
    records: list[dict[str, object]] = []
    for version in sorted(counts):
        archives = [
            {"archive": archive, "count": archives_by_version[version][archive]}
            for archive in sorted(archives_by_version[version])
        ]
        records.append({"version": version, "count": counts[version], "archives": archives})
    return records


def _bm_version_count(events: list[PaymentEvent]) -> int:
    return len({event.bm_version for event in events if event.bm_version})


def _bm_carriers(events: list[PaymentEvent]) -> str:
    carriers: list[str] = []
    if any(
        (event.package and "mgt_nbs" in event.package.lower())
        or (event.raw_line and "mgt_nbs" in event.raw_line.lower())
        for event in events
    ):
        carriers.append("НБС")
    if any(
        (event.package and "mgt_askp" in event.package.lower())
        or (event.raw_line and "askp" in event.raw_line.lower())
        for event in events
    ):
        carriers.append("АСКП")
    return ", ".join(dict.fromkeys(carriers)) if carriers else "missing"


def _reader_types(events: list[PaymentEvent]) -> str:
    types = [event.reader_type for event in events if event.reader_type]
    mapping = {"OTI": "ОТИ", "TT": "ТТ"}
    return ", ".join(dict.fromkeys(mapping.get(reader_type, reader_type) for reader_type in types)) if types else "missing"


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


def _is_nbs_event(event: PaymentEvent) -> bool:
    return bool(
        (event.package and "mgt_nbs" in event.package.lower())
        or (event.raw_line and "mgt_nbs" in event.raw_line.lower())
    )


def _is_askp_event(event: PaymentEvent) -> bool:
    return bool(
        (event.package and "mgt_askp" in event.package.lower())
        or (event.raw_line and "askp" in event.raw_line.lower())
    )


def _carrier_markers(event: PaymentEvent) -> set[str]:
    markers: set[str] = set()
    if event.package:
        lowered = event.package.lower()
        if "mgt_nbs" in lowered:
            markers.add("mgt_nbs")
        if "mgt_askp" in lowered:
            markers.add("mgt_askp")
    if event.raw_line:
        lowered = event.raw_line.lower()
        if "mgt_nbs" in lowered:
            markers.add("mgt_nbs")
        if "mgt_askp" in lowered:
            markers.add("mgt_askp")
    return markers


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
  const bmGroupedTableRoot = document.getElementById('bm-grouped-table-root');
  const bmDateChartRoot = document.getElementById('bm-date-chart-root');
  const bmUnclassifiedRoot = document.getElementById('bm-unclassified-root');
  const activeFiltersRoot = document.getElementById('active-filters');
  const bmFilterRoot = document.getElementById('bm-filter-root');
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
            <strong>${escapeHtml(String(item.version || item.carrier || item.reader || item.date || ''))}</strong>
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
      dates: 'Даты из логов и количество событий на каждой дате.'
    };
    const rendered = items.map((item) => {
      const titleValue = item.version || item.carrier || item.reader || item.date || 'Данные';
      const count = Number(item.count || 0);
      const archiveRows = (item.archives || []).map((archive) => {
        return `<li>${escapeHtml(String(archive.archive || ''))} · ${escapeHtml(String(archive.count || 0))}</li>`;
      }).join('');
      const markerRows = item.markers && item.markers.length
        ? `<ul class="modal-files">${item.markers.map((value) => `<li>${escapeHtml(String(value))}</li>`).join('')}</ul>`
        : '';
      return `
        <div class="modal-item">
          <div class="modal-item-head">
            <strong>${escapeHtml(String(titleValue))}</strong>
            <span>${escapeHtml(String(count))} событий</span>
          </div>
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
    const allowed = allowedArchivesForGroup(group.name);
    const selected = filterState[group.name];
    const visibleItems = items.filter((item) => {
      const key = String(item[groupKey(group.name)] || '');
      const itemArchives = archivesForItem(item);
      return selected.has(key) || allowed === null || intersects(itemArchives, allowed);
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
    const allowed = allowedArchivesForGroup(groupName);
    const itemArchives = archivesForItem(item);
    const visible = selected || allowed === null || intersects(itemArchives, allowed);
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

  function selectedArchives() {
    const activeGroups = Object.entries(filterState).filter(([, bucket]) => bucket.size > 0);
    if (!activeGroups.length) {
      return null;
    }
    let current = null;
    activeGroups.forEach(([group, bucket]) => {
      const union = new Set();
      bucket.forEach((key) => {
        const record = metaIndex[group].get(key);
        if (!record) {
          return;
        }
        (record.archives || []).forEach((archive) => {
          if (archive && archive.archive) {
            union.add(String(archive.archive));
          }
        });
      });
      if (current === null) {
        current = union;
      } else {
        current = intersect(current, union);
      }
    });
    return current || new Set();
  }

  function allowedArchivesForGroup(groupName) {
    const activeGroups = Object.entries(filterState).filter(([name, bucket]) => name !== groupName && bucket.size > 0);
    if (!activeGroups.length) {
      return null;
    }
    let current = null;
    activeGroups.forEach(([group, bucket]) => {
      const union = new Set();
      bucket.forEach((key) => {
        const record = metaIndex[group].get(key);
        if (!record) {
          return;
        }
        (record.archives || []).forEach((archive) => {
          if (archive && archive.archive) {
            union.add(String(archive.archive));
          }
        });
      });
      if (current === null) {
        current = union;
      } else {
        current = intersect(current, union);
      }
    });
    return current || new Set();
  }

  function archivesForItem(item) {
    return new Set((item.archives || []).map((archive) => String(archive.archive || '')).filter(Boolean));
  }

  function intersects(left, right) {
    if (!left.size || !right.size) {
      return false;
    }
    for (const value of left) {
      if (right.has(value)) {
        return true;
      }
    }
    return false;
  }

  function intersect(left, right) {
    const result = new Set();
    left.forEach((value) => {
      if (right.has(value)) {
        result.add(value);
      }
    });
    return result;
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
    const allowed = selectedArchives();
    const events = (REPORT.events || []).filter((event) => allowed === null || allowed.has(String(event.archive || '')));
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
  }

  function renderStatusTable(events) {
    const counts = countByStatus(events);
    const total = events.length;
    const rows = statusOrder.map((status) => renderStatusRow(status, counts.get(status) || 0, total, events)).join('');
    const unclassified = counts.get('Не классифицировано') || 0;
    return `
      <div class="table-wrap bm-table-wrap">
        <table class="status-table">
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
      <tr class="${classes.join(' ')}"${count > 0 ? ` data-kind="status" data-status="${escapeHtml(status)}" data-payload="${escapeHtml(payload)}" tabindex="0"` : ''}>
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
    const result = { success: 0, errors: 0, declines: 0, unclassified: 0 };
    events.forEach((event) => {
      const group = groupLabel(event.status);
      if (group === 'Успех') {
        result.success += 1;
      } else if (group === 'Ошибки') {
        result.errors += 1;
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
    const maxValue = Math.max(1, ...dates.map((date) => Math.max(byDate[date].success, byDate[date].errors, byDate[date].declines)));
    const xStep = plotWidth / Math.max(dates.length - 1, 1);
    const seriesSpecs = [
      ['Успех', '#137752', 'success'],
      ['Ошибки', '#d14343', 'errors'],
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
        grouped[date] = { success: 0, errors: 0, declines: 0 };
      }
      const group = groupLabel(event.status);
      if (group === 'Успех') {
        grouped[date].success += 1;
      } else if (group === 'Ошибки') {
        grouped[date].errors += 1;
      } else if (group === 'Отказы') {
        grouped[date].declines += 1;
      }
    });
    return grouped;
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
    modal.hidden = false;
    document.body.classList.add('modal-open');
    syncFilterButtonState();
  }

  function closeModal() {
    modal.hidden = true;
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
    const interactive = event.target.closest('[data-kind]');
    if (!interactive || interactive.closest('[data-filter-group]')) {
      return;
    }
    event.preventDefault();
    openModal(interactive);
  });
  document.addEventListener('click', (event) => {
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

  renderAll();
})();
</script>
""".strip()


def _css() -> str:
    return """
:root { color-scheme: light; --bg: #eef2f6; --panel: #ffffff; --line: #d9e0e7; --text: #1f2933; --muted: #667085; --soft: #f6f8fb; --blue: #2764a3; }
* { box-sizing: border-box; }
body { margin: 0; background: linear-gradient(180deg, #f6f8fb 0%, #eef2f6 100%); color: var(--text); font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
main { max-width: 1120px; margin: 0 auto; padding: 28px; }
h1, h2, p { margin: 0; }
.header { display: flex; align-items: flex-end; justify-content: space-between; gap: 18px; margin-bottom: 18px; padding: 16px 18px; border: 1px solid var(--line); border-radius: 14px; background: var(--panel); box-shadow: 0 8px 24px rgba(31, 41, 51, 0.04); }
.header p { color: var(--muted); font-size: 13px; }
.version { display: inline-block; margin-top: 6px; color: var(--muted); font-size: 13px; }
.header h1 { font-size: 30px; line-height: 1.1; }
.header-badge { min-width: 130px; background: var(--soft); border: 1px solid var(--line); border-radius: 12px; padding: 12px; }
.header-badge span, .metric span, .bm-meta-card span { display: block; color: var(--muted); font-size: 12px; }
.header-badge strong, .metric strong, .bm-meta-card strong { display: block; margin-top: 4px; font-size: 18px; line-height: 1.2; }
.section { margin-top: 14px; background: var(--panel); border: 1px solid var(--line); border-radius: 14px; padding: 18px; box-shadow: 0 8px 24px rgba(31, 41, 51, 0.04); }
.section-title { display: flex; align-items: baseline; justify-content: space-between; gap: 16px; margin-bottom: 12px; }
.section-title h2 { font-size: 18px; line-height: 1.2; }
.section-title--compact { margin: 14px 0 10px; }
.section-title--compact h3 { font-size: 15px; line-height: 1.2; color: var(--muted); }
.section-title p, .muted { color: var(--muted); }
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
.filter-group__clear:hover { background: #f2f5f8; border-color: #dbe3ea; }
.filter-option-grid { display: flex; flex-wrap: wrap; gap: 8px; }
.filter-option { appearance: none; border: 1px solid var(--line); background: var(--soft); color: inherit; font: inherit; border-radius: 999px; padding: 7px 10px; cursor: pointer; display: inline-flex; align-items: center; gap: 8px; }
.filter-option span { font-size: 13px; }
.filter-option strong { color: var(--muted); font-size: 12px; }
.filter-option:hover { background: #f2f5f8; border-color: #dbe3ea; }
.filter-option--active { background: #dbeafe; border-color: #7fb0de; }
.filter-option:focus-visible { outline: 2px solid #9db9d6; outline-offset: 2px; }
.active-filters { margin-top: 12px; }
.active-filters__row { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
.active-filters__empty { color: var(--muted); font-size: 13px; }
.filter-chip, .filter-clear, .modal-select { appearance: none; border: 1px solid var(--line); background: var(--soft); color: inherit; font: inherit; }
.filter-chip { display: inline-flex; align-items: center; gap: 6px; padding: 7px 10px; border-radius: 999px; cursor: pointer; }
.filter-chip--active { background: #e6f1ff; border-color: #a9c6e4; }
.filter-chip span { color: var(--muted); font-size: 12px; }
.filter-chip strong { font-size: 13px; font-weight: 600; }
.filter-chip i { font-style: normal; color: var(--muted); }
.filter-chip:hover, .filter-clear:hover { background: #f2f5f8; border-color: #dbe3ea; }
.filter-chip:focus-visible, .filter-clear:focus-visible, .modal-select:focus-visible { outline: 2px solid #9db9d6; outline-offset: 2px; }
.filter-clear { padding: 7px 12px; border-radius: 999px; cursor: pointer; }
.modal-select { display: block; width: 100%; text-align: left; cursor: pointer; }
.modal-select:hover { background: #f2f5f8; border-color: #dbe3ea; }
.modal-select--active { background: #dbeafe; border-color: #7fb0de; box-shadow: 0 0 0 1px rgba(39, 100, 163, 0.10) inset; }
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
.modal-item-head { display: flex; justify-content: space-between; gap: 12px; align-items: baseline; }
.modal-item-head span { color: var(--muted); font-size: 12px; white-space: nowrap; }
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
  .notes { grid-template-columns: 1fr; }
}
""".strip()

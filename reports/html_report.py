from __future__ import annotations

from html import escape
from pathlib import Path

from analytics.archive_inventory import (
    archive_category_date_range,
    archive_category_totals,
    bm_log_count,
    explicit_reader_log_count,
    explicit_system_log_count,
    stopper_log_count,
)
from analytics.bm_statuses import bm_status_summary_rows
from analytics.check_cases import run_builtin_checks
from analytics.classifiers import CODE_DESCRIPTIONS, classify_code
from analytics.comparisons import file_error_overview_rows
from analytics.log_inventory import (
    error_status_counts_by_type,
    reader_firmware_counts,
    reader_model_counts,
)
from core.models import AnalysisResult, PaymentEvent, PipelineStats
from core.version import __version__, format_version


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


def render_html_report(
    events: list[PaymentEvent],
    result: AnalysisResult,
    *,
    stats: PipelineStats | None = None,
) -> str:
    error_events = [event for event in events if classify_code(event.code) != "success"]
    check_results = run_builtin_checks(events)
    source_files = stats.analyzed_files if stats else sorted({event.source_file for event in events})
    main_issues = _main_issues(result, check_results)

    return "\n".join(
        [
            "<!doctype html>",
            '<html lang="ru">',
            "<head>",
            '<meta charset="utf-8">',
            '<meta name="viewport" content="width=device-width, initial-scale=1">',
            f"<title>{escape(format_version())} Report</title>",
            f"<style>{_css()}</style>",
            "</head>",
            "<body>",
            "<main>",
            '<header class="report-header">',
            "<div>",
            "<p>Факт из логов</p>",
            "<h1>BM Log Analyzer</h1>",
            f'<span class="version">version {escape(__version__)}</span>',
            "</div>",
            _health_badge(result),
            "</header>",
            '<section class="section section--lead">',
            '<div class="section-title">',
            "<h2>Коротко</h2>",
            "<p>Главные числа и найденные проблемы без гипотез и догадок.</p>",
            "</div>",
            _summary_cards(result, stats),
            _main_issues_panel(main_issues),
            "</section>",
            _archive_inventory_section(stats, result),
            '<section class="section">',
            '<div class="section-title">',
            "<h2>Статусы BM</h2>",
            "<p>Количество и процент по поддержанным статусам PaymentStart resp.</p>",
            "</div>",
            _bm_status_table(events),
            "</section>",
            '<section class="section">',
            '<div class="section-title">',
            "<h2>Где смотреть в первую очередь</h2>",
            "<p>Файлы с наибольшим количеством non-success событий.</p>",
            "</div>",
            _file_error_table(events, limit=20),
            "</section>",
            '<section class="section two-column">',
            "<div>",
            '<div class="section-title">',
            "<h2>Коды результатов</h2>",
            "<p>Все коды сохранены, неизвестные не отбрасываются.</p>",
            "</div>",
            _by_code_table(result),
            "</div>",
            "<div>",
            '<div class="section-title">',
            "<h2>Длительности</h2>",
            "<p>P90/P95 и распределение по бакетам.</p>",
            "</div>",
            _duration_panel(result),
            "</div>",
            "</section>",
            _details_section(
                "Сработавшие проверки",
                "Evidence-backed результаты встроенных проверок. Полный список есть в check_results.csv.",
                _checks_table(check_results),
                open_by_default=bool(check_results),
            ),
            _details_section(
                "Ошибочные события",
                "Первые non-success события с исходной строкой лога. Полный список есть в error_events.csv.",
                _events_table(error_events[:100]) + _limit_note(len(error_events), 100),
            ),
            _details_section(
                "Какие логи анализировались",
                "Список фактических source files, использованных в анализе.",
                _source_files_table(source_files),
            ),
            "<section class=\"section notes\">",
            "<div>",
            "<h2>Гипотеза</h2>",
            "<p>Не формируется автоматически. Этот HTML содержит только факты из распарсенных строк логов.</p>",
            "</div>",
            "<div>",
            "<h2>Что проверить</h2>",
            "<p>Открыть строки из таблиц выше в исходных логах по имени файла, номеру строки и raw evidence.</p>",
            "</div>",
            "</section>",
            "</main>",
            "</body>",
            "</html>",
        ]
    )


def _summary_cards(result: AnalysisResult, stats: PipelineStats | None) -> str:
    cards = [
        ("Всего PaymentStart resp", result.total),
        ("Success", f"{result.success_count} ({result.success_percent:.2f}%)"),
        ("Decline", f"{result.decline_count} ({result.decline_percent:.2f}%)"),
        ("Technical error", f"{result.technical_error_count} ({result.technical_error_percent:.2f}%)"),
        ("Unknown", f"{result.unknown_count} ({result.unknown_percent:.2f}%)"),
        ("P90 duration", _ms(result.p90_ms)),
        ("P95 duration", _ms(result.p95_ms)),
    ]
    if stats:
        cards.extend(
            [
                ("Строк просканировано", stats.scanned_lines),
                ("Файлов анализа", len(stats.analyzed_files)),
                ("Архивов пропущено", stats.skipped_archives),
            ]
        )
    items = "".join(
        f"<article><span>{escape(label)}</span><strong>{escape(str(value))}</strong></article>"
        for label, value in cards
    )
    return f'<div class="summary-grid">{items}</div>'


def _archive_inventory_section(stats: PipelineStats | None, result: AnalysisResult) -> str:
    inventory = stats.log_inventory if stats else []
    archive_inventory = stats.archive_inventory if stats else []
    cards = [
        ("Логов BM", bm_log_count(archive_inventory)),
        ("Логов Stopper", stopper_log_count(archive_inventory)),
        ("Логов ридеров", explicit_reader_log_count(archive_inventory)),
        ("Логов системы", explicit_system_log_count(archive_inventory)),
        ("Даты BM", archive_category_date_range(archive_inventory, {"BM rotate", "BM stdout"})),
    ]
    card_html = "".join(
        f"<article><span>{escape(label)}</span><strong>{escape(str(value))}</strong></article>"
        for label, value in cards
    )
    return "\n".join(
        [
            '<section class="section">',
            '<div class="section-title">',
            "<h2>Состав архива</h2>",
            "<p>Классификация файлов по структуре архивов, без смешивания BM и Stopper.</p>",
            "</div>",
            f'<div class="summary-grid">{card_html}</div>',
            "<h3>Категории файлов</h3>",
            _simple_count_table("category", archive_category_totals(archive_inventory)),
            "<h3>По архивам</h3>",
            _archive_inventory_table(archive_inventory),
            '<div class="two-column inventory-tables">',
            "<div>",
            "<h3>Версии BM</h3>",
            _simple_count_table("bm_version", result.by_bm_version),
            "</div>",
            "<div>",
            "<h3>Модели ридеров</h3>",
            _simple_count_table("model", reader_model_counts(inventory)),
            "</div>",
            "<div>",
            "<h3>Прошивки ридеров</h3>",
            _reader_firmware_table(inventory),
            "</div>",
            "<div>",
            "<h3>Ошибки reader/system</h3>",
            _typed_error_table(inventory),
            "</div>",
            "</div>",
            "</section>",
        ]
    )


def _source_files_table(source_files: list[str]) -> str:
    rows = "".join(f"<tr><td>{escape(path)}</td></tr>" for path in source_files)
    return _table(["source_file"], rows or '<tr><td class="muted">missing</td></tr>')


def _by_code_table(result: AnalysisResult) -> str:
    rows = []
    for code, count in result.by_code.items():
        int_code = _to_int(code)
        classification = classify_code(int_code)
        description = CODE_DESCRIPTIONS.get(int_code, "unknown")
        rows.append(
            "<tr>"
            f"<td>{escape(str(code))}</td>"
            f"<td>{escape(description)}</td>"
            f"<td>{escape(classification)}</td>"
            f"<td>{count}</td>"
            "</tr>"
        )
    return _table(["code", "description", "classification", "count"], "".join(rows))


def _bm_status_table(events: list[PaymentEvent]) -> str:
    rows = "".join(
        "<tr>"
        f"<td>{escape(str(item['status']))}</td>"
        f"<td>{item['count']}</td>"
        f"<td>{item['percent']:.2f}%</td>"
        "</tr>"
        for item in bm_status_summary_rows(events)
    )
    return _table(["Статус", "Количество", "Процент"], rows)


def _simple_count_table(label: str, values: dict[str, int]) -> str:
    rows = "".join(f"<tr><td>{escape(key)}</td><td>{count}</td></tr>" for key, count in values.items())
    return _table([label, "count"], rows)


def _archive_inventory_table(rows) -> str:
    html_rows = "".join(
        "<tr>"
        f"<td>{escape(Path(row.archive).name)}</td>"
        f"<td>{escape(row.category)}</td>"
        f"<td>{row.count}</td>"
        f"<td>{escape(row.date_from or '')}</td>"
        f"<td>{escape(row.date_to or '')}</td>"
        f"<td>{escape(' | '.join(row.examples))}</td>"
        "</tr>"
        for row in rows
    )
    return _table(["archive", "category", "count", "date_from", "date_to", "examples"], html_rows)


def _reader_firmware_table(stats_or_inventory) -> str:
    inventory = stats_or_inventory.log_inventory if hasattr(stats_or_inventory, "log_inventory") else stats_or_inventory
    rows = "".join(
        f"<tr><td>{escape(model)}</td><td>{escape(firmware)}</td><td>{count}</td></tr>"
        for (model, firmware), count in reader_firmware_counts(inventory).items()
    )
    return _table(["model", "firmware_version", "count"], rows)


def _typed_error_table(inventory) -> str:
    rows = []
    for log_type in ("reader", "system"):
        counts = error_status_counts_by_type(inventory, log_type)
        total = sum(counts.values())
        for status, count in counts.items():
            percent = 0.0 if total == 0 else round((count / total) * 100, 2)
            rows.append(
                "<tr>"
                f"<td>{escape(log_type)}</td>"
                f"<td>{escape(status)}</td>"
                f"<td>{count}</td>"
                f"<td>{percent:.2f}%</td>"
                "</tr>"
            )
    return _table(["log_type", "status", "count", "percent"], "".join(rows))


def _file_error_table(events: list[PaymentEvent], *, limit: int | None = None) -> str:
    rows = []
    all_items = file_error_overview_rows(events)
    items = all_items
    if limit is not None:
        items = items[:limit]
    for item in items:
        rows.append(
            "<tr>"
            f"<td>{escape(str(item['source_file']))}</td>"
            f"<td>{item['total_events']}</td>"
            f"<td>{item['error_events']}</td>"
            f"<td>{item['decline']}</td>"
            f"<td>{item['technical_error']}</td>"
            f"<td>{item['unknown']}</td>"
            "</tr>"
        )
    return _table(["source_file", "total", "errors", "decline", "technical_error", "unknown"], "".join(rows)) + (
        _limit_note(len(all_items), limit) if limit is not None else ""
    )


def _checks_table(check_results) -> str:
    rows = []
    for item in check_results[:100]:
        rows.append(
            "<tr>"
            f"<td>{escape(item.check_id)}</td>"
            f"<td>{escape(item.title)}</td>"
            f"<td>{escape(item.severity)}</td>"
            f"<td>{escape(item.source_file)}</td>"
            f"<td>{item.line_number or ''}</td>"
            f"<td>{escape(str(item.code) if item.code is not None else '')}</td>"
            f"<td>{escape(item.message or '')}</td>"
            f"<td>{escape(item.evidence)}</td>"
            "</tr>"
        )
    table = _table(["check_id", "title", "severity", "source_file", "line", "code", "message", "evidence"], "".join(rows))
    return table + _limit_note(len(check_results), 100)


def _events_table(events: list[PaymentEvent]) -> str:
    rows = []
    for event in events:
        rows.append(
            "<tr>"
            f"<td>{escape(event.source_file)}</td>"
            f"<td>{event.line_number}</td>"
            f"<td>{escape(event.timestamp.isoformat(sep=' ') if event.timestamp else '')}</td>"
            f"<td>{escape(classify_code(event.code))}</td>"
            f"<td>{escape(str(event.code) if event.code is not None else '')}</td>"
            f"<td>{escape(event.message or '')}</td>"
            f"<td>{escape(event.bm_version or '')}</td>"
            f"<td>{escape(event.reader_type or '')}</td>"
            f"<td><code>{escape(event.raw_line)}</code></td>"
            "</tr>"
        )
    return _table(["source_file", "line", "timestamp", "classification", "code", "message", "bm", "reader", "raw_line"], "".join(rows))


def _table(headers: list[str], rows: str) -> str:
    header_html = "".join(f"<th>{escape(header)}</th>" for header in headers)
    body = rows or f'<tr><td colspan="{len(headers)}" class="muted">Нет данных</td></tr>'
    return f"<div class=\"table-wrap\"><table><thead><tr>{header_html}</tr></thead><tbody>{body}</tbody></table></div>"


def _health_badge(result: AnalysisResult) -> str:
    if result.total == 0:
        return '<div class="health health--neutral"><span>Нет событий</span><strong>0</strong></div>'
    if result.unknown_count:
        return f'<div class="health health--critical"><span>Есть unknown</span><strong>{result.unknown_count}</strong></div>'
    if result.technical_error_count:
        label = "Есть тех. ошибки"
        value = result.technical_error_count
        return f'<div class="health health--warning"><span>{escape(label)}</span><strong>{value}</strong></div>'
    if result.decline_count:
        return f'<div class="health health--info"><span>Есть decline</span><strong>{result.decline_count}</strong></div>'
    return f'<div class="health health--ok"><span>Success</span><strong>{result.success_percent:.2f}%</strong></div>'


def _main_issues(result: AnalysisResult, check_results) -> list[tuple[str, str, str]]:
    issues: list[tuple[str, str, str]] = []
    if result.technical_error_count:
        issues.append(
            ("warning", "Technical errors", f"{result.technical_error_count} событий ({result.technical_error_percent:.2f}%)")
        )
    if result.unknown_count:
        issues.append(("critical", "Unknown codes", f"{result.unknown_count} событий ({result.unknown_percent:.2f}%)"))
    if result.decline_count:
        issues.append(("info", "Declines", f"{result.decline_count} событий ({result.decline_percent:.2f}%)"))
    if check_results:
        issues.append(("info", "Сработавшие проверки", f"{len(check_results)} результатов"))
    return issues


def _main_issues_panel(issues: list[tuple[str, str, str]]) -> str:
    if not issues:
        return (
            '<div class="issue-panel issue-panel--empty">'
            "<strong>Критичных сигналов не найдено</strong>"
            "<span>В рамках распарсенных PaymentStart resp событий.</span>"
            "</div>"
        )
    items = "".join(
        f'<li class="issue issue--{escape(level)}"><strong>{escape(title)}</strong><span>{escape(text)}</span></li>'
        for level, title, text in issues
    )
    return f'<div class="issue-panel"><h3>Главные сигналы</h3><ul>{items}</ul></div>'


def _duration_panel(result: AnalysisResult) -> str:
    rows = "".join(f"<tr><td>{escape(bucket)}</td><td>{count}</td></tr>" for bucket, count in result.duration_buckets.items())
    return (
        '<div class="duration-kpis">'
        f'<article><span>P90</span><strong>{escape(_ms(result.p90_ms))}</strong></article>'
        f'<article><span>P95</span><strong>{escape(_ms(result.p95_ms))}</strong></article>'
        "</div>"
        + _table(["bucket", "count"], rows)
    )


def _details_section(title: str, description: str, content: str, *, open_by_default: bool = False) -> str:
    open_attr = " open" if open_by_default else ""
    return (
        f'<details class="section details-section"{open_attr}>'
        f"<summary><span>{escape(title)}</span><small>{escape(description)}</small></summary>"
        f"{content}"
        "</details>"
    )


def _limit_note(total: int, limit: int) -> str:
    if total <= limit:
        return ""
    return f'<p class="muted">Показаны первые {limit} из {total} строк. Полные данные доступны в CSV-отчётах.</p>'


def _ms(value: float | None) -> str:
    if value is None:
        return "missing"
    return f"{value:.2f} ms"


def _to_int(value: object) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _css() -> str:
    return """
:root { color-scheme: light; --bg: #f3f5f7; --panel: #fff; --line: #d9e0e7; --text: #1f2933; --muted: #667085; --soft: #eef2f6; --green: #147a4b; --amber: #a15c06; --red: #b42318; --blue: #2764a3; }
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--text); font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
main { max-width: 1180px; margin: 0 auto; padding: 28px; }
h1, h2, h3, p { margin: 0; }
.report-header { display: flex; align-items: flex-end; justify-content: space-between; gap: 18px; margin-bottom: 20px; }
.report-header p { color: var(--muted); font-size: 13px; }
.version { display: inline-block; margin-top: 6px; color: var(--muted); font-size: 13px; }
h1 { font-size: 30px; line-height: 1.1; }
.section { margin-top: 14px; background: var(--panel); border: 1px solid var(--line); border-radius: 8px; padding: 18px; }
.section--lead { display: grid; gap: 14px; }
.section-title { display: flex; align-items: baseline; justify-content: space-between; gap: 16px; margin-bottom: 12px; }
.section-title h2, summary span { font-size: 18px; line-height: 1.2; }
.section-title p, summary small, .muted { color: var(--muted); }
.two-column { display: grid; grid-template-columns: minmax(0, 1fr) minmax(320px, .7fr); gap: 18px; }
.summary-grid, .duration-kpis { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; }
.summary-grid article, .duration-kpis article { background: #f8fafc; border: 1px solid var(--line); border-radius: 8px; padding: 12px; }
.summary-grid span, .duration-kpis span, .health span { display: block; color: var(--muted); font-size: 12px; }
.summary-grid strong, .duration-kpis strong, .health strong { display: block; margin-top: 4px; font-size: 18px; line-height: 1.2; }
.health { min-width: 150px; background: var(--panel); border: 1px solid var(--line); border-left: 5px solid var(--blue); border-radius: 8px; padding: 12px; }
.health--ok { border-left-color: var(--green); }
.health--warning { border-left-color: var(--amber); }
.health--critical { border-left-color: var(--red); }
.health--neutral { border-left-color: var(--muted); }
.issue-panel { border: 1px solid var(--line); border-radius: 8px; padding: 14px; background: #fbfcfe; }
.issue-panel h3 { margin-bottom: 10px; font-size: 15px; }
.issue-panel ul { display: grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap: 10px; padding: 0; margin: 0; list-style: none; }
.issue { border-left: 4px solid var(--blue); background: var(--panel); border-radius: 6px; padding: 10px 12px; }
.issue strong, .issue span { display: block; }
.issue span { color: var(--muted); margin-top: 2px; }
.issue--warning { border-left-color: var(--amber); }
.issue--critical { border-left-color: var(--red); }
.issue-panel--empty strong, .issue-panel--empty span { display: block; }
.table-wrap { overflow-x: auto; border: 1px solid var(--line); border-radius: 8px; }
table { width: 100%; border-collapse: collapse; background: var(--panel); }
th, td { padding: 8px 10px; border-bottom: 1px solid #e8edf2; text-align: left; vertical-align: top; }
th { background: var(--soft); font-size: 12px; color: #475467; position: sticky; top: 0; }
tr:last-child td { border-bottom: 0; }
code { white-space: pre-wrap; word-break: break-word; }
.details-section { padding: 0; }
summary { cursor: pointer; display: grid; gap: 3px; padding: 16px 18px; list-style: none; }
summary::-webkit-details-marker { display: none; }
.details-section[open] summary { border-bottom: 1px solid var(--line); }
.details-section > .table-wrap, .details-section > .muted { margin: 14px 18px 18px; }
.notes { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; }
.notes h2 { margin-bottom: 8px; font-size: 16px; }
@media (max-width: 760px) {
  main { padding: 16px; }
  .report-header, .section-title { align-items: flex-start; flex-direction: column; }
  .two-column, .notes { grid-template-columns: 1fr; }
}
""".strip()

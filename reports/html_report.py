from __future__ import annotations

from html import escape
from pathlib import Path

from analytics.check_cases import run_builtin_checks
from analytics.classifiers import CODE_DESCRIPTIONS, classify_code
from analytics.comparisons import file_error_overview_rows
from core.models import AnalysisResult, PaymentEvent, PipelineStats


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

    return "\n".join(
        [
            "<!doctype html>",
            '<html lang="ru">',
            "<head>",
            '<meta charset="utf-8">',
            '<meta name="viewport" content="width=device-width, initial-scale=1">',
            "<title>BM Log Analyzer Report</title>",
            f"<style>{_css()}</style>",
            "</head>",
            "<body>",
            "<main>",
            "<h1>BM Log Analyzer Report</h1>",
            "<section>",
            "<h2>Факт из логов</h2>",
            _summary_cards(result, stats),
            "</section>",
            "<section>",
            "<h2>Какие логи анализировались</h2>",
            _source_files_table(source_files),
            "</section>",
            "<section>",
            "<h2>Коды результатов</h2>",
            _by_code_table(result),
            "</section>",
            "<section>",
            "<h2>Логи с ошибками</h2>",
            _file_error_table(events),
            "</section>",
            "<section>",
            "<h2>Сработавшие проверки</h2>",
            _checks_table(check_results),
            "</section>",
            "<section>",
            "<h2>Ошибочные события</h2>",
            _events_table(error_events[:300]),
            _limit_note(len(error_events), 300),
            "</section>",
            "<section>",
            "<h2>Гипотеза</h2>",
            "<p>Не формируется автоматически. Этот отчёт содержит только факты из распарсенных строк логов.</p>",
            "</section>",
            "<section>",
            "<h2>Что проверить</h2>",
            "<p>Проверить строки из таблиц выше в исходных логах по имени файла и номеру строки.</p>",
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
                ("Scanned lines", stats.scanned_lines),
                ("Analyzed files", len(stats.analyzed_files)),
                ("Skipped archives", stats.skipped_archives),
            ]
        )
    items = "".join(f"<div><span>{escape(label)}</span><strong>{escape(str(value))}</strong></div>" for label, value in cards)
    return f'<div class="summary-grid">{items}</div>'


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


def _file_error_table(events: list[PaymentEvent]) -> str:
    rows = []
    for item in file_error_overview_rows(events):
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
    return _table(["source_file", "total", "errors", "decline", "technical_error", "unknown"], "".join(rows))


def _checks_table(check_results) -> str:
    rows = []
    for item in check_results[:300]:
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
    return table + _limit_note(len(check_results), 300)


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
body { margin: 0; background: #f6f7f9; color: #1f2933; font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
main { max-width: 1280px; margin: 0 auto; padding: 28px; }
h1 { margin: 0 0 22px; font-size: 28px; }
h2 { margin: 28px 0 12px; font-size: 18px; }
p { margin: 0 0 12px; }
.summary-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 10px; }
.summary-grid div { background: #fff; border: 1px solid #d8dee6; border-radius: 6px; padding: 12px; }
.summary-grid span { display: block; color: #667085; font-size: 12px; }
.summary-grid strong { display: block; margin-top: 4px; font-size: 18px; }
.table-wrap { overflow-x: auto; background: #fff; border: 1px solid #d8dee6; border-radius: 6px; }
table { width: 100%; border-collapse: collapse; }
th, td { padding: 8px 10px; border-bottom: 1px solid #e6e9ee; text-align: left; vertical-align: top; }
th { background: #eef2f6; font-size: 12px; color: #475467; position: sticky; top: 0; }
code { white-space: pre-wrap; word-break: break-word; }
.muted { color: #667085; }
""".strip()

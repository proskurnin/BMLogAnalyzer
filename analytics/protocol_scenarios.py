from __future__ import annotations

from dataclasses import asdict
import json
import os
from pathlib import Path
from uuid import uuid4

from core.models import PaymentEvent, ProtocolScenario, ProtocolScenarioResult, ProtocolScenarioStep


DEFAULT_PROTOCOL_SCENARIOS = [
    ProtocolScenario(
        scenario_id="payment_start_followup_chain",
        title="Цепочка PaymentStart",
        description="Фиксирует ожидаемую цепочку после PaymentStart по документу взаимодействия КЗП и БМ.",
        source_document="API_КЗП_БМ_TLV_2_53_13-05-2026.docx",
        source_section="PaymentStart",
        source_sections=["PaymentStart"],
        source_quote="PaymentStart. Таймаут по умолчанию 3 секунды.",
        source_quotes=["PaymentStart. Таймаут по умолчанию 3 секунды."],
        steps=[
            ProtocolScenarioStep(
                kind="match",
                label="Найти PaymentStart resp",
                event_type="PaymentStart resp",
                source_section="PaymentStart",
            ),
            ProtocolScenarioStep(
                kind="branch",
                label="Если code=0, ожидается PaymentConfirm",
                code_eq=0,
                next_event_type="PaymentConfirm",
                within_seconds=20,
                source_section="PaymentStart",
            ),
            ProtocolScenarioStep(
                kind="branch",
                label="Если code!=0, ожидается PaymentCancel",
                code_ne=0,
                next_event_type="PaymentCancel",
                within_seconds=20,
                source_section="PaymentStart",
            ),
        ],
    ),
    ProtocolScenario(
        scenario_id="info_command",
        title="Сценарий Info",
        description="Фиксирует обнаружение команды Info в протоколе взаимодействия КЗП и БМ.",
        source_document="API_КЗП_БМ_TLV_2_53_13-05-2026.docx",
        source_section="Info",
        source_sections=["Info"],
        steps=[
            ProtocolScenarioStep(
                kind="match",
                label="Найти Info",
                event_type="Info",
                raw_contains="Info",
                source_section="Info",
            ),
        ],
    ),
    ProtocolScenario(
        scenario_id="payment_confirm_command",
        title="Сценарий PaymentConfirm",
        description="Фиксирует обнаружение команды PaymentConfirm в протоколе взаимодействия КЗП и БМ.",
        source_document="API_КЗП_БМ_TLV_2_53_13-05-2026.docx",
        source_section="PaymentConfirm",
        source_sections=["PaymentConfirm"],
        steps=[
            ProtocolScenarioStep(
                kind="match",
                label="Найти PaymentConfirm",
                event_type="PaymentConfirm",
                raw_contains="PaymentConfirm",
                source_section="PaymentConfirm",
            ),
        ],
    ),
    ProtocolScenario(
        scenario_id="payment_cancel_command",
        title="Сценарий PaymentCancel",
        description="Фиксирует обнаружение команды PaymentCancel в протоколе взаимодействия КЗП и БМ.",
        source_document="API_КЗП_БМ_TLV_2_53_13-05-2026.docx",
        source_section="PaymentCancel",
        source_sections=["PaymentCancel"],
        steps=[
            ProtocolScenarioStep(
                kind="match",
                label="Найти PaymentCancel",
                event_type="PaymentCancel",
                raw_contains="PaymentCancel",
                source_section="PaymentCancel",
            ),
        ],
    ),
    ProtocolScenario(
        scenario_id="init_update_command",
        title="Сценарий InitUpdate",
        description="Фиксирует обнаружение команды InitUpdate в протоколе взаимодействия КЗП и БМ.",
        source_document="API_КЗП_БМ_TLV_2_53_13-05-2026.docx",
        source_section="InitUpdate",
        source_sections=["InitUpdate"],
        steps=[
            ProtocolScenarioStep(
                kind="match",
                label="Найти InitUpdate",
                event_type="InitUpdate",
                raw_contains="InitUpdate",
                source_section="InitUpdate",
            ),
        ],
    ),
    ProtocolScenario(
        scenario_id="get_output_command",
        title="Сценарий GetOutput",
        description="Фиксирует обнаружение команды GetOutput в протоколе взаимодействия КЗП и БМ.",
        source_document="API_КЗП_БМ_TLV_2_53_13-05-2026.docx",
        source_section="GetOutput",
        source_sections=["GetOutput"],
        steps=[
            ProtocolScenarioStep(
                kind="match",
                label="Найти GetOutput",
                event_type="GetOutput",
                raw_contains="GetOutput",
                source_section="GetOutput",
            ),
        ],
    ),
    ProtocolScenario(
        scenario_id="update_configuration_command",
        title="Сценарий UpdateConfiguration",
        description="Фиксирует обнаружение команды UpdateConfiguration в протоколе взаимодействия КЗП и БМ.",
        source_document="API_КЗП_БМ_TLV_2_53_13-05-2026.docx",
        source_section="UpdateConfiguration",
        source_sections=["UpdateConfiguration"],
        steps=[
            ProtocolScenarioStep(
                kind="match",
                label="Найти UpdateConfiguration",
                event_type="UpdateConfiguration",
                raw_contains="UpdateConfiguration",
                source_section="UpdateConfiguration",
            ),
        ],
    ),
    ProtocolScenario(
        scenario_id="set_time_command",
        title="Сценарий SetTime",
        description="Фиксирует обнаружение команды SetTime в протоколе взаимодействия КЗП и БМ.",
        source_document="API_КЗП_БМ_TLV_2_53_13-05-2026.docx",
        source_section="SetTime",
        source_sections=["SetTime"],
        steps=[
            ProtocolScenarioStep(
                kind="match",
                label="Найти SetTime",
                event_type="SetTime",
                raw_contains="SetTime",
                source_section="SetTime",
            ),
        ],
    ),
    ProtocolScenario(
        scenario_id="pass_through_command",
        title="Сценарий PassThrough",
        description="Фиксирует обнаружение команды PassThrough в протоколе взаимодействия КЗП и БМ.",
        source_document="API_КЗП_БМ_TLV_2_53_13-05-2026.docx",
        source_section="PassThrough",
        source_sections=["PassThrough"],
        steps=[
            ProtocolScenarioStep(
                kind="match",
                label="Найти PassThrough",
                event_type="PassThrough",
                raw_contains="PassThrough",
                source_section="PassThrough",
            ),
        ],
    ),
]


def load_protocol_scenarios(storage_path: Path | None = None) -> list[ProtocolScenario]:
    path = storage_path or _default_protocol_scenarios_path()
    if not path.exists():
        return list(DEFAULT_PROTOCOL_SCENARIOS)
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("scenarios", payload) if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        return list(DEFAULT_PROTOCOL_SCENARIOS)
    scenarios: list[ProtocolScenario] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        scenario = _scenario_from_payload(row)
        if scenario is not None:
            scenarios.append(scenario)
    if not scenarios:
        return list(DEFAULT_PROTOCOL_SCENARIOS)
    return _merge_default_protocol_scenarios(scenarios)


def list_protocol_scenarios(storage_path: Path | None = None) -> list[ProtocolScenario]:
    return load_protocol_scenarios(storage_path)


def save_protocol_scenarios(scenarios: list[ProtocolScenario], storage_path: Path | None = None) -> None:
    path = storage_path or _default_protocol_scenarios_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "bm-log-analyzer.protocol-scenarios.v4",
        "scenarios": [asdict(scenario) for scenario in scenarios],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def create_protocol_scenario(
    *,
    title: str,
    description: str,
    steps: str,
    enabled: bool = True,
    source_document: str = "",
    source_section: str = "",
    source_sections: str = "",
    source_quotes: str = "",
    source_quote: str = "",
    storage_path: Path | None = None,
) -> ProtocolScenario:
    scenarios = load_protocol_scenarios(storage_path)
    normalized_source_sections = _normalize_source_sections(source_sections, source_section)
    normalized_source_quotes = _normalize_source_quotes(source_quotes, source_quote)
    scenario = ProtocolScenario(
        scenario_id=f"custom_{_slugify(title) or uuid4().hex[:8]}",
        title=title.strip() or "Новый сценарий",
        description=description.strip(),
        enabled=enabled,
        version="1",
        source_document=source_document.strip(),
        source_section=_source_section_text(normalized_source_sections),
        source_sections=normalized_source_sections,
        source_quote=_source_quote_text(normalized_source_quotes),
        source_quotes=normalized_source_quotes,
        steps=_parse_steps(steps),
    )
    _validate_protocol_scenario(scenario)
    if any(item.scenario_id == scenario.scenario_id for item in scenarios):
        scenario = ProtocolScenario(
            scenario_id=f"{scenario.scenario_id}_{uuid4().hex[:8]}",
            title=scenario.title,
            description=scenario.description,
            enabled=scenario.enabled,
            version=scenario.version,
            source_document=scenario.source_document,
            source_section=scenario.source_section,
            source_sections=scenario.source_sections,
            source_quote=scenario.source_quote,
            source_quotes=scenario.source_quotes,
            steps=scenario.steps,
        )
    scenarios.append(scenario)
    save_protocol_scenarios(scenarios, storage_path)
    return scenario


def update_protocol_scenario(
    scenario_id: str,
    *,
    title: str,
    description: str,
    steps: str,
    enabled: bool,
    source_document: str = "",
    source_section: str = "",
    source_sections: str = "",
    source_quotes: str = "",
    source_quote: str = "",
    storage_path: Path | None = None,
) -> ProtocolScenario:
    scenarios = load_protocol_scenarios(storage_path)
    updated: ProtocolScenario | None = None
    next_scenarios: list[ProtocolScenario] = []
    normalized_source_sections = _normalize_source_sections(source_sections, source_section)
    normalized_source_quotes = _normalize_source_quotes(source_quotes, source_quote)
    for scenario in scenarios:
        if scenario.scenario_id != scenario_id:
            next_scenarios.append(scenario)
            continue
        updated = ProtocolScenario(
            scenario_id=scenario.scenario_id,
            title=title.strip() or scenario.title,
            description=description.strip() or scenario.description,
            enabled=enabled,
            version=_increment_version(scenario.version),
            source_document=source_document.strip() or scenario.source_document,
            source_section=_source_section_text(normalized_source_sections) or scenario.source_section,
            source_sections=normalized_source_sections or scenario.source_sections,
            source_quote=_source_quote_text(normalized_source_quotes) or scenario.source_quote,
            source_quotes=normalized_source_quotes or scenario.source_quotes,
            steps=_parse_steps(steps) or scenario.steps,
        )
        _validate_protocol_scenario(updated)
        next_scenarios.append(updated)
    if updated is None:
        raise ValueError(f"unknown protocol scenario: {scenario_id}")
    save_protocol_scenarios(next_scenarios, storage_path)
    return updated


def delete_protocol_scenario(scenario_id: str, storage_path: Path | None = None) -> None:
    scenarios = load_protocol_scenarios(storage_path)
    updated = [item for item in scenarios if item.scenario_id != scenario_id]
    if len(updated) == len(scenarios):
        raise ValueError(f"unknown protocol scenario: {scenario_id}")
    if not updated:
        updated = list(DEFAULT_PROTOCOL_SCENARIOS)
    save_protocol_scenarios(updated, storage_path)


def reset_protocol_scenarios(storage_path: Path | None = None) -> None:
    path = storage_path or _default_protocol_scenarios_path()
    if path.exists():
        path.unlink()


def run_protocol_scenarios(
    events: list[PaymentEvent],
    scenarios: list[ProtocolScenario] | None = None,
) -> list[ProtocolScenarioResult]:
    active_scenarios = [scenario for scenario in (scenarios or load_protocol_scenarios()) if scenario.enabled]
    grouped_events = _group_events_by_source(events)
    results: list[ProtocolScenarioResult] = []
    for scenario in active_scenarios:
        for source_file, source_events in grouped_events.items():
            result = _evaluate_protocol_scenario(scenario, source_events)
            if result is None:
                continue
            results.append(
                ProtocolScenarioResult(
                    scenario_id=scenario.scenario_id,
                    title=scenario.title,
                    status=result["status"],
                    source_document=scenario.source_document,
                    source_section=scenario.source_section,
                    source_sections=scenario.source_sections,
                    source_quote=scenario.source_quote,
                    source_quotes=scenario.source_quotes,
                    source_file=source_file,
                    line_number=result["line_number"],
                    timestamp=result["timestamp"],
                    evidence=result["evidence"],
                    raw_line=result["raw_line"],
                    matched_event_type=result.get("matched_event_type", ""),
                    matched_code=result.get("matched_code"),
                )
            )
    return sorted(results, key=lambda item: (item.source_file, item.line_number or 0, item.scenario_id))


def _default_protocol_scenarios_path() -> Path:
    configured = os.getenv("BM_PROTOCOL_SCENARIOS_PATH", "").strip()
    if configured:
        return Path(configured)
    return Path(os.getenv("BM_DATA_DIR", "./_workdir")) / "web_settings" / "protocol_scenarios.json"


def _scenario_from_payload(row: dict[str, object]) -> ProtocolScenario | None:
    scenario_id = str(row.get("scenario_id") or "").strip()
    title = str(row.get("title") or "").strip()
    steps = _parse_steps(row.get("steps"))
    if not scenario_id or not title or not steps:
        return None
    scenario = ProtocolScenario(
        scenario_id=scenario_id,
        title=title,
        description=str(row.get("description") or "").strip(),
        enabled=bool(row.get("enabled", True)),
        version=str(row.get("version") or "1").strip() or "1",
        source_document=str(row.get("source_document") or "").strip(),
        source_section=_source_section_text(_normalize_source_sections(row.get("source_sections") or row.get("source_section"))),
        source_sections=_normalize_source_sections(row.get("source_sections") or row.get("source_section")),
        source_quote=_source_quote_text(_normalize_source_quotes(row.get("source_quotes") or row.get("source_quote"))),
        source_quotes=_normalize_source_quotes(row.get("source_quotes") or row.get("source_quote")),
        steps=steps,
    )
    _validate_protocol_scenario(scenario)
    return scenario


def _parse_steps(value: object) -> list[ProtocolScenarioStep]:
    if value is None:
        return []
    if isinstance(value, list):
        if value and all(isinstance(item, dict) for item in value):
            steps = [_step_from_payload(item) for item in value if isinstance(item, dict)]
            return [step for step in steps if step is not None]
        return [_step_from_line(str(item)) for item in value if str(item).strip()]
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        if raw.startswith("["):
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, list):
                return _parse_steps(parsed)
        return [_step_from_line(line) for line in raw.splitlines() if line.strip()]
    return []


def _step_from_payload(item: dict[str, object]) -> ProtocolScenarioStep | None:
    kind = str(item.get("kind") or "").strip().lower() or "match"
    label = str(item.get("label") or "").strip()
    event_type = str(item.get("event_type") or "").strip()
    message_contains = str(item.get("message_contains") or "").strip()
    raw_contains = str(item.get("raw_contains") or "").strip()
    next_event_type = str(item.get("next_event_type") or "").strip()
    code_eq = _optional_int(item.get("code_eq"))
    code_ne = _optional_int(item.get("code_ne"))
    within_seconds = _optional_float(item.get("within_seconds"))
    source_section = str(item.get("source_section") or "").strip()
    if not label:
        label = event_type or next_event_type or "Шаг сценария"
    step = ProtocolScenarioStep(
        kind=kind,
        label=label,
        event_type=event_type,
        message_contains=message_contains,
        raw_contains=raw_contains,
        code_eq=code_eq,
        code_ne=code_ne,
        within_seconds=within_seconds,
        next_event_type=next_event_type,
        source_section=source_section,
    )
    _validate_protocol_step(step)
    return step


def _step_from_line(line: str) -> ProtocolScenarioStep:
    text = line.strip()
    if not text:
        raise ValueError("protocol scenario step is required")
    if text.lower().startswith("if ") and "->" in text:
        left, right = text[3:].split("->", maxsplit=1)
        condition = left.strip()
        next_event = right.strip()
        if "!=" in condition:
            left_name, right_value = condition.split("!=", maxsplit=1)
            step = ProtocolScenarioStep(
                kind="branch",
                label=text,
                code_ne=int(right_value.strip()),
                next_event_type=next_event,
                event_type=left_name.strip() or "PaymentStart resp",
            )
        elif "=" in condition:
            left_name, right_value = condition.split("=", maxsplit=1)
            step = ProtocolScenarioStep(
                kind="branch",
                label=text,
                code_eq=int(right_value.strip()),
                next_event_type=next_event,
                event_type=left_name.strip() or "PaymentStart resp",
            )
        else:
            step = ProtocolScenarioStep(kind="match", label=text, event_type=text)
        _validate_protocol_step(step)
        return step
    step = ProtocolScenarioStep(kind="match", label=text, event_type=text)
    _validate_protocol_step(step)
    return step


def _validate_protocol_scenario(scenario: ProtocolScenario) -> None:
    if not scenario.title.strip():
        raise ValueError("protocol scenario title is required")
    if not scenario.steps:
        raise ValueError("protocol scenario steps are required")
    for step in scenario.steps:
        _validate_protocol_step(step)


def _merge_default_protocol_scenarios(scenarios: list[ProtocolScenario]) -> list[ProtocolScenario]:
    by_id = {scenario.scenario_id: scenario for scenario in scenarios}
    builtin_ids = {default.scenario_id for default in DEFAULT_PROTOCOL_SCENARIOS}
    merged = [by_id.get(default.scenario_id, default) for default in DEFAULT_PROTOCOL_SCENARIOS]
    merged.extend(scenario for scenario in scenarios if scenario.scenario_id not in builtin_ids)
    return merged


def _normalize_source_quotes(value: object, fallback: object = "") -> list[str]:
    quotes = _normalize_source_quote_items(value)
    if quotes:
        return quotes
    return _normalize_source_quote_items(fallback)


def _normalize_source_sections(value: object, fallback: object = "") -> list[str]:
    sections = _normalize_source_section_items(value)
    if sections:
        return sections
    return _normalize_source_section_items(fallback)


def _normalize_source_section_items(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [line.strip() for line in value.splitlines() if line.strip()]
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    return [text] if text else []


def _normalize_source_quote_items(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [line.strip() for line in value.splitlines() if line.strip()]
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    return [text] if text else []


def _source_quote_text(source_quotes: list[str]) -> str:
    return "\n".join(source_quotes)


def _source_section_text(source_sections: list[str]) -> str:
    return "\n".join(source_sections)


def _validate_protocol_step(step: ProtocolScenarioStep) -> ProtocolScenarioStep:
    if step.kind not in {"match", "branch"}:
        raise ValueError(f"unsupported protocol step kind: {step.kind}")
    if not step.label.strip():
        raise ValueError("protocol scenario step label is required")
    if not (step.event_type or step.message_contains or step.raw_contains or step.next_event_type or step.code_eq is not None or step.code_ne is not None):
        raise ValueError("protocol scenario step requires a matching condition")
    if step.kind == "branch":
        if step.code_eq is None and step.code_ne is None:
            raise ValueError("branch step requires a code condition")
        if not step.next_event_type:
            raise ValueError("branch step requires next_event_type")
    return step


def _group_events_by_source(events: list[PaymentEvent]) -> dict[str, list[PaymentEvent]]:
    grouped: dict[str, list[PaymentEvent]] = {}
    for event in sorted(events, key=_event_sort_key):
        grouped.setdefault(event.source_file, []).append(event)
    return grouped


def _evaluate_protocol_scenario(scenario: ProtocolScenario, events: list[PaymentEvent]) -> dict[str, object] | None:
    if not scenario.steps or not events:
        return None

    first_match = _find_matching_event(events, 0, scenario.steps[0], prefer_next_event_type=False)
    if first_match is None:
        return None

    current_index, current_event = first_match
    base_time = current_event.timestamp
    matched_steps = [f"{scenario.steps[0].label}@{current_event.line_number}"]

    for step in scenario.steps[1:]:
        if step.kind == "branch":
            if not _branch_applies(step, current_event):
                continue
            next_match = _find_matching_event(events, current_index + 1, step, base_time=base_time, prefer_next_event_type=True)
            if next_match is None:
                return {
                    "status": "not_matched",
                    "line_number": current_event.line_number,
                    "timestamp": current_event.timestamp,
                    "raw_line": current_event.raw_line,
                    "matched_event_type": current_event.event_type,
                    "matched_code": current_event.code,
                    "evidence": _branch_failure_evidence(step, current_event),
                }
            current_index, current_event = next_match
            base_time = current_event.timestamp
            matched_steps.append(f"{step.label}@{current_event.line_number}")
            continue

        next_match = _find_matching_event(events, current_index + 1, step, base_time=base_time, prefer_next_event_type=False)
        if next_match is None:
            return {
                "status": "not_matched",
                "line_number": current_event.line_number,
                "timestamp": current_event.timestamp,
                "raw_line": current_event.raw_line,
                "matched_event_type": current_event.event_type,
                "matched_code": current_event.code,
                "evidence": f"Не найден следующий шаг: {step.label}",
            }
        current_index, current_event = next_match
        base_time = current_event.timestamp
        matched_steps.append(f"{step.label}@{current_event.line_number}")

    evidence = " -> ".join(matched_steps)
    if scenario.source_document or scenario.source_section or scenario.source_quote:
        prefix = "Источник протокола"
        if scenario.source_document:
            prefix = f"{prefix}: {scenario.source_document}"
        source_section_text = " / ".join(scenario.source_sections) if scenario.source_sections else scenario.source_section.replace(chr(10), " / ")
        if source_section_text:
            prefix = f"{prefix}, раздел: {source_section_text}"
        source_quote_text = " / ".join(scenario.source_quotes) if scenario.source_quotes else scenario.source_quote.replace(chr(10), " / ")
        if source_quote_text:
            prefix = f"{prefix}, цитата: {source_quote_text.replace(chr(10), ' / ')}"
        evidence = f"{prefix}. {evidence}"

    return {
        "status": "matched",
        "line_number": first_match[1].line_number,
        "timestamp": first_match[1].timestamp,
        "raw_line": first_match[1].raw_line,
        "matched_event_type": first_match[1].event_type,
        "matched_code": first_match[1].code,
        "evidence": evidence,
    }


def _find_matching_event(
    events: list[PaymentEvent],
    start_index: int,
    step: ProtocolScenarioStep,
    *,
    base_time=None,
    prefer_next_event_type: bool,
) -> tuple[int, PaymentEvent] | None:
    pattern = _step_pattern(step, prefer_next_event_type=prefer_next_event_type)
    for index in range(start_index, len(events)):
        event = events[index]
        if pattern and pattern not in event.event_type.lower() and pattern not in event.raw_line.lower():
            continue
        if not _event_matches_step(event, step):
            continue
        if step.within_seconds is not None and base_time is not None and event.timestamp is not None:
            delta = abs((event.timestamp - base_time).total_seconds())
            if delta > step.within_seconds:
                continue
        return index, event
    return None


def _step_pattern(step: ProtocolScenarioStep, *, prefer_next_event_type: bool) -> str:
    if prefer_next_event_type:
        return (step.next_event_type or step.event_type).strip().lower()
    return (step.event_type or step.next_event_type).strip().lower()


def _event_matches_step(event: PaymentEvent, step: ProtocolScenarioStep) -> bool:
    if step.code_eq is not None and event.code != step.code_eq:
        return False
    if step.code_ne is not None and event.code == step.code_ne:
        return False
    if step.message_contains:
        haystack = f"{event.message or ''} {event.raw_line}"
        if step.message_contains.lower() not in haystack.lower():
            return False
    if step.raw_contains and step.raw_contains.lower() not in event.raw_line.lower():
        return False
    return True


def _branch_applies(step: ProtocolScenarioStep, event: PaymentEvent) -> bool:
    if step.code_eq is not None and event.code != step.code_eq:
        return False
    if step.code_ne is not None and event.code == step.code_ne:
        return False
    return True


def _branch_failure_evidence(step: ProtocolScenarioStep, event: PaymentEvent) -> str:
    if step.code_eq is not None:
        return f"Ожидался код {step.code_eq}, фактический code={event.code}."
    if step.code_ne is not None:
        return f"Ожидался code != {step.code_ne}, фактический code={event.code}."
    return f"Не выполнено условие перехода: {step.label}."


def _optional_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _optional_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def _event_sort_key(event: PaymentEvent) -> tuple[int, object, int]:
    timestamp = event.timestamp.timestamp() if event.timestamp else -1
    return (0 if event.timestamp else 1, timestamp, event.line_number)


def _slugify(value: str) -> str:
    slug = "".join(char if char.isalnum() else "_" for char in value.strip().lower()).strip("_")
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug[:48]


def _increment_version(value: str) -> str:
    try:
        return str(int(value) + 1)
    except ValueError:
        return "1"

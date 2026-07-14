from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from core.models import PaymentEvent, PipelineStats


LOG_TYPE_LABELS: dict[str, str] = {
    "bm": "БМ",
    "reader": "ридера",
    "oti_reader_library": "библиотеки ридера ОТИ",
    "stopper": "ПО стоппера",
    "validator_app": "ПО валидатора",
    "system": "операционной системы",
    "other": "прочие логи",
}


@dataclass(frozen=True)
class ReportSectionDefinition:
    section_id: str
    title: str
    required_log_types: tuple[str, ...] = ()
    data_source: str = ""


SECTION_DEFINITIONS: tuple[ReportSectionDefinition, ...] = (
    ReportSectionDefinition("summary", "Сводка", data_source="сводные показатели анализа"),
    ReportSectionDefinition("upload_composition", "Состав загрузки", data_source="загруженные файлы"),
    ReportSectionDefinition("bm_meta", "BM сведения", ("bm",)),
    ReportSectionDefinition("log_files", "Log-файлы", data_source="структура архива"),
    ReportSectionDefinition("other_files", "Прочие файлы", data_source="структура архива"),
    ReportSectionDefinition("validation_checks", "Проверки", ("bm",)),
    ReportSectionDefinition("suspicious", "Подозрительно", ("bm",)),
    ReportSectionDefinition("protocol_scenarios", "Сценарии из протокола взаимодействия", ("bm",)),
    ReportSectionDefinition("device_boot_speed", "Скорость загрузки устройства", ("validator_app", "bm")),
    ReportSectionDefinition("bm_statuses", "BM-статусы", ("bm",)),
    ReportSectionDefinition("grouped_statuses", "Группировка статусов", ("bm",)),
    ReportSectionDefinition("date_dynamics", "Динамика по датам", ("bm",)),
    ReportSectionDefinition("unclassified_diagnostics", "Не классифицировано", ("bm",)),
    ReportSectionDefinition("validator_analytics", "Аналитика по валидаторам", ("bm",)),
)

SECTION_DEFINITIONS_BY_ID = {definition.section_id: definition for definition in SECTION_DEFINITIONS}


def build_section_sources(
    stats: PipelineStats | None,
    events: Iterable[PaymentEvent] | None = None,
    section_ids: Iterable[str] | None = None,
) -> dict[str, dict[str, object]]:
    available = available_log_types(stats, events)
    selected_ids = list(section_ids) if section_ids is not None else [item.section_id for item in SECTION_DEFINITIONS]
    return {
        section_id: _section_source_payload(SECTION_DEFINITIONS_BY_ID[section_id], available)
        for section_id in selected_ids
        if section_id in SECTION_DEFINITIONS_BY_ID
    }


def available_log_types(stats: PipelineStats | None, events: Iterable[PaymentEvent] | None = None) -> set[str]:
    available: set[str] = set()
    if stats:
        available.update(item.log_type for item in stats.log_inventory if item.log_type)
        for source in stats.input_source_summaries:
            available.update(log_type for log_type in source.log_types if log_type)
    if events is not None and any(True for _ in events):
        available.add("bm")
    return available


def _section_source_payload(definition: ReportSectionDefinition, available: set[str]) -> dict[str, object]:
    required = list(definition.required_log_types)
    matched = [log_type for log_type in required if log_type in available]
    missing = [log_type for log_type in required if log_type not in available]
    if not required:
        status = "not_required"
    elif not missing:
        status = "available"
    elif matched:
        status = "partial"
    else:
        status = "missing"
    note = _source_note(definition, status, matched, missing)
    return {
        "section_id": definition.section_id,
        "title": definition.title,
        "status": status,
        "required_log_types": required,
        "required_log_type_labels": [_log_type_label(log_type) for log_type in required],
        "matched_log_types": matched,
        "matched_log_type_labels": [_log_type_label(log_type) for log_type in matched],
        "missing_log_types": missing,
        "missing_log_type_labels": [_log_type_label(log_type) for log_type in missing],
        "available_log_types": sorted(available),
        "available_log_type_labels": [_log_type_label(log_type) for log_type in sorted(available)],
        "data_source": definition.data_source,
        "note": note,
    }


def _source_note(
    definition: ReportSectionDefinition,
    status: str,
    matched_log_types: list[str],
    missing_log_types: list[str],
) -> str:
    if not definition.required_log_types:
        return f"Источник данных: {definition.data_source}." if definition.data_source else "Источник данных: отчёт."
    if status == "available":
        return f"Источник данных: {_join_labels(matched_log_types)}."
    if status == "partial":
        return (
            f"Источник данных: частично {_join_labels(matched_log_types)}; "
            f"не найдены: {_join_labels(missing_log_types)}."
        )
    return f"Источник данных недоступен: нужны {_join_labels(missing_log_types)}."


def _join_labels(log_types: list[str]) -> str:
    return ", ".join(_log_type_label(log_type) for log_type in log_types)


def _log_type_label(log_type: str) -> str:
    return LOG_TYPE_LABELS.get(log_type, log_type)

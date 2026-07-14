from __future__ import annotations

from collections import defaultdict

from core.models import InputSourceSummary, LogFileInventory

LOG_TYPE_LABELS = {
    "bm": "БМ",
    "reader": "ридера",
    "validator_app": "ПО валидатора",
    "system": "операционной системы",
    "other": "неопределённые",
}
LOG_TYPE_ORDER = {
    "bm": 0,
    "reader": 1,
    "validator_app": 2,
    "system": 3,
    "other": 99,
}


def build_input_source_summaries(
    *,
    direct_files: list[str],
    source_archives: list[str],
    extracted_file_origins: dict[str, str],
    log_inventory: list[LogFileInventory],
) -> list[InputSourceSummary]:
    inventory_by_source = {item.source_file: item for item in log_inventory}
    extracted_by_origin: dict[str, list[str]] = defaultdict(list)
    for extracted_file, origin in extracted_file_origins.items():
        extracted_by_origin[origin].append(extracted_file)

    summaries: list[InputSourceSummary] = []
    for source_file in sorted({*source_archives, *direct_files}):
        if source_file in source_archives:
            extracted_files = sorted(set(extracted_by_origin.get(source_file, [])))
            related_inventory = [
                inventory_by_source[path]
                for path in extracted_files
                if path in inventory_by_source
            ]
            summaries.append(
                _build_summary(
                    source_file=source_file,
                    input_kind="archive",
                    related_inventory=related_inventory,
                    analyzed_files=[item.source_file for item in related_inventory],
                    extracted_files=extracted_files,
                )
            )
            continue

        related_inventory = [inventory_by_source[source_file]] if source_file in inventory_by_source else []
        summaries.append(
            _build_summary(
                source_file=source_file,
                input_kind="log_file",
                related_inventory=related_inventory,
                analyzed_files=[item.source_file for item in related_inventory],
                extracted_files=[],
            )
        )

    return summaries


def _build_summary(
    *,
    source_file: str,
    input_kind: str,
    related_inventory: list[LogFileInventory],
    analyzed_files: list[str],
    extracted_files: list[str],
) -> InputSourceSummary:
    log_types = sorted(
        {item.log_type for item in related_inventory},
        key=lambda value: (LOG_TYPE_ORDER.get(value, 50), value),
    )
    return InputSourceSummary(
        source_file=source_file,
        input_kind=input_kind,
        log_types=log_types,
        log_type_labels=[LOG_TYPE_LABELS.get(log_type, log_type) for log_type in log_types],
        analyzed_files=sorted(set(analyzed_files)),
        extracted_files=sorted(set(extracted_files)),
        evidence=sorted(
            {
                f"{item.source_file}: {item.evidence}"
                for item in related_inventory
                if item.evidence
            }
        ),
    )

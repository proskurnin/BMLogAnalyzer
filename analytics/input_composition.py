from __future__ import annotations

from collections import Counter
from collections import defaultdict
from pathlib import Path

from core.log_types import archive_category_is_log, archive_category_log_type, log_type_label, log_type_sort_key
from core.models import ArchiveInventoryRow, InputSourceSummary, LogFileInventory


def build_input_source_summaries(
    *,
    direct_files: list[str],
    source_archives: list[str],
    extracted_file_origins: dict[str, str],
    analyzed_files: list[str],
    log_inventory: list[LogFileInventory],
    archive_inventory: list[ArchiveInventoryRow] | None = None,
) -> list[InputSourceSummary]:
    inventory_by_source = {item.source_file: item for item in log_inventory}
    analyzed_set = set(analyzed_files)
    extracted_by_origin: dict[str, list[str]] = defaultdict(list)
    for extracted_file, origin in extracted_file_origins.items():
        extracted_by_origin[origin].append(extracted_file)
    archive_rows_by_source: dict[str, list[ArchiveInventoryRow]] = defaultdict(list)
    for row in archive_inventory or []:
        archive_rows_by_source[row.archive].append(row)

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
                    analyzed_files=[path for path in extracted_files if path in analyzed_set],
                    extracted_files=extracted_files,
                    archive_rows=archive_rows_by_source.get(source_file, []),
                )
            )
            continue

        related_inventory = [inventory_by_source[source_file]] if source_file in inventory_by_source else []
        summaries.append(
            _build_summary(
                source_file=source_file,
                input_kind="log_file",
                related_inventory=related_inventory,
                analyzed_files=[source_file] if source_file in analyzed_set else [],
                extracted_files=[],
                archive_rows=[],
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
    archive_rows: list[ArchiveInventoryRow],
) -> InputSourceSummary:
    log_type_counts = _log_type_counts(related_inventory, archive_rows)
    log_types = sorted(log_type_counts, key=log_type_sort_key)
    log_type_evidence = _log_type_evidence(related_inventory)
    analyzed_unique = sorted(set(analyzed_files))
    extracted_unique = sorted(set(extracted_files))
    archive_file_count = sum(row.count for row in archive_rows)
    if not archive_rows and input_kind != "archive":
        archive_file_count = 1
    inventory_log_file_count = sum(row.count for row in archive_rows if _is_log_inventory_category(row.category))
    detected_log_file_count = sum(1 for item in related_inventory if item.log_type != "other")
    log_file_count = max(inventory_log_file_count, detected_log_file_count)
    if not archive_rows and input_kind != "archive":
        log_file_count = 1 if any(item.log_type != "other" for item in related_inventory) else 0
    processed_archive_container_count = _processed_archive_container_count(archive_rows)
    reportable_archive_file_count = max(log_file_count, archive_file_count - processed_archive_container_count)
    other_file_count = max(0, reportable_archive_file_count - log_file_count)
    skipped_reasons = _skipped_reasons(
        other_file_count=other_file_count,
        extracted_file_count=len(extracted_unique),
        analyzed_file_count=len(analyzed_unique),
    )
    return InputSourceSummary(
        source_file=source_file,
        input_kind=input_kind,
        log_types=log_types,
        log_type_labels=[log_type_label(log_type) for log_type in log_types],
        log_type_counts=log_type_counts,
        log_type_evidence=log_type_evidence,
        analyzed_files=analyzed_unique,
        extracted_files=extracted_unique,
        evidence=sorted(
            {
                f"{item.source_file}: {item.evidence}"
                for item in related_inventory
                if item.evidence
            }
        ),
        archive_file_count=reportable_archive_file_count,
        log_file_count=log_file_count,
        other_file_count=other_file_count,
        extracted_file_count=len(extracted_unique),
        analyzed_file_count=len(analyzed_unique),
        skipped_file_count=sum(skipped_reasons.values()),
        skipped_reasons=skipped_reasons,
    )


def _log_type_counts(
    related_inventory: list[LogFileInventory],
    archive_rows: list[ArchiveInventoryRow],
) -> dict[str, int]:
    if archive_rows:
        archive_log_file_count = sum(row.count for row in archive_rows if archive_category_is_log(row.category))
        if related_inventory and len(related_inventory) >= archive_log_file_count:
            return _inventory_log_type_counts(related_inventory)
        counts: Counter[str] = Counter()
        for row in archive_rows:
            log_type = archive_category_log_type(row.category)
            if log_type:
                counts[log_type] += row.count
        _reclassify_other_log_like_counts(counts, related_inventory, archive_rows)
        return dict(sorted(counts.items(), key=lambda entry: log_type_sort_key(entry[0])))

    return _inventory_log_type_counts(related_inventory)


def _inventory_log_type_counts(related_inventory: list[LogFileInventory]) -> dict[str, int]:
    return dict(
        sorted(
            Counter(item.log_type for item in related_inventory).items(),
            key=lambda entry: log_type_sort_key(entry[0]),
        )
    )


def _reclassify_other_log_like_counts(
    counts: Counter[str],
    related_inventory: list[LogFileInventory],
    archive_rows: list[ArchiveInventoryRow],
) -> None:
    archive_name = Path(archive_rows[0].archive).name if archive_rows else ""
    other_members = {
        str(file_name)
        for row in archive_rows
        if row.category == "Other log-like"
        for file_name in (row.files or row.file_sizes or row.examples)
    }
    if not archive_name or not other_members or not counts.get("other"):
        return

    reclassified: set[str] = set()
    for item in related_inventory:
        if item.log_type == "other":
            continue
        member_name = _inventory_member_name(item.source_file, archive_name)
        if member_name not in other_members or member_name in reclassified:
            continue
        counts["other"] -= 1
        counts[item.log_type] += 1
        reclassified.add(member_name)

    if counts.get("other") == 0:
        del counts["other"]


def _inventory_member_name(source_file: str, archive_name: str) -> str:
    normalized = source_file.replace("\\", "/")
    marker = f"/{archive_name}/"
    if marker in normalized:
        return _archive_member_name_from_extracted(normalized.split(marker, 1)[1])
    return _archive_member_name_from_extracted(normalized)


def _archive_member_name_from_extracted(value: str) -> str:
    if value.endswith(".gz.log"):
        return value[:-4]
    return value


def _log_type_evidence(inventory: list[LogFileInventory]) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = defaultdict(list)
    for item in inventory:
        evidence = item.evidence
        if not evidence or evidence == "no known markers":
            continue
        row = f"{item.source_file}: {evidence}"
        bucket = grouped[item.log_type]
        if row not in bucket:
            bucket.append(row)
    return {
        log_type: values[:5]
        for log_type, values in sorted(grouped.items(), key=lambda entry: log_type_sort_key(entry[0]))
    }


def _is_log_inventory_category(category: str) -> bool:
    return archive_category_is_log(category)


def _processed_archive_container_count(rows: list[ArchiveInventoryRow]) -> int:
    return sum(
        1
        for row in rows
        for file_name in row.files or row.file_sizes or row.examples
        if _is_archive_container(file_name)
    )


def _is_archive_container(file_name: str) -> bool:
    name = file_name.lower()
    return (
        name.endswith(".zip")
        or name.endswith(".rar")
        or name.endswith(".tar.gz")
        or name.endswith(".tgz")
    )


def _skipped_reasons(
    *,
    other_file_count: int,
    extracted_file_count: int,
    analyzed_file_count: int,
) -> dict[str, int]:
    reasons: dict[str, int] = {}
    if other_file_count:
        reasons["не анализировались: прочие файлы не относятся к log-файлам"] = other_file_count
    not_scanned = max(0, extracted_file_count - analyzed_file_count)
    if not_scanned:
        reasons["не сканировались: путь не попал в поддерживаемые источники сканера"] = not_scanned
    return reasons

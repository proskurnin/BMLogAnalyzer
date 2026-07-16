from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LogTypeSpec:
    key: str
    label: str
    archive_categories: tuple[str, ...] = ()


LOG_TYPE_SPECS: tuple[LogTypeSpec, ...] = (
    LogTypeSpec("bm", "БМ", ("BM rotate", "BM stdout")),
    LogTypeSpec("stopper", "ПО стоппера", ("Stopper rotate", "Stopper stdout")),
    LogTypeSpec("vil", "VIL", ("VIL logs",)),
    LogTypeSpec("validator_app", "ПО валидатора", ("Validator app logs",)),
    LogTypeSpec("reader", "ридера", ("Reader logs",)),
    LogTypeSpec("oti_reader_library", "библиотеки ридера ОТИ"),
    LogTypeSpec("system", "операционной системы", ("System logs",)),
    LogTypeSpec("other", "неопределённые log-файлы", ("Other log-like",)),
)

LOG_TYPE_LABELS: dict[str, str] = {item.key: item.label for item in LOG_TYPE_SPECS}
LOG_TYPE_ORDER: dict[str, int] = {item.key: index for index, item in enumerate(LOG_TYPE_SPECS)}
ARCHIVE_CATEGORY_TO_LOG_TYPE: dict[str, str] = {
    category: item.key
    for item in LOG_TYPE_SPECS
    for category in item.archive_categories
}
LOG_ARCHIVE_CATEGORIES: set[str] = set(ARCHIVE_CATEGORY_TO_LOG_TYPE)


def log_type_label(log_type: str) -> str:
    return LOG_TYPE_LABELS.get(log_type, log_type)


def log_type_sort_key(log_type: str) -> tuple[int, str]:
    return (LOG_TYPE_ORDER.get(log_type, 50), log_type)


def archive_category_log_type(category: str) -> str | None:
    return ARCHIVE_CATEGORY_TO_LOG_TYPE.get(category)


def archive_category_is_log(category: str) -> bool:
    return category in LOG_ARCHIVE_CATEGORIES


def archive_log_group_specs() -> list[tuple[str, set[str]]]:
    return [
        (item.label, set(item.archive_categories))
        for item in LOG_TYPE_SPECS
        if item.archive_categories
    ]

from __future__ import annotations

import re
from collections import Counter

from core.models import PaymentEvent

BM_STATUS_ORDER = [
    "Успешный онлайн (БЕЗ МИР)",
    "Успешный онлайн МИР",
    "Успешный оффлайн",
    "Проход зафейлен (онлайн - не получили конфирм и зарегали как фейл)",
    "Отказ, повторное предъявление",
    "Отказ, ошибка чтения карты",
    "Отказ, карта в стоп листе",
    "Отказ, коллизия",
    "Отказ, ошибка ODA/CDA",
    "Отказ, нет карты в поле",
]
UNCLASSIFIED_STATUS = "Не классифицировано"

ONLINE_RE = re.compile(r"\bonline\b|онлайн", re.IGNORECASE)
OFFLINE_RE = re.compile(r"\boffline\b|оффлайн", re.IGNORECASE)
MIR_RE = re.compile(r"\bmir\b|мир", re.IGNORECASE)
APPROVED_RE = re.compile(r"\bодобрено\b|\bapproved\b", re.IGNORECASE)
PASS_RE = re.compile(r"\bпроходите\b|\bpass\b", re.IGNORECASE)
AUTHORIZATION_RE = re.compile(r"авторизац", re.IGNORECASE)
NO_CONFIRM_RE = re.compile(r"confirm|конфирм", re.IGNORECASE)
REPEAT_RE = re.compile(r"повтор|следующий проход|repeat", re.IGNORECASE)
READ_ERROR_RE = re.compile(r"чтени[яе] карт|read", re.IGNORECASE)
STOP_LIST_RE = re.compile(r"stop[- ]?list|стоп", re.IGNORECASE)
COLLISION_RE = re.compile(r"collision|коллизи|одну карту", re.IGNORECASE)
ODA_CDA_RE = re.compile(r"\b(?:oda|cda)\b", re.IGNORECASE)
NO_CARD_RE = re.compile(r"нет карты|no card", re.IGNORECASE)


def bm_status_summary_rows(events: list[PaymentEvent]) -> list[dict[str, object]]:
    total = len(events)
    counts = Counter(classify_bm_status(event) for event in events)
    statuses = [*BM_STATUS_ORDER, UNCLASSIFIED_STATUS]
    return [
        {
            "status": status,
            "count": counts.get(status, 0),
            "percent": _percent(counts.get(status, 0), total),
        }
        for status in statuses
        if counts.get(status, 0) or status in BM_STATUS_ORDER
    ]


def classify_bm_status(event: PaymentEvent) -> str:
    text = " ".join(part for part in [event.message, event.raw_line] if part)
    code = event.code
    payment_type = event.payment_type
    auth_type = event.auth_type

    if code == 0:
        if OFFLINE_RE.search(text):
            return "Успешный оффлайн"
        if MIR_RE.search(text):
            return "Успешный онлайн МИР"
        if "error: no error" in text.lower() or ONLINE_RE.search(text) or PASS_RE.search(text) or APPROVED_RE.search(text) or AUTHORIZATION_RE.search(text):
            if payment_type == 2 and auth_type == 0:
                return "Успешный онлайн (БЕЗ МИР)"
            if auth_type == 1 or AUTHORIZATION_RE.search(text):
                return "Успешный онлайн МИР"
            return "Успешный онлайн (БЕЗ МИР)"
        return UNCLASSIFIED_STATUS

    if NO_CONFIRM_RE.search(text):
        return "Проход зафейлен (онлайн - не получили конфирм и зарегали как фейл)"
    if code == 1 or REPEAT_RE.search(text):
        return "Отказ, повторное предъявление"
    if code == 3 or READ_ERROR_RE.search(text):
        return "Отказ, ошибка чтения карты"
    if code == 4 or STOP_LIST_RE.search(text):
        return "Отказ, карта в стоп листе"
    if code == 6 or COLLISION_RE.search(text):
        return "Отказ, коллизия"
    if ODA_CDA_RE.search(text):
        return "Отказ, ошибка ODA/CDA"
    if code == 17 or NO_CARD_RE.search(text):
        return "Отказ, нет карты в поле"

    return UNCLASSIFIED_STATUS


def _percent(count: int, total: int) -> float:
    if total == 0:
        return 0.0
    return round((count / total) * 100, 2)

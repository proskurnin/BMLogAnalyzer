SUCCESS_CODES = {0}
DECLINE_CODES = {1, 4, 6, 12, 14, 255}
TECHNICAL_ERROR_CODES = {3, 16, 17}

CODE_CLASSIFICATIONS = {
    **{code: "success" for code in SUCCESS_CODES},
    **{code: "decline" for code in DECLINE_CODES},
    **{code: "technical_error" for code in TECHNICAL_ERROR_CODES},
}

CODE_DESCRIPTIONS = {
    0: "success",
    1: "Следующий проход через 20 минут",
    12: "QR-КОД НЕДЕЙСТВИТЕЛЕН",
    14: "Операция отклонена",
    3: "Ошибка чтения карты",
    4: "Карта в стоп-листе",
    6: "Приложите одну карту",
    16: "Истек таймаут",
    17: "Нет карты. Приложите еще раз",
    255: "Операция отклонена",
}


def classify_code(code: int | None) -> str:
    if code is None:
        return "unknown"
    return CODE_CLASSIFICATIONS.get(code, "unknown")


def is_known_code(code: int | str | None) -> bool:
    if code is None:
        return False
    try:
        normalized = int(code)
    except (TypeError, ValueError):
        return False
    return normalized in CODE_CLASSIFICATIONS

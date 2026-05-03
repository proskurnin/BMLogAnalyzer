SUCCESS_CODES = {0}
TECHNICAL_ERROR_CODES = {3, 16, 17}
DECLINE_CODES: set[int] = set()


def classify_code(code: int | None) -> str:
    if code in SUCCESS_CODES:
        return "success"
    if code in TECHNICAL_ERROR_CODES:
        return "technical_error"
    if code in DECLINE_CODES:
        return "decline"
    return "unknown"

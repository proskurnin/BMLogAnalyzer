from analytics.bm_statuses import bm_status_summary_rows, classify_bm_status
from tests.test_counters import make_event


def test_classifies_supported_bm_statuses_from_codes_and_markers():
    assert classify_bm_status(make_event(0, message="offline")) == "Успешный оффлайн"
    assert classify_bm_status(make_event(0, message="Проходите", payment_type=0, auth_type=0)) == "Успешный онлайн (БЕЗ МИР)"
    assert classify_bm_status(make_event(0, message="ОДОБРЕНО\nПРОХОДИТЕ", payment_type=2, auth_type=0)) == "Успешный онлайн (БЕЗ МИР)"
    assert classify_bm_status(make_event(0, message="Авторизация, не убирайте карту", payment_type=0, auth_type=1)) == "Успешный онлайн МИР"
    assert classify_bm_status(make_event(1, message="Следующий проход через 20 минут")) == "Отказ, повторное предъявление"
    assert classify_bm_status(make_event(3, message="Ошибка чтения карты")) == "Отказ, ошибка чтения карты"
    assert classify_bm_status(make_event(4, message="Карта в стоп-листе")) == "Отказ, карта в стоп листе"
    assert classify_bm_status(make_event(6, message="Приложите одну карту")) == "Отказ, коллизия"
    assert classify_bm_status(make_event(17, message="Нет карты. Приложите еще раз")) == "Отказ, нет карты в поле"
    assert classify_bm_status(make_event(999, message="ODA failed")) == "Отказ, ошибка ODA/CDA"


def test_bm_status_summary_keeps_requested_rows_and_unclassified():
    rows = bm_status_summary_rows([make_event(0, message="OK"), make_event(3, message="Ошибка чтения карты")])
    by_status = {row["status"]: row for row in rows}

    assert by_status["Отказ, ошибка чтения карты"]["count"] == 1
    assert by_status["Отказ, ошибка чтения карты"]["percent"] == 50.0
    assert by_status["Не классифицировано"]["count"] == 1
    assert "Успешный онлайн МИР" in by_status

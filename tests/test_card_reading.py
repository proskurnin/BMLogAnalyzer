import json

from core.pipeline import run_analysis
from reports.html_report import write_html_report


def test_pipeline_builds_slow_card_reading_report(tmp_path):
    input_dir = tmp_path / "input"
    extracted_dir = tmp_path / "extracted"
    input_dir.mkdir()
    (input_dir / "oti" / "logs" / "bm").mkdir(parents=True)
    (input_dir / "oti" / "2026-07-15-11-19-55-443.log").write_text(
        "\n".join(
            [
                "[2026.07.15 11:41:11.234] {2293755264} I TicketProcessor: чтение карты 1581273837315100 MIFARE Classic 4k (Bank on)",
                "[2026.07.15 11:41:12.762] {2293755264} T Validator: статус онлайн-валидации: ошибка: Problem with the SSL CA cert (curl error - 77)",
                "[2026.07.15 11:41:12.851] {2388773343} D bm::Connection: Send Commands::paymentStart with timeout: 10000",
                "[2026.07.15 11:41:14.620] {2388773343} T bm::Connection: Send Commands::paymentStart succeed",
                "[2026.07.15 11:41:14.738] {2388773343} D bm::Connection: Send Commands::paymentConfirm with timeout: 10000",
                "[2026.07.15 11:41:16.881] {2388773343} T bm::Connection: Send Commands::paymentConfirm succeed",
                "[2026.07.15 11:41:17.238] {2293755264} T Validator: валидация завершена (Банк)",
            ]
        ),
        encoding="utf-8",
    )
    (input_dir / "oti" / "logs" / "bm" / "bm-rotate.log").write_text(
        "\n".join(
            [
                'time="2026-07-15 11:41:13.174" level=info msg="PaymentStart, req: {Sum:0} p: mgt_nbs-oti-4.5.13"',
                'time="2026-07-15 11:41:14.613" level=info msg="PaymentStart, resp: 1.455050946s, reader=1.240090414s, {AuthType:1 Code:0 MessageRus:Авторизация} p: mgt_nbs-oti-4.5.13"',
                'time="2026-07-15 11:41:16.825" level=info msg="PaymentConfirm, resp: Code:0, Проходите"',
            ]
        ),
        encoding="utf-8",
    )

    events, result, stats = run_analysis(input_dir, extracted_dir=extracted_dir)

    assert result.total == 1
    assert len(stats.card_reading_reports) == 1
    report = stats.card_reading_reports[0]
    assert report.reader_type == "OTI"
    assert report.card_id == "1581273837315100"
    assert report.total_seconds == 6.004
    assert report.payment_start_code == 0
    assert report.auth_type == 1
    assert report.payment_confirm_code == 0
    assert report.result == "PaymentStart Code 0, AuthType 1; проход разрешён"
    components = {item.title: item.duration_seconds for item in report.components}
    assert components["ЛИБА vil_api/libcore"] == 1.24
    assert components["БМ без библиотеки"] == 2.79
    assert components["НБС"] == 1.974

    write_html_report(events, result, tmp_path / "analysis_report.html", stats=stats)
    html = (tmp_path / "analysis_report.html").read_text(encoding="utf-8")
    manifest = json.loads((tmp_path / "analysis_report.json").read_text(encoding="utf-8"))
    assert "Долгое чтение и валидация карт" in html
    assert "Кейсов дольше 3 сек: 1" in html
    assert "ЛИБА vil_api/libcore" in html
    assert "card_reading_speed" in manifest["stable_sections"]
    assert manifest["card_reading_speed"][0]["card_id"] == "1581273837315100"


def test_shift_opening_card_reading_without_payment_start(tmp_path):
    input_dir = tmp_path / "input"
    extracted_dir = tmp_path / "extracted"
    input_dir.mkdir()
    (input_dir / "tt").mkdir()
    (input_dir / "tt" / "2026-07-15-11-21-21-949.log").write_text(
        "\n".join(
            [
                "[2026.07.15 11:22:01.984] {628576508} I TicketProcessor: чтение карты 1267167192048000 MIFARE Classic 4k (Bank off)",
                "[2026.07.15 11:22:03.058] {628576508} I TicketProcessor: Открытие смены (была приложена БСК-Авторизации)",
                "[2026.07.15 11:22:11.374] {628576508} I TicketProcessor: Открытие - успешно",
                "[2026.07.15 11:22:14.562] {628576508} T Validator: валидация завершена (карта Выхода или Авторизации)",
            ]
        ),
        encoding="utf-8",
    )

    _, _, stats = run_analysis(input_dir, extracted_dir=extracted_dir)

    assert len(stats.card_reading_reports) == 1
    report = stats.card_reading_reports[0]
    assert report.reader_type == "TT"
    assert report.total_seconds == 12.578
    assert report.result == "Открытие смены; PaymentStart не выполнялся"
    components = {item.title: item.duration_seconds for item in report.components}
    assert components["НБС"] == 12.578
    assert components["БМ без библиотеки"] == 0.0
    assert components["ЛИБА vil_api/libcore"] is None

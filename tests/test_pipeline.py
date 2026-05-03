from core.pipeline import run_analysis


def test_pipeline_collects_diagnostics_for_malformed_payment_resp(tmp_path):
    input_dir = tmp_path / "input"
    extracted_dir = tmp_path / "extracted"
    input_dir.mkdir()
    (input_dir / "sample.log").write_text(
        "\n".join(
            [
                "2026-04-29 20:50:41.343 PaymentStart, resp: {Code:3 Message:Ошибка чтения карты} duration=412 ms p: mgt_nbs-oti-4.4.12",
                "2026-04-29 20:50:42.000 PaymentStart, resp: malformed",
            ]
        ),
        encoding="utf-8",
    )

    events, result, stats = run_analysis(input_dir, extracted_dir=extracted_dir)

    assert len(events) == 1
    assert result.total == 1
    assert stats.scanned_lines == 2
    assert stats.malformed_payment_lines == 1
    assert stats.diagnostics[0].reason == "payment_start_resp_parse_failed"

from core.pipeline import run_analysis
import zipfile


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
    assert [step.name for step in stats.steps] == [
        "extract_archives",
        "inventory_archives",
        "scan_and_parse_logs",
        "aggregate_statistics",
    ]
    assert stats.steps[2].status == "completed_with_errors"
    assert stats.steps[2].errors == 1
    assert stats.steps[2].details["scanned_lines"] == 2
    assert stats.steps[2].details["parsed_events"] == 1
    assert len(stats.files) == 1
    assert stats.files[0].scanned_lines == 2
    assert stats.files[0].payment_resp_lines == 2
    assert stats.files[0].parsed_payment_resp_lines == 1
    assert stats.files[0].selected_payment_resp_events == 1
    assert stats.files[0].malformed_payment_resp_lines == 1
    assert stats.input_files == [str(input_dir / "sample.log")]
    assert stats.analyzed_files == [str(input_dir / "sample.log")]
    assert stats.archive_inventory == []
    assert len(stats.log_inventory) == 1
    assert stats.log_inventory[0].log_type == "bm"
    assert stats.log_inventory[0].bm_versions == ["4.4.12"]


def test_pipeline_does_not_analyze_zip_twice(tmp_path):
    input_dir = tmp_path / "input"
    extracted_dir = tmp_path / "extracted"
    input_dir.mkdir()
    archive_path = input_dir / "logs.zip"
    line = "2026-04-29 20:50:41.343 PaymentStart, resp: {Code:0 Message:OK} duration=412 ms p: mgt_nbs-oti-4.4.12"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("nested/a.log", line)

    events, result, stats = run_analysis(input_dir, extracted_dir=extracted_dir)

    assert len(events) == 1
    assert result.total == 1
    assert stats.input_files == [str(archive_path)]
    assert stats.extracted_file_paths == [str(extracted_dir / "logs.zip" / "nested" / "a.log")]
    assert stats.analyzed_files == [str(extracted_dir / "logs.zip" / "nested" / "a.log")]
    assert all("!" not in source for source in stats.analyzed_files)
    assert stats.log_inventory[0].log_type == "bm"
    assert stats.archive_inventory[0].archive == str(archive_path)
    assert stats.archive_inventory[0].category == "Other log-like"


def test_pipeline_falls_back_on_invalid_gzip(tmp_path):
    input_dir = tmp_path / "input"
    extracted_dir = tmp_path / "extracted"
    input_dir.mkdir()
    archive_path = input_dir / "broken.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr(
            "broken.log.gz",
            b"2026-04-29 20:50:41.343 PaymentStart, resp: {Code:0 Message:OK} duration=412 ms p: mgt_nbs-oti-4.4.12\n",
        )

    events, result, stats = run_analysis(input_dir, extracted_dir=extracted_dir)

    assert len(events) == 1
    assert result.total == 1
    assert stats.scanned_lines == 1
    assert stats.analyzed_files == [str(extracted_dir / "broken.zip" / "broken.log.gz")]

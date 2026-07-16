import io
import gzip
import zipfile

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
    assert [step.name for step in stats.steps] == [
        "extract_archives",
        "inventory_archives",
        "scan_and_parse_logs",
        "aggregate_statistics",
        "finalize_pipeline_stats",
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
    assert len(stats.input_source_summaries) == 1
    input_summary = stats.input_source_summaries[0]
    assert input_summary.source_file == str(input_dir / "sample.log")
    assert input_summary.input_kind == "log_file"
    assert input_summary.log_types == ["bm"]
    assert input_summary.log_type_labels == ["БМ"]
    assert input_summary.log_type_counts == {"bm": 1}
    assert "bm" in input_summary.log_type_evidence
    assert input_summary.analyzed_files == [str(input_dir / "sample.log")]
    assert input_summary.archive_file_count == 1
    assert input_summary.log_file_count == 1
    assert input_summary.other_file_count == 0
    assert input_summary.extracted_file_count == 0
    assert input_summary.analyzed_file_count == 1
    assert input_summary.skipped_file_count == 0
    assert stats.steps[4].details["log_inventory"] == 1


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
    assert stats.steps[0].details["cache_misses"] == 0
    assert stats.extraction_archive_stats[0].cache_status == "not_used"
    assert len(stats.input_source_summaries) == 1
    input_summary = stats.input_source_summaries[0]
    assert input_summary.source_file == str(archive_path)
    assert input_summary.input_kind == "archive"
    assert input_summary.log_types == ["bm"]
    assert input_summary.log_type_labels == ["БМ"]
    assert input_summary.log_type_counts == {"bm": 1}
    assert "bm" in input_summary.log_type_evidence
    assert input_summary.extracted_files == [str(extracted_dir / "logs.zip" / "nested" / "a.log")]
    assert input_summary.analyzed_files == [str(extracted_dir / "logs.zip" / "nested" / "a.log")]
    assert input_summary.archive_file_count == 1
    assert input_summary.log_file_count == 1
    assert input_summary.other_file_count == 0
    assert input_summary.extracted_file_count == 1
    assert input_summary.analyzed_file_count == 1
    assert input_summary.skipped_file_count == 0
    assert input_summary.skipped_reasons == {}


def test_pipeline_maps_nested_archives_to_top_level_input_summary(tmp_path):
    input_dir = tmp_path / "input"
    extracted_dir = tmp_path / "extracted"
    input_dir.mkdir()
    archive_path = input_dir / "logs.zip"
    bm_line = "2026-04-29 20:50:41.343 PaymentStart, resp: {Code:0 Message:OK} duration=412 ms p: mgt_nbs-oti-4.4.12"
    validator_line = "[2026-Jul-13 14:18:26.636522] [VALIDATOR] STARTED"
    nested_buffer = io.BytesIO()
    with zipfile.ZipFile(nested_buffer, "w") as nested:
        nested.writestr("bm/a.log", bm_line)
        nested.writestr("validator/start.log", validator_line)
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("nested.zip", nested_buffer.getvalue())

    events, result, stats = run_analysis(input_dir, extracted_dir=extracted_dir)

    assert len(events) == 1
    assert result.total == 1
    assert stats.input_files == [str(archive_path)]
    assert len(stats.input_source_summaries) == 1
    input_summary = stats.input_source_summaries[0]
    assert input_summary.source_file == str(archive_path)
    assert input_summary.input_kind == "archive"
    assert input_summary.log_types == ["bm", "validator_app"]
    assert input_summary.log_type_labels == ["БМ", "ПО валидатора"]
    assert input_summary.log_type_counts == {"bm": 1, "validator_app": 1}
    assert set(input_summary.log_type_evidence) == {"bm", "validator_app"}
    assert input_summary.extracted_files == [
        str(extracted_dir / "nested.zip" / "bm" / "a.log"),
        str(extracted_dir / "nested.zip" / "validator" / "start.log"),
    ]
    assert input_summary.archive_file_count == 2
    assert input_summary.log_file_count == 2
    assert input_summary.other_file_count == 0
    assert input_summary.extracted_file_count == 2
    assert input_summary.analyzed_file_count == 2
    assert input_summary.skipped_reasons == {}


def test_pipeline_preserves_nested_gzip_paths_with_same_file_names(tmp_path):
    input_dir = tmp_path / "input"
    extracted_dir = tmp_path / "extracted"
    input_dir.mkdir()
    archive_path = input_dir / "logs.zip"
    first_line = "2026-04-29 20:50:41.343 PaymentStart, resp: {Code:0 Message:OK} duration=412 ms p: mgt_nbs-oti-4.4.12"
    second_line = "2026-04-29 20:50:42.343 PaymentStart, resp: {Code:3 Message:Ошибка чтения карты} duration=512 ms p: mgt_nbs-tt-4.4.13"
    first_gz = io.BytesIO()
    second_gz = io.BytesIO()
    with gzip.GzipFile(fileobj=first_gz, mode="wb") as handle:
        handle.write((first_line + "\n").encode("utf-8"))
    with gzip.GzipFile(fileobj=second_gz, mode="wb") as handle:
        handle.write((second_line + "\n").encode("utf-8"))
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("device-a/logs/bm/bm-rotate.log.gz", first_gz.getvalue())
        archive.writestr("device-b/logs/bm/bm-rotate.log.gz", second_gz.getvalue())

    events, result, stats = run_analysis(input_dir, extracted_dir=extracted_dir)

    assert len(events) == 2
    assert result.total == 2
    assert stats.steps[2].details["scanned_files"] == 2
    assert stats.extracted_files == 2
    assert len(stats.extracted_file_paths) == 2
    assert len(stats.analyzed_files) == 2
    assert len({path for path in stats.analyzed_files}) == 2
    assert all("device-" in path for path in stats.analyzed_files)
    input_summary = stats.input_source_summaries[0]
    assert input_summary.extracted_file_count == 2
    assert input_summary.analyzed_file_count == 2
    assert input_summary.skipped_reasons == {}


def test_pipeline_extracts_and_scans_stdout_logs_without_extension(tmp_path):
    input_dir = tmp_path / "input"
    extracted_dir = tmp_path / "extracted"
    input_dir.mkdir()
    archive_path = input_dir / "logs.zip"
    bm_line = 'time="2026-07-15 10:00:00.000" level=info msg="PaymentStart, resp: 100ms, {Code:0 MessageRus:Проходите} tid: a, p: mgt_nbs-tt-4.5.13"'
    stopper_line = 'time="2026-07-15 10:00:01.000" level=info msg="readerConfiguration: ReaderConfiguration, req p: stopper-arm7_32-4.5.13"'
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("device/logs/bm-std/bm.20260715-100000-AAAA", bm_line + "\n")
        archive.writestr("device/logs/stopper-std/stopper.20260715-100001-BBBB", stopper_line + "\n")

    events, result, stats = run_analysis(input_dir, extracted_dir=extracted_dir)

    assert len(events) == 1
    assert result.total == 1
    assert stats.steps[2].details["scanned_files"] == 2
    assert len(stats.extracted_file_paths) == 2
    assert len(stats.analyzed_files) == 2
    input_summary = stats.input_source_summaries[0]
    assert input_summary.log_type_counts == {"bm": 1, "stopper": 1}
    assert input_summary.log_file_count == 2
    assert input_summary.extracted_file_count == 2
    assert input_summary.analyzed_file_count == 2
    assert input_summary.skipped_reasons == {}


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
    assert stats.analyzed_files == [str(extracted_dir / "broken.zip" / "broken.log.gz.log")]


def test_pipeline_reuses_archive_cache(tmp_path):
    input_dir = tmp_path / "input"
    extracted_dir = tmp_path / "extracted"
    cache_dir = tmp_path / "cache"
    input_dir.mkdir()
    archive_path = input_dir / "logs.zip"
    line = "2026-04-29 20:50:41.343 PaymentStart, resp: {Code:0 Message:OK} duration=412 ms p: mgt_nbs-oti-4.4.12"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("nested/a.log", line)

    run_analysis(input_dir, extracted_dir=extracted_dir, archive_cache_dir=cache_dir)
    events, result, stats = run_analysis(input_dir, extracted_dir=extracted_dir, archive_cache_dir=cache_dir)

    assert len(events) == 1
    assert result.total == 1
    assert stats.steps[0].details["cache_hits"] == 1
    assert stats.steps[0].details["cache_misses"] == 0
    assert stats.extraction_archive_stats[0].cache_status == "hit"

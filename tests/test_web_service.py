from fastapi.testclient import TestClient

from web.app import _index_html, create_app
from web.history import list_history, load_history_run, record_history
from web.service import AnalysisRequest, analyze_request, build_summary_snapshot


def _mock_upload_root(root):
    def _root(storage_dir=None):
        root.mkdir(parents=True, exist_ok=True)
        (root / "items").mkdir(parents=True, exist_ok=True)
        (root / "files").mkdir(parents=True, exist_ok=True)
        return root

    return _root


def _mock_history_root(root):
    def _root(storage_dir=None):
        root.mkdir(parents=True, exist_ok=True)
        (root / "runs").mkdir(parents=True, exist_ok=True)
        return root

    return _root


def test_web_snapshot_builds_from_core(tmp_path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    (input_dir / "sample.log").write_text(
        "2026-04-29 20:50:41.343 PaymentStart, resp: {Code:0 Message:OK} duration=412 ms p: mgt_nbs-oti-4.4.12\n",
        encoding="utf-8",
    )

    snapshot = analyze_request(
        AnalysisRequest(
            input_path=str(input_dir),
            extracted_dir=str(tmp_path / "extracted"),
            reports_dir=str(tmp_path / "reports"),
            generate_reports=False,
        )
    )

    assert snapshot.version
    assert snapshot.schema_version == "bm-log-analyzer.snapshot.v1"
    assert snapshot.analysis.total == 1
    assert snapshot.analysis.success_count == 1
    assert snapshot.archives.bm_logs == 0
    assert snapshot.pipeline.scanned_lines == 1
    assert snapshot.reports.written == []


def test_web_app_requires_optional_fastapi_dependency():
    app = create_app()
    assert app.title == "BM Log Analyzer"


def test_web_index_contains_upload_landing():
    html = _index_html()

    assert "BM Log Analyzer" in html
    assert "версия сервиса" in html
    assert "Выбрать файлы или папку" in html
    assert "Загрузить логи в хранилище" in html
    assert "made with ♥ by Roman A. Proskurnin" in html
    assert "progress_bar" in html
    assert "status_text" in html
    assert "webkitdirectory" in html
    assert "/api/uploads/store" in html
    assert "selection_summary" in html
    assert "Последние сессии" not in html


def test_uploads_page_contains_table_and_actions():
    client = TestClient(create_app())
    response = client.get("/uploads")

    assert response.status_code == 200
    html = response.text
    assert "Загрузки" in html
    assert "Сформировать отчёт по выбранным" in html
    assert "Выбрано 0" in html
    assert "uploads_body" in html
    assert "/api/uploads" in html
    assert "/uploads/download/" in html
    assert "Отчёт для каждой загрузки формируется сразу после приёма файла" in html


def test_web_history_roundtrip(tmp_path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    (input_dir / "sample.log").write_text(
        "2026-04-29 20:50:41.343 PaymentStart, resp: {Code:0 Message:OK} duration=412 ms p: mgt_nbs-oti-4.4.12\n",
        encoding="utf-8",
    )

    snapshot = analyze_request(
        AnalysisRequest(
            input_path=str(input_dir),
            extracted_dir=str(tmp_path / "extracted"),
            reports_dir=str(tmp_path / "reports"),
            generate_reports=False,
        )
    )
    summary = record_history(snapshot, mode="analysis", source="path", storage_dir=tmp_path / "history")

    items = list_history(tmp_path / "history", limit=5)
    assert len(items) == 1
    assert items[0].run_id == summary.run_id
    assert items[0].total == 1

    payload = load_history_run(summary.run_id, tmp_path / "history")
    assert payload["run_id"] == summary.run_id
    assert payload["snapshot"]["analysis"]["total"] == 1
    assert payload["report_path"] == ""
    assert payload["report_url"] == ""
    assert payload["manifest_url"] == ""


def test_web_upload_creates_report_page(tmp_path, monkeypatch):
    history_root = tmp_path / "history"
    upload_root = tmp_path / "uploads"
    monkeypatch.setattr("web.history._history_root", _mock_history_root(history_root))
    monkeypatch.setattr("web.uploads._upload_root", _mock_upload_root(upload_root))

    client = TestClient(create_app())
    store_response = client.post(
        "/api/uploads/store",
        files=[("files", ("sample.log", b"2026-04-29 20:50:41.343 PaymentStart, resp: {Code:0 Message:OK} duration=412 ms p: mgt_nbs-oti-4.4.12\n", "text/plain"))],
    )
    assert store_response.status_code == 200
    store_data = store_response.json()
    assert store_data["status"] == "ok"
    assert store_data["summary"]["uploaded_count"] == 1
    upload_id = store_data["items"][0]["upload_id"]
    assert store_data["items"][0]["report_url"].startswith("/report/")
    assert store_data["items"][0]["status"] == "ready"

    download_response = client.get(f"/uploads/download/{upload_id}")
    assert download_response.status_code == 200
    assert "PaymentStart" in download_response.text

    report_response = client.post(
        "/api/uploads/report",
        json={"upload_ids": [upload_id]},
    )
    assert report_response.status_code == 200
    report_data = report_response.json()
    assert report_data["status"] == "ok"
    assert report_data["report_url"].startswith("/report/")

    run_id = report_data["run_id"]
    report_response = client.get(f"/report/{run_id}")
    assert report_response.status_code == 200
    assert "BM Log Analyzer" in report_response.text

    report_manifest = client.get(f"/report/{run_id}/manifest")
    assert report_manifest.status_code == 200
    assert report_manifest.json()["report_type"] == "analysis_report"
    assert report_manifest.json()["schema_version"] == "bm-log-analyzer.analysis-report.v1"
    assert report_manifest.json()["stable_fields"] == [
        "schema_version",
        "report_type",
        "report_title",
        "generated_at",
        "version",
        "counts",
        "sections",
        "status_groups",
        "grouped_statuses",
        "log_groups",
        "other_groups",
        "validator_sections",
    ]
    assert report_manifest.json()["stable_sections"] == [
        "summary",
        "bm_meta",
        "log_files",
        "other_files",
        "bm_statuses",
        "grouped_statuses",
        "date_dynamics",
        "unclassified_diagnostics",
        "validator_analytics",
    ]

    run_detail = client.get(f"/api/runs/{run_id}")
    assert run_detail.status_code == 200
    assert run_detail.json()["report_url"] == f"/report/{run_id}"
    assert run_detail.json()["manifest_url"] == f"/report/{run_id}/manifest"
    assert run_detail.json()["snapshot"]["schema_version"] == "bm-log-analyzer.snapshot.v1"

    latest = client.get("/api/runs/latest")
    assert latest.status_code == 200
    assert latest.json()["run_id"] == run_id
    assert latest.json()["report_url"] == f"/report/{run_id}"
    assert latest.json()["manifest_url"] == f"/report/{run_id}/manifest"

    latest_report = client.get("/api/runs/latest/report")
    assert latest_report.status_code == 200
    assert "BM Log Analyzer" in latest_report.text

    latest_manifest = client.get("/api/runs/latest/manifest")
    assert latest_manifest.status_code == 200
    assert latest_manifest.json()["report_type"] == "analysis_report"
    assert latest_manifest.json()["schema_version"] == "bm-log-analyzer.analysis-report.v1"
    assert latest_manifest.json()["stable_fields"] == report_manifest.json()["stable_fields"]
    assert latest_manifest.json()["stable_sections"] == report_manifest.json()["stable_sections"]


def test_web_history_filter_by_mode(tmp_path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    (input_dir / "sample.log").write_text(
        "2026-04-29 20:50:41.343 PaymentStart, resp: {Code:0 Message:OK} duration=412 ms p: mgt_nbs-oti-4.4.12\n",
        encoding="utf-8",
    )

    snapshot = analyze_request(
        AnalysisRequest(
            input_path=str(input_dir),
            extracted_dir=str(tmp_path / "extracted"),
            reports_dir=str(tmp_path / "reports"),
            generate_reports=False,
        )
    )
    record_history(snapshot, mode="analysis", source="path", storage_dir=tmp_path / "history")
    record_history(snapshot, mode="summary", source="path", storage_dir=tmp_path / "history")

    items = list_history(tmp_path / "history", limit=10, mode="summary")
    assert len(items) == 1
    assert items[0].mode == "summary"


def test_web_history_query_sort_and_delete(tmp_path, monkeypatch):
    history_root = tmp_path / "history"
    monkeypatch.setattr("web.history._history_root", _mock_history_root(history_root))

    input_dir = tmp_path / "input"
    input_dir.mkdir()
    (input_dir / "sample.log").write_text(
        "2026-04-29 20:50:41.343 PaymentStart, resp: {Code:0 Message:OK} duration=412 ms p: mgt_nbs-oti-4.4.12\n",
        encoding="utf-8",
    )

    snapshot = analyze_request(
        AnalysisRequest(
            input_path=str(input_dir),
            extracted_dir=str(tmp_path / "extracted"),
            reports_dir=str(tmp_path / "reports"),
            generate_reports=False,
        )
    )
    old_item = record_history(
        snapshot,
        mode="analysis",
        source="path",
        created_at="2026-04-28T10:00:00+00:00",
        storage_dir=tmp_path / "history",
    )
    new_item = record_history(
        snapshot,
        mode="summary",
        source="upload",
        created_at="2026-04-29T10:00:00+00:00",
        storage_dir=tmp_path / "history",
    )

    asc_items = list_history(tmp_path / "history", limit=10, sort="asc")
    assert [item.run_id for item in asc_items] == [old_item.run_id, new_item.run_id]

    queried = list_history(tmp_path / "history", limit=10, query="upload")
    assert len(queried) == 1
    assert queried[0].run_id == new_item.run_id

    client = TestClient(create_app())
    delete_response = client.delete(f"/api/runs/{new_item.run_id}")
    assert delete_response.status_code == 200
    assert delete_response.json()["status"] == "ok"

    remaining = list_history(tmp_path / "history", limit=10)
    assert [item.run_id for item in remaining] == [old_item.run_id]


def test_web_summary_snapshot_is_compact(tmp_path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    (input_dir / "sample.log").write_text(
        "2026-04-29 20:50:41.343 PaymentStart, resp: {Code:0 Message:OK} duration=412 ms p: mgt_nbs-oti-4.4.12\n",
        encoding="utf-8",
    )

    snapshot = build_summary_snapshot(
        AnalysisRequest(
            input_path=str(input_dir),
            extracted_dir=str(tmp_path / "extracted"),
            reports_dir=str(tmp_path / "reports"),
            generate_reports=False,
        )
    )

    assert snapshot.stats is None
    assert snapshot.analysis.total == 1
    assert snapshot.archives.bm_logs == 0


def test_uploads_report_links_update(tmp_path, monkeypatch):
    history_root = tmp_path / "history"
    upload_root = tmp_path / "uploads"
    monkeypatch.setattr("web.history._history_root", _mock_history_root(history_root))
    monkeypatch.setattr("web.uploads._upload_root", _mock_upload_root(upload_root))

    client = TestClient(create_app())
    store_response = client.post(
        "/api/uploads/store",
        files=[("files", ("sample.log", b"2026-04-29 20:50:41.343 PaymentStart, resp: {Code:0 Message:OK} duration=412 ms p: mgt_nbs-oti-4.4.12\n", "text/plain"))],
    )
    upload_id = store_response.json()["items"][0]["upload_id"]

    report_response = client.post("/api/uploads/report", json={"upload_ids": [upload_id]})
    assert report_response.status_code == 200
    updated = client.get("/api/uploads")
    assert updated.status_code == 200
    assert updated.json()[0]["report_url"].startswith("/report/")
    assert updated.json()[0]["status"] == "ready"

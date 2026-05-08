from fastapi.testclient import TestClient

from web.app import _index_html, create_app
from web.history import list_history, load_history_run, record_history
from web.service import AnalysisRequest, analyze_request, build_summary_snapshot


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
    assert snapshot.analysis.total == 1
    assert snapshot.analysis.success_count == 1
    assert snapshot.archives.bm_logs == 0
    assert snapshot.pipeline.scanned_lines == 1
    assert snapshot.reports.written == []


def test_web_app_requires_optional_fastapi_dependency():
    app = create_app()
    assert app.title == "BM Log Analyzer"


def test_web_index_contains_analysis_form():
    html = _index_html()

    assert "Файлы или папка" in html
    assert "Загрузить" in html
    assert "Посмотреть отчёт" in html
    assert "/api/upload/analyze" in html
    assert "webkitdirectory" in html
    assert "progress_bar" in html
    assert "status_text" in html
    assert "Сессия загрузки завершена" in html
    assert "report_link" in html
    assert "Последние сессии" in html
    assert "/api/runs" in html
    assert "history_detail" in html


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


def test_web_upload_creates_report_page(tmp_path, monkeypatch):
    history_root = tmp_path / "history"
    monkeypatch.setattr("web.history._history_root", lambda storage_dir=None: history_root)

    client = TestClient(create_app())
    response = client.post(
        "/api/upload/analyze",
        files=[("files", ("sample.log", b"2026-04-29 20:50:41.343 PaymentStart, resp: {Code:0 Message:OK} duration=412 ms p: mgt_nbs-oti-4.4.12\n", "text/plain"))],
    )

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["report_url"].startswith("/report/")

    run_id = data["run_id"]
    report_response = client.get(f"/report/{run_id}")
    assert report_response.status_code == 200
    assert "BM Log Analyzer" in report_response.text

    latest = client.get("/api/runs/latest")
    assert latest.status_code == 200
    assert latest.json()["run_id"] == run_id


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

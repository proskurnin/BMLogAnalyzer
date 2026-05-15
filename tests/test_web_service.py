from fastapi.testclient import TestClient

from web.app import _index_html, create_app
from web.auth import DEFAULT_ADMIN_EMAIL, DEFAULT_ADMIN_PASSWORD
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


def _mock_auth_root(root):
    def _root(storage_dir=None):
        root.mkdir(parents=True, exist_ok=True)
        return root

    return _root


def _login(client):
    response = client.post(
        "/login",
        data={"email": DEFAULT_ADMIN_EMAIL, "password": DEFAULT_ADMIN_PASSWORD},
        follow_redirects=False,
    )
    assert response.status_code == 303


def _create_user(client, *, name="User", email="user@example.com", password="secret", role="user"):
    response = client.post(
        "/admin/users/create",
        data={"name": name, "email": email, "password": password, "role": role},
        follow_redirects=False,
    )
    assert response.status_code == 303


def _login_as(client, email, password):
    response = client.post("/login", data={"email": email, "password": password}, follow_redirects=False)
    assert response.status_code == 303


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


def test_web_requires_login_for_resource(tmp_path, monkeypatch):
    monkeypatch.setattr("web.auth._auth_root", _mock_auth_root(tmp_path / "auth"))
    client = TestClient(create_app())

    response = client.get("/", follow_redirects=False)
    assert response.status_code == 303
    assert response.headers["location"] == "/login"

    api_response = client.get("/api/uploads")
    assert api_response.status_code == 401


def test_login_page_contains_version_and_signature(tmp_path, monkeypatch):
    monkeypatch.setattr("web.auth._auth_root", _mock_auth_root(tmp_path / "auth"))
    client = TestClient(create_app())

    response = client.get("/login")

    assert response.status_code == 200
    assert "версия сервиса 1.2.5" in response.text
    assert 'class="brand"' in response.text
    assert "made with ♥ by Roman A. Proskurnin" in response.text


def test_production_requires_explicit_admin_credentials(tmp_path, monkeypatch):
    monkeypatch.setattr("web.auth._auth_root", _mock_auth_root(tmp_path / "auth"))
    monkeypatch.setenv("BM_APP_ENV", "production")
    monkeypatch.delenv("BM_ADMIN_EMAIL", raising=False)
    monkeypatch.delenv("BM_ADMIN_PASSWORD", raising=False)

    try:
        create_app()
    except RuntimeError as exc:
        assert "Production startup requires explicit" in str(exc)
    else:
        raise AssertionError("production app started without explicit admin credentials")


def test_upload_limits_reject_oversized_file(tmp_path, monkeypatch):
    history_root = tmp_path / "history"
    upload_root = tmp_path / "uploads"
    auth_root = tmp_path / "auth"
    monkeypatch.setattr("web.history._history_root", _mock_history_root(history_root))
    monkeypatch.setattr("web.uploads._upload_root", _mock_upload_root(upload_root))
    monkeypatch.setattr("web.auth._auth_root", _mock_auth_root(auth_root))
    monkeypatch.setenv("BM_MAX_UPLOAD_FILE_MB", "0")

    client = TestClient(create_app())
    _login(client)
    response = client.post(
        "/api/uploads/store",
        files=[("files", ("sample.log", b"x", "text/plain"))],
    )

    assert response.status_code == 413
    assert "Файл слишком большой" in response.json()["detail"]


def test_admin_and_user_navigation_by_role(tmp_path, monkeypatch):
    monkeypatch.setattr("web.auth._auth_root", _mock_auth_root(tmp_path / "auth"))
    client = TestClient(create_app())
    _login(client)
    _create_user(client)

    admin_home = client.get("/")
    assert admin_home.status_code == 200
    assert "История загрузок" in admin_home.text
    assert "Администрирование" in admin_home.text
    assert "Профиль (Administrator)" in admin_home.text
    assert "topbar-left" in admin_home.text
    assert "topbar-right" in admin_home.text
    assert '<span class="nav-separator">|</span>' in admin_home.text
    assert "menu-toggle" in admin_home.text
    assert 'data-active="true"' in admin_home.text

    client.get("/logout")
    _login_as(client, "user@example.com", "secret")
    user_home = client.get("/")
    assert user_home.status_code == 200
    assert "Загрузить логи в хранилище" in user_home.text
    assert "История загрузок" not in user_home.text
    assert "Администрирование" not in user_home.text
    assert "Профиль (User)" in user_home.text

    forbidden = client.get("/admin")
    assert forbidden.status_code == 403


def test_admin_user_management_and_profile_files(tmp_path, monkeypatch):
    history_root = tmp_path / "history"
    upload_root = tmp_path / "uploads"
    auth_root = tmp_path / "auth"
    monkeypatch.setattr("web.history._history_root", _mock_history_root(history_root))
    monkeypatch.setattr("web.uploads._upload_root", _mock_upload_root(upload_root))
    monkeypatch.setattr("web.auth._auth_root", _mock_auth_root(auth_root))
    monkeypatch.setenv("BM_DATA_DIR", str(tmp_path / "data"))

    client = TestClient(create_app())
    _login(client)
    admin_page = client.get("/admin")
    assert admin_page.status_code == 200
    assert "Пользователи" in admin_page.text
    assert "Политики хранения" in admin_page.text
    assert 'name="archive_retention_days"' in admin_page.text
    assert 'value="10"' in admin_page.text
    assert "Каталог проверок" in admin_page.text
    assert "repeat_after_failure_3s" in admin_page.text
    assert "Добавить проверку" in admin_page.text
    assert "Сообщение содержит" in admin_page.text
    assert "Правила применяются при формировании" in admin_page.text
    assert admin_page.text.index("<span>Пользователи</span>") < admin_page.text.index("<span>Политики хранения</span>")
    assert admin_page.text.index("<span>Политики хранения</span>") < admin_page.text.index("<span>Каталог проверок</span>")
    assert admin_page.text.count('<details class="admin-section" data-section-id=') == 3
    assert '<details class="admin-section" open>' not in admin_page.text
    assert 'data-section-id="users"' in admin_page.text
    assert 'data-section-id="storage"' in admin_page.text
    assert 'data-section-id="checks"' in admin_page.text
    assert "Срок хранения архивов, дни" in admin_page.text
    assert "Дата добавления (Мск)" in admin_page.text
    assert 'data-sort-key="name"' in admin_page.text
    assert 'data-sort-key="email"' in admin_page.text
    assert 'data-sort-key="created_at"' in admin_page.text
    assert "bm.admin.openSections" in admin_page.text
    assert "bm.admin.usersSort" in admin_page.text
    assert "section.querySelectorAll('form')" in admin_page.text
    assert "01.01.1970" not in admin_page.text
    create_check = client.post(
        "/admin/check-cases/create",
        data={
            "title": "Custom admin check",
            "description": "custom from admin",
            "condition_type": "message_contains",
            "condition_value": "timeout",
            "severity": "warning",
            "enabled": "on",
        },
        follow_redirects=False,
    )
    assert create_check.status_code == 303
    custom_admin_page = client.get("/admin")
    assert "Custom admin check" in custom_admin_page.text
    assert "custom from admin" in custom_admin_page.text
    update_check = client.post(
        "/admin/check-cases/update",
        data={
            "check_id": "technical_error_code_3",
            "title": "Ошибка чтения карты custom",
            "description": "custom description",
            "severity": "critical",
        },
        follow_redirects=False,
    )
    assert update_check.status_code == 303
    updated_admin_page = client.get("/admin")
    assert "Ошибка чтения карты custom" in updated_admin_page.text
    assert "custom description" in updated_admin_page.text
    assert "critical" in updated_admin_page.text
    reset_checks = client.post("/admin/check-cases/reset", follow_redirects=False)
    assert reset_checks.status_code == 303
    _create_user(client, name="Operator", email="operator@example.com", password="secret", role="user")

    client.get("/logout")
    _login_as(client, "operator@example.com", "secret")
    store_response = client.post(
        "/api/uploads/store",
        files=[("files", ("operator.log", b"2026-04-29 20:50:41.343 PaymentStart, resp: {Code:0 Message:OK}\n", "text/plain"))],
    )
    assert store_response.status_code == 200

    profile = client.get("/profile")
    assert profile.status_code == 200
    assert "operator.log" in profile.text
    assert "Сменить пароль" in profile.text
    assert "Сменить имя" in profile.text

    update_name = client.post("/profile/name", data={"name": "Operator Renamed"}, follow_redirects=False)
    assert update_name.status_code == 303
    renamed = client.get("/profile")
    assert "Профиль (Operator Renamed)" in renamed.text


def test_web_index_contains_upload_landing():
    html = _index_html()

    assert "BM Log Analyzer" in html
    assert "версия сервиса 1.2.5" in html
    assert "picker_menu" not in html
    assert "Выбрать файлы</button>" not in html
    assert "Выбрать папку</button>" not in html
    assert "Загрузить логи в хранилище" in html
    assert "made with ♥ by Roman A. Proskurnin" in html
    assert 'class="upload-center"' in html
    assert "</section>\n        <footer>made with ♥ by Roman A. Proskurnin</footer>" in html
    assert "progress_bar" in html
    assert "status_text" in html
    assert 'accept=".log,.gz,.zip,.tar.gz,.tgz,.rar"' in html
    assert "dropzone" in html
    assert "preparedFiles" in html
    assert "и ещё" not in html
    assert "/api/uploads/store" in html
    assert "selection_summary" in html
    assert "renderUploadComplete" in html
    assert "Открыть отчёт" in html
    assert "Перейти в загрузки" in html
    assert "safeReportUrl" in html
    assert 'id="message_actions"' in html
    assert ".message-actions { display: none; grid-template-columns: repeat(2, minmax(0, 1fr));" in html
    assert "message.textContent = uploadMessage(summary, clientRejectedCount);" in html
    assert "messageActions.dataset.visible = actions ? 'true' : 'false';" in html
    assert "message.innerHTML = `<div>${escapeHtml(uploadMessage" not in html
    assert "Последние сессии" not in html


def test_uploads_page_contains_table_and_actions():
    client = TestClient(create_app())
    _login(client)
    response = client.get("/uploads")

    assert response.status_code == 200
    html = response.text
    assert "Загрузки" in html
    assert "<strong>BM Log Analyzer</strong><span>версия сервиса 1.2.5</span>" in html
    assert "BM Log Analyzer ·" not in html
    assert "Сформировать отчёт по выбранным" in html
    assert "Дата загрузки (Мск)" in html
    assert "formatMoscowDateTime(item.created_at)" in html
    assert "timeZone: 'Europe/Moscow'" in html
    assert "Пользователь" in html
    assert "Размер" in html
    assert html.index("Размер") < html.index("Отчёт")
    assert "formatUploadSize(item.size_bytes)" in html
    assert "window.location.href = data.report_url" in html
    assert "Выбрано 0" in html
    assert "uploads_body" in html
    assert "/api/uploads" in html
    assert "/rebuild" in html
    assert "пересобрать отчёт" in html
    assert "data-rebuild-upload-id" in html
    assert "data-report-cell" in html
    assert "item.download_url" in html
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
    auth_root = tmp_path / "auth"
    monkeypatch.setattr("web.history._history_root", _mock_history_root(history_root))
    monkeypatch.setattr("web.uploads._upload_root", _mock_upload_root(upload_root))
    monkeypatch.setattr("web.auth._auth_root", _mock_auth_root(auth_root))

    client = TestClient(create_app())
    _login(client)
    store_response = client.post(
        "/api/uploads/store",
        files=[("files", ("sample.log", b"2026-04-29 20:50:41.343 PaymentStart, resp: {Code:0 Message:OK} duration=412 ms p: mgt_nbs-oti-4.4.12\n", "text/plain"))],
    )
    assert store_response.status_code == 200
    store_data = store_response.json()
    assert store_data["status"] == "ok"
    assert store_data["summary"]["uploaded_count"] == 1
    assert store_data["run_id"]
    assert store_data["report_url"].startswith("/report/")
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
    assert "Профиль (Administrator)" in report_response.text
    assert "bm-auth-topbar" in report_response.text
    assert "AI-аналитика" in report_response.text
    assert f"/api/runs/{run_id}/ai-analysis" in report_response.text
    assert "formatMoscowDateTime(payload.generated_at)" in report_response.text
    assert 'timeZone: "Europe/Moscow"' in report_response.text
    assert "(Мск)" in report_response.text
    assert "Повторить AI-анализ" in report_response.text
    assert "Обновить статус" in report_response.text
    assert "Что проверить" in report_response.text
    assert "Ограничения" in report_response.text
    assert ".bm-auth-topbar { width: 100%; margin: 0 auto; padding: 24px 24px 0; display: grid; justify-items: center; }" in report_response.text

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
        "suspicious_lines",
        "validation_checks",
    ]
    stable_sections = report_manifest.json()["stable_sections"]
    assert "summary" in stable_sections
    assert "bm_meta" in stable_sections
    assert "suspicious" not in stable_sections
    assert "bm_statuses" in stable_sections
    assert "validator_analytics" in stable_sections
    assert "log_files" not in stable_sections
    assert "other_files" not in stable_sections

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

    ai_status = client.get(f"/api/runs/{run_id}/ai-analysis")
    assert ai_status.status_code == 200
    assert ai_status.json()["status"] == "not_started"
    assert ai_status.json()["enabled"] is False

    ai_start = client.post(f"/api/runs/{run_id}/ai-analysis")
    assert ai_start.status_code == 400
    assert "AI-анализ выключен" in ai_start.json()["detail"]


def test_web_upload_session_report_combines_uploaded_files(tmp_path, monkeypatch):
    history_root = tmp_path / "history"
    upload_root = tmp_path / "uploads"
    auth_root = tmp_path / "auth"
    monkeypatch.setattr("web.history._history_root", _mock_history_root(history_root))
    monkeypatch.setattr("web.uploads._upload_root", _mock_upload_root(upload_root))
    monkeypatch.setattr("web.auth._auth_root", _mock_auth_root(auth_root))

    client = TestClient(create_app())
    _login(client)
    store_response = client.post(
        "/api/uploads/store",
        files=[
            (
                "files",
                (
                    "first.log",
                    b"2026-04-29 20:50:41.343 PaymentStart, resp: {Code:0 Message:SESSION_OK} duration=412 ms p: mgt_nbs-oti-4.4.12\n",
                    "text/plain",
                ),
            ),
            (
                "files",
                (
                    "second.log",
                    b"2026-04-29 20:50:42.343 PaymentStart, resp: {Code:3 Message:SESSION_ERROR} duration=812 ms p: mgt_nbs-tt-4.4.13\n",
                    "text/plain",
                ),
            ),
        ],
    )

    assert store_response.status_code == 200
    data = store_response.json()
    assert data["summary"]["uploaded_count"] == 2
    assert data["report_url"].startswith("/report/")
    assert len(data["report_updates"]) == 2
    assert data["report_url"] not in {item["report_url"] for item in data["report_updates"]}

    report_response = client.get(data["report_url"])
    assert report_response.status_code == 200
    assert "SESSION_OK" in report_response.text
    assert "SESSION_ERROR" in report_response.text


def test_web_upload_rejects_unsupported_files_before_storage(tmp_path, monkeypatch):
    history_root = tmp_path / "history"
    upload_root = tmp_path / "uploads"
    auth_root = tmp_path / "auth"
    monkeypatch.setattr("web.history._history_root", _mock_history_root(history_root))
    monkeypatch.setattr("web.uploads._upload_root", _mock_upload_root(upload_root))
    monkeypatch.setattr("web.auth._auth_root", _mock_auth_root(auth_root))

    client = TestClient(create_app())
    _login(client)
    store_response = client.post(
        "/api/uploads/store",
        files=[
            ("files", ("sample.log", b"2026-04-29 20:50:41.343 PaymentStart, resp: {Code:0 Message:OK}\n", "text/plain")),
            ("files", ("notes.txt", b"not a log upload source", "text/plain")),
        ],
    )

    assert store_response.status_code == 200
    data = store_response.json()
    assert data["summary"]["uploaded_count"] == 1
    assert data["summary"]["rejected_count"] == 1
    assert data["rejected_files"] == ["notes.txt"]
    assert "1 файл не загружен" in data["summary"]["message"]
    assert len(list((upload_root / "items").glob("*.json"))) == 1


def test_web_upload_summary_omits_zero_rejections(tmp_path, monkeypatch):
    history_root = tmp_path / "history"
    upload_root = tmp_path / "uploads"
    auth_root = tmp_path / "auth"
    monkeypatch.setattr("web.history._history_root", _mock_history_root(history_root))
    monkeypatch.setattr("web.uploads._upload_root", _mock_upload_root(upload_root))
    monkeypatch.setattr("web.auth._auth_root", _mock_auth_root(auth_root))

    client = TestClient(create_app())
    _login(client)
    store_response = client.post(
        "/api/uploads/store",
        files=[("files", ("sample.log", b"2026-04-29 20:50:41.343 PaymentStart, resp: {Code:0 Message:OK}\n", "text/plain"))],
    )

    assert store_response.status_code == 200
    message = store_response.json()["summary"]["message"]
    assert "не загруж" not in message
    assert message.endswith("Загрузка прошла без ошибок. Спасибо.")


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
    auth_root = tmp_path / "auth"
    monkeypatch.setattr("web.history._history_root", _mock_history_root(history_root))
    monkeypatch.setattr("web.auth._auth_root", _mock_auth_root(auth_root))

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
    _login(client)
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
    auth_root = tmp_path / "auth"
    monkeypatch.setattr("web.history._history_root", _mock_history_root(history_root))
    monkeypatch.setattr("web.uploads._upload_root", _mock_upload_root(upload_root))
    monkeypatch.setattr("web.auth._auth_root", _mock_auth_root(auth_root))

    client = TestClient(create_app())
    _login(client)
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


def test_upload_rebuild_report_refreshes_existing_upload(tmp_path, monkeypatch):
    history_root = tmp_path / "history"
    upload_root = tmp_path / "uploads"
    auth_root = tmp_path / "auth"
    monkeypatch.setattr("web.history._history_root", _mock_history_root(history_root))
    monkeypatch.setattr("web.uploads._upload_root", _mock_upload_root(upload_root))
    monkeypatch.setattr("web.auth._auth_root", _mock_auth_root(auth_root))

    client = TestClient(create_app())
    _login(client)
    store_response = client.post(
        "/api/uploads/store",
        files=[("files", ("sample.log", b"2026-04-29 20:50:41.343 PaymentStart, resp: {Code:0 Message:OK} duration=412 ms p: mgt_nbs-oti-4.4.12\n", "text/plain"))],
    )
    assert store_response.status_code == 200
    upload_id = store_response.json()["items"][0]["upload_id"]
    old_report_url = store_response.json()["items"][0]["report_url"]

    rebuild_response = client.post(f"/api/uploads/{upload_id}/rebuild")

    assert rebuild_response.status_code == 200
    payload = rebuild_response.json()
    assert payload["status"] == "ok"
    assert payload["message"] == "Отчёт пересобран."
    assert payload["report_url"].startswith("/report/")
    assert payload["report_url"] != old_report_url
    assert payload["item"]["status"] == "ready"
    assert payload["item"]["report_url"] == payload["report_url"]

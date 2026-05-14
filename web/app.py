from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any
from html import escape
from analytics.ai_analysis import ai_analysis_enabled, run_ai_analysis
from analytics.check_cases import BUILTIN_CHECKS
from core.verification import run_healthchecks, run_readiness_check
from core.version import format_version
from dataclasses import asdict

from web.auth import (
    SESSION_COOKIE,
    authenticate_user,
    cleanup_expired_sessions,
    create_session,
    create_user,
    delete_user,
    destroy_session,
    ensure_default_admin,
    get_user,
    list_users,
    update_user,
    user_from_session,
)
from web.history import delete_history_run, latest_history, list_history, load_history_run, new_run_id, record_history, run_directory, run_report_path
from web.service import AnalysisRequest, analyze_request, execute_uploaded_path_analysis, build_summary_snapshot
from web.settings import load_settings, require_production_bootstrap_settings
from web.retention import cleanup_expired_storage, cleanup_expired_storage_if_due, load_storage_policy, update_storage_policy
from web.uploads import (
    allocate_upload_path,
    collect_upload_paths,
    delete_upload,
    is_allowed_upload_name,
    list_uploads,
    load_upload,
    save_upload_item,
    summary_from_uploads,
    update_upload_status,
    update_upload_reports,
)

try:  # pragma: no cover - optional dependency
    from fastapi import FastAPI, File, Form, Request, UploadFile
    from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
except ImportError:  # pragma: no cover - optional dependency
    FastAPI = None  # type: ignore[assignment]
    File = None  # type: ignore[assignment]
    Form = None  # type: ignore[assignment]
    Request = None  # type: ignore[assignment]
    UploadFile = None  # type: ignore[assignment]
    FileResponse = None  # type: ignore[assignment]
    HTMLResponse = None  # type: ignore[assignment]
    JSONResponse = None  # type: ignore[assignment]
    RedirectResponse = None  # type: ignore[assignment]


def create_app() -> Any:
    if FastAPI is None or File is None or Form is None or Request is None or UploadFile is None or HTMLResponse is None or JSONResponse is None or FileResponse is None or RedirectResponse is None:  # pragma: no cover - optional dependency
        exc = ImportError("fastapi")
        raise RuntimeError("FastAPI is not installed. Install fastapi and uvicorn to use the web app.") from exc

    settings = load_settings()
    require_production_bootstrap_settings(settings)
    ensure_default_admin()
    cleanup_expired_sessions()
    cleanup_expired_storage()
    app = FastAPI(title="BM Log Analyzer", version=format_version())

    @app.middleware("http")
    async def require_auth(request: Request, call_next):
        cleanup_expired_storage_if_due()
        path = request.url.path
        if path in {"/login", "/health"}:
            return await call_next(request)
        user = user_from_session(request.cookies.get(SESSION_COOKIE))
        request.state.user = user
        if user is None:
            if path.startswith("/api/") or request.method != "GET":
                return JSONResponse({"detail": "Требуется авторизация"}, status_code=401)
            return RedirectResponse("/login", status_code=303)
        if _is_admin_path(path) and user.role != "admin":
            return HTMLResponse("<h1>Доступ запрещён</h1>", status_code=403)
        return await call_next(request)

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request) -> str:
        return _landing_html(_request_user(request))

    @app.get("/login", response_class=HTMLResponse)
    def login_page() -> str:
        return _login_html()

    @app.post("/login")
    def login(email: str = Form(...), password: str = Form(...)):
        user = authenticate_user(email, password)
        if user is None:
            return HTMLResponse(_login_html("Неверный email или пароль."), status_code=401)
        response = RedirectResponse("/", status_code=303)
        response.set_cookie(
            SESSION_COOKIE,
            create_session(user.email),
            httponly=True,
            secure=load_settings().cookie_secure,
            samesite="lax",
        )
        return response

    @app.get("/logout")
    def logout(request: Request):
        destroy_session(request.cookies.get(SESSION_COOKIE, ""))
        response = RedirectResponse("/login", status_code=303)
        response.delete_cookie(SESSION_COOKIE)
        return response

    @app.get("/uploads", response_class=HTMLResponse)
    def uploads_page(request: Request) -> str:
        return _uploads_html(_request_user(request))

    @app.get("/adnin")
    def adnin_redirect():
        return RedirectResponse("/admin", status_code=303)

    @app.get("/admin", response_class=HTMLResponse)
    def admin_page(request: Request) -> str:
        return _admin_html(_request_user(request))

    @app.post("/admin/users/create")
    def admin_create_user(
        name: str = Form(...),
        email: str = Form(...),
        password: str = Form(...),
        role: str = Form(...),
    ):
        try:
            create_user(name=name, email=email, password=password, role=role)
        except ValueError as exc:
            return HTMLResponse(_admin_html(None, str(exc)), status_code=400)
        return RedirectResponse("/admin", status_code=303)

    @app.post("/admin/users/update")
    def admin_update_user(
        email: str = Form(...),
        name: str = Form(...),
        new_email: str = Form(...),
        password: str = Form(""),
        role: str = Form(...),
    ):
        try:
            update_user(email, name=name, new_email=new_email, password=password, role=role)
        except ValueError as exc:
            return HTMLResponse(_admin_html(None, str(exc)), status_code=400)
        return RedirectResponse("/admin", status_code=303)

    @app.post("/admin/users/delete")
    def admin_delete_user(email: str = Form(...)):
        try:
            delete_user(email)
        except ValueError as exc:
            return HTMLResponse(_admin_html(None, str(exc)), status_code=400)
        return RedirectResponse("/admin", status_code=303)

    @app.post("/admin/settings")
    def admin_update_settings(archive_retention_days: int = Form(...)):
        try:
            update_storage_policy(archive_retention_days=archive_retention_days)
            cleanup_expired_storage()
        except ValueError as exc:
            return HTMLResponse(_admin_html(None, str(exc)), status_code=400)
        return RedirectResponse("/admin", status_code=303)

    @app.get("/profile", response_class=HTMLResponse)
    def profile_page(request: Request) -> str:
        return _profile_html(_request_user(request))

    @app.post("/profile/name")
    def profile_update_name(request: Request, name: str = Form(...)):
        user = _request_user(request)
        update_user(user.email, name=name, new_email=user.email, password="", role=user.role)
        return RedirectResponse("/profile", status_code=303)

    @app.post("/profile/password")
    def profile_update_password(request: Request, password: str = Form(...)):
        user = _request_user(request)
        update_user(user.email, name=user.name, new_email=user.email, password=password, role=user.role)
        return RedirectResponse("/profile", status_code=303)

    @app.post("/profile/uploads/{upload_id}/delete")
    def profile_delete_upload(request: Request, upload_id: str):
        user = _request_user(request)
        item = load_upload(upload_id)
        if item.owner_email != user.email and user.role != "admin":
            return HTMLResponse("<h1>Доступ запрещён</h1>", status_code=403)
        delete_upload(upload_id)
        return RedirectResponse("/profile", status_code=303)

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"status": "ok", "version": format_version()}

    @app.get("/healthchecks")
    def healthchecks() -> list[dict[str, str]]:
        return [outcome.__dict__ for outcome in run_healthchecks()]

    @app.get("/readiness")
    def readiness() -> list[dict[str, str]]:
        return [outcome.__dict__ for outcome in run_readiness_check()]

    @app.get("/report/{run_id}", response_class=HTMLResponse)
    def report(request: Request, run_id: str):
        access_error = _report_access_error(run_id, _request_user(request))
        if access_error:
            return access_error
        return _render_report(run_id, _request_user(request))

    @app.get("/report/{run_id}/manifest", response_class=JSONResponse)
    def report_manifest(request: Request, run_id: str):
        access_error = _report_access_error(run_id, _request_user(request), json_response=True)
        if access_error:
            return access_error
        return _render_report_manifest(run_id)

    @app.get("/api/runs/{run_id}/ai-analysis")
    def get_ai_analysis(request: Request, run_id: str) -> dict[str, Any]:
        access_error = _report_access_error(run_id, _request_user(request), json_response=True)
        if access_error:
            return access_error
        report_path = _report_file_for_run(run_id)
        if report_path is None:
            return JSONResponse({"detail": "Отчёт не найден"}, status_code=404)
        result_path = report_path.with_suffix(".ai.json")
        if result_path.exists():
            return json.loads(result_path.read_text(encoding="utf-8"))
        return {
            "status": "not_started",
            "enabled": ai_analysis_enabled(),
            "detail": "AI-анализ ещё не запускался.",
        }

    @app.post("/api/runs/{run_id}/ai-analysis")
    def start_ai_analysis(request: Request, run_id: str) -> dict[str, Any]:
        access_error = _report_access_error(run_id, _request_user(request), json_response=True)
        if access_error:
            return access_error
        report_path = _report_file_for_run(run_id)
        if report_path is None:
            return JSONResponse({"detail": "Отчёт не найден"}, status_code=404)
        context_path = report_path.with_suffix(".ai_context.json")
        if not context_path.exists():
            return JSONResponse({"detail": "AI context не найден для этого отчёта"}, status_code=404)
        context = json.loads(context_path.read_text(encoding="utf-8"))
        try:
            result = run_ai_analysis(context)
        except RuntimeError as exc:
            return JSONResponse({"detail": str(exc)}, status_code=400)
        result_path = report_path.with_suffix(".ai.json")
        result_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        return result

    @app.get("/api/runs/latest/report", response_class=HTMLResponse)
    def latest_report(request: Request):
        user = _request_user(request)
        owner = None if user.role == "admin" else user.email
        item = latest_history(owner_email=owner)
        if not item:
            return HTMLResponse("<h1>Отчёт не найден</h1>", status_code=404)
        return _render_report(item.run_id, _request_user(request))

    @app.get("/api/runs/latest/manifest", response_class=JSONResponse)
    def latest_manifest(request: Request):
        user = _request_user(request)
        owner = None if user.role == "admin" else user.email
        item = latest_history(owner_email=owner)
        if not item:
            return JSONResponse({"detail": "Отчёт не найден"}, status_code=404)
        return _render_report_manifest(item.run_id)

    @app.post("/api/analyze")
    def analyze(request: Request, payload: dict[str, Any]) -> dict[str, Any]:
        user = _request_user(request)
        request = AnalysisRequest(
            input_path=payload.get("input_path"),
            config_path=str(payload.get("config_path", "./config/config.yaml")),
            extracted_dir=payload.get("extracted_dir"),
            reports_dir=payload.get("reports_dir"),
            date=payload.get("date"),
            reader=payload.get("reader"),
            bm=payload.get("bm"),
            generate_reports=bool(payload.get("generate_reports", False)),
        )
        snapshot = analyze_request(request)
        report_path = Path(request.reports_dir) / "analysis_report.html" if request.generate_reports else None
        record_history(
            snapshot,
            mode="analysis",
            source="path",
            report_path=report_path,
            owner_email=user.email,
            owner_name=user.name,
        )
        return asdict(snapshot)

    @app.post("/api/summary")
    def summary(request: Request, payload: dict[str, Any]) -> dict[str, Any]:
        user = _request_user(request)
        request = AnalysisRequest(
            input_path=payload.get("input_path"),
            config_path=str(payload.get("config_path", "./config/config.yaml")),
            extracted_dir=payload.get("extracted_dir"),
            reports_dir=payload.get("reports_dir"),
            date=payload.get("date"),
            reader=payload.get("reader"),
            bm=payload.get("bm"),
            generate_reports=False,
        )
        snapshot = build_summary_snapshot(request)
        record_history(snapshot, mode="summary", source="path", owner_email=user.email, owner_name=user.name)
        return asdict(snapshot)

    @app.post("/api/upload/analyze")
    async def upload_analyze(
        request: Request,
        files: list[UploadFile] = File(...),
    ) -> dict[str, Any]:
        user = _request_user(request)
        run_id = new_run_id()
        report_root = run_directory(run_id)
        staged_files = await _spool_upload_files(files, report_root / "received")
        request = AnalysisRequest(
            config_path="./config/config.yaml",
            reports_dir=str(report_root),
            extracted_dir=None,
            date=None,
            reader=None,
            bm=None,
            generate_reports=False,
        )
        bundle = execute_uploaded_path_analysis(request, staged_files, summary=False, storage_dir=report_root)
        report_path = run_report_path(run_id)
        from reports.html_report import write_html_report

        write_html_report(bundle.events, bundle.result, report_path, stats=bundle.stats)
        record_history(
            bundle.snapshot,
            mode="analysis",
            source="upload",
            run_id=run_id,
            report_path=report_path,
            owner_email=user.email,
            owner_name=user.name,
        )
        return {
            "run_id": run_id,
            "status": "ok",
            "report_url": f"/report/{run_id}",
            "report_path": str(report_path),
            "snapshot": asdict(bundle.snapshot),
        }

    @app.post("/api/upload/summary")
    async def upload_summary(
        request: Request,
        files: list[UploadFile] = File(...),
    ) -> dict[str, Any]:
        user = _request_user(request)
        run_id = new_run_id()
        report_root = run_directory(run_id)
        staged_files = await _spool_upload_files(files, report_root / "received")
        request = AnalysisRequest(
            config_path="./config/config.yaml",
            reports_dir=str(report_root),
            extracted_dir=None,
            date=None,
            reader=None,
            bm=None,
            generate_reports=False,
        )
        bundle = execute_uploaded_path_analysis(request, staged_files, summary=True, storage_dir=report_root)
        report_path = run_report_path(run_id)
        from reports.html_report import write_html_report

        write_html_report(bundle.events, bundle.result, report_path, stats=bundle.stats)
        record_history(
            bundle.snapshot,
            mode="summary",
            source="upload",
            run_id=run_id,
            report_path=report_path,
            owner_email=user.email,
            owner_name=user.name,
        )
        return {
            "run_id": run_id,
            "status": "ok",
            "report_url": f"/report/{run_id}",
            "report_path": str(report_path),
            "snapshot": asdict(bundle.snapshot),
        }

    @app.get("/api/runs")
    def runs(request: Request, limit: int = 12, mode: str | None = None) -> list[dict[str, Any]]:
        user = _request_user(request)
        owner = None if user.role == "admin" else user.email
        return [asdict(item) for item in list_history(limit=limit, mode=mode, owner_email=owner)]

    @app.get("/api/runs/latest")
    def latest_run(request: Request) -> dict[str, Any]:
        user = _request_user(request)
        owner = None if user.role == "admin" else user.email
        item = latest_history(owner_email=owner)
        return asdict(item) if item else {}

    @app.get("/api/runs/{run_id}")
    def run_detail(request: Request, run_id: str) -> dict[str, Any]:
        access_error = _report_access_error(run_id, _request_user(request), json_response=True)
        if access_error:
            return access_error
        return load_history_run(run_id)

    @app.delete("/api/runs/{run_id}")
    def delete_run(request: Request, run_id: str) -> dict[str, Any]:
        access_error = _report_access_error(run_id, _request_user(request), json_response=True)
        if access_error:
            return access_error
        if not delete_history_run(run_id):
            return {"detail": "Сессия не найдена"}
        return {"status": "ok", "run_id": run_id}

    @app.get("/api/uploads")
    def uploads(request: Request, limit: int = 200) -> list[dict[str, Any]]:
        user = _request_user(request)
        owner = None if user.role == "admin" else user.email
        return [asdict(item) for item in list_uploads(limit=limit, owner_email=owner)]

    @app.post("/api/uploads/store")
    async def store(request: Request, files: list[UploadFile] = File(...)) -> dict[str, Any]:
        user = _request_user(request)
        stored, rejected_files, limit_error = await _store_uploads_streaming(files, owner_email=user.email, owner_name=user.name)
        if limit_error:
            return JSONResponse({"detail": limit_error}, status_code=413)
        summary = summary_from_uploads(stored, rejected_count=len(rejected_files))
        report_updates: list[dict[str, Any]] = []
        for item in stored:
            update_upload_status(item.upload_id, status="processing", status_message="Формируем отчёт", storage_dir=None)
            report_updates.append(_build_upload_report(item, user))
        session_report = _upload_session_report(stored, report_updates, user)
        refreshed_items = [asdict(load_upload(item.upload_id)) for item in stored]
        return {
            "status": "ok",
            "summary": summary,
            "items": refreshed_items,
            "report_updates": report_updates,
            "run_id": session_report.get("run_id", ""),
            "report_url": session_report.get("report_url", ""),
            "rejected_files": rejected_files,
            "message": summary["message"],
        }

    @app.post("/api/uploads/report")
    async def uploads_report(request: Request, payload: dict[str, Any]) -> dict[str, Any]:
        user = _request_user(request)
        upload_ids = [str(item) for item in payload.get("upload_ids", []) if str(item).strip()]
        if not upload_ids:
            return JSONResponse({"detail": "Не выбраны загрузки"}, status_code=400)
        selected_files = collect_upload_paths(upload_ids)
        if not selected_files:
            return JSONResponse({"detail": "Выбранные загрузки не найдены"}, status_code=404)
        if user.role != "admin":
            for upload_id in upload_ids:
                if load_upload(upload_id).owner_email != user.email:
                    return JSONResponse({"detail": "Доступ запрещён"}, status_code=403)
        run_id = new_run_id()
        staging_dir = run_directory(run_id)
        request = AnalysisRequest(
            config_path="./config/config.yaml",
            reports_dir=str(staging_dir),
            date=None,
            reader=None,
            bm=None,
            generate_reports=False,
        )
        bundle = execute_uploaded_path_analysis(request, selected_files, summary=False, storage_dir=staging_dir)
        report_path = run_report_path(run_id)
        from reports.html_report import write_html_report

        write_html_report(bundle.events, bundle.result, report_path, stats=bundle.stats)
        record_history(
            bundle.snapshot,
            mode="analysis",
            source="uploads",
            run_id=run_id,
            report_path=report_path,
            owner_email=user.email,
            owner_name=user.name,
        )
        update_upload_reports(upload_ids, report_run_id=run_id, report_url=f"/report/{run_id}")
        return {
            "status": "ok",
            "run_id": run_id,
            "report_url": f"/report/{run_id}",
            "message": f"Отчёт сформирован для {len(upload_ids)} файлов.",
        }

    @app.post("/api/uploads/{upload_id}/rebuild")
    def rebuild_upload_report(request: Request, upload_id: str) -> dict[str, Any]:
        user = _request_user(request)
        try:
            item = load_upload(upload_id)
        except FileNotFoundError:
            return JSONResponse({"detail": "Загрузка не найдена"}, status_code=404)
        if item.owner_email != user.email and user.role != "admin":
            return JSONResponse({"detail": "Доступ запрещён"}, status_code=403)
        if not Path(item.stored_path).exists():
            update_upload_status(upload_id, status="error", status_message="Исходный файл удалён")
            return JSONResponse({"detail": "Исходный файл удалён"}, status_code=404)

        update_upload_status(upload_id, status="processing", status_message="Формируем отчёт")
        try:
            report = _build_upload_report(item, user)
        except Exception as exc:
            update_upload_status(upload_id, status="error", status_message="Ошибка формирования отчёта")
            return JSONResponse({"detail": f"Не удалось сформировать отчёт: {exc}"}, status_code=500)
        refreshed = asdict(load_upload(upload_id))
        return {
            "status": "ok",
            "item": refreshed,
            "run_id": report["run_id"],
            "report_url": report["report_url"],
            "message": "Отчёт пересобран.",
        }

    @app.get("/uploads/download/{upload_id}")
    def upload_download(request: Request, upload_id: str):
        user = _request_user(request)
        item = load_upload(upload_id)
        if item.owner_email != user.email and user.role != "admin":
            return HTMLResponse("<h1>Доступ запрещён</h1>", status_code=403)
        path = Path(item.stored_path)
        if not path.exists():
            return JSONResponse({"detail": "Файл не найден"}, status_code=404)
        filename = Path(item.original_name).name or path.name
        return FileResponse(path, filename=filename)

    return app


def _render_report(run_id: str, user=None):
    report_path = run_report_path(run_id)
    if report_path.exists():
        return HTMLResponse(_inject_report_topbar(report_path.read_text(encoding="utf-8"), user, run_id=run_id))
    payload = load_history_run(run_id)
    payload_report_path = payload.get("report_path") or ""
    if payload_report_path:
        file_path = Path(payload_report_path)
        if file_path.exists():
            return HTMLResponse(_inject_report_topbar(file_path.read_text(encoding="utf-8"), user, run_id=run_id))
    return HTMLResponse("<h1>Отчёт не найден</h1>", status_code=404)


def _render_report_manifest(run_id: str):
    report_path = run_report_path(run_id)
    manifest_path = report_path.with_suffix(".json")
    if manifest_path.exists():
        return JSONResponse(json.loads(manifest_path.read_text(encoding="utf-8")))
    payload = load_history_run(run_id)
    payload_report_path = payload.get("report_path") or ""
    if payload_report_path:
        file_path = Path(payload_report_path).with_suffix(".json")
        if file_path.exists():
            return JSONResponse(json.loads(file_path.read_text(encoding="utf-8")))
    return JSONResponse({"detail": "Отчёт не найден"}, status_code=404)


def _report_file_for_run(run_id: str) -> Path | None:
    report_path = run_report_path(run_id)
    if report_path.exists():
        return report_path
    try:
        payload = load_history_run(run_id)
    except FileNotFoundError:
        return None
    payload_report_path = payload.get("report_path") or ""
    if payload_report_path:
        file_path = Path(payload_report_path)
        if file_path.exists():
            return file_path
    return None


def _build_upload_report(item, user) -> dict[str, Any]:
    run_id = new_run_id()
    report_root = run_directory(run_id)
    report_path = run_report_path(run_id)
    analysis_request = AnalysisRequest(
        config_path="./config/config.yaml",
        reports_dir=str(report_root),
        extracted_dir=None,
        date=None,
        reader=None,
        bm=None,
        generate_reports=False,
    )
    bundle = execute_uploaded_path_analysis(
        analysis_request,
        [(item.original_name, Path(item.stored_path))],
        summary=False,
        storage_dir=report_root,
    )
    from reports.html_report import write_html_report

    write_html_report(bundle.events, bundle.result, report_path, stats=bundle.stats)
    record_history(
        bundle.snapshot,
        mode="analysis",
        source="upload",
        run_id=run_id,
        report_path=report_path,
        owner_email=user.email,
        owner_name=user.name,
    )
    update_upload_reports([item.upload_id], report_run_id=run_id, report_url=f"/report/{run_id}")
    return {"upload_id": item.upload_id, "run_id": run_id, "report_url": f"/report/{run_id}"}


def _upload_session_report(items, report_updates: list[dict[str, Any]], user) -> dict[str, Any]:
    if not items:
        return {}
    if len(items) == 1 and report_updates:
        return report_updates[0]

    selected_files = [(item.original_name, Path(item.stored_path)) for item in items if Path(item.stored_path).exists()]
    if not selected_files:
        return {}

    run_id = new_run_id()
    report_root = run_directory(run_id)
    report_path = run_report_path(run_id)
    analysis_request = AnalysisRequest(
        config_path="./config/config.yaml",
        reports_dir=str(report_root),
        extracted_dir=None,
        date=None,
        reader=None,
        bm=None,
        generate_reports=False,
    )
    bundle = execute_uploaded_path_analysis(
        analysis_request,
        selected_files,
        summary=False,
        storage_dir=report_root,
    )
    from reports.html_report import write_html_report

    write_html_report(bundle.events, bundle.result, report_path, stats=bundle.stats)
    record_history(
        bundle.snapshot,
        mode="analysis",
        source="upload_session",
        run_id=run_id,
        report_path=report_path,
        owner_email=user.email,
        owner_name=user.name,
    )
    return {"run_id": run_id, "report_url": f"/report/{run_id}", "upload_ids": [item.upload_id for item in items]}


def _validate_upload_limits(files: list[tuple[str, bytes]]) -> str:
    settings = load_settings()
    if len(files) > settings.max_upload_files:
        return f"Слишком много файлов: {len(files)}. Максимум: {settings.max_upload_files}."
    total_size = sum(len(content) for _name, content in files)
    if total_size > settings.max_upload_session_bytes:
        return (
            f"Слишком большой общий размер загрузки: {_format_mb(total_size)} Mb. "
            f"Максимум: {_format_mb(settings.max_upload_session_bytes)} Mb."
        )
    oversized = [
        name
        for name, content in files
        if len(content) > settings.max_upload_file_bytes
    ]
    if oversized:
        return (
            f"Файл слишком большой: {oversized[0]}. "
            f"Максимум на файл: {_format_mb(settings.max_upload_file_bytes)} Mb."
        )
    return ""


async def _spool_upload_files(files: list[UploadFile], destination: Path) -> list[tuple[str, Path]]:
    destination.mkdir(parents=True, exist_ok=True)
    staged: list[tuple[str, Path]] = []
    chunk_size = 1024 * 1024
    for index, file in enumerate(files, start=1):
        original_name = file.filename or "upload.bin"
        target_name = Path(original_name).name or "upload.bin"
        target = destination / f"{index}-{target_name}"
        with target.open("wb") as handle:
            while True:
                chunk = await file.read(chunk_size)
                if not chunk:
                    break
                handle.write(chunk)
        staged.append((original_name, target))
    return staged


async def _store_uploads_streaming(
    files: list[UploadFile],
    *,
    owner_email: str = "",
    owner_name: str = "",
) -> tuple[list[Any], list[str], str]:
    settings = load_settings()
    if len(files) > settings.max_upload_files:
        return [], [], f"Слишком много файлов: {len(files)}. Максимум: {settings.max_upload_files}."

    stored: list[Any] = []
    rejected_files: list[str] = []
    total_size = 0
    chunk_size = 1024 * 1024

    try:
        for file in files:
            original_name = file.filename or "upload.bin"
            if not is_allowed_upload_name(original_name):
                rejected_files.append(original_name)
                continue

            upload_id, stored_path = allocate_upload_path(original_name)
            file_size = 0
            try:
                with stored_path.open("wb") as target:
                    while True:
                        chunk = await file.read(chunk_size)
                        if not chunk:
                            break
                        file_size += len(chunk)
                        total_size += len(chunk)
                        if file_size > settings.max_upload_file_bytes:
                            raise ValueError(
                                f"Файл слишком большой: {original_name}. "
                                f"Максимум на файл: {_format_mb(settings.max_upload_file_bytes)} Mb."
                            )
                        if total_size > settings.max_upload_session_bytes:
                            raise ValueError(
                                f"Слишком большой общий размер загрузки: {_format_mb(total_size)} Mb. "
                                f"Максимум: {_format_mb(settings.max_upload_session_bytes)} Mb."
                            )
                        target.write(chunk)
            except Exception:
                if stored_path.parent.exists():
                    shutil.rmtree(stored_path.parent, ignore_errors=True)
                raise

            stored.append(
                save_upload_item(
                    upload_id=upload_id,
                    original_name=original_name,
                    stored_path=stored_path,
                    size_bytes=file_size,
                    owner_email=owner_email,
                    owner_name=owner_name,
                )
            )
    except ValueError as exc:
        for item in stored:
            delete_upload(item.upload_id)
        return [], rejected_files, str(exc)

    return stored, rejected_files, ""


def _format_mb(size_bytes: int) -> str:
    return f"{size_bytes / (1024 * 1024):.2f}"


def _version_number() -> str:
    return format_version().removeprefix("BM Log Analyzer ").strip()


def _report_access_error(run_id: str, user, *, json_response: bool = False):
    try:
        payload = load_history_run(run_id)
    except FileNotFoundError:
        if json_response:
            return JSONResponse({"detail": "Отчёт не найден"}, status_code=404)
        return HTMLResponse("<h1>Отчёт не найден</h1>", status_code=404)
    owner_email = payload.get("owner_email") or ""
    if user.role == "admin" or not owner_email or owner_email == user.email:
        return None
    if json_response:
        return JSONResponse({"detail": "Доступ запрещён"}, status_code=403)
    return HTMLResponse("<h1>Доступ запрещён</h1>", status_code=403)


def _inject_report_topbar(html: str, user, *, run_id: str = "") -> str:
    if user is None or "bm-auth-topbar" in html:
        return html
    topbar = f"""
    <style>{_topbar_css()}</style>
    <div class="bm-auth-topbar">
      {_page_topbar(user)}
    </div>
    """
    ai_panel = _report_ai_panel(run_id) if run_id else ""
    if ai_panel and "</main>" in html:
        html = html.replace("</main>", f"{ai_panel}</main>", 1)
    if "<body>" in html:
        return html.replace("<body>", f"<body>{topbar}", 1)
    return topbar + html


def _report_ai_panel(run_id: str) -> str:
    safe_run_id = escape(run_id)
    endpoint = f"/api/runs/{safe_run_id}/ai-analysis"
    return f"""
    <details class="collapsible bm-ai-analysis">
      <summary>
        <span>
          <strong>AI-аналитика</strong>
          <em>Отдельный слой гипотез на основе фактов отчёта.</em>
        </span>
      </summary>
      <div class="collapsible-body">
        <div class="bm-ai-actions">
          <button type="button" class="bm-ai-button" id="bm-ai-run">Запустить AI-анализ</button>
          <button type="button" class="bm-ai-button bm-ai-button--secondary" id="bm-ai-refresh">Обновить статус</button>
          <span class="muted" id="bm-ai-status">AI-анализ не запускался.</span>
        </div>
        <div id="bm-ai-result" class="bm-ai-result"></div>
      </div>
    </details>
    <style>
      .bm-ai-actions {{ display:flex; flex-wrap:wrap; gap:10px; align-items:center; margin-bottom:12px; }}
      .bm-ai-button {{ appearance:none; border:0; border-radius:10px; background:var(--blue,#2457a6); color:#fff; padding:10px 14px; font:inherit; font-weight:700; cursor:pointer; }}
      .bm-ai-button--secondary {{ background:var(--soft,#f6f8fb); color:var(--blue,#2457a6); border:1px solid var(--line,#d9e0e7); }}
      .bm-ai-button:disabled {{ opacity:.6; cursor:progress; }}
      .bm-ai-result {{ display:grid; gap:12px; }}
      .bm-ai-card {{ border:1px solid var(--line,#d9e0e7); border-radius:12px; padding:12px; background:var(--soft,#f6f8fb); }}
      .bm-ai-card h3 {{ margin:0 0 6px; font-size:15px; }}
      .bm-ai-card p {{ margin:6px 0; }}
      .bm-ai-card ul {{ margin:6px 0 0 18px; padding:0; }}
    </style>
    <script>
      (() => {{
        const endpoint = "{endpoint}";
        const runButton = document.getElementById("bm-ai-run");
        const refreshButton = document.getElementById("bm-ai-refresh");
        const status = document.getElementById("bm-ai-status");
        const resultRoot = document.getElementById("bm-ai-result");
        if (!runButton || !refreshButton || !status || !resultRoot) return;

        function escapeHtml(value) {{
          return String(value || "")
            .replaceAll("&", "&amp;")
            .replaceAll("<", "&lt;")
            .replaceAll(">", "&gt;")
            .replaceAll('"', "&quot;")
            .replaceAll("'", "&#39;");
        }}

        function formatMoscowDateTime(value) {{
          if (!value) return "";
          const date = new Date(value);
          if (Number.isNaN(date.getTime())) return String(value);
          const parts = Object.fromEntries(new Intl.DateTimeFormat("ru-RU", {{
            timeZone: "Europe/Moscow",
            year: "numeric",
            month: "2-digit",
            day: "2-digit",
            hour: "2-digit",
            minute: "2-digit",
            second: "2-digit",
            hour12: false,
          }}).formatToParts(date).map((part) => [part.type, part.value]));
          return `${{parts.day}}.${{parts.month}}.${{parts.year}} (${{parts.hour}}:${{parts.minute}}:${{parts.second}}) (Мск)`;
        }}

        function render(payload) {{
          const analysis = payload.analysis || payload;
          const hypotheses = Array.isArray(analysis.hypotheses) ? analysis.hypotheses : [];
          const whatToCheck = Array.isArray(analysis.what_to_check) ? analysis.what_to_check : [];
          const limitations = Array.isArray(analysis.limitations) ? analysis.limitations : [];
          status.textContent = payload.generated_at ? `Готово: ${{formatMoscowDateTime(payload.generated_at)}}` : "AI-анализ готов.";
          runButton.textContent = "Повторить AI-анализ";
          resultRoot.innerHTML = `
            <div class="bm-ai-card">
              <h3>Кратко</h3>
              <p>${{escapeHtml(analysis.summary || "Нет данных.")}}</p>
            </div>
            ${{hypotheses.map((item) => `
              <div class="bm-ai-card">
                <h3>${{escapeHtml(item.title || "Гипотеза")}}</h3>
                <p><strong>Гипотеза:</strong> ${{escapeHtml(item.hypothesis || "")}}</p>
                <p><strong>Уверенность:</strong> ${{escapeHtml(item.confidence || "")}}</p>
                <p><strong>Evidence:</strong> ${{escapeHtml((item.evidence_refs || []).join(", "))}}</p>
                ${{Array.isArray(item.what_to_check) && item.what_to_check.length ? `<ul>${{item.what_to_check.map((value) => `<li>${{escapeHtml(value)}}</li>`).join("")}}</ul>` : ""}}
              </div>
            `).join("")}}
            ${{whatToCheck.length ? `
              <div class="bm-ai-card">
                <h3>Что проверить</h3>
                <ul>${{whatToCheck.map((value) => `<li>${{escapeHtml(value)}}</li>`).join("")}}</ul>
              </div>
            ` : ""}}
            ${{limitations.length ? `
              <div class="bm-ai-card">
                <h3>Ограничения</h3>
                <ul>${{limitations.map((value) => `<li>${{escapeHtml(value)}}</li>`).join("")}}</ul>
              </div>
            ` : ""}}
          `;
        }}

        async function loadExisting() {{
          status.textContent = "Проверяем сохранённый AI-анализ...";
          const response = await fetch(endpoint);
          const payload = await response.json();
          if (response.ok && payload.schema_version) {{
            render(payload);
          }} else if (payload.enabled === false) {{
            status.textContent = "AI-анализ выключен в настройках сервера.";
          }} else {{
            status.textContent = "AI-анализ ещё не запускался.";
          }}
        }}

        runButton.addEventListener("click", async () => {{
          runButton.disabled = true;
          status.textContent = "AI-анализ выполняется...";
          resultRoot.innerHTML = "";
          try {{
            const response = await fetch(endpoint, {{ method: "POST" }});
            const payload = await response.json();
            if (!response.ok) throw new Error(payload.detail || "Не удалось выполнить AI-анализ.");
            render(payload);
          }} catch (error) {{
            status.textContent = error instanceof Error ? error.message : String(error);
          }} finally {{
            runButton.disabled = false;
          }}
        }});
        refreshButton.addEventListener("click", () => {{
          loadExisting().catch((error) => {{
            status.textContent = error instanceof Error ? error.message : String(error);
          }});
        }});
        loadExisting();
      }})();
    </script>
    """


def _request_user(request: Any):
    return request.state.user


def _is_admin_path(path: str) -> bool:
    return path == "/uploads" or path == "/adnin" or path.startswith("/admin") or path == "/api/uploads/report"


def _profile_link(user) -> str:
    return f'<a class="profile-link" href="/profile">Профиль ({escape(user.name)})</a>'


def _topbar_links(user) -> str:
    items = [("/", "Загрузка файлов")]
    if user.role == "admin":
        items.extend(
            [
                ("/uploads", "История загрузок"),
                ("/admin", "Администрирование"),
            ]
        )
    return _join_nav_items(
        [f'<a class="nav-link" href="{href}" data-path="{href}">{escape(label)}</a>' for href, label in items]
    )


def _join_nav_items(items: list[str]) -> str:
    return '<span class="nav-separator">|</span>'.join(items)


def _page_topbar(user) -> str:
    return f"""
      <header class="topbar" data-menu-open="false">
        <button class="menu-toggle" type="button" aria-label="Открыть меню" aria-expanded="false">☰</button>
        <nav class="topbar-nav topbar-left" aria-label="Основное меню">
          {_topbar_links(user)}
        </nav>
        <nav class="topbar-nav topbar-right" aria-label="Профиль">
          {_join_nav_items([
              f'<a class="nav-link profile-link" href="/profile" data-path="/profile">Профиль ({escape(user.name)})</a>',
              '<a class="nav-link" href="/logout">Выйти</a>',
          ])}
        </nav>
      </header>
      <script>
        (() => {{
          const header = document.currentScript.previousElementSibling;
          if (!header || !header.classList.contains('topbar')) return;
          const current = window.location.pathname === '/adnin' ? '/admin' : window.location.pathname;
          header.querySelectorAll('a[data-path]').forEach((link) => {{
            const path = link.dataset.path;
            const active = path === '/' ? current === '/' : current === path || current.startsWith(`${{path}}/`);
            if (active) link.dataset.active = 'true';
          }});
          const toggle = header.querySelector('.menu-toggle');
          toggle?.addEventListener('click', () => {{
            const open = header.dataset.menuOpen !== 'true';
            header.dataset.menuOpen = String(open);
            toggle.setAttribute('aria-expanded', String(open));
          }});
        }})();
      </script>
    """


def _topbar_css() -> str:
    return """
      .bm-auth-topbar { width: 100%; margin: 0 auto; padding: 24px 24px 0; display: grid; justify-items: center; }
      .topbar { width: 1180px; max-width: 100%; display: flex; align-items: center; justify-content: space-between; gap: 24px; min-height: 48px; color: var(--text, #18212f); }
      .topbar-nav { display: flex; align-items: center; gap: 10px; min-width: 0; }
      .topbar-left { justify-content: flex-start; }
      .topbar-right { justify-content: flex-end; margin-left: auto; }
      .topbar a { color: var(--text, #18212f); text-decoration: none; font-weight: 600; line-height: 1.2; padding: 8px 10px; border-radius: 999px; box-shadow: inset 0 0 0 1px transparent; transition: background-color 140ms ease, color 140ms ease, box-shadow 140ms ease; white-space: nowrap; }
      .topbar a:hover { background: #f2f6fb; color: var(--blue, #2457a6); box-shadow: inset 0 0 0 1px #c8d9f0; }
      .topbar a[data-active="true"] { background: #eaf2fc; color: var(--blue, #2457a6); box-shadow: inset 0 0 0 1px #9eb6d1; }
      .nav-separator { color: var(--muted, #667085); font-weight: 400; user-select: none; }
      .menu-toggle { display: none; appearance: none; border: 1px solid var(--line, #d9e0e7); border-radius: 999px; background: #fff; color: var(--text, #18212f); padding: 8px 11px; font: inherit; line-height: 1; cursor: pointer; }
      .menu-toggle:hover { background: #f2f6fb; border-color: #c8d9f0; color: var(--blue, #2457a6); }
      @media (max-width: 720px) {
        .bm-auth-topbar { padding: 16px 16px 0; }
        .topbar { width: 1180px; max-width: 100%; }
        .topbar { position: relative; flex-wrap: wrap; align-items: flex-start; }
        .menu-toggle { display: inline-flex; align-items: center; justify-content: center; }
        .topbar-nav { display: none; width: 100%; justify-content: flex-start; padding-top: 8px; gap: 8px; }
        .topbar[data-menu-open="true"] .topbar-nav { display: flex; flex-wrap: wrap; }
        .topbar-right { margin-left: 0; }
      }
    """


def _login_html(error: str = "") -> str:
    error_html = f'<div class="error">{escape(error)}</div>' if error else ""
    return f"""
<!doctype html>
<html lang="ru">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Вход · BM Log Analyzer</title>
    <style>
      :root {{ --bg:#eef2f6; --panel:#fff; --line:#d9e0e7; --text:#18212f; --muted:#667085; --blue:#2457a6; }}
      * {{ box-sizing: border-box; }}
      body {{ margin:0; min-height:100vh; display:grid; place-items:center; background:var(--bg); color:var(--text); font:14px/1.5 system-ui,sans-serif; }}
      main {{ width:min(420px, calc(100% - 32px)); display:grid; gap:12px; justify-items:center; }}
      form {{ width:min(420px, calc(100% - 32px)); display:grid; gap:14px; padding:28px; border:1px solid var(--line); border-radius:16px; background:var(--panel); box-shadow:0 20px 60px rgba(24,33,47,.08); }}
      .brand {{ display:grid; gap:2px; }}
      h1 {{ margin:0; font-size:28px; }}
      .version {{ color:var(--muted); font-size:13px; }}
      label {{ display:grid; gap:6px; color:var(--muted); font-size:12px; }}
      input {{ width:100%; border:1px solid var(--line); border-radius:10px; padding:12px; font:inherit; color:var(--text); }}
      button {{ border:0; border-radius:12px; padding:12px 16px; background:var(--blue); color:#fff; font:inherit; font-weight:700; cursor:pointer; }}
      .error {{ padding:10px 12px; border:1px solid #f4b4a5; border-radius:10px; background:#fff5f3; color:#9f1d12; }}
      .muted {{ color:var(--muted); }}
      footer {{ color:var(--muted); font-size:12px; }}
    </style>
  </head>
  <body>
    <main>
      <form method="post" action="/login">
        <div class="brand">
          <h1>BM Log Analyzer</h1>
          <div class="version">версия сервиса {_version_number()}</div>
        </div>
        <div class="muted">Авторизация</div>
        {error_html}
        <label>Email<input name="email" type="email" autocomplete="username" required></label>
        <label>Пароль<input name="password" type="password" autocomplete="current-password" required></label>
        <button type="submit">Войти</button>
      </form>
      <footer>made with ♥ by Roman A. Proskurnin</footer>
    </main>
  </body>
</html>
""".strip()


def _admin_html(user=None, error: str = "", policy=None) -> str:
    effective_user = user or get_user("admin@example.com")
    policy = policy or load_storage_policy()
    rows = []
    for item in list_users():
        role_options = "".join(
            f'<option value="{role}" {"selected" if item.role == role else ""}>{label}</option>'
            for role, label in (("user", "пользователь"), ("admin", "администратор"))
        )
        rows.append(
            f"""
            <tr>
              <td>
                <form method="post" action="/admin/users/update" class="row-form">
                  <input type="hidden" name="email" value="{escape(item.email)}">
                  <input name="name" value="{escape(item.name)}" required>
              </td>
              <td><input name="new_email" type="email" value="{escape(item.email)}" required></td>
              <td><input name="password" type="password" placeholder="Оставить без изменений"></td>
              <td><select name="role">{role_options}</select></td>
              <td class="actions">
                  <button type="submit">Сохранить</button>
                </form>
                <form method="post" action="/admin/users/delete">
                  <input type="hidden" name="email" value="{escape(item.email)}">
                  <button type="submit" class="danger">Удалить</button>
                </form>
              </td>
            </tr>
            """
        )
    check_rows = []
    for check in BUILTIN_CHECKS:
        check_rows.append(
            f"""
            <tr>
              <td><code>{escape(check.check_id)}</code></td>
              <td><strong>{escape(check.title)}</strong><br><span class="muted">{escape(check.description)}</span></td>
              <td>{escape(check.severity)}</td>
              <td>{"включена" if check.enabled else "выключена"}</td>
              <td>{escape(check.version)}</td>
            </tr>
            """
        )
    error_html = f'<div class="error">{escape(error)}</div>' if error else ""
    return f"""
<!doctype html>
<html lang="ru">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Администрирование · BM Log Analyzer</title>
    {_shared_page_css()}
  </head>
  <body>
    <main class="page">
      {_page_topbar(effective_user)}
      <section class="panel">
        <h1>Администрирование</h1>
        <h2>Пользователи</h2>
        {error_html}
        <form method="post" action="/admin/users/create" class="create-form">
          <input name="name" placeholder="Имя" required>
          <input name="email" type="email" placeholder="Email / логин" required>
          <input name="password" type="password" placeholder="Пароль" required>
          <select name="role">
            <option value="user">пользователь</option>
            <option value="admin">администратор</option>
          </select>
          <button type="submit">Добавить</button>
        </form>
        <h2>Хранение архивов</h2>
        <form method="post" action="/admin/settings" class="create-form">
          <label>Срок хранения, дни
            <input name="archive_retention_days" type="number" min="1" step="1" value="{policy.archive_retention_days}" required>
          </label>
          <button type="submit">Сохранить</button>
        </form>
        <div class="muted">Через заданное число дней удаляются архивы и распаковки. Табличные данные сохраняются, ссылка на скачивание исчезает.</div>
        <h2>Каталог проверок</h2>
        <div class="muted">Текущие built-in правила анализа. Следующий этап - включение, выключение и настройка правил без изменения кода.</div>
        <div class="table-wrap">
          <table>
            <thead><tr><th>ID</th><th>Проверка</th><th>Severity</th><th>Статус</th><th>Версия</th></tr></thead>
            <tbody>{"".join(check_rows)}</tbody>
          </table>
        </div>
        <div class="table-wrap">
          <table>
            <thead><tr><th>Имя</th><th>Email</th><th>Пароль</th><th>Роль</th><th>Действия</th></tr></thead>
            <tbody>{"".join(rows)}</tbody>
          </table>
        </div>
      </section>
    </main>
  </body>
</html>
""".strip()


def _profile_html(user) -> str:
    uploads = list_uploads(owner_email=user.email)
    rows = []
    for item in uploads:
        download_cell = (
            f'<a href="{escape(item.download_url)}">Скачать</a>'
            if item.download_url
            else '<span class="muted">Срок хранения истёк</span>'
        )
        rows.append(
            f"""
            <tr>
              <td>{escape(item.created_at)}</td>
              <td>{escape(item.original_name)}</td>
              <td>{download_cell}</td>
              <td>
                <form method="post" action="/profile/uploads/{escape(item.upload_id)}/delete">
                  <button type="submit" class="danger">Удалить</button>
                </form>
              </td>
            </tr>
            """
        )
    table_body = "".join(rows) or '<tr><td colspan="4" class="muted">Загрузок пока нет.</td></tr>'
    return f"""
<!doctype html>
<html lang="ru">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Профиль · BM Log Analyzer</title>
    {_shared_page_css()}
  </head>
  <body>
    <main class="page">
      {_page_topbar(user)}
      <section class="panel">
        <h1>Профиль</h1>
        <div class="forms-grid">
          <form method="post" action="/profile/name" class="create-form">
            <input name="name" value="{escape(user.name)}" required>
            <button type="submit">Сменить имя</button>
          </form>
          <form method="post" action="/profile/password" class="create-form">
            <input name="password" type="password" placeholder="Новый пароль" required>
            <button type="submit">Сменить пароль</button>
          </form>
        </div>
        <h2>Мои файлы</h2>
        <div class="table-wrap">
          <table>
            <thead><tr><th>Дата</th><th>Имя файла</th><th>Скачать</th><th>Удалить</th></tr></thead>
            <tbody>{table_body}</tbody>
          </table>
        </div>
      </section>
    </main>
  </body>
</html>
""".strip()


def _shared_page_css() -> str:
    return """
    <style>
      :root { color-scheme: light; --panel:#fff; --bg:#eef2f6; --line:#d9e0e7; --text:#18212f; --muted:#667085; --blue:#2457a6; }
      * { box-sizing: border-box; }
      body { margin:0; background:var(--bg); color:var(--text); font:14px/1.45 system-ui,sans-serif; }
      .page { width:100%; margin:0 auto; padding:24px; display:grid; justify-items:center; gap:16px; }
      a { color:var(--blue); text-decoration:none; font-weight:600; }
      .panel { width:1180px; max-width:100%; padding:20px 22px; border:1px solid var(--line); border-radius:16px; background:var(--panel); box-shadow:0 16px 42px rgba(24,33,47,.06); }
      h1 { margin:0 0 16px; font-size:24px; }
      h2 { margin:16px 0 10px; font-size:18px; }
      .create-form, .row-form, .forms-grid { display:flex; flex-wrap:wrap; gap:10px; align-items:center; }
      .create-form label { display:grid; gap:6px; align-items:start; }
      .create-form { margin-bottom:16px; }
      input, select { border:1px solid var(--line); border-radius:10px; padding:10px 12px; font:inherit; color:var(--text); background:#fff; }
      button { border:0; border-radius:10px; padding:10px 14px; background:var(--blue); color:#fff; font:inherit; font-weight:700; cursor:pointer; }
      .danger { background:#b42318; }
      .table-wrap { overflow:auto; border:1px solid var(--line); border-radius:14px; }
      table { width:100%; border-collapse:collapse; background:#fff; }
      th, td { padding:12px 14px; border-bottom:1px solid #e8edf2; text-align:left; vertical-align:top; }
      th { background:#f3f6fa; font-weight:700; }
      .actions { display:flex; flex-wrap:wrap; gap:8px; }
      .muted { color:var(--muted); }
      .error { margin-bottom:12px; padding:10px 12px; border:1px solid #f4b4a5; border-radius:10px; background:#fff5f3; color:#9f1d12; }
      {{TOPBAR_CSS}}
      @media (max-width: 720px) { .page { padding:16px; } }
    </style>
    """.replace("{{TOPBAR_CSS}}", _topbar_css())


def _index_html() -> str:
    return """
<!doctype html>
<html lang="ru">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>BM Log Analyzer</title>
    <style>
      :root { color-scheme: light; --panel: #fff; --line: #d9e0e7; --text: #1f2933; --muted: #667085; --soft: #f6f8fb; --blue: #2764a3; --green: #137752; --track: #e8eef4; }
      * { box-sizing: border-box; }
      body { margin: 0; background: linear-gradient(180deg, #f6f8fb 0%, #eef2f6 100%); color: var(--text); font: 14px/1.45 system-ui, sans-serif; }
      main { max-width: 840px; margin: 0 auto; padding: 28px; }
      h1, h2, p { margin: 0; }
      .header, .section { background: var(--panel); border: 1px solid var(--line); border-radius: 14px; box-shadow: 0 8px 24px rgba(31, 41, 51, 0.04); }
      .header { padding: 18px; display: grid; gap: 10px; }
      .header h1 { font-size: 30px; line-height: 1.1; }
      .muted { color: var(--muted); }
      .section { margin-top: 14px; padding: 18px; }
      .field { display: grid; gap: 6px; }
      .field label { font-size: 12px; color: var(--muted); }
      .field input[type="file"] { width: 100%; border: 1px solid var(--line); border-radius: 12px; padding: 12px; background: var(--soft); }
      .button { appearance: none; border: 1px solid var(--line); background: var(--blue); color: #fff; border-radius: 10px; padding: 11px 16px; font: inherit; cursor: pointer; }
      .button:disabled { opacity: 0.65; cursor: progress; }
      .status { display: grid; gap: 8px; margin-top: 14px; }
      .status-line { display: flex; justify-content: space-between; gap: 12px; align-items: center; color: var(--muted); }
      .progress-shell { height: 12px; background: var(--track); border: 1px solid var(--line); border-radius: 999px; overflow: hidden; }
      .progress-bar { width: 0%; height: 100%; background: linear-gradient(90deg, #9db9d6, #5b87b4); transition: width 180ms ease; }
      .report-link { display: inline-flex; align-items: center; gap: 8px; margin-top: 14px; color: var(--green); text-decoration: none; font-weight: 600; }
      .report-link[hidden] { display: none; }
      .hint { margin-top: 10px; color: var(--muted); }
      .steps { margin-top: 14px; display: grid; gap: 6px; }
      .step { padding: 10px 12px; border: 1px solid var(--line); border-radius: 10px; background: var(--soft); color: var(--muted); }
      .step[data-active="true"] { background: #eef5fb; color: var(--text); border-color: #91afd0; }
      .history-list { display: grid; gap: 10px; margin-top: 10px; }
      .history-item { display: grid; gap: 8px; padding: 12px 14px; border: 1px solid var(--line); border-radius: 12px; background: var(--soft); text-decoration: none; color: inherit; }
      .history-item:hover { border-color: #91afd0; background: #eef5fb; }
      .history-item[data-active="true"] { border-color: #2764a3; background: #eef5fb; }
      .history-item strong { font-size: 14px; }
      .history-item .meta { display: flex; flex-wrap: wrap; gap: 10px; color: var(--muted); font-size: 12px; }
      .history-filter { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 10px; }
      .history-filter button { appearance: none; border: 1px solid var(--line); background: #fff; color: var(--text); border-radius: 999px; padding: 8px 12px; font: inherit; cursor: pointer; }
      .history-filter button[data-active="true"] { background: #e7f0fb; border-color: #91afd0; color: #184a7a; }
      .history-tools { display: grid; gap: 10px; margin-top: 10px; grid-template-columns: minmax(0, 1fr) 160px; }
      .history-tools input, .history-tools select { width: 100%; border: 1px solid var(--line); background: #fff; color: var(--text); border-radius: 10px; padding: 10px 12px; font: inherit; }
      .history-empty { margin-top: 10px; }
      .session-card { margin-top: 14px; padding: 14px; border: 1px solid var(--line); border-radius: 12px; background: var(--soft); display: grid; gap: 8px; }
      .session-card strong { font-size: 16px; }
      .session-card .meta { display: flex; flex-wrap: wrap; gap: 10px; color: var(--muted); font-size: 12px; }
      .session-card a { color: var(--blue); text-decoration: none; font-weight: 600; }
      .footer-note { margin-top: 12px; color: var(--muted); font-size: 12px; }
    </style>
  </head>
  <body>
    <main>
      <header class="header">
        <h1>BM Log Analyzer</h1>
        <p class="muted">Загрузите архивы или папку с логами. После обработки откроется готовый HTML-отчёт.</p>
      </header>
      <section class="section">
        <div class="field">
          <label for="files">Файлы или папка</label>
          <input id="files" type="file" webkitdirectory multiple>
        </div>
        <div class="status">
          <div class="status-line">
            <span id="status_text">Ожидание загрузки.</span>
            <span id="file_count" class="muted">Файлы не выбраны.</span>
          </div>
          <div class="progress-shell" aria-hidden="true"><div id="progress_bar" class="progress-bar"></div></div>
        </div>
        <div class="steps" id="steps">
          <div class="step" data-step="1">Подготовка файлов</div>
          <div class="step" data-step="2">Загрузка и распаковка</div>
          <div class="step" data-step="3">Обработка логов</div>
          <div class="step" data-step="4">Формирование отчёта</div>
        </div>
        <div class="actions">
          <button id="upload" class="button" type="button">Загрузить</button>
        </div>
        <a id="report_link" class="report-link" href="#" target="_blank" rel="noreferrer" hidden>Посмотреть отчёт</a>
        <div class="footer-note">После завершения появится сообщение «Сессия загрузки завершена».</div>
      </section>
      <section class="section">
        <div class="status-line" style="margin:0 0 10px 0;">
          <strong>Последний отчёт</strong>
          <span class="muted">Быстрый переход к последней успешной сессии.</span>
        </div>
        <div id="latest_report_card" class="session-card" hidden>
          <strong id="latest_report_title"></strong>
          <div id="latest_report_meta" class="meta"></div>
          <div class="meta">
            <a id="latest_report_link" href="/api/runs/latest/report" target="_blank" rel="noreferrer">Открыть последний отчёт</a>
            <a href="/api/runs/latest/manifest" target="_blank" rel="noreferrer">JSON manifest</a>
          </div>
        </div>
        <p id="latest_report_empty" class="muted history-empty">Пока нет сохранённых отчётов.</p>
      </section>
      <section class="section">
        <div class="status-line" style="margin:0 0 10px 0;">
          <strong>Последние сессии</strong>
          <span class="muted">Открывают готовый HTML-отчёт.</span>
        </div>
        <div class="history-filter" id="history_filter">
          <button type="button" data-mode="all" data-active="true">Все</button>
          <button type="button" data-mode="analysis">Анализ</button>
          <button type="button" data-mode="summary">Сводка</button>
        </div>
        <div class="history-tools">
          <input id="history_search" type="search" placeholder="Поиск по дате, пути, версии, сессии">
          <select id="history_sort" aria-label="Сортировка истории">
            <option value="desc">Новые сначала</option>
            <option value="asc">Старые сначала</option>
          </select>
        </div>
        <div id="history_list" class="history-list"></div>
        <p id="history_empty" class="muted history-empty">История пока пуста.</p>
        <div id="history_detail" class="session-card" hidden>
          <strong id="history_detail_title"></strong>
          <div id="history_detail_meta" class="meta"></div>
          <div id="history_detail_counts" class="meta"></div>
          <div class="meta">
            <a id="history_detail_link" href="#" target="_blank" rel="noreferrer">Открыть отчёт</a>
            <a id="history_detail_manifest_link" href="#" target="_blank" rel="noreferrer">JSON manifest</a>
            <button id="history_detail_delete" class="button" type="button" style="padding:8px 12px;background:#b42318;border-color:#b42318;">Удалить</button>
          </div>
        </div>
      </section>
    </main>
    <script>
      const filesInput = document.getElementById('files');
      const uploadButton = document.getElementById('upload');
      const statusText = document.getElementById('status_text');
      const fileCount = document.getElementById('file_count');
      const progressBar = document.getElementById('progress_bar');
      const reportLink = document.getElementById('report_link');
      const historyList = document.getElementById('history_list');
      const historyEmpty = document.getElementById('history_empty');
      const historyDetail = document.getElementById('history_detail');
      const historyDetailTitle = document.getElementById('history_detail_title');
      const historyDetailMeta = document.getElementById('history_detail_meta');
      const historyDetailCounts = document.getElementById('history_detail_counts');
      const historyDetailLink = document.getElementById('history_detail_link');
      const historyDetailManifestLink = document.getElementById('history_detail_manifest_link');
      const latestReportCard = document.getElementById('latest_report_card');
      const latestReportTitle = document.getElementById('latest_report_title');
      const latestReportMeta = document.getElementById('latest_report_meta');
      const latestReportEmpty = document.getElementById('latest_report_empty');
      const historyFilter = document.getElementById('history_filter');
      const historySearch = document.getElementById('history_search');
      const historySort = document.getElementById('history_sort');
      const historyDetailDelete = document.getElementById('history_detail_delete');
      const steps = Array.from(document.querySelectorAll('.step'));
      let selectedHistoryMode = 'all';
      let selectedHistoryRunId = '';

      function setProgress(percent, activeStep, message) {
        progressBar.style.width = `${Math.max(0, Math.min(100, percent))}%`;
        statusText.textContent = message;
        steps.forEach((step) => {
          step.dataset.active = String(step.dataset.step === String(activeStep));
        });
      }

      filesInput.addEventListener('change', () => {
        fileCount.textContent = filesInput.files.length ? `${filesInput.files.length} файлов выбрано.` : 'Файлы не выбраны.';
      });

      function setHistoryDetail(item) {
        if (!item) {
          historyDetail.hidden = true;
          selectedHistoryRunId = '';
          return;
        }
        historyDetail.hidden = false;
        selectedHistoryRunId = item.run_id || '';
        historyDetailTitle.textContent = `${item.created_at} · ${item.mode} · ${item.source}`;
        historyDetailMeta.innerHTML = [
          `<span>${item.version}</span>`,
          `<span>input: ${item.input_path}</span>`,
          `<span>reports: ${item.reports_dir}</span>`,
        ].join('');
        historyDetailCounts.innerHTML = [
          `<span>total ${item.total}</span>`,
          `<span>success ${item.success_count}</span>`,
          `<span>decline ${item.decline_count}</span>`,
          `<span>tech ${item.technical_error_count}</span>`,
          `<span>unknown ${item.unknown_count}</span>`,
        ].join('');
        historyDetailLink.href = item.report_url || `/report/${item.run_id}`;
        historyDetailManifestLink.href = item.manifest_url || `/report/${item.run_id}/manifest`;
      }

      function renderHistoryFilter() {
        historyFilter.querySelectorAll('button').forEach((button) => {
          button.dataset.active = String(button.dataset.mode === selectedHistoryMode);
        });
      }

      async function loadHistory() {
        try {
          const latestResponse = await fetch('/api/runs/latest');
          const latestItem = await latestResponse.json();
          if (latestItem && latestItem.run_id && latestItem.report_url) {
            latestReportEmpty.hidden = true;
            latestReportCard.hidden = false;
            latestReportTitle.textContent = `${latestItem.created_at} · ${latestItem.mode} · ${latestItem.source}`;
            latestReportMeta.innerHTML = [
              `<span>${latestItem.version}</span>`,
              `<span>total ${latestItem.total}</span>`,
            `<span>success ${latestItem.success_count}</span>`,
            `<span>tech ${latestItem.technical_error_count}</span>`,
          ].join('');
            latestReportLink.href = latestItem.report_url;
          } else {
            latestReportCard.hidden = true;
            latestReportEmpty.hidden = false;
          }
          const query = new URLSearchParams({ limit: '8' });
          if (selectedHistoryMode !== 'all') {
            query.set('mode', selectedHistoryMode);
          }
          const searchValue = historySearch.value.trim();
          if (searchValue) {
            query.set('query', searchValue);
          }
          query.set('sort', historySort.value || 'desc');
          const response = await fetch(`/api/runs?${query.toString()}`);
          const items = await response.json();
          if (!Array.isArray(items) || !items.length) {
            historyList.innerHTML = '';
            historyEmpty.hidden = false;
            historyDetail.hidden = true;
            return;
          }
          historyEmpty.hidden = true;
          historyList.innerHTML = items.map((item, index) => `
            <button class="history-item" type="button" data-active="${String(index === 0)}" data-run-id="${item.run_id}">
              <strong>${item.created_at} · ${item.mode} · ${item.source}</strong>
              <div class="meta">
                <span>${item.version}</span>
                <span>total ${item.total}</span>
                <span>success ${item.success_count}</span>
                <span>tech ${item.technical_error_count}</span>
              </div>
            </button>
          `).join('');
          historyList.querySelectorAll('.history-item').forEach((node) => {
            node.addEventListener('click', async () => {
              const runId = node.dataset.runId;
              const response = await fetch(`/api/runs/${encodeURIComponent(runId)}`);
              const item = await response.json();
              setHistoryDetail({
                run_id: item.run_id,
                created_at: item.created_at,
                mode: item.mode,
                source: item.source,
                version: item.version,
                input_path: item.snapshot?.request?.input_path || '',
                reports_dir: item.snapshot?.request?.reports_dir || '',
                total: item.snapshot?.analysis?.total || 0,
                success_count: item.snapshot?.analysis?.success_count || 0,
                decline_count: item.snapshot?.analysis?.decline_count || 0,
                technical_error_count: item.snapshot?.analysis?.technical_error_count || 0,
                unknown_count: item.snapshot?.analysis?.unknown_count || 0,
                report_path: item.report_path || '',
                report_url: item.report_url || '',
                manifest_url: item.manifest_url || '',
              });
              historyList.querySelectorAll('.history-item').forEach((el) => {
                el.dataset.active = String(el === node);
              });
            });
          });
          const first = items[0];
          setHistoryDetail(first);
        } catch (error) {
          historyList.innerHTML = '';
          historyEmpty.hidden = false;
          historyEmpty.textContent = 'Не удалось загрузить историю.';
          historyDetail.hidden = true;
        }
      }

      historySearch.addEventListener('input', () => {
        loadHistory();
      });

      historySort.addEventListener('change', () => {
        loadHistory();
      });

      historyFilter.addEventListener('click', async (event) => {
        const button = event.target.closest('button[data-mode]');
        if (!button) {
          return;
        }
        selectedHistoryMode = button.dataset.mode || 'all';
        renderHistoryFilter();
        await loadHistory();
      });

      historyDetailDelete.addEventListener('click', async () => {
        if (!selectedHistoryRunId) {
          return;
        }
        historyDetailDelete.disabled = true;
        try {
          const response = await fetch(`/api/runs/${encodeURIComponent(selectedHistoryRunId)}`, { method: 'DELETE' });
          if (!response.ok) {
            const data = await response.json().catch(() => ({}));
            throw new Error(data?.detail || 'Не удалось удалить сессию.');
          }
          setHistoryDetail(null);
          await loadHistory();
        } catch (error) {
          statusText.textContent = error instanceof Error ? error.message : String(error);
        } finally {
          historyDetailDelete.disabled = false;
        }
      });

      uploadButton.addEventListener('click', async () => {
        if (!filesInput.files.length) {
          statusText.textContent = 'Сначала выберите файлы или папку.';
          return;
        }
        uploadButton.disabled = true;
        reportLink.hidden = true;
        reportLink.href = '#';
        try {
          setProgress(10, 1, 'Подготовка файлов...');
          const formData = new FormData();
          for (const file of filesInput.files) {
            formData.append('files', file, file.webkitRelativePath || file.name);
          }
          setProgress(30, 2, 'Загрузка и распаковка...');
          const responsePromise = fetch('/api/upload/analyze', { method: 'POST', body: formData });
          setProgress(55, 3, 'Обработка логов...');
          const response = await responsePromise;
          setProgress(80, 4, 'Формирование отчёта...');
          const data = await response.json();
          if (!response.ok) {
            throw new Error(data?.detail || data?.message || 'Не удалось выполнить загрузку.');
          }
          setProgress(100, 4, 'Сессия загрузки завершена.');
          reportLink.href = data.report_url || `/report/${data.run_id}`;
          reportLink.hidden = false;
          await loadHistory();
        } catch (error) {
          statusText.textContent = error instanceof Error ? error.message : String(error);
          progressBar.style.width = '0%';
        } finally {
          uploadButton.disabled = false;
        }
      });

      loadHistory();
      renderHistoryFilter();
    </script>
  </body>
</html>
""".strip()


def _landing_html(user=None) -> str:
    user = user or get_user("admin@example.com")
    return """
<!doctype html>
<html lang="ru">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>BM Log Analyzer</title>
    <style>
      :root { color-scheme: light; --bg: #eef2f6; --panel: #fff; --line: #d9e0e7; --text: #18212f; --muted: #667085; --blue: #2457a6; --green: #137752; --soft: #f7f9fc; }
      * { box-sizing: border-box; }
      body { margin: 0; min-height: 100vh; background: radial-gradient(circle at top, #f9fbfd 0, #eef2f6 48%, #e8edf3 100%); color: var(--text); font: 14px/1.5 system-ui, sans-serif; }
      main { min-height: 100vh; display: grid; grid-template-rows: auto minmax(0, 1fr); justify-items: center; gap: 18px; padding: 24px; }
      @media (max-width: 720px) { main { padding: 16px; } }
      .upload-center { align-self: center; width: min(640px, 100%); display: grid; justify-items: center; gap: 12px; }
      .card { width: min(640px, 100%); padding: 30px; border: 1px solid var(--line); border-radius: 18px; background: var(--panel); box-shadow: 0 24px 70px rgba(24, 33, 47, 0.08); display: grid; gap: 18px; text-align: center; }
      h1 { margin: 0; font-size: 30px; line-height: 1.05; letter-spacing: 0; }
      .version { color: var(--muted); font-size: 13px; }
      .field { display: grid; gap: 8px; text-align: left; }
      .field label { font-size: 12px; color: var(--muted); }
      .native-picker { width: 100%; border: 1px solid var(--line); border-radius: 14px; background: var(--soft); color: var(--text); padding: 14px 16px; font: inherit; cursor: pointer; }
      .dropzone { padding: 16px; border: 1px dashed #9eb6d1; border-radius: 14px; background: #fbfdff; color: var(--muted); text-align: left; }
      .dropzone[data-active="true"] { background: #eef5fb; border-color: #2f6fd1; color: var(--text); }
      .button { appearance: none; border: 0; border-radius: 14px; background: linear-gradient(180deg, #2f6fd1, #2457a6); color: #fff; padding: 14px 18px; font: inherit; font-weight: 700; cursor: pointer; width: 100%; }
      .button:disabled { opacity: 0.75; cursor: progress; }
      .status { display: grid; gap: 10px; text-align: left; }
      .status-line { display: flex; justify-content: space-between; gap: 12px; color: var(--muted); }
      .progress-shell { height: 12px; border-radius: 999px; background: #edf2f7; border: 1px solid var(--line); overflow: hidden; }
      .progress-bar { width: 0%; height: 100%; background: linear-gradient(90deg, #6e98da, #2f6fd1); transition: width 180ms ease; }
      .message { min-height: 52px; padding: 14px; border-radius: 14px; background: #f8fbff; border: 1px dashed #c8d9f0; color: var(--text); text-align: left; }
      .message-actions { display: none; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; width: 100%; margin: 2px auto 0; }
      .message-actions[data-visible="true"] { display: grid; }
      .message-action { display: inline-flex; align-items: center; justify-content: center; min-height: 44px; border-radius: 12px; padding: 10px 14px; background: #2f6fd1; color: #fff; font-weight: 700; box-shadow: 0 10px 22px rgba(47, 111, 209, .18); }
      .message-action--secondary { background: #ffffff; color: #2457a6; border: 1px solid #c8d9f0; box-shadow: 0 8px 18px rgba(36, 87, 166, .08); }
      @media (max-width: 520px) { .message-actions { grid-template-columns: 1fr; } }
      .signature { color: var(--muted); font-size: 12px; }
      .hint { color: var(--muted); font-size: 12px; text-align: left; }
      .selection-summary { padding: 10px 12px; border: 1px solid var(--line); border-radius: 12px; background: #fafcff; color: var(--text); text-align: left; min-height: 44px; }
      .selection-summary ul { margin: 0; padding-left: 18px; }
      .selection-summary li + li { margin-top: 4px; }
      .rejected-list { color: #9a3412; }
      footer { color: var(--muted); font-size: 12px; }
      a { color: var(--blue); text-decoration: none; font-weight: 600; }
      {{TOPBAR_CSS}}
    </style>
  </head>
  <body>
    <main>
      {{TOPBAR}}
      <div class="upload-center">
        <section class="card">
          <div>
            <h1>BM Log Analyzer</h1>
            <div class="version">версия сервиса {{VERSION}}</div>
          </div>
          <div class="field">
            <label for="files">Логи и архивы</label>
            <input id="files" class="native-picker" type="file" multiple accept=".log,.gz,.zip,.tar.gz,.tgz,.rar">
            <div id="dropzone" class="dropzone" role="button" tabindex="0">Можно перетащить сюда файлы или папку с логами. В поддерживаемых браузерах клик здесь открывает выбор папки.</div>
            <div id="selection_summary" class="selection-summary">Пока ничего не выбрано.</div>
            <div class="hint">Поддерживаются отдельные файлы, папки и архивы разных типов.</div>
          </div>
          <button id="upload" class="button" type="button">Загрузить логи в хранилище</button>
          <div class="status">
            <div class="status-line">
              <span id="status_text">Ожидание выбора файлов.</span>
              <span id="file_count">0 файлов</span>
            </div>
            <div class="progress-shell" aria-hidden="true"><div id="progress_bar" class="progress-bar"></div></div>
          </div>
          <div id="message" class="message">Выберите архивы или отдельные файлы, затем загрузите их в хранилище.</div>
          <div id="message_actions" class="message-actions" aria-label="Действия после загрузки"></div>
        </section>
        <footer>made with ♥ by Roman A. Proskurnin</footer>
      </div>
    </main>
    <script>
      const filesInput = document.getElementById('files');
      const uploadButton = document.getElementById('upload');
      const statusText = document.getElementById('status_text');
      const fileCount = document.getElementById('file_count');
      const progressBar = document.getElementById('progress_bar');
      const message = document.getElementById('message');
      const messageActions = document.getElementById('message_actions');
      const selectionSummary = document.getElementById('selection_summary');
      const dropzone = document.getElementById('dropzone');
      const allowedSuffixes = ['.log', '.gz', '.zip', '.tar.gz', '.tgz', '.rar'];
      const preparedFiles = new Map();
      const rejectedFiles = new Map();

      function fileKey(file) {
        return `${file.webkitRelativePath || file.relativePath || file.name}:${file.size}:${file.lastModified || 0}`;
      }

      function displayName(file) {
        return file.webkitRelativePath || file.relativePath || file.name;
      }

      function isAllowedFile(file) {
        const name = displayName(file).toLowerCase();
        return allowedSuffixes.some((suffix) => name.endsWith(suffix));
      }

      function addFiles(files) {
        for (const file of files) {
          const target = isAllowedFile(file) ? preparedFiles : rejectedFiles;
          target.set(fileKey(file), file);
        }
        updateSelectionSummary();
      }

      function selectedFiles() {
        return [...preparedFiles.values()];
      }

      function setProgress(value, text) {
        progressBar.style.width = `${Math.max(0, Math.min(100, value))}%`;
        statusText.textContent = text;
      }

      function updateSelectionSummary() {
        const selected = selectedFiles();
        const rejected = [...rejectedFiles.values()];
        const total = selected.length;
        fileCount.textContent = rejected.length ? `${total} подготовлено, ${rejected.length} отклонено` : `${total} подготовлено`;
        if (!total && !rejected.length) {
          selectionSummary.textContent = 'Пока ничего не выбрано.';
          return;
        }
        const acceptedHtml = selected.length
          ? `<div>Подготовлены к загрузке:</div><ul>${selected.map((file) => `<li>${displayName(file)} · ${formatSize(file.size)}</li>`).join('')}</ul>`
          : '<div>Нет файлов, соответствующих требованиям.</div>';
        const rejectedHtml = rejected.length
          ? `<div class="rejected-list">Не будут загружены:</div><ul class="rejected-list">${rejected.map((file) => `<li>${displayName(file)} · ${formatSize(file.size)}</li>`).join('')}</ul>`
          : '';
        selectionSummary.innerHTML = `${acceptedHtml}${rejectedHtml}`;
      }

      function formatSize(bytes) {
        return `${(bytes / (1024 * 1024)).toFixed(2)} Mb`;
      }

      function archiveWord(count) {
        if (count % 10 === 1 && count % 100 !== 11) {
          return 'архив';
        }
        if ([2, 3, 4].includes(count % 10) && ![12, 13, 14].includes(count % 100)) {
          return 'архива';
        }
        return 'архивов';
      }

      function fileWord(count) {
        if (count % 10 === 1 && count % 100 !== 11) {
          return 'файл';
        }
        if ([2, 3, 4].includes(count % 10) && ![12, 13, 14].includes(count % 100)) {
          return 'файла';
        }
        return 'файлов';
      }

      function notUploadedPhrase(count) {
        const verb = count % 10 === 1 && count % 100 !== 11 ? 'не загружен' : 'не загружены';
        return `${count} ${fileWord(count)} ${verb}`;
      }

      function uploadMessage(summary, clientRejectedCount) {
        const uploadedCount = summary.uploaded_count || 0;
        const rejectedCount = clientRejectedCount + (summary.rejected_count || 0);
        const totalSizeMb = Number(summary.total_size_mb || 0).toFixed(2);
        const rejectedMessage = rejectedCount
          ? ` ${notUploadedPhrase(rejectedCount)}, потому что они не соответствуют требованиям.`
          : '';
        return `Загружено ${uploadedCount} ${archiveWord(uploadedCount)} с логами, общим размером ${totalSizeMb} Mb. Загрузка прошла без ошибок.${rejectedMessage} Спасибо.`;
      }

      function escapeHtml(value) {
        return String(value).replace(/[&<>"']/g, (char) => ({
          '&': '&amp;',
          '<': '&lt;',
          '>': '&gt;',
          '"': '&quot;',
          "'": '&#039;',
        }[char]));
      }

      function safeReportUrl(value) {
        const url = String(value || '');
        return url.startsWith('/report/') ? url : '';
      }

      function renderUploadComplete(summary, clientRejectedCount, reportUrl) {
        const actions = [
          safeReportUrl(reportUrl)
            ? `<a class="message-action" href="${escapeHtml(safeReportUrl(reportUrl))}">Открыть отчёт</a>`
            : '',
          '<a class="message-action message-action--secondary" href="/uploads">Перейти в загрузки</a>',
        ].filter(Boolean).join('');
        message.textContent = uploadMessage(summary, clientRejectedCount);
        messageActions.innerHTML = actions;
        messageActions.dataset.visible = actions ? 'true' : 'false';
      }

      function clearUploadActions() {
        messageActions.innerHTML = '';
        messageActions.dataset.visible = 'false';
      }

      async function collectDirectoryHandleFiles(directoryHandle, prefix = '') {
        const files = [];
        for await (const [name, handle] of directoryHandle.entries()) {
          if (handle.kind === 'file') {
            const file = await handle.getFile();
            Object.defineProperty(file, 'relativePath', { value: `${prefix}${name}` });
            files.push(file);
          } else if (handle.kind === 'directory') {
            files.push(...await collectDirectoryHandleFiles(handle, `${prefix}${name}/`));
          }
        }
        return files;
      }

      async function collectEntryFiles(entry, prefix = '') {
        if (!entry) {
          return [];
        }
        if (entry.isFile) {
          return new Promise((resolve) => {
            entry.file((file) => {
              Object.defineProperty(file, 'relativePath', { value: `${prefix}${file.name}` });
              resolve([file]);
            });
          });
        }
        if (!entry.isDirectory) {
          return [];
        }
        const reader = entry.createReader();
        const entries = [];
        async function readBatch() {
          const batch = await new Promise((resolve) => reader.readEntries(resolve));
          if (!batch.length) {
            return;
          }
          entries.push(...batch);
          await readBatch();
        }
        await readBatch();
        const nested = await Promise.all(entries.map((child) => collectEntryFiles(child, `${prefix}${entry.name}/`)));
        return nested.flat();
      }

      filesInput.addEventListener('change', () => {
        addFiles(filesInput.files);
        filesInput.value = '';
      });

      dropzone.addEventListener('click', async () => {
        if (!window.showDirectoryPicker) {
          message.textContent = 'Этот браузер не открывает папку по клику. Перетащите папку в область выбора.';
          clearUploadActions();
          return;
        }
        try {
          const handle = await window.showDirectoryPicker();
          addFiles(await collectDirectoryHandleFiles(handle, `${handle.name}/`));
        } catch (error) {
          if (!(error instanceof DOMException && error.name === 'AbortError')) {
            message.textContent = error instanceof Error ? error.message : String(error);
            clearUploadActions();
          }
        }
      });
      dropzone.addEventListener('keydown', (event) => {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          dropzone.click();
        }
      });

      dropzone.addEventListener('dragover', (event) => {
        event.preventDefault();
        dropzone.dataset.active = 'true';
      });
      dropzone.addEventListener('dragleave', () => {
        dropzone.dataset.active = 'false';
      });
      dropzone.addEventListener('drop', async (event) => {
        event.preventDefault();
        dropzone.dataset.active = 'false';
        const items = [...event.dataTransfer.items];
        const entries = items.map((item) => item.webkitGetAsEntry ? item.webkitGetAsEntry() : null).filter(Boolean);
        if (entries.length) {
          const files = await Promise.all(entries.map((entry) => collectEntryFiles(entry)));
          addFiles(files.flat());
        } else {
          addFiles(event.dataTransfer.files);
        }
      });

      uploadButton.addEventListener('click', async () => {
        const payloadFiles = selectedFiles();
        if (!payloadFiles.length) {
          statusText.textContent = 'Нет файлов, соответствующих требованиям.';
          return;
        }
        uploadButton.disabled = true;
        message.textContent = 'Идёт загрузка...';
        clearUploadActions();
        try {
          setProgress(10, 'Подготовка файлов...');
          const formData = new FormData();
          for (const file of payloadFiles) {
            formData.append('files', file, displayName(file));
          }
          setProgress(35, 'Передача файлов...');
          const responsePromise = fetch('/api/uploads/store', { method: 'POST', body: formData });
          setProgress(70, 'Загрузка завершена. Идёт обработка архива...');
          const response = await responsePromise;
          setProgress(100, 'Сессия загрузки завершена.');
          const data = await response.json();
          if (!response.ok) {
            throw new Error(data?.detail || data?.message || 'Не удалось загрузить файлы.');
          }
          const summary = data.summary || {};
          renderUploadComplete(summary, rejectedFiles.size, data.report_url);
          preparedFiles.clear();
          rejectedFiles.clear();
          updateSelectionSummary();
        } catch (error) {
          message.textContent = error instanceof Error ? error.message : String(error);
          clearUploadActions();
          progressBar.style.width = '0%';
        } finally {
          uploadButton.disabled = false;
        }
      });

      updateSelectionSummary();
    </script>
  </body>
</html>
""".replace("{{VERSION}}", _version_number()).replace("{{TOPBAR}}", _page_topbar(user)).replace("{{TOPBAR_CSS}}", _topbar_css()).strip()


def _uploads_html(user=None) -> str:
    user = user or get_user("admin@example.com")
    return """
<!doctype html>
<html lang="ru">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Загрузки · BM Log Analyzer</title>
    <style>
      :root { color-scheme: light; --panel: #fff; --bg: #eef2f6; --line: #d9e0e7; --text: #18212f; --muted: #667085; --blue: #2457a6; --green: #137752; }
      * { box-sizing: border-box; }
      body { margin: 0; background: var(--bg); color: var(--text); font: 14px/1.45 system-ui, sans-serif; }
      main { width: 100%; margin: 0 auto; padding: 24px; display: grid; justify-items: center; gap: 16px; }
      .header, .panel { background: var(--panel); border: 1px solid var(--line); border-radius: 16px; box-shadow: 0 16px 42px rgba(24, 33, 47, 0.06); }
      .header, .panel { width: 1180px; max-width: 100%; }
      .header { padding: 20px 22px; display: flex; justify-content: space-between; gap: 16px; align-items: baseline; flex-wrap: wrap; }
      h1 { margin: 0; font-size: 24px; }
      .muted { color: var(--muted); }
      .panel { padding: 18px 22px; }
      .toolbar { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; justify-content: space-between; margin-bottom: 16px; }
      .toolbar-actions { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }
      .button { appearance: none; border: 0; border-radius: 12px; background: linear-gradient(180deg, #2f6fd1, #2457a6); color: #fff; padding: 11px 16px; font: inherit; font-weight: 700; cursor: pointer; }
      .button--ghost { background: #edf2f7; color: var(--text); border: 1px solid var(--line); }
      .button:disabled { opacity: 0.6; cursor: progress; }
      .icon-button { width: 34px; height: 34px; display: inline-grid; place-items: center; border: 1px solid var(--line); border-radius: 10px; background: #edf2f7; color: var(--blue); font: 700 18px/1 system-ui, sans-serif; cursor: pointer; }
      .icon-button:hover { background: #e5edf7; }
      .icon-button:disabled { opacity: 0.5; cursor: progress; }
      .table-wrap { overflow: auto; border: 1px solid var(--line); border-radius: 14px; }
      table { width: 100%; border-collapse: collapse; background: var(--panel); }
      th, td { padding: 12px 14px; border-bottom: 1px solid #e8edf2; text-align: left; vertical-align: top; }
      th { background: #f3f6fa; font-weight: 700; position: sticky; top: 0; z-index: 1; }
      tr:hover td { background: #f8fbff; }
      .table-controls { display: flex; gap: 8px; align-items: center; color: var(--muted); font-size: 12px; }
      .row-checkbox { width: 18px; height: 18px; }
      .file-link, .report-link { color: var(--blue); text-decoration: none; font-weight: 600; }
      .report-empty { color: var(--muted); }
      .message { margin-top: 12px; padding: 14px; border: 1px dashed #c8d9f0; border-radius: 12px; background: #f8fbff; }
      .footer { color: var(--muted); font-size: 12px; margin-top: 8px; }
      a { color:var(--blue); text-decoration:none; font-weight:600; }
      {{TOPBAR_CSS}}
      @media (max-width: 720px) { main { padding: 16px; } }
    </style>
  </head>
  <body>
    <main>
      {{TOPBAR}}
      <header class="header">
        <div>
          <h1>Загрузки</h1>
          <div class="muted">Загруженные файлы и отчёты по ним.</div>
        </div>
        <div class="muted">BM Log Analyzer · {{VERSION}}</div>
      </header>
      <section class="panel">
        <div class="toolbar">
          <div class="table-controls">
            <span id="selected_count">Выбрано 0</span>
            <span id="uploads_count"></span>
          </div>
          <div class="toolbar-actions">
            <button id="refresh" class="button button--ghost" type="button">Обновить</button>
            <button id="build_report" class="button" type="button">Сформировать отчёт по выбранным</button>
          </div>
        </div>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th style="width:44px;"></th>
                <th>Дата загрузки (Мск)</th>
                <th>Пользователь</th>
                <th>Имя загруженного файла</th>
                <th>Отчёт</th>
                <th style="width:64px;">Действия</th>
              </tr>
            </thead>
            <tbody id="uploads_body"></tbody>
          </table>
        </div>
        <div id="uploads_message" class="message muted">Отчёт для каждой загрузки формируется сразу после приёма файла. Ниже можно собрать отдельный отчёт по выбранным строкам.</div>
        <div class="footer">Новые загрузки появляются без доступа к аналитике. Отчёты доступны отдельно.</div>
      </section>
    </main>
    <script>
      const uploadsBody = document.getElementById('uploads_body');
      const uploadsCount = document.getElementById('uploads_count');
      const selectedCount = document.getElementById('selected_count');
      const uploadsMessage = document.getElementById('uploads_message');
      const buildReportButton = document.getElementById('build_report');
      const refreshButton = document.getElementById('refresh');
      const selectedUploads = new Set();

      function renderSelectedCount() {
        selectedCount.textContent = `Выбрано ${selectedUploads.size}`;
        buildReportButton.disabled = selectedUploads.size === 0;
      }

      function rowHtml(item) {
        const status = (item.status || '').toLowerCase();
        const isProcessing = status === 'processing';
        const isSelected = selectedUploads.has(item.upload_id);
        const reportHtml = isProcessing
          ? `<span class="report-empty">${item.status_message || 'Формируем отчёт'}</span>`
          : item.report_url
            ? `<a class="report-link" href="${item.report_url}" target="_blank" rel="noreferrer">Открыть отчёт</a>`
            : status === 'error'
              ? `<span class="report-empty">${item.status_message || 'Ошибка формирования отчёта'}</span>`
              : `<span class="report-empty">${item.status_message || 'Ожидает обработки'}</span>`;
        const fileHtml = item.download_url
          ? `<a class="file-link" href="${item.download_url}" target="_blank" rel="noreferrer">${item.original_name}</a>`
          : `<span class="report-empty">${item.original_name} · ${item.status_message || 'Срок хранения истёк'}</span>`;
        const rebuildDisabled = isProcessing || !item.download_url ? 'disabled' : '';
        return `
          <tr>
            <td><input class="row-checkbox" type="checkbox" data-upload-id="${item.upload_id}" ${isSelected ? 'checked' : ''}></td>
            <td>${formatMoscowDateTime(item.created_at)}</td>
            <td>${item.owner_name || item.owner_email || 'Не указан'}</td>
            <td>${fileHtml}</td>
            <td data-report-cell="${item.upload_id}">${reportHtml}</td>
            <td>
              <button class="icon-button" type="button" title="пересобрать отчёт" aria-label="пересобрать отчёт" data-rebuild-upload-id="${item.upload_id}" ${rebuildDisabled}>↻</button>
            </td>
          </tr>`;
      }

      async function loadUploads() {
        const response = await fetch('/api/uploads?limit=200');
        const items = await response.json();
        uploadsCount.textContent = `Файлов: ${Array.isArray(items) ? items.length : 0}`;
        uploadsBody.innerHTML = Array.isArray(items) && items.length
          ? items.map((item) => rowHtml(item)).join('')
          : '<tr><td colspan="6" class="muted">Загрузок пока нет.</td></tr>';
        uploadsBody.querySelectorAll('input[data-upload-id]').forEach((checkbox) => {
          checkbox.addEventListener('change', () => {
            if (checkbox.checked) {
              selectedUploads.add(checkbox.dataset.uploadId);
            } else {
              selectedUploads.delete(checkbox.dataset.uploadId);
            }
            renderSelectedCount();
          });
        });
        uploadsBody.querySelectorAll('button[data-rebuild-upload-id]').forEach((button) => {
          button.addEventListener('click', () => rebuildUploadReport(button.dataset.rebuildUploadId, button));
        });
        renderSelectedCount();
      }

      function formatMoscowDateTime(value) {
        const date = new Date(value);
        if (Number.isNaN(date.getTime())) {
          return value || '';
        }
        const parts = new Intl.DateTimeFormat('ru-RU', {
          timeZone: 'Europe/Moscow',
          day: '2-digit',
          month: '2-digit',
          year: 'numeric',
          hour: '2-digit',
          minute: '2-digit',
          second: '2-digit',
          hour12: false,
        }).formatToParts(date).reduce((acc, part) => {
          acc[part.type] = part.value;
          return acc;
        }, {});
        return `${parts.day}.${parts.month}.${parts.year} (${parts.hour}:${parts.minute}:${parts.second})`;
      }

      async function rebuildUploadReport(uploadId, button) {
        if (!uploadId) {
          return;
        }
        button.disabled = true;
        const reportCell = uploadsBody.querySelector(`[data-report-cell="${uploadId}"]`);
        if (reportCell) {
          reportCell.innerHTML = '<span class="report-empty">Формируем отчёт</span>';
        }
        uploadsMessage.textContent = 'Формируем отчёт...';
        try {
          const response = await fetch(`/api/uploads/${encodeURIComponent(uploadId)}/rebuild`, { method: 'POST' });
          const data = await response.json();
          if (!response.ok) {
            throw new Error(data?.detail || data?.message || 'Не удалось пересобрать отчёт.');
          }
          uploadsMessage.innerHTML = `${data.message || 'Отчёт пересобран.'} <a class="report-link" href="${data.report_url}" target="_blank" rel="noreferrer">Открыть отчёт</a>`;
          await loadUploads();
        } catch (error) {
          uploadsMessage.textContent = error instanceof Error ? error.message : String(error);
          await loadUploads();
        }
      }

      buildReportButton.addEventListener('click', async () => {
        if (!selectedUploads.size) {
          uploadsMessage.textContent = 'Сначала выберите хотя бы один файл.';
          return;
        }
        buildReportButton.disabled = true;
        uploadsMessage.textContent = 'Формируем отчёт...';
        try {
          const response = await fetch('/api/uploads/report', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ upload_ids: [...selectedUploads] }),
          });
          const data = await response.json();
          if (!response.ok) {
            throw new Error(data?.detail || data?.message || 'Не удалось сформировать отчёт.');
          }
          uploadsMessage.innerHTML = `${data.message || 'Отчёт сформирован.'} <a class="report-link" href="${data.report_url}" target="_blank" rel="noreferrer">Открыть отчёт</a>`;
          selectedUploads.clear();
          await loadUploads();
          if (data.report_url) {
            window.location.href = data.report_url;
          }
        } catch (error) {
          uploadsMessage.textContent = error instanceof Error ? error.message : String(error);
        } finally {
          buildReportButton.disabled = false;
        }
      });

      refreshButton.addEventListener('click', loadUploads);
      loadUploads();
    </script>
  </body>
</html>
""".replace("{{VERSION}}", format_version()).replace("{{TOPBAR}}", _page_topbar(user)).replace("{{TOPBAR_CSS}}", _topbar_css()).strip()


_index_html = _landing_html

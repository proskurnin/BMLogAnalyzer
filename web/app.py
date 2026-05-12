from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from core.verification import run_healthchecks, run_readiness_check
from core.version import format_version
from dataclasses import asdict

from web.history import delete_history_run, latest_history, list_history, load_history_run, new_run_id, record_history, run_directory, run_report_path
from web.service import AnalysisRequest, analyze_request, execute_uploaded_analysis, build_summary_snapshot
from web.uploads import (
    collect_upload_files,
    delete_upload,
    list_uploads,
    load_upload,
    store_uploads,
    summary_from_uploads,
    update_upload_status,
    update_upload_reports,
)

try:  # pragma: no cover - optional dependency
    from fastapi import FastAPI, File, UploadFile
    from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
except ImportError:  # pragma: no cover - optional dependency
    FastAPI = None  # type: ignore[assignment]
    File = None  # type: ignore[assignment]
    UploadFile = None  # type: ignore[assignment]
    FileResponse = None  # type: ignore[assignment]
    HTMLResponse = None  # type: ignore[assignment]
    JSONResponse = None  # type: ignore[assignment]


def create_app() -> Any:
    if FastAPI is None or File is None or UploadFile is None or HTMLResponse is None or JSONResponse is None or FileResponse is None:  # pragma: no cover - optional dependency
        exc = ImportError("fastapi")
        raise RuntimeError("FastAPI is not installed. Install fastapi and uvicorn to use the web app.") from exc

    app = FastAPI(title="BM Log Analyzer", version=format_version())

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return _landing_html()

    @app.get("/uploads", response_class=HTMLResponse)
    def uploads_page() -> str:
        return _uploads_html()

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
    def report(run_id: str):
        return _render_report(run_id)

    @app.get("/report/{run_id}/manifest", response_class=JSONResponse)
    def report_manifest(run_id: str):
        return _render_report_manifest(run_id)

    @app.get("/api/runs/latest/report", response_class=HTMLResponse)
    def latest_report():
        item = latest_history()
        if not item:
            return HTMLResponse("<h1>Отчёт не найден</h1>", status_code=404)
        return _render_report(item.run_id)

    @app.get("/api/runs/latest/manifest", response_class=JSONResponse)
    def latest_manifest():
        item = latest_history()
        if not item:
            return JSONResponse({"detail": "Отчёт не найден"}, status_code=404)
        return _render_report_manifest(item.run_id)

    @app.post("/api/analyze")
    def analyze(payload: dict[str, Any]) -> dict[str, Any]:
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
        record_history(snapshot, mode="analysis", source="path", report_path=report_path)
        return asdict(snapshot)

    @app.post("/api/summary")
    def summary(payload: dict[str, Any]) -> dict[str, Any]:
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
        record_history(snapshot, mode="summary", source="path")
        return asdict(snapshot)

    @app.post("/api/upload/analyze")
    async def upload_analyze(
        files: list[UploadFile] = File(...),
    ) -> dict[str, Any]:
        run_id = new_run_id()
        payload_files = [(file.filename or "upload.bin", await file.read()) for file in files]
        request = AnalysisRequest(
            config_path="./config/config.yaml",
            reports_dir=str(run_directory(run_id)),
            extracted_dir=None,
            date=None,
            reader=None,
            bm=None,
            generate_reports=False,
        )
        bundle = execute_uploaded_analysis(request, payload_files, summary=False, storage_dir=run_directory(run_id))
        report_path = run_report_path(run_id)
        from reports.html_report import write_html_report

        write_html_report(bundle.events, bundle.result, report_path, stats=bundle.stats)
        record_history(
            bundle.snapshot,
            mode="analysis",
            source="upload",
            run_id=run_id,
            report_path=report_path,
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
        files: list[UploadFile] = File(...),
    ) -> dict[str, Any]:
        run_id = new_run_id()
        payload_files = [(file.filename or "upload.bin", await file.read()) for file in files]
        request = AnalysisRequest(
            config_path="./config/config.yaml",
            reports_dir=str(run_directory(run_id)),
            extracted_dir=None,
            date=None,
            reader=None,
            bm=None,
            generate_reports=False,
        )
        bundle = execute_uploaded_analysis(request, payload_files, summary=True, storage_dir=run_directory(run_id))
        report_path = run_report_path(run_id)
        from reports.html_report import write_html_report

        write_html_report(bundle.events, bundle.result, report_path, stats=bundle.stats)
        record_history(
            bundle.snapshot,
            mode="summary",
            source="upload",
            run_id=run_id,
            report_path=report_path,
        )
        return {
            "run_id": run_id,
            "status": "ok",
            "report_url": f"/report/{run_id}",
            "report_path": str(report_path),
            "snapshot": asdict(bundle.snapshot),
        }

    @app.get("/api/runs")
    def runs(limit: int = 12, mode: str | None = None) -> list[dict[str, Any]]:
        return [asdict(item) for item in list_history(limit=limit, mode=mode)]

    @app.get("/api/runs/latest")
    def latest_run() -> dict[str, Any]:
        item = latest_history()
        return asdict(item) if item else {}

    @app.get("/api/runs/{run_id}")
    def run_detail(run_id: str) -> dict[str, Any]:
        return load_history_run(run_id)

    @app.delete("/api/runs/{run_id}")
    def delete_run(run_id: str) -> dict[str, Any]:
        if not delete_history_run(run_id):
            return {"detail": "Сессия не найдена"}
        return {"status": "ok", "run_id": run_id}

    @app.get("/api/uploads")
    def uploads(limit: int = 200) -> list[dict[str, Any]]:
        return [asdict(item) for item in list_uploads(limit=limit)]

    @app.post("/api/uploads/store")
    async def store(files: list[UploadFile] = File(...)) -> dict[str, Any]:
        payload_files = [(file.filename or "upload.bin", await file.read()) for file in files]
        stored = store_uploads(payload_files)
        summary = summary_from_uploads(stored)
        report_updates: list[dict[str, Any]] = []
        for item in stored:
            update_upload_status(item.upload_id, status="processing", status_message="Формируем отчёт", storage_dir=None)
            run_id = new_run_id()
            report_root = run_directory(run_id)
            report_path = run_report_path(run_id)
            request = AnalysisRequest(
                config_path="./config/config.yaml",
                reports_dir=str(report_root),
                extracted_dir=None,
                date=None,
                reader=None,
                bm=None,
                generate_reports=False,
            )
            bundle = execute_uploaded_analysis(
                request,
                [(item.original_name, Path(item.stored_path).read_bytes())],
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
            )
            update_upload_reports([item.upload_id], report_run_id=run_id, report_url=f"/report/{run_id}")
            report_updates.append({"upload_id": item.upload_id, "run_id": run_id, "report_url": f"/report/{run_id}"})
        refreshed_items = [asdict(load_upload(item.upload_id)) for item in stored]
        return {
            "status": "ok",
            "summary": summary,
            "items": refreshed_items,
            "report_updates": report_updates,
            "message": summary["message"],
        }

    @app.post("/api/uploads/report")
    async def uploads_report(payload: dict[str, Any]) -> dict[str, Any]:
        upload_ids = [str(item) for item in payload.get("upload_ids", []) if str(item).strip()]
        if not upload_ids:
            return JSONResponse({"detail": "Не выбраны загрузки"}, status_code=400)
        selected_files = collect_upload_files(upload_ids)
        if not selected_files:
            return JSONResponse({"detail": "Выбранные загрузки не найдены"}, status_code=404)
        run_id = new_run_id()
        staging_dir = run_directory(run_id)
        temp_input = staging_dir / "input"
        temp_input.mkdir(parents=True, exist_ok=True)
        for index, (original_name, content) in enumerate(selected_files, start=1):
            safe_name = original_name.replace("/", "_").replace("\\", "_")
            target = temp_input / f"{index:03d}_{safe_name}"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
        request = AnalysisRequest(
            input_path=str(temp_input),
            config_path="./config/config.yaml",
            reports_dir=str(staging_dir),
            extracted_dir=str(staging_dir / "extracted"),
            date=None,
            reader=None,
            bm=None,
            generate_reports=False,
        )
        bundle = execute_uploaded_analysis(request, selected_files, summary=False, storage_dir=staging_dir)
        report_path = run_report_path(run_id)
        from reports.html_report import write_html_report

        write_html_report(bundle.events, bundle.result, report_path, stats=bundle.stats)
        record_history(
            bundle.snapshot,
            mode="analysis",
            source="uploads",
            run_id=run_id,
            report_path=report_path,
        )
        update_upload_reports(upload_ids, report_run_id=run_id, report_url=f"/report/{run_id}")
        return {
            "status": "ok",
            "run_id": run_id,
            "report_url": f"/report/{run_id}",
            "message": f"Отчёт сформирован для {len(upload_ids)} файлов.",
        }

    @app.get("/uploads/download/{upload_id}")
    def upload_download(upload_id: str):
        item = load_upload(upload_id)
        path = Path(item.stored_path)
        if not path.exists():
            return JSONResponse({"detail": "Файл не найден"}, status_code=404)
        filename = Path(item.original_name).name or path.name
        return FileResponse(path, filename=filename)

    return app


def _render_report(run_id: str):
    report_path = run_report_path(run_id)
    if report_path.exists():
        return HTMLResponse(report_path.read_text(encoding="utf-8"))
    payload = load_history_run(run_id)
    payload_report_path = payload.get("report_path") or ""
    if payload_report_path:
        file_path = Path(payload_report_path)
        if file_path.exists():
            return HTMLResponse(file_path.read_text(encoding="utf-8"))
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


def _landing_html() -> str:
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
      main { min-height: 100vh; display: grid; place-items: center; padding: 24px; }
      .card { width: min(640px, 100%); padding: 30px; border: 1px solid var(--line); border-radius: 18px; background: var(--panel); box-shadow: 0 24px 70px rgba(24, 33, 47, 0.08); display: grid; gap: 18px; text-align: center; }
      h1 { margin: 0; font-size: 30px; line-height: 1.05; letter-spacing: 0; }
      .version { color: var(--muted); font-size: 13px; }
      .field { display: grid; gap: 8px; text-align: left; }
      .field label { font-size: 12px; color: var(--muted); }
      .picker-row { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }
      .picker-button { appearance: none; border: 1px solid var(--line); border-radius: 14px; background: var(--soft); color: var(--text); padding: 14px 16px; font: inherit; cursor: pointer; }
      .picker-button:hover { border-color: #9eb6d1; background: #eef5fb; }
      .button { appearance: none; border: 0; border-radius: 14px; background: linear-gradient(180deg, #2f6fd1, #2457a6); color: #fff; padding: 14px 18px; font: inherit; font-weight: 700; cursor: pointer; width: 100%; }
      .button:disabled { opacity: 0.75; cursor: progress; }
      .picker-menu { display: grid; gap: 8px; margin-top: 8px; padding: 10px; border: 1px solid var(--line); border-radius: 14px; background: var(--soft); text-align: left; }
      .picker-choice { appearance: none; border: 1px solid var(--line); border-radius: 12px; background: #fff; color: var(--text); padding: 10px 12px; font: inherit; cursor: pointer; text-align: left; }
      .picker-choice:hover { background: #eef5fb; border-color: #9eb6d1; }
      .status { display: grid; gap: 10px; text-align: left; }
      .status-line { display: flex; justify-content: space-between; gap: 12px; color: var(--muted); }
      .progress-shell { height: 12px; border-radius: 999px; background: #edf2f7; border: 1px solid var(--line); overflow: hidden; }
      .progress-bar { width: 0%; height: 100%; background: linear-gradient(90deg, #6e98da, #2f6fd1); transition: width 180ms ease; }
      .message { min-height: 52px; padding: 14px; border-radius: 14px; background: #f8fbff; border: 1px dashed #c8d9f0; color: var(--text); text-align: left; }
      .signature { color: var(--muted); font-size: 12px; }
      .hint { color: var(--muted); font-size: 12px; text-align: left; }
      .selection-summary { padding: 10px 12px; border: 1px solid var(--line); border-radius: 12px; background: #fafcff; color: var(--text); text-align: left; min-height: 44px; }
      .selection-summary ul { margin: 0; padding-left: 18px; }
      .input-hidden { position: absolute; left: -9999px; width: 1px; height: 1px; opacity: 0; pointer-events: none; }
    </style>
  </head>
  <body>
    <main>
      <section class="card">
        <div>
          <h1>BM Log Analyzer</h1>
          <div class="version">версия сервиса {{VERSION}}</div>
        </div>
        <div class="field">
          <label>Логи и архивы</label>
          <button id="pick" class="button" type="button">Выбрать файлы или папку</button>
          <div id="picker_menu" class="picker-menu" hidden>
            <button type="button" class="picker-choice" data-target="files">Выбрать файлы</button>
            <button type="button" class="picker-choice" data-target="folder">Выбрать папку</button>
          </div>
          <input id="files" class="input-hidden" type="file" multiple>
          <input id="folder" class="input-hidden" type="file" webkitdirectory multiple>
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
        <div class="signature">made with ♥ by Roman A. Proskurnin</div>
      </section>
    </main>
    <script>
      const filesInput = document.getElementById('files');
      const folderInput = document.getElementById('folder');
      const pickButton = document.getElementById('pick');
      const pickerMenu = document.getElementById('picker_menu');
      const uploadButton = document.getElementById('upload');
      const statusText = document.getElementById('status_text');
      const fileCount = document.getElementById('file_count');
      const progressBar = document.getElementById('progress_bar');
      const message = document.getElementById('message');
      const selectionSummary = document.getElementById('selection_summary');

      function selectedFiles() {
        return [...filesInput.files, ...folderInput.files];
      }

      function setProgress(value, text) {
        progressBar.style.width = `${Math.max(0, Math.min(100, value))}%`;
        statusText.textContent = text;
      }

      function updateSelectionSummary() {
        const selected = selectedFiles();
        const total = selected.length;
        fileCount.textContent = total ? `${total} файлов` : '0 файлов';
        if (!total) {
          selectionSummary.textContent = 'Пока ничего не выбрано.';
          return;
        }
        const names = selected.slice(0, 4).map((file) => file.webkitRelativePath || file.name);
        const extra = total > names.length ? ` и ещё ${total - names.length}` : '';
        selectionSummary.innerHTML = `<ul>${names.map((name) => `<li>${name}</li>`).join('')}</ul>${extra ? `<div class="muted">${extra}</div>` : ''}`;
      }

      pickButton.addEventListener('click', () => {
        pickerMenu.hidden = !pickerMenu.hidden;
      });
      document.addEventListener('click', (event) => {
        if (!pickerMenu.contains(event.target) && event.target !== pickButton) {
          pickerMenu.hidden = true;
        }
      });
      pickerMenu.addEventListener('click', (event) => {
        const choice = event.target.closest('button[data-target]');
        if (!choice) {
          return;
        }
        pickerMenu.hidden = true;
        if (choice.dataset.target === 'files') {
          filesInput.click();
        } else {
          folderInput.click();
        }
      });
      filesInput.addEventListener('change', updateSelectionSummary);
      folderInput.addEventListener('change', updateSelectionSummary);

      uploadButton.addEventListener('click', async () => {
        const payloadFiles = selectedFiles();
        if (!payloadFiles.length) {
          statusText.textContent = 'Сначала выберите папку или файлы.';
          return;
        }
        uploadButton.disabled = true;
        message.textContent = 'Идёт загрузка...';
        try {
          setProgress(10, 'Подготовка файлов...');
          const formData = new FormData();
          for (const file of payloadFiles) {
            formData.append('files', file, file.webkitRelativePath || file.name);
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
          message.textContent = summary.message || data.message || 'Загрузка прошла без ошибок. Спасибо.';
          filesInput.value = '';
          folderInput.value = '';
          updateSelectionSummary();
        } catch (error) {
          message.textContent = error instanceof Error ? error.message : String(error);
          progressBar.style.width = '0%';
        } finally {
          uploadButton.disabled = false;
        }
      });

      updateSelectionSummary();
    </script>
  </body>
</html>
""".replace("{{VERSION}}", format_version()).strip()


def _uploads_html() -> str:
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
      main { max-width: 1180px; margin: 0 auto; padding: 24px; display: grid; gap: 16px; }
      .header, .panel { background: var(--panel); border: 1px solid var(--line); border-radius: 16px; box-shadow: 0 16px 42px rgba(24, 33, 47, 0.06); }
      .header { padding: 20px 22px; display: flex; justify-content: space-between; gap: 16px; align-items: baseline; flex-wrap: wrap; }
      h1 { margin: 0; font-size: 24px; }
      .muted { color: var(--muted); }
      .panel { padding: 18px 22px; }
      .toolbar { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; justify-content: space-between; margin-bottom: 16px; }
      .toolbar-actions { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }
      .button { appearance: none; border: 0; border-radius: 12px; background: linear-gradient(180deg, #2f6fd1, #2457a6); color: #fff; padding: 11px 16px; font: inherit; font-weight: 700; cursor: pointer; }
      .button--ghost { background: #edf2f7; color: var(--text); border: 1px solid var(--line); }
      .button:disabled { opacity: 0.6; cursor: progress; }
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
    </style>
  </head>
  <body>
    <main>
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
                <th>Дата загрузки</th>
                <th>Имя загруженного файла</th>
                <th>Отчёт</th>
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
        const reportHtml = item.report_url
          ? `<a class="report-link" href="${item.report_url}" target="_blank" rel="noreferrer">Открыть отчёт</a>`
          : status === 'processing'
            ? `<span class="report-empty">${item.status_message || 'Формируется отчёт...'}</span>`
            : status === 'error'
              ? `<span class="report-empty">${item.status_message || 'Ошибка формирования отчёта'}</span>`
              : `<span class="report-empty">${item.status_message || 'Ожидает обработки'}</span>`;
        const fileUrl = item.download_url || `/uploads/download/${item.upload_id}`;
        return `
          <tr>
            <td><input class="row-checkbox" type="checkbox" data-upload-id="${item.upload_id}"></td>
            <td>${item.created_at}</td>
            <td><a class="file-link" href="${fileUrl}" target="_blank" rel="noreferrer">${item.original_name}</a></td>
            <td>${reportHtml}</td>
          </tr>`;
      }

      async function loadUploads() {
        const response = await fetch('/api/uploads?limit=200');
        const items = await response.json();
        uploadsCount.textContent = `Файлов: ${Array.isArray(items) ? items.length : 0}`;
        uploadsBody.innerHTML = Array.isArray(items) && items.length
          ? items.map((item) => rowHtml(item)).join('')
          : '<tr><td colspan="4" class="muted">Загрузок пока нет.</td></tr>';
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
        renderSelectedCount();
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
""".replace("{{VERSION}}", format_version()).strip()


_index_html = _landing_html

from __future__ import annotations

from pathlib import Path
from typing import Any

from core.verification import run_healthchecks, run_readiness_check
from core.version import format_version
from dataclasses import asdict

from web.history import latest_history, list_history, load_history_run, new_run_id, record_history, run_directory, run_report_path
from web.service import AnalysisRequest, analyze_request, execute_uploaded_analysis, build_summary_snapshot

try:  # pragma: no cover - optional dependency
    from fastapi import FastAPI, File, UploadFile
    from fastapi.responses import HTMLResponse
except ImportError:  # pragma: no cover - optional dependency
    FastAPI = None  # type: ignore[assignment]
    File = None  # type: ignore[assignment]
    UploadFile = None  # type: ignore[assignment]
    HTMLResponse = None  # type: ignore[assignment]


def create_app() -> Any:
    if FastAPI is None or File is None or UploadFile is None or HTMLResponse is None:  # pragma: no cover - optional dependency
        exc = ImportError("fastapi")
        raise RuntimeError("FastAPI is not installed. Install fastapi and uvicorn to use the web app.") from exc

    app = FastAPI(title="BM Log Analyzer", version=format_version())

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return _index_html()

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
        report_path = run_report_path(run_id)
        if not report_path.exists():
            return HTMLResponse("<h1>Отчёт не найден</h1>", status_code=404)
        return HTMLResponse(report_path.read_text(encoding="utf-8"))

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
    def runs(limit: int = 12) -> list[dict[str, Any]]:
        return [asdict(item) for item in list_history(limit=limit)]

    @app.get("/api/runs/latest")
    def latest_run() -> dict[str, Any]:
        item = latest_history()
        return asdict(item) if item else {}

    @app.get("/api/runs/{run_id}")
    def run_detail(run_id: str) -> dict[str, Any]:
        return load_history_run(run_id)

    return app


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
          <strong>Последние сессии</strong>
          <span class="muted">Открывают готовый HTML-отчёт.</span>
        </div>
        <div id="history_list" class="history-list"></div>
        <p id="history_empty" class="muted history-empty">История пока пуста.</p>
        <div id="history_detail" class="session-card" hidden>
          <strong id="history_detail_title"></strong>
          <div id="history_detail_meta" class="meta"></div>
          <div id="history_detail_counts" class="meta"></div>
          <a id="history_detail_link" href="#" target="_blank" rel="noreferrer">Открыть отчёт</a>
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
      const steps = Array.from(document.querySelectorAll('.step'));

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
          return;
        }
        historyDetail.hidden = false;
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
        historyDetailLink.href = item.report_path ? item.report_path : `/report/${item.run_id}`;
        historyDetailLink.textContent = item.report_path ? 'Открыть отчёт' : 'Открыть отчёт';
      }

      async function loadHistory() {
        try {
          const response = await fetch('/api/runs?limit=8');
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
    </script>
  </body>
</html>
""".strip()

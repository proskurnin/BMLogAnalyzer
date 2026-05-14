from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any


AI_RESULT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "summary": {"type": "string"},
        "hypotheses": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "title": {"type": "string"},
                    "hypothesis": {"type": "string"},
                    "confidence": {"type": "string", "enum": ["low", "medium", "high"]},
                    "evidence_refs": {"type": "array", "items": {"type": "string"}},
                    "what_to_check": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["title", "hypothesis", "confidence", "evidence_refs", "what_to_check"],
            },
        },
        "what_to_check": {"type": "array", "items": {"type": "string"}},
        "limitations": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["summary", "hypotheses", "what_to_check", "limitations"],
}


def ai_analysis_enabled() -> bool:
    return _env_bool("BM_AI_ANALYSIS_ENABLED", False) and bool(os.getenv("OPENAI_API_KEY", "").strip())


def run_ai_analysis(context: dict[str, object]) -> dict[str, object]:
    if not ai_analysis_enabled():
        raise RuntimeError("AI-анализ выключен. Задайте BM_AI_ANALYSIS_ENABLED=true и OPENAI_API_KEY.")
    api_key = os.environ["OPENAI_API_KEY"].strip()
    model = os.getenv("BM_AI_MODEL", "gpt-4.1-mini").strip() or "gpt-4.1-mini"
    payload = {
        "model": model,
        "instructions": (
            "Ты аналитик BM логов. Отвечай только на русском. "
            "Не придумывай факты, числа, версии, причины и строки логов. "
            "Факты бери только из JSON-контекста. Всё, что не является фактом, помечай как гипотезу. "
            "Каждая гипотеза должна иметь evidence_refs на source_file:line_number или метрику из контекста."
        ),
        "input": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": json.dumps(context, ensure_ascii=False),
                    }
                ],
            }
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "bm_ai_analysis",
                "strict": True,
                "schema": AI_RESULT_SCHEMA,
            }
        },
    }
    data = _post_json("https://api.openai.com/v1/responses", payload, api_key=api_key)
    analysis = _extract_output_json(data)
    return {
        "schema_version": "bm-log-analyzer.ai-analysis.v1",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "model": model,
        "analysis": analysis,
    }


def _post_json(url: str, payload: dict[str, object], *, api_key: str) -> dict[str, object]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    opener = _openai_opener()
    try:
        with opener.open(request, timeout=60) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"OpenAI API error {exc.code}: {body}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"OpenAI API network error: {exc.reason}") from exc


def _openai_opener() -> urllib.request.OpenerDirector:
    proxies = _ai_proxies()
    if not proxies:
        return urllib.request.build_opener(urllib.request.ProxyHandler({}))
    return urllib.request.build_opener(urllib.request.ProxyHandler(proxies))


def _ai_proxies() -> dict[str, str]:
    https_proxy = os.getenv("BM_AI_HTTPS_PROXY", "").strip()
    http_proxy = os.getenv("BM_AI_HTTP_PROXY", "").strip()
    proxies: dict[str, str] = {}
    if https_proxy:
        proxies["https"] = https_proxy
    if http_proxy:
        proxies["http"] = http_proxy
    return proxies


def _extract_output_json(response: dict[str, object]) -> dict[str, object]:
    output = response.get("output")
    if isinstance(output, list):
        for item in output:
            if not isinstance(item, dict):
                continue
            content = item.get("content")
            if not isinstance(content, list):
                continue
            for part in content:
                if not isinstance(part, dict):
                    continue
                if part.get("type") == "output_text" and isinstance(part.get("text"), str):
                    return json.loads(part["text"])
    text = response.get("output_text")
    if isinstance(text, str):
        return json.loads(text)
    raise RuntimeError("OpenAI response does not contain structured output text")


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}

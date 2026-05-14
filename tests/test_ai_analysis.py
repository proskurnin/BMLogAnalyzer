from analytics.ai_analysis import _ai_proxies, _openai_opener


def test_ai_proxies_use_dedicated_environment(monkeypatch):
    monkeypatch.setenv("BM_AI_HTTPS_PROXY", "http://user:pass@proxy.example:3128")
    monkeypatch.setenv("BM_AI_HTTP_PROXY", "http://user:pass@proxy.example:3128")
    monkeypatch.setenv("HTTPS_PROXY", "http://global.example:3128")

    assert _ai_proxies() == {
        "https": "http://user:pass@proxy.example:3128",
        "http": "http://user:pass@proxy.example:3128",
    }


def test_ai_proxies_ignore_global_proxy_environment(monkeypatch):
    monkeypatch.delenv("BM_AI_HTTPS_PROXY", raising=False)
    monkeypatch.delenv("BM_AI_HTTP_PROXY", raising=False)
    monkeypatch.setenv("HTTPS_PROXY", "http://global.example:3128")
    monkeypatch.setenv("HTTP_PROXY", "http://global.example:3128")

    assert _ai_proxies() == {}
    assert _openai_opener() is not None

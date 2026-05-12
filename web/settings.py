from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class WebSettings:
    app_env: str
    data_dir: Path
    admin_email: str
    admin_password: str
    admin_name: str
    session_days: int
    cookie_secure: bool
    max_upload_file_bytes: int
    max_upload_session_bytes: int
    max_upload_files: int

    @property
    def production(self) -> bool:
        return self.app_env == "production"


def load_settings() -> WebSettings:
    app_env = os.getenv("BM_APP_ENV", "development").strip().lower() or "development"
    data_dir = Path(os.getenv("BM_DATA_DIR", "./_workdir"))
    admin_email = os.getenv("BM_ADMIN_EMAIL", "admin@example.com").strip()
    admin_password = os.getenv("BM_ADMIN_PASSWORD", "admin").strip()
    admin_name = os.getenv("BM_ADMIN_NAME", "Administrator").strip() or "Administrator"
    cookie_secure = _env_bool("BM_COOKIE_SECURE", app_env == "production")
    return WebSettings(
        app_env=app_env,
        data_dir=data_dir,
        admin_email=admin_email,
        admin_password=admin_password,
        admin_name=admin_name,
        session_days=_env_int("BM_SESSION_DAYS", 14),
        cookie_secure=cookie_secure,
        max_upload_file_bytes=_env_int("BM_MAX_UPLOAD_FILE_MB", 512) * 1024 * 1024,
        max_upload_session_bytes=_env_int("BM_MAX_UPLOAD_SESSION_MB", 2048) * 1024 * 1024,
        max_upload_files=_env_int("BM_MAX_UPLOAD_FILES", 200),
    )


def require_production_bootstrap_settings(settings: WebSettings) -> None:
    if not settings.production:
        return
    missing = [
        name
        for name in ("BM_ADMIN_EMAIL", "BM_ADMIN_PASSWORD")
        if not os.getenv(name, "").strip()
    ]
    if missing:
        names = ", ".join(missing)
        raise RuntimeError(f"Production startup requires explicit {names}")
    if settings.admin_password == "admin":
        raise RuntimeError("Production startup refuses default admin password")


def auth_dir() -> Path:
    return load_settings().data_dir / "auth"


def upload_store_dir() -> Path:
    return load_settings().data_dir / "upload_store"


def web_history_dir() -> Path:
    return load_settings().data_dir / "web_history"


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw)
    except ValueError:
        return default

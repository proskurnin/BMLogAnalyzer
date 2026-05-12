from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import secrets
from pathlib import Path

from web.models import UserModel
from web.settings import auth_dir, load_settings

DEFAULT_ADMIN_EMAIL = "admin@example.com"
DEFAULT_ADMIN_PASSWORD = "admin"
SESSION_COOKIE = "bm_auth_session"
VALID_ROLES = {"user", "admin"}


def _auth_root(storage_dir: Path | None = None) -> Path:
    root = Path(storage_dir) if storage_dir else auth_dir()
    root.mkdir(parents=True, exist_ok=True)
    return root


def _users_path(storage_dir: Path | None = None) -> Path:
    return _auth_root(storage_dir) / "users.json"


def _sessions_path(storage_dir: Path | None = None) -> Path:
    return _auth_root(storage_dir) / "sessions.json"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ensure_default_admin(storage_dir: Path | None = None) -> None:
    users = list_users(storage_dir)
    if users:
        return
    settings = load_settings()
    create_user(
        name=settings.admin_name,
        email=settings.admin_email,
        password=settings.admin_password,
        role="admin",
        storage_dir=storage_dir,
    )


def list_users(storage_dir: Path | None = None) -> list[UserModel]:
    path = _users_path(storage_dir)
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    return [UserModel(**item) for item in payload]


def save_users(users: list[UserModel], storage_dir: Path | None = None) -> None:
    _users_path(storage_dir).write_text(
        json.dumps([asdict(user) for user in users], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def create_user(
    *,
    name: str,
    email: str,
    password: str,
    role: str,
    storage_dir: Path | None = None,
) -> UserModel:
    users = list_users(storage_dir)
    normalized_email = normalize_email(email)
    if not normalized_email:
        raise ValueError("email is required")
    if any(user.email == normalized_email for user in users):
        raise ValueError("email already exists")
    if role not in VALID_ROLES:
        raise ValueError("invalid role")
    if not password:
        raise ValueError("password is required")
    user = UserModel(
        user_id=secrets.token_hex(8),
        name=name.strip() or normalized_email,
        email=normalized_email,
        password_hash=hash_password(password),
        role=role,
        created_at=_utc_now(),
    )
    users.append(user)
    save_users(users, storage_dir)
    return user


def update_user(
    email: str,
    *,
    name: str,
    new_email: str,
    role: str,
    password: str = "",
    storage_dir: Path | None = None,
) -> UserModel:
    users = list_users(storage_dir)
    normalized_email = normalize_email(email)
    normalized_new_email = normalize_email(new_email)
    if role not in VALID_ROLES:
        raise ValueError("invalid role")
    if not normalized_new_email:
        raise ValueError("email is required")
    if normalized_new_email != normalized_email and any(user.email == normalized_new_email for user in users):
        raise ValueError("email already exists")
    updated: UserModel | None = None
    result: list[UserModel] = []
    for user in users:
        if user.email != normalized_email:
            result.append(user)
            continue
        updated = UserModel(
            user_id=user.user_id,
            name=name.strip() or normalized_new_email,
            email=normalized_new_email,
            password_hash=hash_password(password) if password else user.password_hash,
            role=role,
            created_at=user.created_at,
        )
        result.append(updated)
    if updated is None:
        raise ValueError("user not found")
    save_users(result, storage_dir)
    return updated


def delete_user(email: str, storage_dir: Path | None = None) -> bool:
    normalized_email = normalize_email(email)
    users = list_users(storage_dir)
    remaining = [user for user in users if user.email != normalized_email]
    if len(remaining) == len(users):
        return False
    if not any(user.role == "admin" for user in remaining):
        raise ValueError("at least one admin is required")
    save_users(remaining, storage_dir)
    return True


def authenticate_user(email: str, password: str, storage_dir: Path | None = None) -> UserModel | None:
    normalized_email = normalize_email(email)
    for user in list_users(storage_dir):
        if user.email == normalized_email and verify_password(password, user.password_hash):
            return user
    return None


def create_session(email: str, storage_dir: Path | None = None) -> str:
    sessions = _load_sessions(storage_dir)
    token = secrets.token_urlsafe(32)
    expires_at = datetime.now(timezone.utc) + timedelta(days=load_settings().session_days)
    sessions[token] = {"email": normalize_email(email), "expires_at": expires_at.isoformat(timespec="seconds")}
    _save_sessions(sessions, storage_dir)
    return token


def destroy_session(token: str, storage_dir: Path | None = None) -> None:
    sessions = _load_sessions(storage_dir)
    sessions.pop(token, None)
    _save_sessions(sessions, storage_dir)


def user_from_session(token: str | None, storage_dir: Path | None = None) -> UserModel | None:
    if not token:
        return None
    sessions = _load_sessions(storage_dir)
    session = sessions.get(token)
    if not session:
        return None
    expires_at = datetime.fromisoformat(session["expires_at"])
    if expires_at < datetime.now(timezone.utc):
        sessions.pop(token, None)
        _save_sessions(sessions, storage_dir)
        return None
    email = session.get("email", "")
    return get_user(email, storage_dir)


def cleanup_expired_sessions(storage_dir: Path | None = None) -> int:
    sessions = _load_sessions(storage_dir)
    now = datetime.now(timezone.utc)
    kept = {
        token: session
        for token, session in sessions.items()
        if datetime.fromisoformat(session["expires_at"]) >= now
    }
    removed = len(sessions) - len(kept)
    if removed:
        _save_sessions(kept, storage_dir)
    return removed


def get_user(email: str, storage_dir: Path | None = None) -> UserModel | None:
    normalized_email = normalize_email(email)
    return next((user for user in list_users(storage_dir) if user.email == normalized_email), None)


def normalize_email(email: str) -> str:
    return email.strip().lower()


def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("ascii"), 200_000)
    return f"pbkdf2_sha256${salt}${digest.hex()}"


def verify_password(password: str, password_hash: str) -> bool:
    try:
        algorithm, salt, expected = password_hash.split("$", 2)
    except ValueError:
        return False
    if algorithm != "pbkdf2_sha256":
        return False
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("ascii"), 200_000).hex()
    return hmac.compare_digest(digest, expected)


def _load_sessions(storage_dir: Path | None = None) -> dict[str, dict[str, str]]:
    path = _sessions_path(storage_dir)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _save_sessions(sessions: dict[str, dict[str, str]], storage_dir: Path | None = None) -> None:
    _sessions_path(storage_dir).write_text(json.dumps(sessions, ensure_ascii=False, indent=2), encoding="utf-8")

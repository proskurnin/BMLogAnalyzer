from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta

from web.auth import (
    create_session,
    count_auth_events,
    list_auth_events,
    load_auth_policy,
    record_auth_event,
    read_auth_events,
    touch_session,
    update_auth_policy,
    user_from_session,
)


def test_auth_policy_defaults_and_persists(tmp_path):
    assert load_auth_policy(tmp_path).session_idle_minutes == 120

    policy = update_auth_policy(session_idle_minutes=45, storage_dir=tmp_path)
    assert policy.session_idle_minutes == 45
    assert load_auth_policy(tmp_path).session_idle_minutes == 45
    assert json.loads((tmp_path / "auth_policy.json").read_text(encoding="utf-8"))["session_idle_minutes"] == 45


def test_session_touch_extends_expiration(tmp_path):
    update_auth_policy(session_idle_minutes=10, storage_dir=tmp_path)
    token = create_session("user@example.com", storage_dir=tmp_path)
    sessions_path = tmp_path / "sessions.json"
    first_payload = json.loads(sessions_path.read_text(encoding="utf-8"))[token]

    assert first_payload["email"] == "user@example.com"
    assert "created_at" in first_payload
    assert "last_activity_at" in first_payload

    first_payload["last_activity_at"] = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(timespec="seconds")
    first_payload["expires_at"] = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(timespec="seconds")
    payload = json.loads(sessions_path.read_text(encoding="utf-8"))
    payload[token] = first_payload
    sessions_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    assert touch_session(token, storage_dir=tmp_path)
    second_payload = json.loads(sessions_path.read_text(encoding="utf-8"))[token]

    assert second_payload["last_activity_at"] > first_payload["last_activity_at"]
    assert datetime.fromisoformat(second_payload["expires_at"]) > datetime.fromisoformat(first_payload["expires_at"])


def test_session_expired_is_removed_and_journaled(tmp_path):
    token = create_session("user@example.com", storage_dir=tmp_path)
    sessions_path = tmp_path / "sessions.json"
    payload = json.loads(sessions_path.read_text(encoding="utf-8"))
    payload[token]["expires_at"] = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(timespec="seconds")
    sessions_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    assert user_from_session(token, storage_dir=tmp_path) is None
    events = list_auth_events(tmp_path, limit=10)
    assert events[0].event_type == "session_expired"
    assert events[0].status == "expired"


def test_auth_journal_appends_events(tmp_path):
    record_auth_event("login_success", email="user@example.com", user_name="User", status="success", storage_dir=tmp_path)
    record_auth_event("logout", email="user@example.com", user_name="User", status="success", storage_dir=tmp_path)

    events = list_auth_events(tmp_path, limit=10)
    assert [event.event_type for event in events] == ["logout", "login_success"]
    assert count_auth_events(tmp_path) == 2
    assert [event.event_type for event in read_auth_events(tmp_path, limit=1, offset=1)] == ["login_success"]

"""Threat-model tests for secret redaction in persisted discovery state.

These assert that *every* secret type this system holds is redacted — by literal
known value and by shape backstop — not just the patterns the implementation
happens to enumerate.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from denbust.discovery.redaction import redact_secrets, secret_values_from_env
from denbust.discovery.state_paths import write_discovery_run_snapshot, write_json_snapshot

# Fake secrets shaped like the real ones, assembled so no real key is in this file.
_GOOGLE = "AIza" + "Sy" + "C2xd" + "N" * 33  # AIza + 35 chars
_SUPABASE_JWT = "eyJ" + "abc123_" * 4 + ".eyJ" + "role_service_" * 2 + ".sig_" + "x" * 20
_BRAVE = "BSA" + "abcdefgh1234567890ABCDEFghijklmn"
_EXA = "1a2b3c4d-5e6f-7a8b-9c0d-1e2f3a4b5c6d"
_ANTHROPIC = "sk-ant-" + "a" * 24


def test_known_value_redaction_catches_any_format(monkeypatch: pytest.MonkeyPatch) -> None:
    """The literal values of credential-like env vars are scrubbed, whatever format."""
    monkeypatch.setenv("DENBUST_SUPABASE_SERVICE_ROLE_KEY", _SUPABASE_JWT)
    monkeypatch.setenv("DENBUST_BRAVE_SEARCH_API_KEY", _BRAVE)
    monkeypatch.setenv("DENBUST_EXA_API_KEY", _EXA)
    monkeypatch.setenv("ANTHROPIC_API_KEY", _ANTHROPIC)
    # A non-secret-looking env var must NOT be used for redaction.
    monkeypatch.setenv("DENBUST_STATE_ROOT", "state_repo")

    values = secret_values_from_env()
    assert _SUPABASE_JWT in values and _BRAVE in values and _EXA in values
    assert "state_repo" not in values

    for secret, label in [
        (_SUPABASE_JWT, "supabase service-role"),
        (_BRAVE, "brave"),
        (_EXA, "exa"),
        (_ANTHROPIC, "anthropic"),
    ]:
        text = f"error talking to {label}: token was {secret} in body"
        assert secret not in redact_secrets(text), label


def test_shape_backstops_catch_foreign_secrets() -> None:
    """Secrets NOT in our env are still caught by shape patterns (env values disabled)."""
    cases = {
        "google url param": f"for url '...customsearch/v1?key={_GOOGLE}&cx=abc'",
        "supabase jwt in json body": f'{{"apikey":"{_SUPABASE_JWT}"}}',
        "bearer jwt": f"Authorization: Bearer {_SUPABASE_JWT}",
        "brave header": f"422 X-Subscription-Token: {_BRAVE}",
        "exa header": f"401 x-api-key: {_EXA}",
        "anthropic sk key": f"failed with {_ANTHROPIC}",
    }
    for label, text in cases.items():
        out = redact_secrets(text, known_values=[])  # force backstop-only (no env values)
        secret = next(s for s in (_GOOGLE, _SUPABASE_JWT, _BRAVE, _EXA, _ANTHROPIC) if s in text)
        assert secret not in out, f"{label} leaked: {out}"


def test_leaves_ordinary_text_and_url_slugs_untouched() -> None:
    slug = "https://amtsavisen.dk/udland/norsk-forbud-mod-prostitution-virker"
    assert redact_secrets(slug, known_values=[]) == slug
    plain = "walla adapter failed: TimeoutException: timed out after 30s"
    assert redact_secrets(plain, known_values=[]) == plain


def test_redaction_is_idempotent() -> None:
    text = f"google_cse 403 ?key={_GOOGLE}; supabase Bearer {_SUPABASE_JWT}"
    once = redact_secrets(text, known_values=[])
    assert redact_secrets(once, known_values=[]) == once


def test_run_snapshot_writer_scrubs_secret_bearing_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """write_discovery_run_snapshot must never persist a secret in errors[]."""
    monkeypatch.setenv("DENBUST_GOOGLE_CSE_API_KEY", _GOOGLE)
    payload = {
        "run_id": "r1",
        "errors": [f"google_cse: 403 for url '...v1?key={_GOOGLE}&cx=abc'"],
    }
    path = write_discovery_run_snapshot(
        tmp_path, payload, run_timestamp=datetime(2026, 6, 14, tzinfo=UTC)
    )
    assert _GOOGLE not in path.read_text(encoding="utf-8")


def test_batch_snapshot_writer_scrubs_secrets(tmp_path: Path) -> None:
    """write_json_snapshot (backfill batch records) is also redacted."""
    path = write_json_snapshot(
        tmp_path / "batch.json",
        {"batch_id": "b1", "errors": [f'{{"apikey":"{_SUPABASE_JWT}"}}']},
    )
    assert _SUPABASE_JWT not in path.read_text(encoding="utf-8")

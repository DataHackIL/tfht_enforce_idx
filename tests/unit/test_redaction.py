"""Unit tests for secret redaction in persisted discovery state."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from denbust.discovery.redaction import redact_secrets
from denbust.discovery.state_paths import write_discovery_run_snapshot

# A realistic Google-CSE 403 error that echoes the API key in the request URL —
# the exact shape that leaked. Built so no real key appears in this test file.
_FAKE_KEY = "AIza" + "Sy" + "C2xd" + "N" * 33  # AIza + 35 chars
_CSE_ERROR = (
    "google_cse: HTTPStatusError: Client error '403 Forbidden' for url "
    f"'https://customsearch.googleapis.com/customsearch/v1?key={_FAKE_KEY}&cx=abc&q=x'"
)


def test_redacts_url_key_param_and_google_key() -> None:
    out = redact_secrets(_CSE_ERROR)
    assert _FAKE_KEY not in out
    assert "key=REDACTED" in out
    assert "cx=abc" in out  # non-secret identifier preserved
    assert "403 Forbidden" in out  # the useful error context survives


def test_redacts_bearer_and_sk_keys() -> None:
    assert "REDACTED" in redact_secrets("Authorization: Bearer abcdef0123456789ABCDEF")
    assert "abcdef0123456789ABCDEF" not in redact_secrets("Bearer abcdef0123456789ABCDEF")
    sk = "sk-ant-" + "a" * 24
    assert sk not in redact_secrets(f"error using {sk} oops")


def test_leaves_ordinary_text_and_url_slugs_untouched() -> None:
    # URL slugs containing "sk-" (e.g. Danish "norsk-forbud") must not be mangled.
    slug = "https://amtsavisen.dk/udland/norsk-forbud-mod-prostitution-virker"
    assert redact_secrets(slug) == slug
    plain = "walla adapter failed: TimeoutException: timed out after 30s"
    assert redact_secrets(plain) == plain


def test_redaction_is_idempotent() -> None:
    once = redact_secrets(_CSE_ERROR)
    assert redact_secrets(once) == once


def test_run_snapshot_writer_scrubs_secret_bearing_errors(tmp_path: Path) -> None:
    """write_discovery_run_snapshot must never persist a key, even if one slips
    into a run's errors[] (the safety net behind the engine-level redaction)."""
    payload = {
        "run_id": "r1",
        "status": "partial",
        "errors": [_CSE_ERROR],
    }
    path = write_discovery_run_snapshot(
        tmp_path, payload, run_timestamp=datetime(2026, 6, 14, tzinfo=UTC)
    )
    written = path.read_text(encoding="utf-8")
    assert _FAKE_KEY not in written
    assert "key=REDACTED" in written

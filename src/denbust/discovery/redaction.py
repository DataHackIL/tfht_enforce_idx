"""Redact secret-bearing substrings before they are persisted to state.

Some discovery APIs put credentials in the request URL — Google CSE takes the
API key as a ``?key=`` query parameter — so an HTTP client error's text, which
echoes the request URL, can carry a live key. When that error string was stored
verbatim in a run's ``errors[]`` it ended up on disk (run snapshots, backfill
batches), and a seed of that state to a public repo leaked the key.

``redact_secrets`` scrubs known credential shapes so they never reach disk.
Apply it wherever an exception or external string is turned into stored state
(error lists, run snapshots). It is intentionally conservative — it targets
credential patterns (URL key/token params, Google ``AIza`` keys, Bearer/Basic
tokens, ``sk-…`` keys), not arbitrary text — so it will not mangle candidate
URLs or titles.
"""

from __future__ import annotations

import re

_REDACTED = "REDACTED"

# Secret-bearing URL query parameters: ?key=… &token=… &api_key=… etc.
_URL_SECRET_PARAM = re.compile(
    r"([?&](?:key|api[_-]?key|apikey|access[_-]?token|token|auth|password|secret|sig)=)"
    r"[^&\s\"']+",
    re.IGNORECASE,
)
# Google API keys (e.g. CSE): AIza + 35 chars.
_GOOGLE_API_KEY = re.compile(r"AIza[0-9A-Za-z_-]{30,}")
# HTTP Authorization values.
_BEARER_BASIC = re.compile(r"\b(Bearer|Basic)\s+[A-Za-z0-9._=+/-]{16,}", re.IGNORECASE)
# OpenAI/Anthropic-style keys (require a known prefix so URL slugs like
# "norsk-forbud-…" are not matched).
_SK_KEY = re.compile(r"\bsk-(?:ant|proj|live|test|svcacct)-[A-Za-z0-9_-]{12,}")


def redact_secrets(text: str) -> str:
    """Return *text* with known credential shapes replaced by ``REDACTED``."""
    text = _URL_SECRET_PARAM.sub(r"\1" + _REDACTED, text)
    text = _GOOGLE_API_KEY.sub("AIza-" + _REDACTED, text)
    text = _BEARER_BASIC.sub(r"\1 " + _REDACTED, text)
    text = _SK_KEY.sub("sk-" + _REDACTED, text)
    return text

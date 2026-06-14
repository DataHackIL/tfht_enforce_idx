"""Redact secrets from strings before they are persisted to state.

Some discovery/operational APIs put credentials in request URLs, headers, or JSON
error bodies, so an HTTP client error's text can echo a live secret. When such a
string was stored verbatim in a run's ``errors[]`` it reached disk (run snapshots,
backfill batches) and a seed of that state to a public repo leaked a key.

Two layers, applied together:

1. **Known-value redaction (primary).** This system holds its own secrets in the
   environment (``ANTHROPIC_API_KEY``, ``DENBUST_*`` keys/tokens, Supabase /
   object-store / Kaggle / HF credentials). We strip the *literal values* of those
   env vars, so any secret is removed regardless of its format (JWT, UUID, opaque
   token) — you match the bytes you hold, not a guessed shape.

2. **Shape backstops (secondary).** For *foreign* secrets not in our env, a set of
   conservative patterns (URL key/token params, JSON secret fields, header tokens,
   Google ``AIza`` keys, JWTs, ``Bearer``/``Basic``, ``sk-…`` keys). These target
   credential shapes, not arbitrary text, so candidate URLs/titles are untouched.
"""

from __future__ import annotations

import os
import re
from collections.abc import Iterable, Mapping

_REDACTED = "REDACTED"

# Env vars whose VALUES are secrets to scrub by literal match (name heuristic),
# plus a minimum length so short/common values can't cause over-redaction.
_SECRET_ENV_NAME = re.compile(r"KEY|TOKEN|SECRET|PASSWORD|PASSWD|CREDENTIAL", re.IGNORECASE)
_MIN_SECRET_LEN = 8

# Shape backstops for foreign secrets (not present in our environment).
_URL_SECRET_PARAM = re.compile(
    r"([?&](?:key|api[_-]?key|apikey|access[_-]?token|token|auth|password|secret|sig)=)"
    r"[^&\s\"']+",
    re.IGNORECASE,
)
_JSON_SECRET_FIELD = re.compile(
    r"(\"[A-Za-z0-9_]*(?:key|token|secret|password|apikey)[A-Za-z0-9_]*\"\s*:\s*\")[^\"]{8,}\"",
    re.IGNORECASE,
)
_HEADER_SECRET = re.compile(
    r"\b([A-Za-z][A-Za-z-]*(?:api[-_]?key|subscription-token|token)\s*[:=]\s*)[^\s\"',&]{12,}",
    re.IGNORECASE,
)
_GOOGLE_API_KEY = re.compile(r"AIza[0-9A-Za-z_-]{30,}")
_JWT = re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}")
_BEARER_BASIC = re.compile(r"\b(Bearer|Basic)\s+[A-Za-z0-9._=+/-]{16,}", re.IGNORECASE)
_SK_KEY = re.compile(r"\bsk-(?:ant|proj|live|test|svcacct)-[A-Za-z0-9_-]{12,}")


def secret_values_from_env(env: Mapping[str, str] | None = None) -> list[str]:
    """Collect literal secret values from env vars whose name looks credential-like.

    Returned longest-first so a longer secret is redacted before any value that is
    a substring of it.
    """
    source = os.environ if env is None else env
    values = {
        value
        for name, value in source.items()
        if value and len(value) >= _MIN_SECRET_LEN and _SECRET_ENV_NAME.search(name)
    }
    return sorted(values, key=len, reverse=True)


def redact_secrets(text: str, *, known_values: Iterable[str] | None = None) -> str:
    """Return *text* with known secret values and credential shapes replaced.

    ``known_values`` overrides the env-derived secret list (used by tests). When
    omitted, the literal values of credential-like environment variables are used.
    """
    values = list(known_values) if known_values is not None else secret_values_from_env()
    for value in sorted((v for v in values if len(v) >= _MIN_SECRET_LEN), key=len, reverse=True):
        text = text.replace(value, _REDACTED)

    text = _URL_SECRET_PARAM.sub(r"\1" + _REDACTED, text)
    text = _JSON_SECRET_FIELD.sub(r"\1" + _REDACTED + '"', text)
    text = _HEADER_SECRET.sub(r"\1" + _REDACTED, text)
    text = _GOOGLE_API_KEY.sub("AIza-" + _REDACTED, text)
    text = _JWT.sub(_REDACTED, text)
    text = _BEARER_BASIC.sub(r"\1 " + _REDACTED, text)
    text = _SK_KEY.sub("sk-" + _REDACTED, text)
    return text

"""Operational-store factory helpers."""

from __future__ import annotations

from pathlib import Path

from denbust.config import Config, OperationalProvider
from denbust.ops.storage import LocalJsonOperationalStore, NullOperationalStore, OperationalStore
from denbust.ops.supabase import SupabaseOperationalStore


def default_local_json_root(config: Config) -> Path:
    """Derive a shared local operational-store root for a dataset."""
    return config.store.state_root / config.dataset_name / "operational"


def create_operational_store(config: Config) -> OperationalStore:
    """Instantiate the configured operational-store backend for a job run."""
    provider = config.operational.provider
    if provider is OperationalProvider.NONE:
        return NullOperationalStore()
    if provider is OperationalProvider.LOCAL_JSON:
        root_dir = config.operational.root_dir or default_local_json_root(config)
        return LocalJsonOperationalStore(root_dir)
    if provider is OperationalProvider.SUPABASE:
        if not config.supabase_url:
            raise ValueError("DENBUST_SUPABASE_URL is required for the Supabase operational store")
        if not config.supabase_service_role_key:
            raise ValueError(
                "DENBUST_SUPABASE_SERVICE_ROLE_KEY is required for the Supabase operational store"
            )
        return SupabaseOperationalStore(
            base_url=config.supabase_url,
            service_role_key=config.supabase_service_role_key,
            config=config.operational,
        )
    raise ValueError(f"Unsupported operational store provider: {provider}")

"""Latest-backup upload hooks for news_items release bundles."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, cast

from denbust.config import Config
from denbust.publish.backup import BackupManifest, BackupTarget

logger = logging.getLogger(__name__)


def find_latest_release_dir(publication_root: Path) -> Path:
    """Locate the latest built release directory under the publication root."""
    candidates = [
        candidate
        for candidate in publication_root.iterdir()
        if candidate.is_dir() and (candidate / "MANIFEST.json").exists()
    ]
    if not candidates:
        raise FileNotFoundError(f"No release bundles found under {publication_root}")
    return sorted(candidates)[-1]


def _release_version_from_dir(release_dir: Path) -> str | None:
    manifest_path = release_dir / "MANIFEST.json"
    if not manifest_path.exists():
        return None
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        version = payload.get("release_version")
        if isinstance(version, str):
            return version
    return None


class GoogleDriveLatestBackupUploader:
    """Upload the latest release bundle into a designated Google Drive folder."""

    def __init__(self, *, service_account_json: str | None) -> None:
        self._service_account_json = service_account_json

    def upload(self, *, release_dir: Path, folder_id: str) -> list[str]:
        """Upsert the release bundle files into Drive by filename."""
        if not self._service_account_json:
            raise ValueError("Google Drive backup requires DENBUST_DRIVE_SERVICE_ACCOUNT_JSON.")

        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload

        credentials_factory = cast(Any, service_account.Credentials)
        credentials = credentials_factory.from_service_account_file(
            self._service_account_json,
            scopes=["https://www.googleapis.com/auth/drive"],
        )
        service = build("drive", "v3", credentials=credentials, cache_discovery=False)

        uploaded: list[str] = []
        for path in sorted(release_dir.iterdir()):
            if not path.is_file():
                continue
            escaped_name = path.name.replace("'", "\\'")
            query = f"'{folder_id}' in parents and trashed = false and name = '{escaped_name}'"
            existing = (
                service.files()
                .list(q=query, fields="files(id,name)", pageSize=1)
                .execute()
                .get("files", [])
            )
            media = MediaFileUpload(str(path), resumable=False)
            if existing:
                service.files().update(fileId=existing[0]["id"], media_body=media).execute()
            else:
                service.files().create(
                    body={"name": path.name, "parents": [folder_id]},
                    media_body=media,
                    fields="id,name",
                ).execute()
            uploaded.append(path.name)
        logger.info("Uploaded %s files to Google Drive folder %s", len(uploaded), folder_id)
        return uploaded


class ObjectStorageLatestBackupUploader:
    """Upload the latest release bundle into object storage under a fixed prefix."""

    def __init__(
        self,
        *,
        endpoint_url: str | None,
        access_key_id: str | None,
        secret_access_key: str | None,
    ) -> None:
        self._endpoint_url = endpoint_url
        self._access_key_id = access_key_id
        self._secret_access_key = secret_access_key

    def upload(self, *, release_dir: Path, bucket: str, prefix: str) -> list[str]:
        """Upload release bundle files to S3-compatible object storage."""
        if not self._access_key_id or not self._secret_access_key:
            raise ValueError(
                "Object storage backup requires DENBUST_OBJECT_STORE_ACCESS_KEY_ID and DENBUST_OBJECT_STORE_SECRET_ACCESS_KEY."
            )

        import boto3

        client = boto3.client(
            "s3",
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._access_key_id,
            aws_secret_access_key=self._secret_access_key,
        )
        uploaded: list[str] = []
        normalized_prefix = prefix.strip("/")
        for path in sorted(release_dir.iterdir()):
            if not path.is_file():
                continue
            key = f"{normalized_prefix}/{path.name}" if normalized_prefix else path.name
            client.upload_file(str(path), bucket, key)
            uploaded.append(key)
        logger.info("Uploaded %s files to object storage bucket %s", len(uploaded), bucket)
        return uploaded


def execute_latest_backup(config: Config, *, publication_root: Path) -> BackupManifest:
    """Upload the latest release bundle to configured backup targets."""
    release_dir = find_latest_release_dir(publication_root)
    release_version = _release_version_from_dir(release_dir)
    targets: list[BackupTarget] = []

    if config.backup.google_drive.enabled and config.backup.google_drive.folder_id:
        logger.info(
            "Google Drive backup target is active for folder %s",
            config.backup.google_drive.folder_id,
        )
        drive_uploader = GoogleDriveLatestBackupUploader(
            service_account_json=config.drive_service_account_json,
        )
        uploaded = drive_uploader.upload(
            release_dir=release_dir,
            folder_id=config.backup.google_drive.folder_id,
        )
        targets.append(
            BackupTarget(
                name="google_drive",
                kind="google_drive",
                location=config.backup.google_drive.folder_id,
                status="uploaded",
                uploaded_files=uploaded,
            )
        )
    else:
        logger.info(
            "Google Drive backup target is inactive (enabled=%s, folder_id_present=%s)",
            config.backup.google_drive.enabled,
            bool(config.backup.google_drive.folder_id),
        )

    if config.backup.object_storage.enabled and config.backup.object_storage.bucket:
        logger.info(
            "Object storage backup target is active for bucket %s",
            config.backup.object_storage.bucket,
        )
        object_uploader = ObjectStorageLatestBackupUploader(
            endpoint_url=config.object_store_endpoint_url,
            access_key_id=config.object_store_access_key_id,
            secret_access_key=config.object_store_secret_access_key,
        )
        uploaded = object_uploader.upload(
            release_dir=release_dir,
            bucket=config.backup.object_storage.bucket,
            prefix=config.backup.object_storage.prefix,
        )
        targets.append(
            BackupTarget(
                name="object_storage",
                kind="object_storage",
                location=f"{config.backup.object_storage.bucket}/{config.backup.object_storage.prefix}",
                status="uploaded",
                uploaded_files=uploaded,
            )
        )
    else:
        logger.info(
            "Object storage backup target is inactive (enabled=%s, bucket_present=%s)",
            config.backup.object_storage.enabled,
            bool(config.backup.object_storage.bucket),
        )

    return BackupManifest(
        dataset_name=config.dataset_name.value,
        release_version=release_version,
        targets=targets,
        notes="Latest news_items release bundle uploaded to configured backup targets.",
    )

"""Publication hooks for weekly news_items dataset releases."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from denbust.config import Config
from denbust.publish.release import ReleaseManifest

logger = logging.getLogger(__name__)


class KagglePublisher:
    """Publish a release bundle to an existing Kaggle dataset."""

    def __init__(self, *, username: str | None, key: str | None) -> None:
        self._username = username
        self._key = key

    def publish(self, *, release_dir: Path, manifest: ReleaseManifest, dataset_slug: str) -> str:
        """Upload a new version of a Kaggle dataset."""
        if not self._username or not self._key:
            raise ValueError("Kaggle publishing requires KAGGLE_USERNAME and KAGGLE_KEY.")

        import os

        from kaggle.api.kaggle_api_extended import KaggleApi  # type: ignore[import-not-found]

        metadata_path = release_dir / "dataset-metadata.json"
        metadata = {
            "id": dataset_slug,
            "title": f"TFHT {manifest.dataset_name} {manifest.release_version}",
            "subtitle": "Metadata-only public release",
            "licenses": [{"name": "CC-BY-4.0"}],
        }
        metadata_path.write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )

        previous_username = os.environ.get("KAGGLE_USERNAME")
        previous_key = os.environ.get("KAGGLE_KEY")
        os.environ["KAGGLE_USERNAME"] = self._username
        os.environ["KAGGLE_KEY"] = self._key
        try:
            api = KaggleApi()
            api.authenticate()
            api.dataset_create_version(
                folder=str(release_dir),
                version_notes=f"Weekly release {manifest.release_version}",
                delete_old_versions=False,
                quiet=True,
            )
        finally:
            if previous_username is None:
                os.environ.pop("KAGGLE_USERNAME", None)
            else:
                os.environ["KAGGLE_USERNAME"] = previous_username
            if previous_key is None:
                os.environ.pop("KAGGLE_KEY", None)
            else:
                os.environ["KAGGLE_KEY"] = previous_key

        logger.info(
            "Published release %s to Kaggle dataset %s", manifest.release_version, dataset_slug
        )
        return dataset_slug


class HuggingFacePublisher:
    """Publish a release bundle to a Hugging Face dataset repo."""

    def __init__(self, *, token: str | None) -> None:
        self._token = token

    def publish(self, *, release_dir: Path, manifest: ReleaseManifest, repo_id: str) -> str:
        """Upload or update a dataset repository on Hugging Face Hub."""
        if not self._token:
            raise ValueError("Hugging Face publishing requires HF_TOKEN.")

        from huggingface_hub import HfApi

        api = HfApi(token=self._token)
        api.create_repo(repo_id=repo_id, repo_type="dataset", exist_ok=True)
        api.upload_folder(
            repo_id=repo_id,
            repo_type="dataset",
            folder_path=str(release_dir),
            commit_message=f"Publish {manifest.dataset_name} {manifest.release_version}",
        )
        logger.info(
            "Published release %s to Hugging Face dataset repo %s",
            manifest.release_version,
            repo_id,
        )
        return repo_id


def publish_release_bundle(
    *,
    config: Config,
    release_dir: Path,
    manifest: ReleaseManifest,
) -> list[str]:
    """Publish a built release bundle to the configured public targets."""
    targets: list[str] = []
    if config.release.kaggle_dataset:
        logger.info(
            "Kaggle publication target is active for dataset %s", config.release.kaggle_dataset
        )
        kaggle_publisher = KagglePublisher(
            username=config.kaggle_username,
            key=config.kaggle_key,
        )
        targets.append(
            "kaggle:"
            + kaggle_publisher.publish(
                release_dir=release_dir,
                manifest=manifest,
                dataset_slug=config.release.kaggle_dataset,
            )
        )
    else:
        logger.info(
            "Kaggle publication target is inactive; release bundle will not be pushed to Kaggle."
        )

    if config.release.huggingface_repo_id:
        logger.info(
            "Hugging Face publication target is active for repo %s",
            config.release.huggingface_repo_id,
        )
        hf_publisher = HuggingFacePublisher(token=config.huggingface_token)
        targets.append(
            "huggingface:"
            + hf_publisher.publish(
                release_dir=release_dir,
                manifest=manifest,
                repo_id=config.release.huggingface_repo_id,
            )
        )
    else:
        logger.info(
            "Hugging Face publication target is inactive; release bundle will not be pushed to Hugging Face."
        )

    return targets

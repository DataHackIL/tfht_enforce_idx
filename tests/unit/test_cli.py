"""Unit tests for CLI command wiring."""

import runpy
from pathlib import Path

import pytest
import typer
from typer.testing import CliRunner

from denbust.cli import app
from denbust.models.common import DatasetName, JobName

runner = CliRunner()


class TestCli:
    """Tests for the Typer CLI entrypoint."""

    def test_scan_uses_default_config_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Scan should default to the tracked news config when none is provided."""
        captured: dict[str, Path | int | None] = {}

        def fake_run_pipeline(config_path: Path, days_override: int | None = None) -> None:
            captured["config_path"] = config_path
            captured["days_override"] = days_override

        monkeypatch.setattr("denbust.pipeline.run_pipeline", fake_run_pipeline)

        result = runner.invoke(app, ["scan"])

        assert result.exit_code == 0
        assert captured["config_path"] == Path("agents/news/local.yaml")
        assert captured["days_override"] is None

    def test_scan_passes_explicit_flags(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Scan should forward explicit config and day overrides."""
        captured: dict[str, Path | int | None] = {}

        def fake_run_pipeline(config_path: Path, days_override: int | None = None) -> None:
            captured["config_path"] = config_path
            captured["days_override"] = days_override

        monkeypatch.setattr("denbust.pipeline.run_pipeline", fake_run_pipeline)

        result = runner.invoke(app, ["scan", "--config", "agents/custom.yaml", "--days", "5"])

        assert result.exit_code == 0
        assert captured["config_path"] == Path("agents/custom.yaml")
        assert captured["days_override"] == 5

    def test_run_dispatches_dataset_job_flags(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """run should forward dataset/job/config flags to the generic runner."""
        captured: dict[str, object] = {}

        def fake_run_job(
            *,
            config_path: Path,
            dataset_name: DatasetName,
            job_name: JobName,
            days_override: int | None = None,
        ) -> None:
            captured["config_path"] = config_path
            captured["dataset_name"] = dataset_name
            captured["job_name"] = job_name
            captured["days_override"] = days_override

        monkeypatch.setattr("denbust.pipeline.run_job", fake_run_job)

        result = runner.invoke(
            app,
            [
                "run",
                "--dataset",
                "news_items",
                "--job",
                "ingest",
                "--config",
                "agents/custom.yaml",
                "--days",
                "9",
            ],
        )

        assert result.exit_code == 0
        assert captured == {
            "config_path": Path("agents/custom.yaml"),
            "dataset_name": DatasetName.NEWS_ITEMS,
            "job_name": JobName.INGEST,
            "days_override": 9,
        }

    def test_release_and_backup_use_generic_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """release and backup should default to the new namespaced config path."""
        release_calls: list[tuple[Path, DatasetName]] = []
        backup_calls: list[tuple[Path, DatasetName]] = []

        def fake_run_release(*, config_path: Path, dataset_name: DatasetName) -> None:
            release_calls.append((config_path, dataset_name))

        def fake_run_backup(*, config_path: Path, dataset_name: DatasetName) -> None:
            backup_calls.append((config_path, dataset_name))

        monkeypatch.setattr("denbust.pipeline.run_release", fake_run_release)
        monkeypatch.setattr("denbust.pipeline.run_backup", fake_run_backup)

        release_result = runner.invoke(app, ["release"])
        backup_result = runner.invoke(app, ["backup"])

        assert release_result.exit_code == 0
        assert backup_result.exit_code == 0
        assert release_calls == [(Path("agents/news/local.yaml"), DatasetName.NEWS_ITEMS)]
        assert backup_calls == [(Path("agents/news/local.yaml"), DatasetName.NEWS_ITEMS)]

    def test_version_prints_package_version(self) -> None:
        """Version should render the package version string."""
        result = runner.invoke(app, ["version"])

        assert result.exit_code == 0
        assert "denbust version 0.1.0" in result.stdout

    def test_module_main_invokes_typer_app(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Running the module as __main__ should invoke the Typer app."""
        calls: list[bool] = []

        def fake_call(self: typer.Typer, *args: object, **kwargs: object) -> None:
            del self, args, kwargs
            calls.append(True)

        monkeypatch.setattr(typer.Typer, "__call__", fake_call)

        runpy.run_module("denbust.cli", run_name="__main__")

        assert calls == [True]

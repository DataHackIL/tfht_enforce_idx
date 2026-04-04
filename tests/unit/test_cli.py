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
        assert captured["config_path"] == Path("agents/news.yaml")
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

    def test_release_and_backup_use_dedicated_config_defaults(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """release and backup should default to dedicated config locations."""
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
        assert release_calls == [(Path("agents/release/news_items.yaml"), DatasetName.NEWS_ITEMS)]
        assert backup_calls == [(Path("agents/backup/news_items.yaml"), DatasetName.NEWS_ITEMS)]

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

    def test_validation_collect_uses_local_news_config_by_default(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """validation-collect should default to the tracked local config."""
        captured: dict[str, object] = {}

        def fake_run_validation_collect(
            *,
            config_path: Path,
            days_override: int | None = None,
            per_source: int = 10,
            output_path: Path | None = None,
        ) -> object:
            captured["config_path"] = config_path
            captured["days_override"] = days_override
            captured["per_source"] = per_source
            captured["output_path"] = output_path

            class Result:
                total_rows = 0
                output_path = Path("draft.csv")
                per_source_counts: dict[str, int] = {}
                errors: list[str] = []

            return Result()

        monkeypatch.setattr(
            "denbust.validation.run_validation_collect", fake_run_validation_collect
        )

        result = runner.invoke(app, ["validation-collect"])

        assert result.exit_code == 0
        assert captured["config_path"] == Path("agents/news/local.yaml")
        assert captured["days_override"] is None
        assert captured["per_source"] == 10
        assert captured["output_path"] is None

    def test_validation_collect_reports_source_counts_and_errors(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """validation-collect should print per-source counts and surfaced errors."""

        def fake_run_validation_collect(
            *,
            config_path: Path,
            days_override: int | None = None,
            per_source: int = 10,
            output_path: Path | None = None,
        ) -> object:
            del config_path, days_override, per_source, output_path

            class Result:
                total_rows = 3
                output_path = Path("draft.csv")
                per_source_counts = {"mako": 1, "ynet": 2}
                errors = ["walla: boom"]

            return Result()

        monkeypatch.setattr("denbust.validation.run_validation_collect", fake_run_validation_collect)

        result = runner.invoke(app, ["validation-collect"])

        assert result.exit_code == 0
        assert "Wrote 3 draft rows to draft.csv" in result.stdout
        assert "mako: 1" in result.stdout
        assert "ynet: 2" in result.stdout
        assert "error: walla: boom" in result.stderr

    def test_validation_finalize_forwards_flags(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """validation-finalize should forward its input and validation set paths."""
        captured: dict[str, object] = {}

        def fake_run_validation_finalize(
            *,
            input_path: Path,
            validation_set_path: Path,
        ) -> object:
            captured["input_path"] = input_path
            captured["validation_set_path"] = validation_set_path

            class Result:
                added_rows = 1
                validation_set_path = Path("validation.csv")
                skipped_duplicates = 0
                total_rows = 1

            return Result()

        monkeypatch.setattr(
            "denbust.validation.run_validation_finalize",
            fake_run_validation_finalize,
        )

        result = runner.invoke(
            app,
            [
                "validation-finalize",
                "--input",
                "draft.csv",
                "--validation-set",
                "validation.csv",
            ],
        )

        assert result.exit_code == 0
        assert captured["input_path"] == Path("draft.csv")
        assert captured["validation_set_path"] == Path("validation.csv")

    def test_validation_evaluate_uses_default_paths(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """validation-evaluate should default to the tracked assets."""
        captured: dict[str, object] = {}

        def fake_run_validation_evaluate(
            *,
            validation_set_path: Path,
            variants_path: Path,
            output_path: Path | None = None,
        ) -> object:
            captured["validation_set_path"] = validation_set_path
            captured["variants_path"] = variants_path
            captured["output_path"] = output_path

            class Result:
                output_path = Path("report.json")
                rankings: list[object] = []

            return Result()

        monkeypatch.setattr(
            "denbust.validation.run_validation_evaluate",
            fake_run_validation_evaluate,
        )
        monkeypatch.setattr(
            "denbust.validation.evaluate.render_rankings_table",
            lambda _rankings: "",
        )

        result = runner.invoke(app, ["validation-evaluate"])

        assert result.exit_code == 0
        assert captured["validation_set_path"] == Path(
            "validation/news_items/classifier_validation.csv"
        )
        assert captured["variants_path"] == Path("agents/validation/classifier_variants.yaml")
        assert captured["output_path"] is None

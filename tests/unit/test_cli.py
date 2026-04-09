"""Unit tests for CLI command wiring."""

import json
import runpy
from pathlib import Path

import pytest
import typer
from typer.testing import CliRunner

from denbust.cli import app
from denbust.diagnostics import source_health
from denbust.models.common import DatasetName, JobName

runner = CliRunner()


class _FakeImportRow:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def model_dump(self, *, mode: str = "json") -> dict[str, object]:
        del mode
        return self._payload


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

        monkeypatch.setattr(
            "denbust.validation.run_validation_collect", fake_run_validation_collect
        )

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

    def test_validation_import_reviewed_table_forwards_flags(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """validation-import-reviewed-table should forward the reviewed-table adapter args."""
        captured: dict[str, object] = {}

        def fake_import_reviewed_table(
            *,
            input_path: Path,
            format_name: str,
            output_path: Path | None = None,
        ) -> object:
            captured["input_path"] = input_path
            captured["format_name"] = format_name
            captured["output_path"] = output_path

            class Result:
                imported_rows = 2
                skipped_rows = 1
                output_path = Path("reviewed.csv")
                warnings = ["row 7 skipped"]

            return Result()

        monkeypatch.setattr("denbust.validation.import_reviewed_table", fake_import_reviewed_table)

        result = runner.invoke(
            app,
            [
                "validation-import-reviewed-table",
                "--input",
                "docs/manual.xlsx",
                "--format",
                "tfht_manual_tracking_v1",
                "--output",
                "out.csv",
            ],
        )

        assert result.exit_code == 0
        assert captured == {
            "input_path": Path("docs/manual.xlsx"),
            "format_name": "tfht_manual_tracking_v1",
            "output_path": Path("out.csv"),
        }
        assert "Wrote 2 reviewed rows to reviewed.csv" in result.stdout
        assert "warning: row 7 skipped" in result.stderr

    def test_news_items_import_corrections_forwards_rows_to_operational_store(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """news-items-import-corrections should persist imported correction rows."""
        captured: dict[str, object] = {}

        class FakeStore:
            def upsert_news_item_corrections(
                self, dataset_name: str, records: list[dict[str, object]]
            ) -> None:
                captured["dataset_name"] = dataset_name
                captured["records"] = records

            def close(self) -> None:
                captured["closed"] = True

        monkeypatch.setattr(
            "denbust.config.load_config",
            lambda _path: type("Cfg", (), {"dataset_name": DatasetName.NEWS_ITEMS})(),
        )
        monkeypatch.setattr(
            "denbust.ops.factory.create_operational_store",
            lambda _config: FakeStore(),
        )
        monkeypatch.setattr(
            "denbust.news_items.annotations.import_news_item_corrections_csv",
            lambda _path: ([_FakeImportRow({"record_id": "row-1"})], ["row 2 skipped"]),
        )

        result = runner.invoke(app, ["news-items-import-corrections", "--input", "corrections.csv"])

        assert result.exit_code == 0
        assert captured["dataset_name"] == "news_items"
        assert captured["records"] == [{"record_id": "row-1"}]
        assert captured["closed"] is True
        assert "warning: row 2 skipped" in result.stderr

    def test_news_items_import_corrections_rejects_unsupported_format(self) -> None:
        """news-items-import-corrections should reject unsupported import formats."""
        result = runner.invoke(
            app,
            [
                "news-items-import-corrections",
                "--input",
                "corrections.csv",
                "--format",
                "unknown_format",
            ],
        )

        assert result.exit_code != 0
        assert "Unsupported corrections format: unknown_format" in result.stderr

    def test_news_items_import_missing_items_forwards_rows_to_operational_store(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """news-items-import-missing-items should persist imported missing-item rows."""
        captured: dict[str, object] = {}

        class FakeStore:
            def upsert_missing_news_items(
                self, dataset_name: str, records: list[dict[str, object]]
            ) -> None:
                captured["dataset_name"] = dataset_name
                captured["records"] = records

            def close(self) -> None:
                captured["closed"] = True

        monkeypatch.setattr(
            "denbust.config.load_config",
            lambda _path: type("Cfg", (), {"dataset_name": DatasetName.NEWS_ITEMS})(),
        )
        monkeypatch.setattr(
            "denbust.ops.factory.create_operational_store",
            lambda _config: FakeStore(),
        )
        monkeypatch.setattr(
            "denbust.news_items.annotations.import_missing_news_items_csv",
            lambda _path: ([_FakeImportRow({"annotation_id": "missing-1"})], ["row 3 skipped"]),
        )

        result = runner.invoke(app, ["news-items-import-missing-items", "--input", "missing.csv"])

        assert result.exit_code == 0
        assert captured["dataset_name"] == "news_items"
        assert captured["records"] == [{"annotation_id": "missing-1"}]
        assert captured["closed"] is True
        assert "warning: row 3 skipped" in result.stderr

    def test_news_items_import_missing_items_rejects_unsupported_format(self) -> None:
        """news-items-import-missing-items should reject unsupported import formats."""
        result = runner.invoke(
            app,
            [
                "news-items-import-missing-items",
                "--input",
                "missing.csv",
                "--format",
                "unknown_format",
            ],
        )

        assert result.exit_code != 0
        assert "Unsupported missing-items format: unknown_format" in result.stderr

    def test_validation_commands_use_canonical_default_constants(self) -> None:
        """CLI defaults should reuse the shared tracked validation asset paths."""
        from denbust.validation import common as validation_common

        validation_finalize_command = next(
            command for command in app.registered_commands if command.name == "validation-finalize"
        )
        validation_evaluate_command = next(
            command for command in app.registered_commands if command.name == "validation-evaluate"
        )

        assert validation_finalize_command.callback.__defaults__ == (
            validation_common.DEFAULT_VALIDATION_SET_PATH,
        )
        assert validation_evaluate_command.callback.__defaults__ == (
            validation_common.DEFAULT_VALIDATION_SET_PATH,
            validation_common.DEFAULT_VARIANT_MATRIX_PATH,
            None,
        )

    def test_diagnose_sources_forwards_flags(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """diagnose-sources should pass its flags into the diagnostics runner."""
        captured: dict[str, object] = {}

        def fake_run_source_diagnostics(
            *,
            config_path: Path,
            source_names: list[str] | None = None,
            days_override: int | None = None,
            include_artifacts: bool = True,
            include_live: bool = True,
            sample_keywords: list[str] | None = None,
        ) -> object:
            captured["config_path"] = config_path
            captured["source_names"] = source_names
            captured["days_override"] = days_override
            captured["include_artifacts"] = include_artifacts
            captured["include_live"] = include_live
            captured["sample_keywords"] = sample_keywords
            return object()

        monkeypatch.setattr(
            "denbust.diagnostics.run_source_diagnostics",
            fake_run_source_diagnostics,
        )
        monkeypatch.setattr(
            "denbust.diagnostics.render_source_diagnostic_report",
            lambda report: f"rendered:{type(report).__name__}",
        )

        result = runner.invoke(
            app,
            [
                "diagnose-sources",
                "--config",
                "agents/custom.yaml",
                "--source",
                "ynet",
                "--days",
                "5",
                "--sample-keyword",
                "זנות",
                "--sample-keyword",
                "בית בושת",
            ],
        )

        assert result.exit_code == 0
        assert captured == {
            "config_path": Path("agents/custom.yaml"),
            "source_names": ["ynet"],
            "days_override": 5,
            "include_artifacts": True,
            "include_live": True,
            "sample_keywords": ["זנות", "בית בושת"],
        }

    def test_diagnose_sources_json_output(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """diagnose-sources should emit machine-readable JSON when requested."""
        report = source_health.SourceDiagnosticReport(
            config_path="agents/news.yaml",
            days=21,
            sample_keywords=["זנות"],
            artifact_analysis_enabled=True,
            live_probe_enabled=True,
            results=[],
        )

        def fake_run_source_diagnostics(**_kwargs: object) -> object:
            return report

        monkeypatch.setattr(
            "denbust.diagnostics.run_source_diagnostics",
            fake_run_source_diagnostics,
        )

        result = runner.invoke(app, ["diagnose-sources", "--format", "json"])

        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert payload["config_path"] == "agents/news.yaml"
        assert payload["days"] == 21

    def test_diagnose_sources_writes_output_file(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """diagnose-sources should write JSON to the requested output path."""
        report = source_health.SourceDiagnosticReport(
            config_path="agents/news.yaml",
            days=21,
            sample_keywords=["זנות"],
            artifact_analysis_enabled=True,
            live_probe_enabled=True,
            results=[],
        )
        output_path = tmp_path / "diagnostics.json"

        def fake_run_source_diagnostics(**_kwargs: object) -> object:
            return report

        monkeypatch.setattr(
            "denbust.diagnostics.run_source_diagnostics",
            fake_run_source_diagnostics,
        )
        monkeypatch.setattr(
            "denbust.diagnostics.render_source_diagnostic_report",
            lambda report: f"rendered:{type(report).__name__}",
        )

        result = runner.invoke(app, ["diagnose-sources", "--output", str(output_path)])

        assert result.exit_code == 0
        assert (
            json.loads(output_path.read_text(encoding="utf-8"))["config_path"] == "agents/news.yaml"
        )

    def test_diagnose_sources_rejects_conflicting_mode_flags(self) -> None:
        """diagnose-sources should reject mutually exclusive mode flags."""
        result = runner.invoke(app, ["diagnose-sources", "--artifacts-only", "--live-only"])

        assert result.exit_code != 0
        assert "Choose at most one" in result.stderr

    def test_diagnose_sources_surfaces_invalid_source_selection_as_bad_parameter(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """diagnose-sources should convert runner validation errors into CLI parameter errors."""

        def fake_run_source_diagnostics(**_kwargs: object) -> object:
            raise ValueError("Unknown or disabled sources: ghost")

        monkeypatch.setattr(
            "denbust.diagnostics.run_source_diagnostics",
            fake_run_source_diagnostics,
        )

        result = runner.invoke(app, ["diagnose-sources", "--source", "ghost"])

        assert result.exit_code != 0
        assert "Unknown or disabled sources: ghost" in result.stderr

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
                markdown_path = Path("report.md")
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
        assert "Saved JSON report to report.json" in result.stdout
        assert "Saved Markdown report to report.md" in result.stdout

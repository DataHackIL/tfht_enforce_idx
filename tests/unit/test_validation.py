"""Unit tests for validation-set collection, finalization, and evaluation."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml
from pydantic import HttpUrl

from denbust.config import Config
from denbust.data_models import (
    Category,
    ClassificationResult,
    ClassifiedArticle,
    RawArticle,
    SubCategory,
)
from denbust.validation.collect import (
    collect_validation_draft,
    run_validation_collect,
    select_promising_candidates,
)
from denbust.validation.common import (
    DEFAULT_VARIANT_MATRIX_PATH,
    DRAFT_COLUMNS,
    VALIDATION_SET_COLUMNS,
    canonicalize_csv_url,
    default_collect_output_path,
    default_evaluation_output_path,
    parse_bool,
    parse_datetime,
    read_csv_rows,
    relaxed_validation_keywords,
    validation_drafts_dir,
    validation_reports_dir,
    validation_state_dir,
    write_csv_rows,
)
from denbust.validation.dataset import (
    ValidationFinalizeResult,
    _parse_existing_validation_row,
    finalize_validation_set,
    run_validation_finalize,
)
from denbust.validation.evaluate import (
    ValidationLabel,
    _build_dataset_summary,
    _load_validation_examples,
    _load_variant_matrix,
    _resolve_report_paths,
    _score_predictions,
    evaluate_classifier_variants,
    render_rankings_table,
    run_validation_evaluate,
)
from denbust.validation.models import ClassifierVariantSpec, VariantMetrics


def build_raw_article(
    url: str,
    *,
    title: str,
    snippet: str,
    source_name: str = "test",
    day: int = 1,
) -> RawArticle:
    """Create a sample raw article."""
    return RawArticle(
        url=HttpUrl(url),
        title=title,
        snippet=snippet,
        date=datetime(2026, 3, day, tzinfo=UTC),
        source_name=source_name,
    )


def build_classified_article(
    article: RawArticle,
    *,
    relevant: bool,
    enforcement_related: bool = False,
    category: Category,
    sub_category: SubCategory | None = None,
) -> ClassifiedArticle:
    """Build a classified article tied to a raw article."""
    return ClassifiedArticle(
        article=article,
        classification=ClassificationResult(
            relevant=relevant,
            enforcement_related=enforcement_related,
            category=category,
            sub_category=sub_category,
            confidence="high",
        ),
    )


class FakeSource:
    """Simple validation collection source stub."""

    def __init__(self, name: str, articles: list[RawArticle]) -> None:
        self.name = name
        self._articles = articles

    async def fetch(self, days: int, keywords: list[str]) -> list[RawArticle]:
        del days, keywords
        return self._articles


class FakeClassifier:
    """Simple classifier stub."""

    def __init__(self, results: list[ClassifiedArticle]) -> None:
        self._results = results

    async def classify_batch(self, articles: list[RawArticle]) -> list[ClassifiedArticle]:
        by_url = {str(item.article.url): item for item in self._results}
        return [by_url[str(article.url)] for article in articles]


class TestValidationCollection:
    """Tests for draft collection behavior."""

    def test_select_promising_candidates_ranks_and_deduplicates(self) -> None:
        """Candidates should be deduplicated by canonical URL and ranked deterministically."""
        articles = [
            build_raw_article(
                "https://example.com/a?utm_source=x",
                title="פשיטה על בית בושת",
                snippet="ללא הקשר נוסף",
                day=1,
            ),
            build_raw_article(
                "https://example.com/a",
                title="פשיטה על בית בושת",
                snippet="ללא הקשר נוסף",
                day=2,
            ),
            build_raw_article(
                "https://example.com/b",
                title="כתבה כללית",
                snippet="חשד לזנות בעיר",
                day=3,
            ),
            build_raw_article(
                "https://example.com/c",
                title="מכון עיסוי שנחשד ככיסוי",
                snippet="רקע נוסף",
                day=4,
            ),
        ]

        ranked = select_promising_candidates(
            articles,
            strict_keywords=["בית בושת", "זנות"],
            relaxed_keywords=["בית בושת", "זנות", "מכון עיסוי"],
            per_source=3,
            canonicalize_url=lambda url: url.split("?")[0],
        )

        assert [candidate.canonical_url for candidate in ranked] == [
            "https://example.com/a",
            "https://example.com/b",
            "https://example.com/c",
        ]

    @pytest.mark.asyncio
    async def test_collect_validation_draft_limits_per_source_and_prefills_labels(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Draft collection should cap rows per source and prefill review columns."""
        source_one_articles = [
            build_raw_article(
                "https://example.com/one",
                title="פשיטה על בית בושת",
                snippet="המשטרה פשטה",
                source_name="ynet",
                day=1,
            ),
            build_raw_article(
                "https://example.com/two",
                title="כתבה על זנות",
                snippet="אירוע נוסף",
                source_name="ynet",
                day=2,
            ),
        ]
        source_two_articles = [
            build_raw_article(
                "https://example.com/three",
                title="חשד לסחר בבני אדם",
                snippet="אירוע אחד",
                source_name="mako",
                day=3,
            ),
        ]
        classified_results = [
            build_classified_article(
                source_one_articles[1],
                relevant=True,
                enforcement_related=True,
                category=Category.BROTHEL,
                sub_category=SubCategory.CLOSURE,
            ),
            build_classified_article(
                source_two_articles[0],
                relevant=False,
                category=Category.NOT_RELEVANT,
            ),
        ]

        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        monkeypatch.setattr(
            "denbust.validation.collect.create_sources",
            lambda _config: [
                FakeSource("ynet", source_one_articles),
                FakeSource("mako", source_two_articles),
            ],
        )
        monkeypatch.setattr(
            "denbust.validation.collect.create_classifier",
            lambda **_kwargs: FakeClassifier(classified_results),
        )

        config = Config(
            keywords=["בית בושת", "זנות", "סחר בבני אדם"],
            store={"state_root": tmp_path},
        )
        output_path = tmp_path / "draft.csv"
        result = await collect_validation_draft(
            config,
            days=7,
            per_source=1,
            output_path=output_path,
        )

        assert result.total_rows == 2
        assert result.per_source_counts == {"mako": 1, "ynet": 1}
        rows = read_csv_rows(output_path)
        assert len(rows) == 2
        assert [row["review_status"] for row in rows] == ["pending", "pending"]
        assert {row["suggested_enforcement_related"] for row in rows} == {"False", "True"}
        assert rows[0]["suggested_category"]
        assert rows[0]["category"] == rows[0]["suggested_category"]
        assert rows[0]["relevant"] == rows[0]["suggested_relevant"]
        assert rows[0]["enforcement_related"] == rows[0]["suggested_enforcement_related"]

    @pytest.mark.asyncio
    async def test_collect_validation_draft_requires_api_key(self, tmp_path: Path) -> None:
        """Draft collection should fail fast without Anthropic credentials."""
        config = Config(keywords=["בית בושת"], store={"state_root": tmp_path})

        with pytest.raises(ValueError, match="ANTHROPIC_API_KEY"):
            await collect_validation_draft(config)

    @pytest.mark.asyncio
    async def test_collect_validation_draft_rejects_non_positive_per_source(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Draft collection should reject invalid per-source limits."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        config = Config(keywords=["בית בושת"], store={"state_root": tmp_path})

        with pytest.raises(ValueError, match="per_source must be >= 1"):
            await collect_validation_draft(config, per_source=0)

    @pytest.mark.asyncio
    async def test_collect_validation_draft_records_source_errors_and_empty_sources(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Source failures and zero-candidate sources should be reported without crashing."""

        class FailingSource:
            name = "walla"

            async def fetch(self, days: int, keywords: list[str]) -> list[RawArticle]:
                del days, keywords
                raise RuntimeError("boom")

        class EmptySource:
            name = "ynet"

            async def fetch(self, days: int, keywords: list[str]) -> list[RawArticle]:
                del days, keywords
                return []

        class NeverCalledClassifier:
            async def classify_batch(self, articles: list[RawArticle]) -> list[ClassifiedArticle]:
                raise AssertionError(f"classify_batch should not be called for {articles}")

        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        monkeypatch.setattr(
            "denbust.validation.collect.create_sources",
            lambda _config: [FailingSource(), EmptySource()],
        )
        monkeypatch.setattr(
            "denbust.validation.collect.create_classifier",
            lambda **_kwargs: NeverCalledClassifier(),
        )

        config = Config(keywords=["בית בושת"], store={"state_root": tmp_path})
        result = await collect_validation_draft(config, output_path=tmp_path / "draft.csv")

        assert result.total_rows == 0
        assert result.per_source_counts == {"walla": 0, "ynet": 0}
        assert result.errors == ["walla: boom"]
        assert read_csv_rows(tmp_path / "draft.csv") == []

    def test_run_validation_collect_uses_wrapper_defaults(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """The synchronous wrapper should set up logging, load config, and default days to 7."""
        calls: dict[str, object] = {}
        config = Config(keywords=["בית בושת"], store={"state_root": tmp_path})

        async def fake_collect_validation_draft(
            loaded_config: Config,
            *,
            days: int = 7,
            per_source: int = 10,
            output_path: Path | None = None,
        ) -> object:
            calls["config"] = loaded_config
            calls["days"] = days
            calls["per_source"] = per_source
            calls["output_path"] = output_path

            class Result:
                output_path = tmp_path / "draft.csv"
                total_rows = 0
                per_source_counts: dict[str, int] = {}
                errors: list[str] = []

            return Result()

        monkeypatch.setattr(
            "denbust.validation.collect.setup_logging", lambda: calls.setdefault("setup", True)
        )
        monkeypatch.setattr("denbust.validation.collect.load_config", lambda _path: config)
        monkeypatch.setattr(
            "denbust.validation.collect.collect_validation_draft", fake_collect_validation_draft
        )

        result = run_validation_collect(config_path=Path("agents/news/local.yaml"), per_source=3)

        assert calls["setup"] is True
        assert calls["config"] == config
        assert calls["days"] == 7
        assert calls["per_source"] == 3
        assert calls["output_path"] is None
        assert result.output_path == tmp_path / "draft.csv"


class TestValidationCommon:
    """Tests for shared validation helpers."""

    def test_relaxed_validation_keywords_deduplicates_and_skips_blank_values(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Keyword expansion should strip whitespace, drop blanks, and deduplicate case-insensitively."""
        monkeypatch.setattr(
            "denbust.validation.common.DEFAULT_KEYWORDS",
            ["בית בושת", "  ", "זנות"],
        )
        monkeypatch.setattr(
            "denbust.validation.common.RELAXED_KEYWORD_ADDITIONS",
            ["בית בושת", "מכון עיסוי", "  בית בושת  "],
        )
        keywords = relaxed_validation_keywords()

        assert keywords == ["בית בושת", "זנות", "מכון עיסוי"]
        assert "מכון עיסוי" in keywords
        assert "" not in keywords
        assert len({keyword.casefold() for keyword in keywords}) == len(keywords)

    def test_validation_path_helpers_build_expected_locations(self, tmp_path: Path) -> None:
        """Validation helper paths should be rooted under the configured state directory."""
        config = Config(store={"state_root": tmp_path})
        timestamp = datetime(2026, 4, 4, 10, 30, tzinfo=UTC)

        assert validation_state_dir(config) == tmp_path / "validation" / config.dataset_name.value
        assert (
            validation_drafts_dir(config)
            == tmp_path / "validation" / config.dataset_name.value / "drafts"
        )
        assert (
            validation_reports_dir(config)
            == tmp_path / "validation" / config.dataset_name.value / "reports"
        )
        assert (
            default_collect_output_path(config, timestamp).name
            == "classifier_draft_2026-04-04T10-30-00Z.csv"
        )
        assert (
            default_evaluation_output_path(config, timestamp).name
            == "classifier_variant_eval_2026-04-04T10-30-00Z.json"
        )

    def test_parse_bool_and_datetime_cover_edge_cases(self) -> None:
        """Boolean and datetime parsing should handle invalid booleans and naive timestamps."""
        assert parse_bool(" Yes ") is True
        assert parse_bool("n") is False
        assert parse_datetime("2026-03-01T12:30:00").tzinfo == UTC
        assert parse_datetime("2026-03-01T12:30:00+00:00").tzinfo == UTC

        with pytest.raises(ValueError, match="Invalid boolean value"):
            parse_bool("maybe")

    def test_read_csv_rows_and_canonicalize_csv_url_cover_missing_inputs(
        self,
        tmp_path: Path,
    ) -> None:
        """CSV helpers should tolerate missing files and prefer explicit canonical URLs."""
        assert read_csv_rows(tmp_path / "missing.csv") == []
        assert (
            canonicalize_csv_url(
                "https://news.walla.co.il/item/3818937?utm_source=archive",
                "",
            )
            == "https://news.walla.co.il/item/3818937"
        )
        assert (
            canonicalize_csv_url(
                "https://example.com/original",
                " https://news.walla.co.il/item/3818937?utm_source=archive ",
            )
            == "https://news.walla.co.il/item/3818937"
        )


class TestValidationFinalize:
    """Tests for permanent validation-set finalization."""

    def test_finalize_validation_set_merges_and_skips_duplicates(self, tmp_path: Path) -> None:
        """Reviewed rows should merge additively and skip existing article identities."""
        validation_set_path = tmp_path / "classifier_validation.csv"
        write_csv_rows(
            validation_set_path,
            VALIDATION_SET_COLUMNS,
            [
                {
                    "source_name": "ynet",
                    "article_date": "2026-03-01T00:00:00+00:00",
                    "url": "https://example.com/a",
                    "canonical_url": "https://example.com/a",
                    "title": "title a",
                    "snippet": "snippet a",
                    "relevant": "True",
                    "enforcement_related": "True",
                    "category": "brothel",
                    "sub_category": "closure",
                    "review_status": "reviewed",
                    "annotation_notes": "",
                    "collected_at": "2026-03-01T00:00:00+00:00",
                    "finalized_at": "2026-03-02T00:00:00+00:00",
                    "draft_source": "old.csv",
                }
            ],
        )
        draft_path = tmp_path / "draft.csv"
        write_csv_rows(
            draft_path,
            DRAFT_COLUMNS,
            [
                {
                    "source_name": "ynet",
                    "article_date": "2026-03-01T00:00:00+00:00",
                    "url": "https://example.com/a",
                    "canonical_url": "https://example.com/a",
                    "title": "title a",
                    "snippet": "snippet a",
                    "suggested_relevant": "True",
                    "suggested_enforcement_related": "True",
                    "suggested_category": "brothel",
                    "suggested_sub_category": "closure",
                    "suggested_confidence": "high",
                    "relevant": "True",
                    "enforcement_related": "True",
                    "category": "brothel",
                    "sub_category": "closure",
                    "review_status": "reviewed",
                    "annotation_notes": "",
                    "collected_at": "2026-03-01T00:00:00+00:00",
                },
                {
                    "source_name": "mako",
                    "article_date": "2026-03-03T00:00:00+00:00",
                    "url": "https://example.com/b",
                    "canonical_url": "https://example.com/b",
                    "title": "title b",
                    "snippet": "snippet b",
                    "suggested_relevant": "False",
                    "suggested_enforcement_related": "False",
                    "suggested_category": "not_relevant",
                    "suggested_sub_category": "",
                    "suggested_confidence": "high",
                    "relevant": "False",
                    "enforcement_related": "False",
                    "category": "",
                    "sub_category": "",
                    "review_status": "reviewed",
                    "annotation_notes": "",
                    "collected_at": "2026-03-03T00:00:00+00:00",
                },
                {
                    "source_name": "mako",
                    "article_date": "2026-03-04T00:00:00+00:00",
                    "url": "https://example.com/c",
                    "canonical_url": "https://example.com/c",
                    "title": "title c",
                    "snippet": "snippet c",
                    "suggested_relevant": "True",
                    "suggested_enforcement_related": "True",
                    "suggested_category": "trafficking",
                    "suggested_sub_category": "rescue",
                    "suggested_confidence": "high",
                    "relevant": "True",
                    "enforcement_related": "True",
                    "category": "trafficking",
                    "sub_category": "rescue",
                    "review_status": "pending",
                    "annotation_notes": "",
                    "collected_at": "2026-03-04T00:00:00+00:00",
                },
            ],
        )

        result = finalize_validation_set(
            input_path=draft_path,
            validation_set_path=validation_set_path,
        )

        assert result.added_rows == 1
        assert result.skipped_duplicates == 1
        assert result.reviewed_rows == 2
        assert result.total_rows == 2
        rows = read_csv_rows(validation_set_path)
        assert {row["source_name"] for row in rows} == {"ynet", "mako"}

    def test_finalize_validation_set_is_idempotent_for_same_reviewed_import(
        self, tmp_path: Path
    ) -> None:
        """Re-finalizing the same reviewed draft should add no new rows."""
        draft_path = tmp_path / "draft.csv"
        validation_set_path = tmp_path / "classifier_validation.csv"
        write_csv_rows(
            draft_path,
            DRAFT_COLUMNS,
            [
                {
                    "source_name": "maariv",
                    "article_date": "2026-03-01T00:00:00+00:00",
                    "url": "https://example.com/a?utm_source=one",
                    "canonical_url": "https://example.com/a",
                    "title": "title a",
                    "snippet": "snippet a",
                    "suggested_relevant": "True",
                    "suggested_enforcement_related": "True",
                    "suggested_index_relevant": "True",
                    "suggested_taxonomy_version": "1",
                    "suggested_taxonomy_category_id": "brothels",
                    "suggested_taxonomy_subcategory_id": "administrative_closure",
                    "suggested_category": "brothel",
                    "suggested_sub_category": "closure",
                    "suggested_confidence": "high",
                    "relevant": "True",
                    "enforcement_related": "True",
                    "index_relevant": "True",
                    "taxonomy_version": "1",
                    "taxonomy_category_id": "brothels",
                    "taxonomy_subcategory_id": "administrative_closure",
                    "category": "brothel",
                    "sub_category": "closure",
                    "review_status": "reviewed",
                    "annotation_source": "tfht_manual_tracking_v1",
                    "expected_month_bucket": "",
                    "expected_city": "",
                    "expected_status": "",
                    "manual_city": "",
                    "manual_address": "",
                    "manual_event_label": "",
                    "manual_status": "",
                    "annotation_notes": "",
                    "collected_at": "2026-03-01T00:00:00+00:00",
                }
            ],
        )

        first = finalize_validation_set(
            input_path=draft_path,
            validation_set_path=validation_set_path,
        )
        second = finalize_validation_set(
            input_path=draft_path,
            validation_set_path=validation_set_path,
        )

        assert first.added_rows == 1
        assert first.skipped_duplicates == 0
        assert second.added_rows == 0
        assert second.skipped_duplicates == 1
        rows = read_csv_rows(validation_set_path)
        assert len(rows) == 1

    def test_finalize_validation_set_rejects_invalid_labels(self, tmp_path: Path) -> None:
        """Invalid category/sub-category pairs should fail finalization."""
        draft_path = tmp_path / "draft.csv"
        write_csv_rows(
            draft_path,
            DRAFT_COLUMNS,
            [
                {
                    "source_name": "mako",
                    "article_date": "2026-03-03T00:00:00+00:00",
                    "url": "https://example.com/b",
                    "canonical_url": "https://example.com/b",
                    "title": "title b",
                    "snippet": "snippet b",
                    "suggested_relevant": "True",
                    "suggested_enforcement_related": "True",
                    "suggested_category": "brothel",
                    "suggested_sub_category": "closure",
                    "suggested_confidence": "high",
                    "relevant": "True",
                    "enforcement_related": "True",
                    "category": "brothel",
                    "sub_category": "rescue",
                    "review_status": "reviewed",
                    "annotation_notes": "",
                    "collected_at": "2026-03-03T00:00:00+00:00",
                }
            ],
        )

        with pytest.raises(ValueError, match="Invalid sub_category"):
            finalize_validation_set(input_path=draft_path, validation_set_path=tmp_path / "out.csv")

    def test_finalize_validation_set_requires_existing_input(self, tmp_path: Path) -> None:
        """Finalization should fail clearly when the draft CSV is missing."""
        with pytest.raises(FileNotFoundError, match="Draft CSV not found"):
            finalize_validation_set(
                input_path=tmp_path / "missing.csv",
                validation_set_path=tmp_path / "out.csv",
            )

    def test_finalize_validation_set_rejects_relevant_not_relevant_category(
        self,
        tmp_path: Path,
    ) -> None:
        """Reviewed relevant rows cannot keep the not_relevant category."""
        draft_path = tmp_path / "draft.csv"
        write_csv_rows(
            draft_path,
            DRAFT_COLUMNS,
            [
                {
                    "source_name": "mako",
                    "article_date": "2026-03-03T00:00:00+00:00",
                    "url": "https://example.com/b",
                    "canonical_url": "https://example.com/b",
                    "title": "title b",
                    "snippet": "snippet b",
                    "suggested_relevant": "True",
                    "suggested_enforcement_related": "True",
                    "suggested_category": "not_relevant",
                    "suggested_sub_category": "",
                    "suggested_confidence": "high",
                    "relevant": "True",
                    "enforcement_related": "True",
                    "category": "not_relevant",
                    "sub_category": "closure",
                    "review_status": "reviewed",
                    "annotation_notes": "",
                    "collected_at": "2026-03-03T00:00:00+00:00",
                }
            ],
        )

        with pytest.raises(ValueError, match="cannot use category 'not_relevant'"):
            finalize_validation_set(input_path=draft_path, validation_set_path=tmp_path / "out.csv")

    def test_finalize_validation_set_allows_blank_subcategory_for_relevant_rows(
        self,
        tmp_path: Path,
    ) -> None:
        """Reviewed relevant rows may omit a sub-category."""
        draft_path = tmp_path / "draft.csv"
        write_csv_rows(
            draft_path,
            DRAFT_COLUMNS,
            [
                {
                    "source_name": "mako",
                    "article_date": "2026-03-03T00:00:00+00:00",
                    "url": "https://example.com/b",
                    "canonical_url": "https://example.com/b",
                    "title": "title b",
                    "snippet": "snippet b",
                    "suggested_relevant": "True",
                    "suggested_enforcement_related": "False",
                    "suggested_category": "brothel",
                    "suggested_sub_category": "",
                    "suggested_confidence": "high",
                    "relevant": "True",
                    "enforcement_related": "False",
                    "category": "brothel",
                    "sub_category": "",
                    "review_status": "reviewed",
                    "annotation_notes": "",
                    "collected_at": "2026-03-03T00:00:00+00:00",
                }
            ],
        )

        result = finalize_validation_set(
            input_path=draft_path, validation_set_path=tmp_path / "out.csv"
        )
        rows = read_csv_rows(result.validation_set_path)

        assert rows[0]["relevant"] == "True"
        assert rows[0]["enforcement_related"] == "False"
        assert rows[0]["sub_category"] == ""

    def test_finalize_validation_set_requires_subcategory_for_enforcement_rows(
        self,
        tmp_path: Path,
    ) -> None:
        """Reviewed enforcement-related rows without taxonomy labels must include a sub-category."""
        draft_path = tmp_path / "draft.csv"
        write_csv_rows(
            draft_path,
            DRAFT_COLUMNS,
            [
                {
                    "source_name": "mako",
                    "article_date": "2026-03-03T00:00:00+00:00",
                    "url": "https://example.com/b",
                    "canonical_url": "https://example.com/b",
                    "title": "title b",
                    "snippet": "snippet b",
                    "suggested_relevant": "True",
                    "suggested_enforcement_related": "True",
                    "suggested_category": "brothel",
                    "suggested_sub_category": "",
                    "suggested_confidence": "high",
                    "relevant": "True",
                    "enforcement_related": "True",
                    "category": "brothel",
                    "sub_category": "",
                    "review_status": "reviewed",
                    "annotation_notes": "",
                    "collected_at": "2026-03-03T00:00:00+00:00",
                }
            ],
        )

        with pytest.raises(ValueError, match="must include a non-empty sub_category"):
            finalize_validation_set(input_path=draft_path, validation_set_path=tmp_path / "out.csv")

    def test_finalize_validation_set_allows_taxonomy_labeled_enforcement_rows_without_legacy_subcategory(
        self,
        tmp_path: Path,
    ) -> None:
        """Taxonomy-labeled enforcement rows may omit the legacy sub-category when the leaf is valid."""
        draft_path = tmp_path / "draft.csv"
        write_csv_rows(
            draft_path,
            DRAFT_COLUMNS,
            [
                {
                    "source_name": "mako",
                    "article_date": "2026-03-03T00:00:00+00:00",
                    "url": "https://example.com/b",
                    "canonical_url": "https://example.com/b",
                    "title": "title b",
                    "snippet": "snippet b",
                    "suggested_relevant": "True",
                    "suggested_enforcement_related": "True",
                    "suggested_index_relevant": "True",
                    "suggested_taxonomy_version": "1",
                    "suggested_taxonomy_category_id": "brothels",
                    "suggested_taxonomy_subcategory_id": "keeping_brothel",
                    "suggested_category": "brothel",
                    "suggested_sub_category": "",
                    "suggested_confidence": "high",
                    "relevant": "True",
                    "enforcement_related": "True",
                    "index_relevant": "True",
                    "taxonomy_version": "1",
                    "taxonomy_category_id": "brothels",
                    "taxonomy_subcategory_id": "keeping_brothel",
                    "category": "brothel",
                    "sub_category": "",
                    "review_status": "reviewed",
                    "annotation_source": "tfht_manual_tracking_v1",
                    "annotation_notes": "",
                    "collected_at": "2026-03-03T00:00:00+00:00",
                }
            ],
        )

        result = finalize_validation_set(
            input_path=draft_path,
            validation_set_path=tmp_path / "out.csv",
        )
        rows = read_csv_rows(result.validation_set_path)

        assert rows[0]["enforcement_related"] == "True"
        assert rows[0]["taxonomy_category_id"] == "brothels"
        assert rows[0]["taxonomy_subcategory_id"] == "keeping_brothel"
        assert rows[0]["sub_category"] == ""

    @pytest.mark.parametrize(
        (
            "taxonomy_category_id",
            "taxonomy_subcategory_id",
            "taxonomy_version",
            "index_relevant",
            "match",
        ),
        [
            ("brothels", "", "1", "True", "must include both category and subcategory ids"),
            ("brothels", "not_a_leaf", "1", "True", "Invalid taxonomy pair"),
            ("brothels", "administrative_closure", "999", "True", "Unsupported taxonomy version"),
            (
                "brothels",
                "administrative_closure",
                "1",
                "False",
                "index_relevant does not match the packaged taxonomy",
            ),
        ],
    )
    def test_finalize_validation_set_rejects_invalid_taxonomy_metadata(
        self,
        tmp_path: Path,
        taxonomy_category_id: str,
        taxonomy_subcategory_id: str,
        taxonomy_version: str,
        index_relevant: str,
        match: str,
    ) -> None:
        draft_path = tmp_path / "draft.csv"
        write_csv_rows(
            draft_path,
            DRAFT_COLUMNS,
            [
                {
                    "source_name": "mako",
                    "article_date": "2026-03-03T00:00:00+00:00",
                    "url": "https://example.com/b",
                    "canonical_url": "https://example.com/b",
                    "title": "title b",
                    "snippet": "snippet b",
                    "suggested_relevant": "True",
                    "suggested_enforcement_related": "True",
                    "suggested_index_relevant": index_relevant,
                    "suggested_taxonomy_version": taxonomy_version,
                    "suggested_taxonomy_category_id": taxonomy_category_id,
                    "suggested_taxonomy_subcategory_id": taxonomy_subcategory_id,
                    "suggested_category": "brothel",
                    "suggested_sub_category": "closure",
                    "suggested_confidence": "high",
                    "relevant": "True",
                    "enforcement_related": "True",
                    "index_relevant": index_relevant,
                    "taxonomy_version": taxonomy_version,
                    "taxonomy_category_id": taxonomy_category_id,
                    "taxonomy_subcategory_id": taxonomy_subcategory_id,
                    "category": "brothel",
                    "sub_category": "closure",
                    "review_status": "reviewed",
                    "annotation_source": "tfht_manual_tracking_v1",
                    "manual_city": "",
                    "manual_address": "",
                    "manual_event_label": "",
                    "manual_status": "",
                    "annotation_notes": "",
                    "collected_at": "2026-03-03T00:00:00+00:00",
                }
            ],
        )

        with pytest.raises(ValueError, match=match):
            finalize_validation_set(input_path=draft_path, validation_set_path=tmp_path / "out.csv")

    def test_finalize_validation_set_normalizes_valid_taxonomy_version(
        self, tmp_path: Path
    ) -> None:
        draft_path = tmp_path / "draft.csv"
        write_csv_rows(
            draft_path,
            DRAFT_COLUMNS,
            [
                {
                    "source_name": "mako",
                    "article_date": "2026-03-03T00:00:00+00:00",
                    "url": "https://example.com/b",
                    "canonical_url": "https://example.com/b",
                    "title": "title b",
                    "snippet": "snippet b",
                    "suggested_relevant": "True",
                    "suggested_enforcement_related": "True",
                    "suggested_index_relevant": "True",
                    "suggested_taxonomy_version": "",
                    "suggested_taxonomy_category_id": "brothels",
                    "suggested_taxonomy_subcategory_id": "administrative_closure",
                    "suggested_category": "brothel",
                    "suggested_sub_category": "closure",
                    "suggested_confidence": "high",
                    "relevant": "True",
                    "enforcement_related": "True",
                    "index_relevant": "True",
                    "taxonomy_version": "",
                    "taxonomy_category_id": "brothels",
                    "taxonomy_subcategory_id": "administrative_closure",
                    "category": "brothel",
                    "sub_category": "closure",
                    "review_status": "reviewed",
                    "annotation_source": "tfht_manual_tracking_v1",
                    "manual_city": "",
                    "manual_address": "",
                    "manual_event_label": "",
                    "manual_status": "",
                    "annotation_notes": "",
                    "collected_at": "2026-03-03T00:00:00+00:00",
                }
            ],
        )

        result = finalize_validation_set(
            input_path=draft_path, validation_set_path=tmp_path / "out.csv"
        )
        rows = read_csv_rows(result.validation_set_path)

        assert rows[0]["taxonomy_version"] == "1"

    def test_finalize_validation_set_persists_expected_fields_and_annotation_source(
        self, tmp_path: Path
    ) -> None:
        draft_path = tmp_path / "draft.csv"
        write_csv_rows(
            draft_path,
            DRAFT_COLUMNS,
            [
                {
                    "source_name": "mako",
                    "article_date": "2026-03-03T00:00:00+00:00",
                    "url": "https://example.com/b",
                    "canonical_url": "https://example.com/b",
                    "title": "title b",
                    "snippet": "snippet b",
                    "suggested_relevant": "True",
                    "suggested_enforcement_related": "True",
                    "suggested_index_relevant": "True",
                    "suggested_taxonomy_version": "1",
                    "suggested_taxonomy_category_id": "brothels",
                    "suggested_taxonomy_subcategory_id": "administrative_closure",
                    "suggested_category": "brothel",
                    "suggested_sub_category": "closure",
                    "suggested_confidence": "high",
                    "relevant": "True",
                    "enforcement_related": "True",
                    "index_relevant": "True",
                    "taxonomy_version": "1",
                    "taxonomy_category_id": "brothels",
                    "taxonomy_subcategory_id": "administrative_closure",
                    "category": "brothel",
                    "sub_category": "closure",
                    "review_status": "reviewed",
                    "annotation_source": "  manual table  ",
                    "expected_month_bucket": " 2026-03 ",
                    "expected_city": " תל אביב ",
                    "expected_status": " closed ",
                    "manual_city": "",
                    "manual_address": "",
                    "manual_event_label": "",
                    "manual_status": "",
                    "annotation_notes": "",
                    "collected_at": "2026-03-03T00:00:00+00:00",
                }
            ],
        )

        result = finalize_validation_set(
            input_path=draft_path, validation_set_path=tmp_path / "out.csv"
        )
        rows = read_csv_rows(result.validation_set_path)

        assert rows[0]["annotation_source"] == "manual table"
        assert rows[0]["expected_month_bucket"] == "2026-03"
        assert rows[0]["expected_city"] == "תל אביב"
        assert rows[0]["expected_status"] == "closed"

    def test_finalize_validation_set_accepts_blank_expected_fields(self, tmp_path: Path) -> None:
        draft_path = tmp_path / "draft.csv"
        write_csv_rows(
            draft_path,
            DRAFT_COLUMNS,
            [
                {
                    "source_name": "mako",
                    "article_date": "2026-03-03T00:00:00+00:00",
                    "url": "https://example.com/b",
                    "canonical_url": "https://example.com/b",
                    "title": "title b",
                    "snippet": "snippet b",
                    "suggested_relevant": "True",
                    "suggested_enforcement_related": "False",
                    "suggested_category": "brothel",
                    "suggested_sub_category": "",
                    "suggested_confidence": "high",
                    "relevant": "True",
                    "enforcement_related": "False",
                    "category": "brothel",
                    "sub_category": "",
                    "review_status": "reviewed",
                    "annotation_source": " ",
                    "expected_month_bucket": " ",
                    "expected_city": " ",
                    "expected_status": " ",
                    "annotation_notes": "",
                    "collected_at": "2026-03-03T00:00:00+00:00",
                }
            ],
        )

        result = finalize_validation_set(
            input_path=draft_path, validation_set_path=tmp_path / "out.csv"
        )
        rows = read_csv_rows(result.validation_set_path)

        assert rows[0]["annotation_source"] == ""
        assert rows[0]["expected_month_bucket"] == ""
        assert rows[0]["expected_city"] == ""
        assert rows[0]["expected_status"] == ""

    def test_finalize_validation_set_round_trips_legacy_and_newer_rows(
        self, tmp_path: Path
    ) -> None:
        validation_set_path = tmp_path / "classifier_validation.csv"
        write_csv_rows(
            validation_set_path,
            [
                "source_name",
                "article_date",
                "url",
                "canonical_url",
                "title",
                "snippet",
                "relevant",
                "enforcement_related",
                "category",
                "sub_category",
                "review_status",
                "annotation_notes",
                "collected_at",
                "finalized_at",
                "draft_source",
            ],
            [
                {
                    "source_name": "ynet",
                    "article_date": "2026-03-01T00:00:00+00:00",
                    "url": "https://example.com/a",
                    "canonical_url": "https://example.com/a",
                    "title": "legacy row",
                    "snippet": "legacy snippet",
                    "relevant": "True",
                    "enforcement_related": "True",
                    "category": "brothel",
                    "sub_category": "closure",
                    "review_status": "reviewed",
                    "annotation_notes": "",
                    "collected_at": "2026-03-01T00:00:00+00:00",
                    "finalized_at": "2026-03-02T00:00:00+00:00",
                    "draft_source": "legacy.csv",
                }
            ],
        )
        draft_path = tmp_path / "draft.csv"
        write_csv_rows(
            draft_path,
            DRAFT_COLUMNS,
            [
                {
                    "source_name": "mako",
                    "article_date": "2026-03-03T00:00:00+00:00",
                    "url": "https://example.com/b",
                    "canonical_url": "https://example.com/b",
                    "title": "new row",
                    "snippet": "new snippet",
                    "suggested_relevant": "True",
                    "suggested_enforcement_related": "True",
                    "suggested_index_relevant": "True",
                    "suggested_taxonomy_version": "1",
                    "suggested_taxonomy_category_id": "brothels",
                    "suggested_taxonomy_subcategory_id": "administrative_closure",
                    "suggested_category": "brothel",
                    "suggested_sub_category": "closure",
                    "suggested_confidence": "high",
                    "relevant": "True",
                    "enforcement_related": "True",
                    "index_relevant": "True",
                    "taxonomy_version": "1",
                    "taxonomy_category_id": "brothels",
                    "taxonomy_subcategory_id": "administrative_closure",
                    "category": "brothel",
                    "sub_category": "closure",
                    "review_status": "reviewed",
                    "annotation_source": "manual table",
                    "expected_month_bucket": "2026-03",
                    "expected_city": "חיפה",
                    "expected_status": "closed",
                    "annotation_notes": "",
                    "collected_at": "2026-03-03T00:00:00+00:00",
                }
            ],
        )

        result = finalize_validation_set(
            input_path=draft_path,
            validation_set_path=validation_set_path,
        )
        rows = read_csv_rows(result.validation_set_path)

        assert result.total_rows == 2
        rows_by_source = {row["source_name"]: row for row in rows}
        assert rows_by_source["ynet"]["expected_month_bucket"] == ""
        assert rows_by_source["ynet"]["expected_city"] == ""
        assert rows_by_source["ynet"]["expected_status"] == ""
        assert rows_by_source["mako"]["taxonomy_category_id"] == "brothels"
        assert rows_by_source["mako"]["taxonomy_subcategory_id"] == "administrative_closure"
        assert rows_by_source["mako"]["expected_month_bucket"] == "2026-03"
        assert rows_by_source["mako"]["expected_city"] == "חיפה"
        assert rows_by_source["mako"]["expected_status"] == "closed"

    def test_run_validation_finalize_delegates(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The finalize wrapper should forward its arguments to the core function."""
        captured: dict[str, object] = {}

        def fake_finalize_validation_set(*, input_path: Path, validation_set_path: Path) -> object:
            captured["input_path"] = input_path
            captured["validation_set_path"] = validation_set_path

            return ValidationFinalizeResult(
                validation_set_path=validation_set_path,
                added_rows=0,
                skipped_duplicates=0,
                reviewed_rows=0,
                total_rows=0,
            )

        monkeypatch.setattr(
            "denbust.validation.dataset.finalize_validation_set",
            fake_finalize_validation_set,
        )

        result = run_validation_finalize(
            input_path=Path("draft.csv"),
            validation_set_path=Path("validation.csv"),
        )

        assert captured == {
            "input_path": Path("draft.csv"),
            "validation_set_path": Path("validation.csv"),
        }
        assert result.validation_set_path == Path("validation.csv")

    def test_parse_existing_validation_row_trims_annotation_source(self) -> None:
        row = _parse_existing_validation_row(
            {
                "source_name": "ynet",
                "article_date": "2026-03-01T00:00:00+00:00",
                "url": "https://example.com/a",
                "canonical_url": "https://example.com/a",
                "title": "title a",
                "snippet": "snippet a",
                "relevant": "True",
                "enforcement_related": "True",
                "index_relevant": "False",
                "taxonomy_version": "",
                "taxonomy_category_id": "",
                "taxonomy_subcategory_id": "",
                "category": "brothel",
                "sub_category": "closure",
                "review_status": "reviewed",
                "annotation_source": "  manual table  ",
                "expected_month_bucket": "",
                "expected_city": "",
                "expected_status": "",
                "manual_city": "",
                "manual_address": "",
                "manual_event_label": "",
                "manual_status": "",
                "annotation_notes": "",
                "collected_at": "2026-03-01T00:00:00+00:00",
                "finalized_at": "2026-03-02T00:00:00+00:00",
                "draft_source": "draft.csv",
            }
        )

        assert row.annotation_source == "manual table"


class TestValidationEvaluate:
    """Tests for classifier variant evaluation."""

    def test_loaders_and_rendering_cover_error_paths(self, tmp_path: Path) -> None:
        """Variant/example loaders and table rendering should handle missing and empty inputs."""
        with pytest.raises(FileNotFoundError, match="Variant matrix not found"):
            _load_variant_matrix(tmp_path / "missing.yaml")

        empty_variants_path = tmp_path / "variants.yaml"
        empty_variants_path.write_text(yaml.safe_dump({"variants": []}), encoding="utf-8")
        with pytest.raises(ValueError, match="at least one variant"):
            _load_variant_matrix(empty_variants_path)

        with pytest.raises(FileNotFoundError, match="Validation set not found"):
            _load_validation_examples(tmp_path / "missing.csv")

        empty_validation_path = tmp_path / "validation.csv"
        write_csv_rows(empty_validation_path, VALIDATION_SET_COLUMNS, [])
        with pytest.raises(ValueError, match="Validation set is empty"):
            _load_validation_examples(empty_validation_path)

        table = render_rankings_table(
            [
                VariantMetrics(
                    name="baseline",
                    description=None,
                    model="claude-sonnet-4-20250514",
                    relevance_precision=1.0,
                    relevance_recall=0.5,
                    relevance_f1=0.667,
                    relevance_accuracy=0.75,
                    enforcement_precision_relevant_only=1.0,
                    enforcement_recall_relevant_only=1.0,
                    enforcement_f1_relevant_only=1.0,
                    enforcement_accuracy_relevant_only=1.0,
                    category_accuracy_relevant_only=0.5,
                    subcategory_accuracy_relevant_only=0.5,
                    overall_exact_match=0.5,
                    tp=1,
                    fp=0,
                    fn=1,
                    tn=2,
                    total_examples=4,
                )
            ]
        )
        assert "rank" in table
        assert "enf_f1" in table
        assert "baseline" in table
        assert "0.667" in table

    def test_load_tracked_variant_matrix_exposes_baseline_and_v1_taxonomy(self) -> None:
        """The tracked asset should exercise the two-variant Phase C matrix shape."""
        matrix = _load_variant_matrix(DEFAULT_VARIANT_MATRIX_PATH)

        assert [variant.name for variant in matrix.variants] == ["baseline", "v1_taxonomy"]
        assert matrix.defaults.model == "claude-sonnet-4-20250514"
        assert matrix.variants[0].system_prompt is None
        assert matrix.variants[0].user_prompt_template is None
        assert matrix.variants[1].system_prompt is not None
        assert "Return only JSON" in matrix.variants[1].system_prompt
        assert matrix.variants[1].user_prompt_template is not None
        assert "taxonomy_category_id" in matrix.variants[1].user_prompt_template
        assert "{title}" in matrix.variants[1].user_prompt_template
        assert "{snippet}" in matrix.variants[1].user_prompt_template

    def test_load_validation_examples_defaults_missing_enforcement_flag_false(
        self, tmp_path: Path
    ) -> None:
        """Older validation rows without enforcement_related should remain readable."""
        validation_path = tmp_path / "validation.csv"
        write_csv_rows(
            validation_path,
            [
                "source_name",
                "article_date",
                "url",
                "canonical_url",
                "title",
                "snippet",
                "relevant",
                "category",
                "sub_category",
                "review_status",
                "annotation_notes",
                "collected_at",
                "finalized_at",
                "draft_source",
            ],
            [
                {
                    "source_name": "ynet",
                    "article_date": "2026-03-01T00:00:00+00:00",
                    "url": "https://example.com/a",
                    "canonical_url": "https://example.com/a",
                    "title": "title a",
                    "snippet": "snippet a",
                    "relevant": "True",
                    "category": "brothel",
                    "sub_category": "closure",
                    "review_status": "reviewed",
                    "annotation_notes": "",
                    "collected_at": "2026-03-01T00:00:00+00:00",
                    "finalized_at": "2026-03-02T00:00:00+00:00",
                    "draft_source": "draft.csv",
                }
            ],
        )

        _articles, labels = _load_validation_examples(validation_path)

        assert labels == [(True, False, "brothel", "closure")]

    def test_load_validation_examples_accepts_expanded_schema_rows(self, tmp_path: Path) -> None:
        validation_path = tmp_path / "validation.csv"
        write_csv_rows(
            validation_path,
            VALIDATION_SET_COLUMNS,
            [
                {
                    "source_name": "ynet",
                    "article_date": "2026-03-01T00:00:00+00:00",
                    "url": "https://example.com/a",
                    "canonical_url": "https://example.com/a",
                    "title": "title a",
                    "snippet": "snippet a",
                    "relevant": "True",
                    "enforcement_related": "True",
                    "index_relevant": "True",
                    "taxonomy_version": "1",
                    "taxonomy_category_id": "brothels",
                    "taxonomy_subcategory_id": "administrative_closure",
                    "category": "brothel",
                    "sub_category": "closure",
                    "review_status": "reviewed",
                    "annotation_source": "manual table",
                    "expected_month_bucket": "2026-03",
                    "expected_city": "חיפה",
                    "expected_status": "closed",
                    "manual_city": "",
                    "manual_address": "",
                    "manual_event_label": "",
                    "manual_status": "",
                    "annotation_notes": "",
                    "collected_at": "2026-03-01T00:00:00+00:00",
                    "finalized_at": "2026-03-02T00:00:00+00:00",
                    "draft_source": "draft.csv",
                }
            ],
        )

        _articles, labels = _load_validation_examples(validation_path)

        assert labels[0].index_relevant is True
        assert labels[0].taxonomy_version == "1"
        assert labels[0].taxonomy_category_id == "brothels"
        assert labels[0].taxonomy_subcategory_id == "administrative_closure"

    @pytest.mark.asyncio
    async def test_evaluate_classifier_variants_requires_api_key(self, tmp_path: Path) -> None:
        """Evaluation should fail fast without Anthropic credentials."""
        validation_set_path = tmp_path / "validation.csv"
        write_csv_rows(
            validation_set_path,
            VALIDATION_SET_COLUMNS,
            [
                {
                    "source_name": "ynet",
                    "article_date": "2026-03-01T00:00:00+00:00",
                    "url": "https://example.com/a",
                    "canonical_url": "https://example.com/a",
                    "title": "title a",
                    "snippet": "snippet a",
                    "relevant": "True",
                    "enforcement_related": "True",
                    "category": "brothel",
                    "sub_category": "closure",
                    "review_status": "reviewed",
                    "annotation_notes": "",
                    "collected_at": "2026-03-01T00:00:00+00:00",
                    "finalized_at": "2026-03-02T00:00:00+00:00",
                    "draft_source": "draft.csv",
                }
            ],
        )
        variants_path = tmp_path / "variants.yaml"
        variants_path.write_text(
            yaml.safe_dump({"variants": [{"name": "baseline"}]}),
            encoding="utf-8",
        )

        with pytest.raises(ValueError, match="ANTHROPIC_API_KEY"):
            await evaluate_classifier_variants(
                validation_set_path=validation_set_path,
                variants_path=variants_path,
                output_path=tmp_path / "report.json",
            )

    @pytest.mark.asyncio
    async def test_evaluate_classifier_variants_ranks_relevance_first(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Variants should rank by relevance F1 before category/sub-category metrics."""
        validation_set_path = tmp_path / "validation.csv"
        write_csv_rows(
            validation_set_path,
            VALIDATION_SET_COLUMNS,
            [
                {
                    "source_name": "ynet",
                    "article_date": "2026-03-01T00:00:00+00:00",
                    "url": "https://example.com/a",
                    "canonical_url": "https://example.com/a",
                    "title": "title a",
                    "snippet": "snippet a",
                    "relevant": "True",
                    "enforcement_related": "True",
                    "index_relevant": "False",
                    "taxonomy_version": "",
                    "taxonomy_category_id": "",
                    "taxonomy_subcategory_id": "",
                    "category": "brothel",
                    "sub_category": "closure",
                    "review_status": "reviewed",
                    "annotation_source": "",
                    "expected_month_bucket": "",
                    "expected_city": "",
                    "expected_status": "",
                    "manual_city": "",
                    "manual_address": "",
                    "manual_event_label": "",
                    "manual_status": "",
                    "annotation_notes": "",
                    "collected_at": "2026-03-01T00:00:00+00:00",
                    "finalized_at": "2026-03-02T00:00:00+00:00",
                    "draft_source": "draft.csv",
                },
                {
                    "source_name": "mako",
                    "article_date": "2026-03-02T00:00:00+00:00",
                    "url": "https://example.com/b",
                    "canonical_url": "https://example.com/b",
                    "title": "title b",
                    "snippet": "snippet b",
                    "relevant": "False",
                    "enforcement_related": "False",
                    "index_relevant": "False",
                    "taxonomy_version": "",
                    "taxonomy_category_id": "",
                    "taxonomy_subcategory_id": "",
                    "category": "not_relevant",
                    "sub_category": "",
                    "review_status": "reviewed",
                    "annotation_source": "",
                    "expected_month_bucket": "",
                    "expected_city": "",
                    "expected_status": "",
                    "manual_city": "",
                    "manual_address": "",
                    "manual_event_label": "",
                    "manual_status": "",
                    "annotation_notes": "",
                    "collected_at": "2026-03-02T00:00:00+00:00",
                    "finalized_at": "2026-03-03T00:00:00+00:00",
                    "draft_source": "draft.csv",
                },
            ],
        )
        variants_path = tmp_path / "variants.yaml"
        variants_path.write_text(
            yaml.safe_dump(
                {
                    "defaults": {"model": "claude-sonnet-4-20250514"},
                    "variants": [
                        {"name": "baseline"},
                        {"name": "prompt-tuned", "system_prompt": "override"},
                    ],
                }
            ),
            encoding="utf-8",
        )

        prediction_sets = {
            "baseline": [
                ClassificationResult(
                    relevant=False,
                    enforcement_related=False,
                    category=Category.NOT_RELEVANT,
                    sub_category=None,
                    confidence="high",
                ),
                ClassificationResult(
                    relevant=False,
                    enforcement_related=False,
                    category=Category.NOT_RELEVANT,
                    sub_category=None,
                    confidence="high",
                ),
            ],
            "prompt-tuned": [
                ClassificationResult(
                    relevant=True,
                    enforcement_related=True,
                    category=Category.PROSTITUTION,
                    sub_category=None,
                    confidence="high",
                ),
                ClassificationResult(
                    relevant=False,
                    enforcement_related=False,
                    category=Category.NOT_RELEVANT,
                    sub_category=None,
                    confidence="high",
                ),
            ],
        }

        class VariantClassifier:
            def __init__(self, variant_name: str) -> None:
                self.variant_name = variant_name

            async def classify_batch(self, articles: list[RawArticle]) -> list[ClassifiedArticle]:
                return [
                    ClassifiedArticle(article=article, classification=prediction)
                    for article, prediction in zip(
                        articles,
                        prediction_sets[self.variant_name],
                        strict=True,
                    )
                ]

        def fake_create_classifier(
            *,
            api_key: str,
            model: str,
            system_prompt: str | None = None,
            user_prompt_template: str | None = None,
        ) -> VariantClassifier:
            del api_key, model, user_prompt_template
            return VariantClassifier("prompt-tuned" if system_prompt else "baseline")

        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        monkeypatch.setattr("denbust.validation.evaluate.create_classifier", fake_create_classifier)

        result = await evaluate_classifier_variants(
            validation_set_path=validation_set_path,
            variants_path=variants_path,
            output_path=tmp_path / "report.json",
        )

        assert [metric.name for metric in result.rankings] == ["prompt-tuned", "baseline"]
        assert result.output_path.exists()
        assert result.markdown_path.exists()
        report = json.loads(result.output_path.read_text(encoding="utf-8"))
        assert report["dataset_summary"]["total_examples"] == 2
        assert report["dataset_summary"]["legacy_only_examples"] == 2
        assert report["dataset_summary"]["legacy_category_counts_relevant_only"][0] == {
            "label": "brothel",
            "evaluated_examples": 1,
        }
        first = report["rankings"][0]
        assert first["relevance_stage"]["evaluated_examples"] == 2
        assert first["enforcement_stage_relevant_only"]["evaluated_examples"] == 1
        assert first["category_stage_relevant_only"]["evaluated_examples"] == 1
        assert first["taxonomy_subcategory_stage_taxonomy_labeled"]["evaluated_examples"] == 0
        assert first["index_relevance_stage_taxonomy_labeled"]["evaluated_examples"] == 0
        assert first["legacy_category_breakdown_relevant_only"][0]["label"] == "brothel"
        markdown = result.markdown_path.read_text(encoding="utf-8")
        assert "## Dataset Coverage" in markdown
        assert "Legacy-only examples" in markdown
        assert "## Validation Set Typology Coverage" in markdown
        assert "label    n" in markdown
        assert "correct" in markdown
        assert "### prompt-tuned" in markdown

    def test_score_predictions_uses_only_relevant_rows_for_category_metrics(self) -> None:
        """Category and sub-category metrics should ignore non-relevant gold rows."""
        metrics = _score_predictions(
            labels=[
                (True, True, "brothel", "closure"),
                (False, False, "not_relevant", ""),
            ],
            predictions=[
                (True, True, "brothel", "closure"),
                (False, False, "brothel", "closure"),
            ],
            variant=ClassifierVariantSpec(name="baseline"),
            model="claude-sonnet-4-20250514",
        )

        assert metrics.enforcement_accuracy_relevant_only == 1.0
        assert metrics.category_stage_relevant_only.evaluated_examples == 1
        assert metrics.category_stage_relevant_only.correct == 1
        assert metrics.category_accuracy_relevant_only == 1.0
        assert metrics.subcategory_stage_relevant_only.evaluated_examples == 1
        assert metrics.subcategory_stage_relevant_only.correct == 1
        assert metrics.subcategory_accuracy_relevant_only == 1.0

    def test_score_predictions_requires_predicted_relevance_for_category_credit(self) -> None:
        """Category and sub-category accuracy should not award credit to irrelevant predictions."""
        metrics = _score_predictions(
            labels=[
                (True, True, "brothel", "closure"),
            ],
            predictions=[
                (False, False, "brothel", "closure"),
            ],
            variant=ClassifierVariantSpec(name="baseline"),
            model="claude-sonnet-4-20250514",
        )

        assert metrics.relevance_f1 == 0.0
        assert metrics.enforcement_f1_relevant_only == 0.0
        assert metrics.category_stage_relevant_only.evaluated_examples == 1
        assert metrics.category_stage_relevant_only.correct == 0
        assert metrics.category_accuracy_relevant_only == 0.0
        assert metrics.subcategory_stage_relevant_only.evaluated_examples == 1
        assert metrics.subcategory_stage_relevant_only.correct == 0
        assert metrics.subcategory_accuracy_relevant_only == 0.0

    def test_score_predictions_counts_false_positives(self) -> None:
        """Scoring should count false positives and exact matches correctly."""
        metrics = _score_predictions(
            labels=[
                (False, False, "not_relevant", ""),
                (True, True, "brothel", "closure"),
            ],
            predictions=[
                (True, False, "brothel", "closure"),
                (True, True, "brothel", "closure"),
            ],
            variant=ClassifierVariantSpec(name="baseline"),
            model="claude-sonnet-4-20250514",
        )

        assert metrics.fp == 1
        assert metrics.tp == 1
        assert metrics.fn == 0
        assert metrics.tn == 0
        assert metrics.overall_exact_match == 0.5

    def test_score_predictions_tracks_enforcement_metrics_for_relevant_examples(self) -> None:
        """Enforcement metrics should be computed on the relevant subset."""
        metrics = _score_predictions(
            labels=[
                (True, True, "brothel", "closure"),
                (True, False, "prostitution", ""),
            ],
            predictions=[
                (True, False, "brothel", "closure"),
                (True, False, "prostitution", ""),
            ],
            variant=ClassifierVariantSpec(name="baseline"),
            model="claude-sonnet-4-20250514",
        )

        assert metrics.enforcement_precision_relevant_only == 0.0
        assert metrics.enforcement_recall_relevant_only == 0.0
        assert metrics.enforcement_f1_relevant_only == 0.0
        assert metrics.enforcement_stage_relevant_only.evaluated_examples == 2
        assert metrics.enforcement_stage_relevant_only.tp == 0
        assert metrics.enforcement_stage_relevant_only.fp == 0
        assert metrics.enforcement_stage_relevant_only.fn == 1
        assert metrics.enforcement_stage_relevant_only.tn == 1
        assert metrics.enforcement_accuracy_relevant_only == 0.5

    def test_score_predictions_ignores_enforcement_when_prediction_is_irrelevant(self) -> None:
        """Irrelevant predictions should not receive enforcement credit."""
        metrics = _score_predictions(
            labels=[
                (True, True, "trafficking", "rescue"),
            ],
            predictions=[
                (False, True, "not_relevant", ""),
            ],
            variant=ClassifierVariantSpec(name="baseline"),
            model="claude-sonnet-4-20250514",
        )

        assert metrics.fn == 1
        assert metrics.relevance_stage.evaluated_examples == 1
        assert metrics.relevance_stage.fn == 1
        assert metrics.enforcement_precision_relevant_only == 0.0
        assert metrics.enforcement_recall_relevant_only == 0.0
        assert metrics.enforcement_f1_relevant_only == 0.0
        assert metrics.enforcement_accuracy_relevant_only == 0.0

    def test_score_predictions_counts_enforcement_false_positive_on_relevant_rows(self) -> None:
        """Predicted enforcement should count as FP on relevant rows when gold is non-enforcement."""
        metrics = _score_predictions(
            labels=[
                (True, False, "prostitution", ""),
            ],
            predictions=[
                (True, True, "prostitution", ""),
            ],
            variant=ClassifierVariantSpec(name="baseline"),
            model="claude-sonnet-4-20250514",
        )

        assert metrics.enforcement_precision_relevant_only == 0.0
        assert metrics.enforcement_recall_relevant_only == 0.0
        assert metrics.enforcement_f1_relevant_only == 0.0
        assert metrics.enforcement_accuracy_relevant_only == 0.0

    def test_build_dataset_summary_rejects_partial_taxonomy_ids(self) -> None:
        with pytest.raises(ValueError, match="partial taxonomy ids"):
            _build_dataset_summary(
                [
                    (
                        ValidationLabel(
                            relevant=True,
                            enforcement_related=True,
                            category="brothel",
                            sub_category="closure",
                            index_relevant=False,
                            taxonomy_version="1",
                            taxonomy_category_id="brothels",
                            taxonomy_subcategory_id="",
                        )
                    )
                ]
            )

    def test_build_dataset_summary_counts_taxonomy_labeled_rows(self) -> None:
        summary = _build_dataset_summary(
            [
                ValidationLabel(
                    relevant=True,
                    enforcement_related=True,
                    category="brothel",
                    sub_category="closure",
                    index_relevant=True,
                    taxonomy_version="1",
                    taxonomy_category_id="brothels",
                    taxonomy_subcategory_id="administrative_closure",
                )
            ]
        )

        assert summary.total_examples == 1
        assert summary.legacy_only_examples == 0
        assert summary.taxonomy_labeled_examples == 1
        assert summary.taxonomy_category_counts[0].label == "brothels"
        assert summary.taxonomy_subcategory_counts[0].label == "administrative_closure"

    def test_resolve_report_paths_rejects_markdown_json_collision(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        report_path = tmp_path / "report.json"
        monkeypatch.setattr(
            "denbust.validation.evaluate._markdown_path_for_json",
            lambda path: path,
        )

        with pytest.raises(ValueError, match="must differ"):
            _resolve_report_paths(
                config=Config(),
                collected_at=datetime(2026, 4, 10, tzinfo=UTC),
                output_path=report_path,
            )

    @pytest.mark.asyncio
    async def test_evaluate_classifier_variants_rejects_non_json_output_path(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        validation_set_path = tmp_path / "validation.csv"
        write_csv_rows(
            validation_set_path,
            VALIDATION_SET_COLUMNS,
            [
                {
                    "source_name": "ynet",
                    "article_date": "2026-03-01T00:00:00+00:00",
                    "url": "https://example.com/a",
                    "canonical_url": "https://example.com/a",
                    "title": "title a",
                    "snippet": "snippet a",
                    "relevant": "False",
                    "enforcement_related": "False",
                    "index_relevant": "False",
                    "taxonomy_version": "",
                    "taxonomy_category_id": "",
                    "taxonomy_subcategory_id": "",
                    "category": "not_relevant",
                    "sub_category": "",
                    "review_status": "reviewed",
                    "annotation_source": "",
                    "expected_month_bucket": "",
                    "expected_city": "",
                    "expected_status": "",
                    "manual_city": "",
                    "manual_address": "",
                    "manual_event_label": "",
                    "manual_status": "",
                    "annotation_notes": "",
                    "collected_at": "2026-03-01T00:00:00+00:00",
                    "finalized_at": "2026-03-02T00:00:00+00:00",
                    "draft_source": "draft.csv",
                }
            ],
        )
        variants_path = tmp_path / "variants.yaml"
        variants_path.write_text(
            yaml.safe_dump(
                {
                    "defaults": {"model": "claude-sonnet-4-20250514"},
                    "variants": [{"name": "baseline"}],
                }
            ),
            encoding="utf-8",
        )

        class VariantClassifier:
            async def classify_batch(self, articles: list[RawArticle]) -> list[ClassifiedArticle]:
                return [
                    ClassifiedArticle(
                        article=article,
                        classification=ClassificationResult(
                            relevant=False,
                            enforcement_related=False,
                            category=Category.NOT_RELEVANT,
                            sub_category=None,
                            confidence="high",
                        ),
                    )
                    for article in articles
                ]

        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        monkeypatch.setattr(
            "denbust.validation.evaluate.create_classifier",
            lambda **_kwargs: VariantClassifier(),
        )

        with pytest.raises(ValueError, match="must end with \\.json"):
            await evaluate_classifier_variants(
                validation_set_path=validation_set_path,
                variants_path=variants_path,
                output_path=tmp_path / "report.md",
            )

    def test_run_validation_evaluate_delegates(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """The evaluate wrapper should set up logging and delegate through asyncio.run."""
        calls: dict[str, object] = {}

        async def fake_evaluate_classifier_variants(
            *,
            validation_set_path: Path,
            variants_path: Path,
            output_path: Path | None = None,
        ) -> object:
            calls["validation_set_path"] = validation_set_path
            calls["variants_path"] = variants_path
            calls["output_path"] = output_path

            class Result:
                output_path = tmp_path / "report.json"
                markdown_path = tmp_path / "report.md"
                rankings: list[object] = []

            return Result()

        monkeypatch.setattr(
            "denbust.validation.evaluate.setup_logging",
            lambda: calls.setdefault("setup", True),
        )
        monkeypatch.setattr(
            "denbust.validation.evaluate.evaluate_classifier_variants",
            fake_evaluate_classifier_variants,
        )

        result = run_validation_evaluate(
            validation_set_path=Path("validation.csv"),
            variants_path=Path("variants.yaml"),
            output_path=Path("report.json"),
        )

        assert calls == {
            "setup": True,
            "validation_set_path": Path("validation.csv"),
            "variants_path": Path("variants.yaml"),
            "output_path": Path("report.json"),
        }
        assert result.output_path == tmp_path / "report.json"

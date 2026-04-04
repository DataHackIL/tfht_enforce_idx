"""Unit tests for validation-set collection, finalization, and evaluation."""

from __future__ import annotations

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
from denbust.validation.collect import collect_validation_draft, select_promising_candidates
from denbust.validation.common import (
    DEFAULT_VALIDATION_SET_PATH,
    DRAFT_COLUMNS,
    VALIDATION_SET_COLUMNS,
    read_csv_rows,
    write_csv_rows,
)
from denbust.validation.dataset import finalize_validation_set
from denbust.validation.evaluate import _score_predictions, evaluate_classifier_variants
from denbust.validation.models import ClassifierVariantSpec


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
    category: Category,
    sub_category: SubCategory | None = None,
) -> ClassifiedArticle:
    """Build a classified article tied to a raw article."""
    return ClassifiedArticle(
        article=article,
        classification=ClassificationResult(
            relevant=relevant,
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
        assert rows[0]["suggested_category"]
        assert rows[0]["category"] == rows[0]["suggested_category"]
        assert rows[0]["relevant"] == rows[0]["suggested_relevant"]


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
                    "suggested_category": "brothel",
                    "suggested_sub_category": "closure",
                    "suggested_confidence": "high",
                    "relevant": "True",
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
                    "suggested_category": "not_relevant",
                    "suggested_sub_category": "",
                    "suggested_confidence": "high",
                    "relevant": "False",
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
                    "suggested_category": "trafficking",
                    "suggested_sub_category": "rescue",
                    "suggested_confidence": "high",
                    "relevant": "True",
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
                    "suggested_category": "brothel",
                    "suggested_sub_category": "closure",
                    "suggested_confidence": "high",
                    "relevant": "True",
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


class TestValidationEvaluate:
    """Tests for classifier variant evaluation."""

    @pytest.mark.asyncio
    async def test_evaluate_classifier_variants_ranks_relevance_first(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Variants should rank by relevance F1 before category/sub-category metrics."""
        validation_set_path = tmp_path / "validation.csv"
        header = DEFAULT_VALIDATION_SET_PATH.read_text(encoding="utf-8").strip()
        validation_set_path.write_text(
            "\n".join(
                [
                    header,
                    ",".join(
                        [
                            "ynet",
                            "2026-03-01T00:00:00+00:00",
                            "https://example.com/a",
                            "https://example.com/a",
                            "title a",
                            "snippet a",
                            "True",
                            "brothel",
                            "closure",
                            "reviewed",
                            "",
                            "2026-03-01T00:00:00+00:00",
                            "2026-03-02T00:00:00+00:00",
                            "draft.csv",
                        ]
                    ),
                    ",".join(
                        [
                            "mako",
                            "2026-03-02T00:00:00+00:00",
                            "https://example.com/b",
                            "https://example.com/b",
                            "title b",
                            "snippet b",
                            "False",
                            "not_relevant",
                            "",
                            "reviewed",
                            "",
                            "2026-03-02T00:00:00+00:00",
                            "2026-03-03T00:00:00+00:00",
                            "draft.csv",
                        ]
                    ),
                ]
            ),
            encoding="utf-8",
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
                    category=Category.NOT_RELEVANT,
                    sub_category=None,
                    confidence="high",
                ),
                ClassificationResult(
                    relevant=False,
                    category=Category.NOT_RELEVANT,
                    sub_category=None,
                    confidence="high",
                ),
            ],
            "prompt-tuned": [
                ClassificationResult(
                    relevant=True,
                    category=Category.PROSTITUTION,
                    sub_category=None,
                    confidence="high",
                ),
                ClassificationResult(
                    relevant=False,
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

    def test_score_predictions_uses_only_relevant_rows_for_category_metrics(self) -> None:
        """Category and sub-category metrics should ignore non-relevant gold rows."""
        metrics = _score_predictions(
            labels=[
                (True, "brothel", "closure"),
                (False, "not_relevant", ""),
            ],
            predictions=[
                (True, "brothel", "closure"),
                (False, "brothel", "closure"),
            ],
            variant=ClassifierVariantSpec(name="baseline"),
            model="claude-sonnet-4-20250514",
        )

        assert metrics.category_accuracy_relevant_only == 1.0
        assert metrics.subcategory_accuracy_relevant_only == 1.0

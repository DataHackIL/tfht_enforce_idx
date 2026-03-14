"""Unit tests for pipeline orchestration helpers."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import HttpUrl

from denbust.config import Config, OutputConfig, OutputFormat, SourceConfig, SourceType
from denbust.data_models import (
    Category,
    ClassificationResult,
    ClassifiedArticle,
    RawArticle,
    SourceReference,
    SubCategory,
    UnifiedItem,
)
from denbust.pipeline import (
    classify_articles,
    create_sources,
    fetch_all_sources,
    mark_seen,
    run_pipeline,
    run_pipeline_async,
    setup_logging,
)


def build_raw_article(url: str = "https://example.com/article") -> RawArticle:
    """Create a sample raw article."""
    return RawArticle(
        url=HttpUrl(url),
        title="פשיטה על בית בושת",
        snippet="המשטרה ביצעה פשיטה.",
        date=datetime(2026, 3, 1, tzinfo=UTC),
        source_name="test",
    )


def build_classified_article(
    url: str = "https://example.com/article",
    *,
    relevant: bool = True,
) -> ClassifiedArticle:
    """Create a sample classified article."""
    return ClassifiedArticle(
        article=build_raw_article(url),
        classification=ClassificationResult(
            relevant=relevant,
            category=Category.BROTHEL if relevant else Category.NOT_RELEVANT,
            sub_category=SubCategory.CLOSURE if relevant else None,
            confidence="high" if relevant else "low",
        ),
    )


def build_unified_item(url: str = "https://example.com/article") -> UnifiedItem:
    """Create a sample unified item."""
    return UnifiedItem(
        headline="פשיטה על בית בושת",
        summary="סיכום",
        sources=[SourceReference(source_name="test", url=HttpUrl(url))],
        date=datetime(2026, 3, 1, tzinfo=UTC),
        category=Category.BROTHEL,
        sub_category=SubCategory.CLOSURE,
    )


class FakeSource:
    """Simple async source stub."""

    def __init__(self, name: str, fetch_result: list[RawArticle] | Exception) -> None:
        self.name = name
        self._fetch_result = fetch_result

    async def fetch(self, days: int, keywords: list[str]) -> list[RawArticle]:
        del days, keywords
        if isinstance(self._fetch_result, Exception):
            raise self._fetch_result
        return self._fetch_result


class TestSetupLogging:
    """Tests for setup_logging."""

    def test_setup_logging_defaults_to_info(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Default setup should use INFO level."""
        captured: dict[str, object] = {}

        def fake_basic_config(**kwargs: object) -> None:
            captured.update(kwargs)

        monkeypatch.setattr(logging, "basicConfig", fake_basic_config)

        setup_logging()

        assert captured["level"] == logging.INFO

    def test_setup_logging_verbose_uses_debug(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verbose setup should use DEBUG level."""
        captured: dict[str, object] = {}

        def fake_basic_config(**kwargs: object) -> None:
            captured.update(kwargs)

        monkeypatch.setattr(logging, "basicConfig", fake_basic_config)

        setup_logging(verbose=True)

        assert captured["level"] == logging.DEBUG


class TestCreateSourcesWarnings:
    """Tests for create_sources warning branches."""

    def test_create_sources_warns_on_missing_rss_url(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """RSS sources without URLs should be skipped with a warning."""
        config = Config(sources=[SourceConfig(name="ynet", type=SourceType.RSS)])
        mock_logger = MagicMock()
        monkeypatch.setattr("denbust.pipeline.logger", mock_logger)

        sources = create_sources(config)

        assert sources == []
        mock_logger.warning.assert_called_once()

    def test_create_sources_warns_on_unknown_scraper(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Unknown scraper names should be skipped with a warning."""
        config = Config(sources=[SourceConfig(name="unknown", type=SourceType.SCRAPER)])
        mock_logger = MagicMock()
        monkeypatch.setattr("denbust.pipeline.logger", mock_logger)

        sources = create_sources(config)

        assert sources == []
        mock_logger.warning.assert_called_once()


class TestFetchAndClassifyHelpers:
    """Tests for fetch and classification helpers."""

    @pytest.mark.asyncio
    async def test_fetch_all_sources_continues_after_source_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """One failing source should not prevent collecting later articles."""
        articles = [build_raw_article("https://example.com/ok")]
        sources = [
            FakeSource("bad", RuntimeError("boom")),
            FakeSource("good", articles),
        ]
        mock_logger = MagicMock()
        monkeypatch.setattr("denbust.pipeline.logger", mock_logger)

        found = await fetch_all_sources(sources, days=3, keywords=["זנות"])

        assert found == articles
        mock_logger.error.assert_called_once()

    @pytest.mark.asyncio
    async def test_classify_articles_filters_non_relevant(self) -> None:
        """Classifier helper should drop non-relevant articles."""
        classifier = MagicMock()
        classifier.classify_batch = AsyncMock(
            return_value=[
                build_classified_article("https://example.com/1", relevant=True),
                build_classified_article("https://example.com/2", relevant=False),
            ]
        )

        relevant = await classify_articles([build_raw_article()], classifier)

        assert len(relevant) == 1
        assert relevant[0].classification.relevant is True

    def test_mark_seen_collects_all_source_urls(self, tmp_path: Path) -> None:
        """mark_seen should persist every source URL from unified items."""
        from denbust.store.seen import SeenStore

        store = SeenStore(tmp_path / "seen.json")
        item = UnifiedItem(
            headline="Headline",
            summary="Summary",
            sources=[
                SourceReference(source_name="a", url=HttpUrl("https://a.com/1")),
                SourceReference(source_name="b", url=HttpUrl("https://b.com/1")),
            ],
            date=datetime(2026, 3, 1, tzinfo=UTC),
            category=Category.BROTHEL,
            sub_category=SubCategory.CLOSURE,
        )

        mark_seen([item], store)

        assert store.is_seen("https://a.com/1")
        assert store.is_seen("https://b.com/1")


class TestRunPipelineAsync:
    """Tests for async pipeline control flow."""

    @pytest.mark.asyncio
    async def test_run_pipeline_async_requires_api_key(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Missing API key should short-circuit before any work starts."""
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

        result = await run_pipeline_async(Config(), days=3)

        assert result == []

    @pytest.mark.asyncio
    async def test_run_pipeline_async_handles_missing_sources(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No configured sources should return early."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [])

        result = await run_pipeline_async(Config(), days=3)

        assert result == []

    @pytest.mark.asyncio
    async def test_run_pipeline_async_handles_no_articles(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No fetched articles should return early."""

        def fake_create_deduplicator(*, threshold: float) -> MagicMock:
            del threshold
            return MagicMock()

        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [MagicMock()])
        monkeypatch.setattr("denbust.pipeline.create_classifier", lambda **_kwargs: MagicMock())
        monkeypatch.setattr("denbust.pipeline.create_deduplicator", fake_create_deduplicator)
        monkeypatch.setattr("denbust.pipeline.create_seen_store", lambda _path: MagicMock())
        monkeypatch.setattr("denbust.pipeline.fetch_all_sources", AsyncMock(return_value=[]))

        result = await run_pipeline_async(Config(), days=3)

        assert result == []

    @pytest.mark.asyncio
    async def test_run_pipeline_async_handles_all_seen(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Already-seen articles should stop before classification."""

        def fake_create_deduplicator(*, threshold: float) -> MagicMock:
            del threshold
            return MagicMock()

        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        article = build_raw_article()
        monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [MagicMock()])
        monkeypatch.setattr("denbust.pipeline.create_classifier", lambda **_kwargs: MagicMock())
        monkeypatch.setattr("denbust.pipeline.create_deduplicator", fake_create_deduplicator)
        monkeypatch.setattr("denbust.pipeline.create_seen_store", lambda _path: MagicMock())
        monkeypatch.setattr("denbust.pipeline.fetch_all_sources", AsyncMock(return_value=[article]))
        monkeypatch.setattr("denbust.pipeline.filter_seen", lambda _articles, _seen_store: [])

        result = await run_pipeline_async(Config(), days=3)

        assert result == []

    @pytest.mark.asyncio
    async def test_run_pipeline_async_warns_on_max_articles_and_handles_no_relevant(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Large unseen batches should warn, then return early if nothing is relevant."""

        def fake_create_deduplicator(*, threshold: float) -> MagicMock:
            del threshold
            return MagicMock()

        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        articles = [build_raw_article(f"https://example.com/{i}") for i in range(2)]
        mock_logger = MagicMock()
        monkeypatch.setattr("denbust.pipeline.logger", mock_logger)
        monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [MagicMock()])
        monkeypatch.setattr("denbust.pipeline.create_classifier", lambda **_kwargs: MagicMock())
        monkeypatch.setattr("denbust.pipeline.create_deduplicator", fake_create_deduplicator)
        monkeypatch.setattr("denbust.pipeline.create_seen_store", lambda _path: MagicMock())
        monkeypatch.setattr("denbust.pipeline.fetch_all_sources", AsyncMock(return_value=articles))
        monkeypatch.setattr("denbust.pipeline.filter_seen", lambda articles, _seen_store: articles)
        monkeypatch.setattr("denbust.pipeline.classify_articles", AsyncMock(return_value=[]))

        result = await run_pipeline_async(Config(max_articles=1), days=3)

        assert result == []
        mock_logger.warning.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_pipeline_async_happy_path(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Successful runs should return unified items after marking them seen."""

        def fake_create_classifier(*, api_key: str, model: str) -> MagicMock:
            del api_key, model
            return classifier

        def fake_create_deduplicator(*, threshold: float) -> MagicMock:
            del threshold
            return deduplicator

        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        article = build_raw_article()
        classified = [build_classified_article()]
        unified = [build_unified_item()]
        seen_store = MagicMock()
        classifier = MagicMock()
        deduplicator = MagicMock()
        mark_seen_mock = MagicMock()

        monkeypatch.setattr("denbust.pipeline.create_sources", lambda _config: [MagicMock()])
        monkeypatch.setattr("denbust.pipeline.create_classifier", fake_create_classifier)
        monkeypatch.setattr("denbust.pipeline.create_deduplicator", fake_create_deduplicator)
        monkeypatch.setattr("denbust.pipeline.create_seen_store", lambda _path: seen_store)
        monkeypatch.setattr("denbust.pipeline.fetch_all_sources", AsyncMock(return_value=[article]))
        monkeypatch.setattr(
            "denbust.pipeline.filter_seen",
            lambda articles, _current_seen_store: articles,
        )
        monkeypatch.setattr(
            "denbust.pipeline.classify_articles",
            AsyncMock(return_value=classified),
        )
        monkeypatch.setattr("denbust.pipeline.deduplicate_articles", lambda _articles, _d: unified)
        monkeypatch.setattr("denbust.pipeline.mark_seen", mark_seen_mock)

        result = await run_pipeline_async(Config(), days=3)

        assert result == unified
        mark_seen_mock.assert_called_once_with(unified, seen_store)


class TestRunPipeline:
    """Tests for the sync run_pipeline wrapper."""

    def test_run_pipeline_exits_on_missing_config(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Missing config files should print a helpful message and exit 1."""
        missing = tmp_path / "missing.yaml"

        with pytest.raises(SystemExit) as exc_info:
            run_pipeline(missing)

        assert exc_info.value.code == 1
        assert f"Error: Config file not found: {missing}" in capsys.readouterr().out

    def test_run_pipeline_exits_on_invalid_config(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Other config load failures should also print and exit 1."""
        config_path = tmp_path / "broken.yaml"
        monkeypatch.setattr(
            "denbust.pipeline.load_config",
            MagicMock(side_effect=ValueError("bad yaml")),
        )

        with pytest.raises(SystemExit) as exc_info:
            run_pipeline(config_path)

        assert exc_info.value.code == 1
        assert "Error loading config: bad yaml" in capsys.readouterr().out

    def test_run_pipeline_uses_days_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """run_pipeline should prefer explicit day overrides over config defaults."""
        config = Config(days=3, output=OutputConfig(formats=[OutputFormat.CLI]))
        run_pipeline_async_mock = AsyncMock(return_value=[build_unified_item()])
        output_items_mock = MagicMock()

        monkeypatch.setattr("denbust.pipeline.setup_logging", MagicMock())
        monkeypatch.setattr("denbust.pipeline.load_config", MagicMock(return_value=config))
        monkeypatch.setattr("denbust.pipeline.run_pipeline_async", run_pipeline_async_mock)
        monkeypatch.setattr("denbust.pipeline.output_items", output_items_mock)

        run_pipeline(Path("agents/news.yaml"), days_override=7)

        run_pipeline_async_mock.assert_awaited_once_with(config, 7)
        output_items_mock.assert_called_once()

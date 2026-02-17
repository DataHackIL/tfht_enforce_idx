"""Cross-source deduplication using title similarity."""

import logging
from difflib import SequenceMatcher

from denbust.models import (
    Category,
    ClassifiedArticle,
    SourceReference,
    SubCategory,
    UnifiedItem,
)

logger = logging.getLogger(__name__)


class ArticleGroup:
    """A group of articles about the same story."""

    def __init__(self, article: ClassifiedArticle) -> None:
        """Initialize with a primary article.

        Args:
            article: Primary article for this group.
        """
        self.articles: list[ClassifiedArticle] = [article]

    def add(self, article: ClassifiedArticle) -> None:
        """Add an article to this group.

        Args:
            article: Article to add.
        """
        self.articles.append(article)

    @property
    def primary(self) -> ClassifiedArticle:
        """Get the primary (best) article.

        Returns the article with the longest snippet, or earliest date as tiebreaker.
        """
        return max(
            self.articles,
            key=lambda a: (len(a.article.snippet), -a.article.date.timestamp()),
        )

    @property
    def headline(self) -> str:
        """Get the best headline for this group."""
        return self.primary.article.title

    @property
    def category(self) -> Category:
        """Get the category from the primary article."""
        return self.primary.classification.category

    @property
    def sub_category(self) -> SubCategory | None:
        """Get the sub-category from the primary article."""
        return self.primary.classification.sub_category


class Deduplicator:
    """Deduplicate articles by grouping similar stories."""

    def __init__(self, similarity_threshold: float = 0.7) -> None:
        """Initialize deduplicator.

        Args:
            similarity_threshold: Minimum similarity (0-1) to consider articles
                as the same story. Default 0.7.
        """
        self._threshold = similarity_threshold

    def group(self, articles: list[ClassifiedArticle]) -> list[ArticleGroup]:
        """Group articles by similarity.

        Args:
            articles: List of classified articles.

        Returns:
            List of article groups.
        """
        if not articles:
            return []

        groups: list[ArticleGroup] = []

        for article in articles:
            # Try to find a matching group
            matching_group = self._find_matching_group(article, groups)

            if matching_group:
                matching_group.add(article)
            else:
                # Create a new group
                groups.append(ArticleGroup(article))

        logger.info(f"Grouped {len(articles)} articles into {len(groups)} unique stories")
        return groups

    def deduplicate(self, articles: list[ClassifiedArticle]) -> list[UnifiedItem]:
        """Deduplicate articles and return unified items.

        Args:
            articles: List of classified articles.

        Returns:
            List of unified items.
        """
        groups = self.group(articles)
        return [self._group_to_unified(group) for group in groups]

    def _find_matching_group(
        self, article: ClassifiedArticle, groups: list[ArticleGroup]
    ) -> ArticleGroup | None:
        """Find a group that this article belongs to.

        Args:
            article: Article to match.
            groups: Existing groups.

        Returns:
            Matching group or None.
        """
        for group in groups:
            if self._is_similar(article, group.primary):
                return group
        return None

    def _is_similar(self, article1: ClassifiedArticle, article2: ClassifiedArticle) -> bool:
        """Check if two articles are about the same story.

        Args:
            article1: First article.
            article2: Second article.

        Returns:
            True if articles are similar enough.
        """
        title1 = article1.article.title.lower()
        title2 = article2.article.title.lower()

        # Use SequenceMatcher for similarity
        ratio = SequenceMatcher(None, title1, title2).ratio()

        return ratio >= self._threshold

    def _group_to_unified(self, group: ArticleGroup) -> UnifiedItem:
        """Convert an article group to a unified item.

        Args:
            group: Article group.

        Returns:
            Unified item.
        """
        primary = group.primary

        # Build source references
        sources = [
            SourceReference(
                source_name=a.article.source_name,
                url=a.article.url,
            )
            for a in group.articles
        ]

        return UnifiedItem(
            headline=primary.article.title,
            summary=primary.article.snippet,
            sources=sources,
            date=primary.article.date,
            category=primary.classification.category,
            sub_category=primary.classification.sub_category,
        )


def create_deduplicator(threshold: float = 0.7) -> Deduplicator:
    """Create a deduplicator instance.

    Args:
        threshold: Similarity threshold (0-1).

    Returns:
        Deduplicator instance.
    """
    return Deduplicator(similarity_threshold=threshold)

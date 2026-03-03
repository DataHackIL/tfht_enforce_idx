"""Run history logging for tracking pipeline executions."""

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from denbust.data_models import ClassifiedArticle, RawArticle

logger = logging.getLogger(__name__)


class RunLogger:
    """Logs pipeline run results to JSON files."""

    def __init__(self, runs_dir: Path) -> None:
        """Initialize run logger.

        Args:
            runs_dir: Directory to store run logs.
        """
        self._runs_dir = runs_dir
        self._run_id = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%S")
        self._timestamp = datetime.now(UTC).isoformat()
        self._config: dict[str, Any] = {}
        self._source_stats: dict[str, dict[str, Any]] = {}
        self._keyword_matched: list[dict[str, Any]] = []
        self._classified_relevant: list[dict[str, Any]] = []
        self._classified_not_relevant: list[dict[str, Any]] = []
        self._issues: list[dict[str, Any]] = []

    @property
    def run_id(self) -> str:
        """Get the run ID."""
        return self._run_id

    def set_config(
        self,
        days: int,
        keywords: list[str],
        sources: list[str],
    ) -> None:
        """Set the configuration snapshot.

        Args:
            days: Number of days searched.
            keywords: Keywords used.
            sources: Source names.
        """
        self._config = {
            "days": days,
            "keywords": keywords,
            "sources": sources,
        }

    def log_source_result(
        self,
        source_name: str,
        source_type: str,
        status: str,
        fetched: int,
        keyword_matched: int,
        error: str | None = None,
        note: str | None = None,
    ) -> None:
        """Log result from a source fetch.

        Args:
            source_name: Name of the source.
            source_type: Type of source (rss/scraper).
            status: Status (success/error/blocked).
            fetched: Number of articles fetched.
            keyword_matched: Number matching keywords.
            error: Error message if any.
            note: Additional notes.
        """
        stats: dict[str, Any] = {
            "status": status,
            "type": source_type,
            "fetched": fetched,
            "keyword_matched": keyword_matched,
        }
        if error:
            stats["error"] = error
        if note:
            stats["note"] = note
        self._source_stats[source_name] = stats

    def log_keyword_matched(
        self,
        article: RawArticle,
        matched_keyword: str | None = None,
    ) -> None:
        """Log an article that matched keywords.

        Args:
            article: The matched article.
            matched_keyword: Which keyword matched.
        """
        entry: dict[str, Any] = {
            "url": str(article.url),
            "title": article.title,
            "source": article.source_name,
            "snippet": article.snippet[:200] if article.snippet else "",
        }
        if matched_keyword:
            entry["keyword_matched"] = matched_keyword
        self._keyword_matched.append(entry)

    def log_classification(
        self,
        classified: ClassifiedArticle,
        rejection_reason: str | None = None,
    ) -> None:
        """Log a classification result.

        Args:
            classified: The classified article.
            rejection_reason: Reason if not relevant.
        """
        article = classified.article
        classification = classified.classification

        entry: dict[str, Any] = {
            "url": str(article.url),
            "title": article.title,
            "source": article.source_name,
            "category": classification.category.value,
            "confidence": classification.confidence,
        }

        if classification.relevant:
            if classification.sub_category:
                entry["sub_category"] = classification.sub_category.value
            self._classified_relevant.append(entry)
        else:
            if rejection_reason:
                entry["rejection_reason"] = rejection_reason
            self._classified_not_relevant.append(entry)

    def log_issue(
        self,
        severity: str,
        source: str,
        issue: str,
        suggested_fix: str | None = None,
    ) -> None:
        """Log an issue encountered during the run.

        Args:
            severity: Severity level (high/medium/low).
            source: Source affected.
            issue: Description of the issue.
            suggested_fix: Suggested fix if any.
        """
        entry: dict[str, Any] = {
            "severity": severity,
            "source": source,
            "issue": issue,
        }
        if suggested_fix:
            entry["suggested_fix"] = suggested_fix
        self._issues.append(entry)

    def save(self) -> Path:
        """Save the run log to disk.

        Returns:
            Path to the saved log file.
        """
        # Ensure runs directory exists
        self._runs_dir.mkdir(parents=True, exist_ok=True)

        # Calculate totals
        totals = {
            "total_keyword_matched": len(self._keyword_matched),
            "classified_relevant": len(self._classified_relevant),
            "classified_not_relevant": len(self._classified_not_relevant),
        }

        # Build log data
        data: dict[str, Any] = {
            "run_id": self._run_id,
            "timestamp": self._timestamp,
            "config": self._config,
            "results": {
                "sources": self._source_stats,
                "totals": totals,
            },
            "articles": {
                "keyword_matched": self._keyword_matched,
                "classified_relevant": self._classified_relevant,
                "classified_not_relevant": self._classified_not_relevant,
            },
        }

        if self._issues:
            data["issues"] = self._issues

        # Save to file
        file_path = self._runs_dir / f"{self._run_id}.json"
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved run log to {file_path}")
        except OSError as e:
            logger.error(f"Failed to save run log: {e}")

        return file_path


def create_run_logger(runs_dir: Path | None = None) -> RunLogger:
    """Create a run logger instance.

    Args:
        runs_dir: Directory to store run logs. Defaults to data/runs/.

    Returns:
        RunLogger instance.
    """
    if runs_dir is None:
        runs_dir = Path("data/runs")
    return RunLogger(runs_dir)

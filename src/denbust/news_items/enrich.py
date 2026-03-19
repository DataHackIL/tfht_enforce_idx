"""LLM-backed metadata enrichment for news_items records."""

from __future__ import annotations

import json
import logging
import re

import anthropic
from anthropic.types import TextBlock

from denbust.data_models import UnifiedItem
from denbust.models.policies import PrivacyRisk
from denbust.news_items.models import NewsItemEnrichment
from denbust.news_items.normalize import deduplicate_strings

logger = logging.getLogger(__name__)

_SUMMARY_PROMPT = """You are enriching a metadata-only public dataset of Israeli news items.

Given a news item title, snippet, category, and source list, produce strict factual metadata.

Rules:
- summary_one_sentence must be one factual sentence in Hebrew.
- Do not quote the article directly unless unavoidable.
- Do not use sensational or rhetorical language.
- organizations_mentioned should be a short list of organizations explicitly named.
- topic_tags should be short English kebab-case tags useful for dataset filtering.
- privacy_risk_level must be one of:
  low | medium | high | sensitive_sexual_offence | minor_involved | victim_identifying_risk

Item:
כותרת: {headline}
תקציר: {summary}
קטגוריה: {category}
תת-קטגוריה: {sub_category}
מקורות: {sources}

Return JSON only:
{{
  "summary_one_sentence": "...",
  "geography_region": null,
  "geography_city": null,
  "organizations_mentioned": [],
  "topic_tags": [],
  "privacy_risk_level": "low"
}}
"""


def sanitize_summary_one_sentence(candidate: str, fallback: str) -> str:
    """Normalize a summary candidate into a single factual sentence."""
    text = " ".join(candidate.replace("\n", " ").split()).strip().strip("\"'“”")
    if not text:
        text = " ".join(fallback.replace("\n", " ").split()).strip()
    if not text:
        return "אין תקציר זמין."

    parts = re.split(r"(?<=[.!?])\s+", text)
    sentence = parts[0].strip()
    sentence = re.sub(r"\s+", " ", sentence).strip(" -")
    if not sentence:
        sentence = text[:220].strip()
    if sentence and sentence[-1] not in ".!?":
        sentence += "."
    return sentence


def fallback_enrichment(item: UnifiedItem) -> NewsItemEnrichment:
    """Build a deterministic fallback enrichment when the LLM path fails."""
    fallback_summary = sanitize_summary_one_sentence(item.summary or item.headline, item.headline)
    base_tags = [item.category.value.replace("_", "-")]
    if item.sub_category:
        base_tags.append(item.sub_category.value.replace("_", "-"))
    return NewsItemEnrichment(
        summary_one_sentence=fallback_summary,
        topic_tags=deduplicate_strings(base_tags),
        privacy_risk_level=PrivacyRisk.LOW,
    )


class NewsItemEnricher:
    """LLM-backed enrichment for public metadata rows."""

    def __init__(self, *, api_key: str, model: str) -> None:
        self._client = anthropic.Anthropic(api_key=api_key)
        self._model = model

    @property
    def model_name(self) -> str:
        """Expose the model used for summary generation."""
        return self._model

    async def enrich(self, item: UnifiedItem) -> NewsItemEnrichment:
        """Enrich a unified item with summary and metadata."""
        prompt = _SUMMARY_PROMPT.format(
            headline=item.headline,
            summary=item.summary[:500],
            category=item.category.value,
            sub_category=item.sub_category.value if item.sub_category else "",
            sources=", ".join(source.source_name for source in item.sources),
        )

        try:
            response = self._client.messages.create(
                model=self._model,
                max_tokens=400,
                messages=[{"role": "user", "content": prompt}],
            )
            text = ""
            if response.content:
                first_block = response.content[0]
                if isinstance(first_block, TextBlock):
                    text = first_block.text
            return self._parse_response(text, item)
        except Exception as exc:
            logger.warning("Failed to enrich news item '%s': %s", item.headline, exc)
            return fallback_enrichment(item)

    def _parse_response(self, text: str, item: UnifiedItem) -> NewsItemEnrichment:
        """Parse LLM JSON output into the typed enrichment model."""
        try:
            payload = json.loads(text.strip())
        except json.JSONDecodeError:
            logger.warning("Failed to parse news-item enrichment payload: %s", text)
            return fallback_enrichment(item)

        summary = sanitize_summary_one_sentence(
            str(payload.get("summary_one_sentence", "")),
            item.summary or item.headline,
        )
        try:
            privacy_risk = PrivacyRisk(str(payload.get("privacy_risk_level", PrivacyRisk.LOW)))
        except ValueError:
            privacy_risk = PrivacyRisk.LOW

        return NewsItemEnrichment(
            summary_one_sentence=summary,
            geography_region=payload.get("geography_region"),
            geography_city=payload.get("geography_city"),
            organizations_mentioned=deduplicate_strings(payload.get("organizations_mentioned", [])),
            topic_tags=deduplicate_strings(payload.get("topic_tags", [])),
            privacy_risk_level=privacy_risk,
        )

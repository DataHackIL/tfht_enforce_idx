import {
  applyFilters,
  errorResponse,
  jsonResponse,
  normalizeCandidate,
  normalizeNewsItem,
  parseLimit,
  parseOffset,
  requireReviewer,
  sortItems,
  supabaseFetch,
} from "../_shared.js";

const NEWS_SELECT = [
  "id",
  "source_name",
  "source_domain",
  "url",
  "canonical_url",
  "publication_datetime",
  "retrieval_datetime",
  "title",
  "category",
  "sub_category",
  "summary_one_sentence",
  "organizations_mentioned",
  "topic_tags",
  "rights_class",
  "privacy_risk_level",
  "review_status",
  "publication_status",
  "event_candidate_ids",
  "classification_confidence",
  "content_basis",
  "record_confidence",
  "taxonomy_version",
  "taxonomy_category_id",
  "taxonomy_subcategory_id",
  "index_relevant",
  "manual_status",
  "manual_city",
  "manual_event_label",
  "manually_reviewed",
  "annotation_notes",
  "created_at",
  "updated_at",
].join(",");

const CANDIDATE_SELECT = [
  "candidate_id",
  "canonical_url",
  "current_url",
  "domain",
  "first_seen_at",
  "last_seen_at",
  "candidate_status",
  "scrape_attempt_count",
  "last_scrape_error_code",
  "last_scrape_error_message",
  "content_basis",
  "needs_review",
  "metadata",
  "titles",
  "snippets",
  "discovered_via",
  "discovery_queries",
  "source_hints",
].join(",");

export async function onRequestGet({ request, env }) {
  const reviewer = requireReviewer(request, env);
  if (!reviewer.ok) return reviewer.response;

  try {
    const url = new URL(request.url);
    const limit = parseLimit(url.searchParams.get("limit"));
    const offset = parseOffset(url.searchParams.get("offset"));
    const fetchLimit = Math.min(Math.max(limit * 2, 500), 1000);

    const newsParams = new URLSearchParams({
      select: NEWS_SELECT,
      order: "publication_datetime.desc.nullslast,created_at.desc",
      limit: String(Math.min(fetchLimit, 500)),
    });
    const candidateParams = new URLSearchParams({
      select: CANDIDATE_SELECT,
      order: "last_seen_at.desc.nullslast,first_seen_at.desc",
      limit: String(fetchLimit),
    });

    const [newsResult, candidateResult] = await Promise.all([
      supabaseFetch(env, "news_items", { params: newsParams }),
      supabaseFetch(env, "persistent_candidates", { params: candidateParams }),
    ]);

    const newsItems = Array.isArray(newsResult.payload) ? newsResult.payload.map(normalizeNewsItem) : [];
    const linkedCandidateIds = new Set(
      (Array.isArray(newsResult.payload) ? newsResult.payload : [])
        .flatMap((row) => (Array.isArray(row.event_candidate_ids) ? row.event_candidate_ids : []))
        .filter(Boolean),
    );
    const candidates = (Array.isArray(candidateResult.payload) ? candidateResult.payload : [])
      .filter((row) => !linkedCandidateIds.has(row.candidate_id))
      .map(normalizeCandidate);

    const allItems = sortItems(applyFilters([...newsItems, ...candidates], url.searchParams));
    const pageItems = allItems.slice(offset, offset + limit);

    return jsonResponse({
      reviewer: reviewer.email,
      offset,
      limit,
      returned: pageItems.length,
      totalFiltered: allItems.length,
      fetched: {
        newsItems: newsItems.length,
        candidates: candidates.length,
        newsTotal: newsResult.count,
        candidateTotal: candidateResult.count,
      },
      items: pageItems,
      nextOffset: offset + pageItems.length < allItems.length ? offset + pageItems.length : null,
    });
  } catch (error) {
    return errorResponse("Could not load review candidates.", 500, error.message);
  }
}

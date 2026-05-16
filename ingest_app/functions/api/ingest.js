import {
  requireReviewer,
  supabaseFetch,
  findTaxonomyPair,
  decisionToStatuses,
  domainFromUrl,
  generateId,
  jsonResponse,
  errorResponse,
  TAXONOMY,
} from "../_shared.js";

async function fetchUrlMetadata(url) {
  try {
    const res = await fetch(url, {
      signal: AbortSignal.timeout(6000),
      headers: { "User-Agent": "TFHT-Ingest-Bot/1.0", Accept: "text/html" },
      redirect: "follow",
    });
    if (!res.ok) return { title: "", snippet: "", fetchStatus: "failed" };
    const html = await res.text();
    const titleMatch = html.match(/<title[^>]*>([^<]{1,500})<\/title>/i);
    const descMatch =
      html.match(/<meta[^>]+name=["']description["'][^>]+content=["']([^"']{1,1000})["']/i) ||
      html.match(/<meta[^>]+content=["']([^"']{1,1000})["'][^>]+name=["']description["']/i) ||
      html.match(/<meta[^>]+property=["']og:description["'][^>]+content=["']([^"']{1,1000})["']/i);
    return {
      title: titleMatch ? titleMatch[1].trim() : "",
      snippet: descMatch ? descMatch[1].trim() : "",
      fetchStatus: "ok",
    };
  } catch {
    return { title: "", snippet: "", fetchStatus: "timeout" };
  }
}

export async function onRequestPost({ request, env }) {
  const reviewer = requireReviewer(request, env);
  if (!reviewer.ok) return reviewer.response;

  let body;
  try {
    body = await request.json();
  } catch {
    return errorResponse("Invalid JSON body.", 400);
  }

  const rawUrls = Array.isArray(body.urls) ? body.urls : [];
  const urls = rawUrls
    .map((u) => String(u).trim())
    .filter(Boolean)
    .slice(0, 50);
  if (urls.length === 0) return errorResponse("At least one URL is required.", 400);

  const decision = String(body.decision || "internal_only");
  if (!["include", "exclude", "needs_review", "internal_only"].includes(decision)) {
    return errorResponse("Invalid decision.", 400);
  }

  const taxonomyCategoryId = String(body.taxonomyCategoryId || "");
  const taxonomySubcategoryId = String(body.taxonomySubcategoryId || "");
  const taxonomyPair = findTaxonomyPair(taxonomyCategoryId, taxonomySubcategoryId);
  if (decision === "include" && !taxonomyPair) {
    return errorResponse("A valid taxonomy category and subcategory are required for include decisions.", 400);
  }

  const validCategoryId = taxonomyPair ? taxonomyCategoryId : "";
  const validSubcategoryId = taxonomyPair ? taxonomySubcategoryId : "";
  const indexRelevant = decision === "include" ? Boolean(body.indexRelevant) : false;
  const tags = Array.isArray(body.tags)
    ? body.tags
        .map((tag) => String(tag).trim())
        .filter(Boolean)
        .slice(0, 20)
    : [];
  const notes = String(body.notes || "").slice(0, 5000);
  const manualEventLabel = String(body.manualEventLabel || "").slice(0, 500);
  const manualCity = String(body.manualCity || "").slice(0, 200);
  const statuses = decisionToStatuses(decision);
  const now = new Date().toISOString();

  const results = await Promise.all(
    urls.map(async (url) => {
      const { title, snippet, fetchStatus } = await fetchUrlMetadata(url);
      const id = generateId();
      const domain = domainFromUrl(url);
      const row = {
        id,
        url,
        canonical_url: url,
        source_name: domain,
        source_domain: domain,
        title: title || url,
        summary_one_sentence: snippet || "",
        publication_status: statuses.publication_status,
        review_status: statuses.review_status,
        suppression_reason: statuses.suppression_reason,
        rights_class: "metadata_only",
        takedown_status: "none",
        index_relevant: indexRelevant,
        taxonomy_category_id: validCategoryId || null,
        taxonomy_subcategory_id: validSubcategoryId || null,
        taxonomy_version: validCategoryId ? TAXONOMY.version : null,
        manual_event_label: manualEventLabel || null,
        manual_status: manualEventLabel || null,
        manual_city: manualCity || null,
        geography_city: manualCity || null,
        topic_tags: tags.length > 0 ? tags : null,
        annotation_notes: notes || null,
        reviewer: reviewer.email,
        reviewed_at: now,
        manually_reviewed: true,
        manually_overridden: true,
        annotation_source: "ingest_app",
        content_basis: "candidate_only",
      };
      try {
        const result = await supabaseFetch(env, "news_items", {
          method: "POST",
          body: row,
        });
        const savedRow = Array.isArray(result.payload) ? result.payload[0] : result.payload;
        return { url, id, title: row.title, fetchStatus, status: "ingested", savedId: savedRow?.id || id };
      } catch (err) {
        return { url, id, title: row.title, fetchStatus, status: "error", error: err.message };
      }
    }),
  );

  return jsonResponse({
    ingested: results.filter((r) => r.status === "ingested").length,
    total: urls.length,
    results,
  });
}

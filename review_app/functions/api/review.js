import {
  errorResponse,
  jsonResponse,
  parseReviewBody,
  requireReviewer,
  supabaseFetch,
} from "../_shared.js";

export async function onRequestPost({ request, env }) {
  const reviewer = requireReviewer(request, env);
  if (!reviewer.ok) return reviewer.response;

  try {
    const review = parseReviewBody(await request.json());
    if (review.itemType === "news_item") {
      const result = await updateNewsItem(env, review, reviewer.email);
      return jsonResponse({ ok: true, itemType: review.itemType, id: review.id, row: result });
    }
    const result = await updateCandidate(env, review, reviewer.email);
    return jsonResponse({ ok: true, itemType: review.itemType, id: review.id, row: result });
  } catch (error) {
    return errorResponse("Could not save review.", 400, error.message);
  }
}

async function updateNewsItem(env, review, reviewerEmail) {
  const now = new Date().toISOString();
  const patch = {
    updated_at: now,
    reviewed_at: now,
    reviewer: reviewerEmail,
    manually_reviewed: true,
    manually_overridden: true,
    annotation_source: "tfht_review_workbench",
    annotation_notes: review.notes,
    manual_event_label: review.manualEventLabel || null,
    manual_city: review.manualCity || null,
    manual_status: review.decision,
    taxonomy_version: review.taxonomyVersion || null,
    taxonomy_category_id: review.taxonomyCategoryId || null,
    taxonomy_subcategory_id: review.taxonomySubcategoryId || null,
    index_relevant: review.decision === "include" ? review.indexRelevant : false,
  };
  if (review.tags.length > 0) {
    patch.topic_tags = review.tags;
  }

  if (review.decision === "include") {
    patch.publication_status = "approved";
    patch.review_status = "none";
    patch.suppression_reason = null;
  } else if (review.decision === "exclude") {
    patch.publication_status = "suppressed";
    patch.review_status = "none";
    patch.suppression_reason = "review_app_rejected";
  } else if (review.decision === "needs_review") {
    patch.publication_status = "internal_only";
    patch.review_status = "needs_fact_review";
    patch.suppression_reason = "review_app_needs_review";
  } else {
    patch.publication_status = "internal_only";
    patch.review_status = "none";
    patch.suppression_reason = "review_app_internal_only";
  }

  const params = new URLSearchParams({ id: `eq.${review.id}` });
  const response = await supabaseFetch(env, "news_items", { method: "PATCH", params, body: patch });
  return Array.isArray(response.payload) ? response.payload[0] : response.payload;
}

async function updateCandidate(env, review, reviewerEmail) {
  const readParams = new URLSearchParams({
    select: "candidate_id,metadata",
    candidate_id: `eq.${review.id}`,
    limit: "1",
  });
  const existing = await supabaseFetch(env, "persistent_candidates", { params: readParams });
  const row = Array.isArray(existing.payload) ? existing.payload[0] : null;
  if (!row) throw new Error("Candidate was not found.");

  const now = new Date().toISOString();
  const metadata = {
    ...(row.metadata || {}),
    review_app_annotation: {
      reviewedAt: now,
      reviewer: reviewerEmail,
      decision: review.decision,
      taxonomyVersion: review.taxonomyVersion,
      taxonomyCategoryId: review.taxonomyCategoryId,
      taxonomySubcategoryId: review.taxonomySubcategoryId,
      indexRelevant: review.decision === "include" ? review.indexRelevant : false,
      notes: review.notes,
      manualEventLabel: review.manualEventLabel,
      manualCity: review.manualCity,
      tags: review.tags,
    },
  };

  const patch = {
    metadata,
    needs_review: review.decision === "needs_review",
  };
  const params = new URLSearchParams({ candidate_id: `eq.${review.id}` });
  const response = await supabaseFetch(env, "persistent_candidates", { method: "PATCH", params, body: patch });
  return Array.isArray(response.payload) ? response.payload[0] : response.payload;
}

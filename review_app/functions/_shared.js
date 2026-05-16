const DEFAULT_LIMIT = 250;
const MAX_LIMIT = 1000;

export const TAXONOMY = {
  version: "1",
  categories: [
    {
      id: "human_trafficking",
      labelHe: "סחר בבני אדם",
      labelEn: "Human trafficking",
      subcategories: [
        {
          id: "trafficking_sexual_exploitation",
          labelHe: "סחר למטרת ניצול מיני (זנות)",
          labelEn: "Trafficking for sexual exploitation",
          indexRelevant: true,
        },
        {
          id: "trafficking_forced_marriage",
          labelHe: "סחר למטרת נישואין בכפייה",
          labelEn: "Trafficking for forced marriage",
          indexRelevant: true,
        },
        {
          id: "trafficking_forced_labor",
          labelHe: "סחר למטרת עבודת כפייה",
          labelEn: "Trafficking for forced labor",
          indexRelevant: false,
        },
        {
          id: "trafficking_organ_harvesting",
          labelHe: "סחר למטרת נטילת איברים",
          labelEn: "Trafficking for organ harvesting",
          indexRelevant: false,
        },
        {
          id: "trafficking_cross_border_prostitution",
          labelHe: "הבאת אדם למדינה אחרת לשם העיסוק בזנות",
          labelEn: "Cross-border prostitution trafficking",
          indexRelevant: true,
        },
        {
          id: "trafficking_slavery_conditions",
          labelHe: "החזקה בתנאי עבדות",
          labelEn: "Holding in slavery conditions",
          indexRelevant: true,
        },
        {
          id: "sexual_slavery",
          labelHe: "עבדות מינית",
          labelEn: "Sexual slavery",
          indexRelevant: true,
        },
        {
          id: "trafficking_women",
          labelHe: "סחר בנשים",
          labelEn: "Women trafficking",
          indexRelevant: true,
        },
      ],
    },
    {
      id: "pimping_prostitution",
      labelHe: "סרסור וזנות",
      labelEn: "Pimping and prostitution",
      subcategories: [
        { id: "pimping", labelHe: "סרסור", labelEn: "Pimping", indexRelevant: true },
        {
          id: "bringing_into_prostitution",
          labelHe: "הבאת אדם לידי זנות",
          labelEn: "Bringing a person into prostitution",
          indexRelevant: true,
        },
        {
          id: "soliciting_prostitution",
          labelHe: "שידול לזנות",
          labelEn: "Soliciting prostitution",
          indexRelevant: true,
        },
        {
          id: "women_testimonies",
          labelHe: "עדויות של נשים בזנות",
          labelEn: "Testimonies of women in prostitution",
          indexRelevant: false,
        },
        {
          id: "phenomenon_coverage",
          labelHe: "סיקור תופעת הזנות",
          labelEn: "Coverage of prostitution as a phenomenon",
          indexRelevant: false,
        },
        {
          id: "online_prostitution",
          labelHe: "זנות מקוונת",
          labelEn: "Online prostitution",
          indexRelevant: true,
        },
        {
          id: "nordic_model_law",
          labelHe: "חוק איסור צריכת זנות / המודל הנורדי",
          labelEn: "Nordic model / prohibition on buying prostitution",
          indexRelevant: false,
        },
      ],
    },
    {
      id: "brothels",
      labelHe: "בתי בושת",
      labelEn: "Brothels",
      subcategories: [
        {
          id: "keeping_brothel",
          labelHe: "החזקת מקום לשם זנות",
          labelEn: "Keeping a place for prostitution",
          indexRelevant: true,
        },
        {
          id: "renting_brothel",
          labelHe: "השכרת מקום לשם זנות",
          labelEn: "Renting a place for prostitution",
          indexRelevant: true,
        },
        {
          id: "advertising_prostitution",
          labelHe: "פרסום זנות",
          labelEn: "Advertising prostitution",
          indexRelevant: true,
        },
        {
          id: "client_fine",
          labelHe: "קנס בגין צריכת זנות",
          labelEn: "Fine for buying prostitution",
          indexRelevant: true,
        },
        {
          id: "administrative_closure",
          labelHe: "סגירה מנהלית / צו מנהלי לפי חוק הגבלת שימוש במקום לשם ביצוע עבירה",
          labelEn: "Administrative closure order",
          indexRelevant: true,
        },
        {
          id: "closure_appeal",
          labelHe: "ערעור על צו מנהלי",
          labelEn: "Appeal on closure order",
          indexRelevant: true,
        },
        {
          id: "brothel_indictment",
          labelHe: "כתב אישום על החזקת/השכרת מקום לשם זנות",
          labelEn: "Indictment for keeping or renting a brothel",
          indexRelevant: true,
        },
      ],
    },
  ],
};

const STRONG_TERMS = [
  "סחר",
  "סרסור",
  "זנות",
  "בית בושת",
  "בתי בושת",
  "ניצול מיני",
  "עבדות",
  "צו סגירה",
  "החזקת מקום",
  "השכרת מקום",
  "פרסום זנות",
  "צריכת זנות",
];

const WEAK_NOISE_TERMS = [
  "רכילות",
  "סלב",
  "אינסטגרם",
  "וידאו",
  "פודקאסט",
  "ספורט",
  "מניות",
  "נדלן",
];

export function jsonResponse(data, init = {}) {
  const status = init.status || 200;
  const headers = new Headers(init.headers || {});
  headers.set("content-type", "application/json; charset=utf-8");
  headers.set("cache-control", "no-store");
  return new Response(JSON.stringify(data, null, 2), { status, headers });
}

export function errorResponse(message, status = 500, details = undefined) {
  return jsonResponse({ error: message, details }, { status });
}

export function getAccessEmail(request, env) {
  const cfEmail = (
    request.headers.get("cf-access-authenticated-user-email") ||
    request.headers.get("CF-Access-Authenticated-User-Email") ||
    ""
  ).trim();
  if (cfEmail) return cfEmail;
  if (env.TFHT_REVIEW_DEV_MODE && env.TFHT_REVIEW_DEV_EMAIL) {
    return String(env.TFHT_REVIEW_DEV_EMAIL).trim();
  }
  return "";
}

export function parseAllowedEmails(env) {
  return String(env.TFHT_REVIEW_ALLOWED_EMAILS || "")
    .split(",")
    .map((email) => email.trim().toLowerCase())
    .filter(Boolean);
}

export function requireReviewer(request, env) {
  const email = getAccessEmail(request, env).toLowerCase();
  const allowed = parseAllowedEmails(env);
  if (!email) {
    return { ok: false, email: "", response: errorResponse("Cloudflare Access email is missing.", 401) };
  }
  if (allowed.length === 0 || !allowed.includes(email)) {
    return { ok: false, email, response: errorResponse("Reviewer email is not allowed.", 403) };
  }
  return { ok: true, email, response: null };
}

export function getSupabaseConfig(env) {
  const baseUrl = String(env.DENBUST_SUPABASE_URL || "").replace(/\/$/, "");
  const serviceRoleKey = env.DENBUST_SUPABASE_SERVICE_ROLE_KEY;
  if (!baseUrl || !serviceRoleKey) {
    throw new Error("Missing DENBUST_SUPABASE_URL or DENBUST_SUPABASE_SERVICE_ROLE_KEY.");
  }
  return { baseUrl, serviceRoleKey };
}

export async function supabaseFetch(env, table, { method = "GET", params, body } = {}) {
  const { baseUrl, serviceRoleKey } = getSupabaseConfig(env);
  const url = new URL(`${baseUrl}/rest/v1/${table}`);
  if (params) {
    for (const [key, value] of params.entries()) {
      url.searchParams.append(key, value);
    }
  }

  const response = await fetch(url, {
    method,
    headers: {
      apikey: serviceRoleKey,
      authorization: `Bearer ${serviceRoleKey}`,
      "content-type": "application/json",
      prefer: method === "GET" ? "count=exact" : "return=representation",
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });

  const text = await response.text();
  let payload = null;
  if (text) {
    try {
      payload = JSON.parse(text);
    } catch {
      payload = text;
    }
  }
  if (!response.ok) {
    throw new Error(`Supabase ${method} ${table} failed: ${response.status} ${JSON.stringify(payload)}`);
  }
  return { payload, count: parseContentRangeCount(response.headers.get("content-range")) };
}

export function parseLimit(value) {
  const parsed = Number.parseInt(value || "", 10);
  if (Number.isNaN(parsed) || parsed <= 0) return DEFAULT_LIMIT;
  return Math.min(parsed, MAX_LIMIT);
}

export function parseOffset(value) {
  const parsed = Number.parseInt(value || "", 10);
  if (Number.isNaN(parsed) || parsed < 0) return 0;
  return parsed;
}

export function normalizeNewsItem(row) {
  const title = row.title || row.summary_one_sentence || row.url || "Untitled news item";
  const text = [title, row.summary_one_sentence, row.category, row.sub_category, row.source_name]
    .filter(Boolean)
    .join(" ");
  const { score, reasons } = scoreItem({
    text,
    confidence: row.classification_confidence || row.record_confidence,
    status: row.publication_status,
    basis: row.content_basis,
    needsReview: row.review_status && row.review_status !== "none",
    sourceHints: [],
    discoveryHits: [],
  });

  return {
    itemType: "news_item",
    id: row.id,
    url: row.canonical_url || row.url,
    title,
    snippet: row.summary_one_sentence || "",
    sourceName: row.source_name || row.source_domain || "",
    sourceDomain: row.source_domain || domainFromUrl(row.canonical_url || row.url),
    publicationDatetime: row.publication_datetime || row.retrieval_datetime || row.created_at,
    confidence: row.classification_confidence || row.record_confidence || null,
    taxonomyCategoryId: row.taxonomy_category_id || "",
    taxonomySubcategoryId: row.taxonomy_subcategory_id || "",
    taxonomyVersion: row.taxonomy_version || "",
    indexRelevant: row.index_relevant,
    reviewStatus: row.review_status || "",
    publicationStatus: row.publication_status || "",
    contentBasis: row.content_basis || "",
    candidateStatus: "",
    score,
    scoreReasons: reasons,
    metadata: {
      rightsClass: row.rights_class || "",
      privacyRiskLevel: row.privacy_risk_level || "",
      manualStatus: row.manual_status || "",
      manualEventLabel: row.manual_event_label || "",
      manualCity: row.manual_city || "",
      manuallyReviewed: Boolean(row.manually_reviewed),
      annotationNotes: row.annotation_notes || "",
      organizations: row.organizations_mentioned || [],
      topicTags: row.topic_tags || [],
    },
  };
}

export function normalizeCandidate(row) {
  const titles = arrayValue(row.titles);
  const snippets = arrayValue(row.snippets);
  const sourceHints = arrayValue(row.source_hints);
  const discoveryQueries = arrayValue(row.discovery_queries);
  const latestMetadata = row.metadata?.latest_discovery_metadata || {};
  const title = titles[0] || latestMetadata.result_title || row.canonical_url || "Untitled candidate";
  const snippet = snippets[0] || latestMetadata.result_snippet || "";
  const text = [title, snippet, row.domain].filter(Boolean).join(" ");
  const discoveryHits = arrayValue(row.discovered_via);
  const { score, reasons } = scoreItem({
    text,
    confidence: latestMetadata.classification_confidence || null,
    status: row.candidate_status,
    basis: row.content_basis,
    needsReview: Boolean(row.needs_review),
    sourceHints,
    discoveryHits,
  });

  return {
    itemType: "candidate",
    id: row.candidate_id,
    url: row.canonical_url || row.current_url,
    title,
    snippet,
    sourceName: row.domain || "",
    sourceDomain: row.domain || domainFromUrl(row.canonical_url || row.current_url),
    publicationDatetime: latestMetadata.result_published_date || row.first_seen_at,
    confidence: latestMetadata.classification_confidence || null,
    taxonomyCategoryId: row.metadata?.review_app_annotation?.taxonomyCategoryId || "",
    taxonomySubcategoryId: row.metadata?.review_app_annotation?.taxonomySubcategoryId || "",
    taxonomyVersion: row.metadata?.review_app_annotation?.taxonomyVersion || "",
    indexRelevant: row.metadata?.review_app_annotation?.indexRelevant ?? null,
    reviewStatus: row.needs_review ? "needs_review" : "",
    publicationStatus: row.metadata?.review_app_annotation?.decision || "",
    contentBasis: row.content_basis || "",
    candidateStatus: row.candidate_status || "",
    score,
    scoreReasons: reasons,
    metadata: {
      annotation: row.metadata?.review_app_annotation || null,
      sourceHints,
      discoveryQueries,
      discoveredVia: discoveryHits,
      scrapeAttemptCount: row.scrape_attempt_count || 0,
      lastScrapeErrorCode: row.last_scrape_error_code || "",
      lastScrapeErrorMessage: row.last_scrape_error_message || "",
      latestDiscoveryMetadata: latestMetadata,
    },
  };
}

export function applyFilters(items, searchParams) {
  const status = searchParams.get("status") || "pending";
  const query = normalizeSearch(searchParams.get("q") || "");
  const domain = normalizeSearch(searchParams.get("domain") || "");
  const source = normalizeSearch(searchParams.get("source") || "");

  return items.filter((item) => {
    if (status === "pending" && isCompleted(item)) return false;
    if (status === "approved" && item.publicationStatus !== "approved" && item.publicationStatus !== "include") return false;
    if (status === "suppressed" && item.publicationStatus !== "suppressed" && item.publicationStatus !== "exclude") return false;
    if (status === "candidate_only" && item.itemType !== "candidate") return false;
    if (status === "news_items" && item.itemType !== "news_item") return false;

    const haystack = normalizeSearch(
      [
        item.title,
        item.snippet,
        item.sourceName,
        item.sourceDomain,
        item.url,
        item.taxonomyCategoryId,
        item.taxonomySubcategoryId,
      ].join(" "),
    );
    if (query && !haystack.includes(query)) return false;
    if (domain && !normalizeSearch(item.sourceDomain).includes(domain)) return false;
    if (source && !normalizeSearch(item.sourceName).includes(source)) return false;
    return true;
  });
}

export function sortItems(items) {
  return items.sort((a, b) => {
    if (b.score !== a.score) return b.score - a.score;
    return String(b.publicationDatetime || "").localeCompare(String(a.publicationDatetime || ""));
  });
}

export function findTaxonomyPair(categoryId, subcategoryId) {
  const category = TAXONOMY.categories.find((entry) => entry.id === categoryId);
  if (!category) return null;
  const subcategory = category.subcategories.find((entry) => entry.id === subcategoryId);
  if (!subcategory) return null;
  return { category, subcategory };
}

export function parseReviewBody(rawBody) {
  const body = rawBody && typeof rawBody === "object" ? rawBody : {};
  const decision = cleanString(body.decision);
  const itemType = cleanString(body.itemType);
  const id = cleanString(body.id);
  if (!["news_item", "candidate"].includes(itemType)) {
    throw new Error("itemType must be news_item or candidate.");
  }
  if (!id) throw new Error("id is required.");
  if (!["include", "exclude", "needs_review", "internal_only"].includes(decision)) {
    throw new Error("decision must be include, exclude, needs_review, or internal_only.");
  }

  const taxonomyCategoryId = cleanString(body.taxonomyCategoryId);
  const taxonomySubcategoryId = cleanString(body.taxonomySubcategoryId);
  const taxonomyPair = findTaxonomyPair(taxonomyCategoryId, taxonomySubcategoryId);
  if (decision === "include" && !taxonomyPair) {
    throw new Error("A valid taxonomy category and subcategory are required for include decisions.");
  }
  const validCategoryId = taxonomyPair ? taxonomyCategoryId : "";
  const validSubcategoryId = taxonomyPair ? taxonomySubcategoryId : "";

  return {
    itemType,
    id,
    decision,
    taxonomyCategoryId: validCategoryId,
    taxonomySubcategoryId: validSubcategoryId,
    taxonomyVersion: validCategoryId && validSubcategoryId ? TAXONOMY.version : "",
    indexRelevant: Boolean(body.indexRelevant),
    notes: cleanString(body.notes),
    manualEventLabel: cleanString(body.manualEventLabel),
    manualCity: cleanString(body.manualCity),
    tags: Array.isArray(body.tags) ? body.tags.map(cleanString).filter(Boolean).slice(0, 20) : [],
  };
}

function scoreItem({ text, confidence, status, basis, needsReview, sourceHints, discoveryHits }) {
  let score = 0;
  const reasons = [];
  const normalized = normalizeSearch(text);

  for (const term of STRONG_TERMS) {
    if (normalized.includes(normalizeSearch(term))) {
      score += 8;
      reasons.push(`term: ${term}`);
    }
  }

  for (const term of WEAK_NOISE_TERMS) {
    if (normalized.includes(normalizeSearch(term))) {
      score -= 5;
      reasons.push(`noise: ${term}`);
    }
  }

  const resolvedConfidence = resolveConfidence(confidence);
  if (resolvedConfidence !== null) {
    const confidenceScore = Math.round(resolvedConfidence * 20);
    score += confidenceScore;
    reasons.push(`classifier confidence +${confidenceScore}`);
  }
  if (basis === "source_article" || basis === "article") {
    score += 8;
    reasons.push("article content");
  }
  if (needsReview) {
    score += 6;
    reasons.push("needs review");
  }
  if (status === "approved" || status === "include") {
    score += 20;
    reasons.push("already approved");
  }
  if (status === "suppressed" || status === "exclude") {
    score -= 20;
    reasons.push("already excluded");
  }
  if (sourceHints.length > 0) {
    score += Math.min(sourceHints.length * 2, 8);
    reasons.push("source hints");
  }
  if (discoveryHits.length > 1) {
    score += Math.min(discoveryHits.length * 2, 10);
    reasons.push("repeated discovery");
  }
  return { score, reasons: [...new Set(reasons)].slice(0, 8) };
}

function resolveConfidence(confidence) {
  if (confidence === null || confidence === undefined || confidence === "") return null;
  const CONFIDENCE_MAP = { high: 0.9, medium: 0.6, low: 0.3 };
  const lc = String(confidence).toLowerCase();
  if (lc in CONFIDENCE_MAP) return CONFIDENCE_MAP[lc];
  const numeric = Number(confidence);
  return Number.isFinite(numeric) ? numeric : null;
}

function isCompleted(item) {
  // A candidate whose pipeline already decided it is off-topic (unsupported_source)
  // is treated as complete even without an explicit review annotation.
  if (item.candidateStatus === "unsupported_source") return true;
  return ["approved", "suppressed", "include", "exclude", "internal_only"].includes(item.publicationStatus);
}

function parseContentRangeCount(value) {
  if (!value || !value.includes("/")) return null;
  const count = value.split("/").pop();
  if (!count || count === "*") return null;
  const parsed = Number.parseInt(count, 10);
  return Number.isNaN(parsed) ? null : parsed;
}

function domainFromUrl(value) {
  try {
    return new URL(value).hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
}

function arrayValue(value) {
  return Array.isArray(value) ? value.filter(Boolean) : [];
}

function normalizeSearch(value) {
  return String(value || "").trim().toLowerCase();
}

function cleanString(value) {
  return String(value || "").trim();
}

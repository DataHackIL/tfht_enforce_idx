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
  // When no CF Access email is present the app operates in open-access mode —
  // the allowlist is an additional restriction applied on top of CF Access,
  // not a standalone auth gate.
  if (!email) {
    return { ok: true, email: "anonymous", response: null };
  }
  if (allowed.length > 0 && !allowed.includes(email)) {
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

export function findTaxonomyPair(categoryId, subcategoryId) {
  const category = TAXONOMY.categories.find((entry) => entry.id === categoryId);
  if (!category) return null;
  const subcategory = category.subcategories.find((entry) => entry.id === subcategoryId);
  if (!subcategory) return null;
  return { category, subcategory };
}

export function decisionToStatuses(decision) {
  switch (decision) {
    case "include": return { publication_status: "approved", review_status: "none", suppression_reason: null };
    case "exclude": return { publication_status: "suppressed", review_status: "none", suppression_reason: "review_app_rejected" };
    case "needs_review": return { publication_status: "internal_only", review_status: "needs_fact_review", suppression_reason: null };
    case "internal_only": return { publication_status: "internal_only", review_status: "none", suppression_reason: "ingest_app_internal_only" };
    default: return { publication_status: "internal_only", review_status: "none", suppression_reason: null };
  }
}

export function domainFromUrl(url) {
  try { return new URL(url).hostname.replace(/^www\./, ""); } catch { return ""; }
}

export function generateId() {
  return crypto.randomUUID();
}

function parseContentRangeCount(value) {
  if (!value || !value.includes("/")) return null;
  const count = value.split("/").pop();
  if (!count || count === "*") return null;
  const parsed = Number.parseInt(count, 10);
  return Number.isNaN(parsed) ? null : parsed;
}

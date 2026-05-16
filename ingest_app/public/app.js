const TRANSLATIONS = {
  en: {
    appTitle: "TFHT Ingest Workbench",
    nightMode: "Night mode", dayMode: "Day mode", langToggle: "HE",
    checkingAccess: "Checking access...",
    openAccess: "Open access",
    syncLoading: "Loading", syncReady: "Ready", syncError: "Error",
    urlInputLabel: "URLs to ingest",
    urlInputPlaceholder: "Paste one URL or multiple separated by comma or semicolon...",
    urlInputHint: "Separate multiple URLs with comma (,) or semicolon (;)",
    addToBatch: "Add to batch",
    batchItems: "{n} items",
    clearBatch: "Clear batch",
    ingestBtn: "Ingest",
    ingestBtnN: "Ingest {n} items",
    ingesting: "Ingesting...",
    resultsTitle: "Ingest results",
    resultIngested: "Ingested",
    resultError: "Error",
    removeBatchItem: "Remove",
    decisionLegend: "Decision", decisionInclude: "Include", decisionNeedsReview: "Needs review", decisionExclude: "Exclude", decisionInternal: "Internal only",
    taxonomyCategory: "Category", taxonomySubcategory: "Subcategory", indexRelevant: "Index relevant",
    eventLabel: "Case / event label", eventLabelPlaceholder: "Short unique case name",
    cityLabel: "City", cityPlaceholder: "Optional city",
    workflowTagsLegend: "Workflow tags", customTagPlaceholder: "Add custom tag after reviewing the list", addCustomTag: "Add tag",
    selectedTagsNone: "No workflow tags selected", selectedTagsPrefix: "Selected: ",
    notesLabel: "Review notes", notesPlaceholder: "Why this belongs, why it does not, or what needs checking",
  },
  he: {
    appTitle: "ממשק קליטה TFHT",
    nightMode: "מצב לילה", dayMode: "מצב יום", langToggle: "EN",
    checkingAccess: "בודק גישה...",
    openAccess: "גישה פתוחה",
    syncLoading: "טוען", syncReady: "מוכן", syncError: "שגיאה",
    urlInputLabel: "כתובות URL לקליטה",
    urlInputPlaceholder: "הדבק כתובת URL אחת או מספר כתובות מופרדות בפסיק או נקודה-פסיק...",
    urlInputHint: "הפרד מספר כתובות באמצעות פסיק (,) או נקודה-פסיק (;)",
    addToBatch: "הוסף לאצווה",
    batchItems: "{n} פריטים",
    clearBatch: "נקה אצווה",
    ingestBtn: "קלוט",
    ingestBtnN: "קלוט {n} פריטים",
    ingesting: "קולט...",
    resultsTitle: "תוצאות קליטה",
    resultIngested: "נקלט",
    resultError: "שגיאה",
    removeBatchItem: "הסר",
    decisionLegend: "החלטה", decisionInclude: "כלול", decisionNeedsReview: "דרוש סקירה", decisionExclude: "החרג", decisionInternal: "פנימי בלבד",
    taxonomyCategory: "קטגוריה", taxonomySubcategory: "תת-קטגוריה", indexRelevant: "רלוונטי למדד",
    eventLabel: "תווית אירוע", eventLabelPlaceholder: "שם קצר ייחודי לתיק/אירוע",
    cityLabel: "עיר", cityPlaceholder: "עיר (אופציונלי)",
    workflowTagsLegend: "תגיות עבודה", customTagPlaceholder: "הוסף תגית מותאמת לאחר סקירת הרשימה", addCustomTag: "הוסף תגית",
    selectedTagsNone: "לא נבחרו תגיות", selectedTagsPrefix: "נבחרו: ",
    notesLabel: "הערות סקירה", notesPlaceholder: "מדוע שייך, מדוע לא, או מה דרוש בדיקה",
  },
};

const WORKFLOW_TAGS = [
  { id: "strong-positive", label: "Strong positive", labelHe: "חיובי חזק" },
  { id: "weak-positive", label: "Weak positive", labelHe: "חיובי חלש" },
  { id: "false-positive", label: "False positive", labelHe: "חיובי שגוי" },
  { id: "duplicate-risk", label: "Duplicate risk", labelHe: "סיכון כפילות" },
  { id: "needs-source-check", label: "Needs source check", labelHe: "בדיקת מקור" },
  { id: "needs-fact-check", label: "Needs fact check", labelHe: "בדיקת עובדות" },
  { id: "needs-privacy-check", label: "Needs privacy check", labelHe: "בדיקת פרטיות" },
  { id: "paywall", label: "Paywall", labelHe: "חסום תשלום" },
  { id: "partial-page", label: "Partial page", labelHe: "דף חלקי" },
  { id: "policy-context", label: "Policy context", labelHe: "הקשר מדיניות" },
  { id: "court", label: "Court", labelHe: "בית משפט" },
  { id: "police", label: "Police", labelHe: "משטרה" },
  { id: "welfare", label: "Welfare", labelHe: "רווחה" },
  { id: "reporting-context", label: "Reporting context", labelHe: "הקשר עיתונאי" },
];

const state = {
  taxonomy: null,
  batch: [],
  lang: localStorage.getItem("tfht-ingest-lang") || "he",
  theme: localStorage.getItem("tfht-ingest-theme") || "day",
};

const els = {
  reviewerEmail: document.querySelector("#reviewerEmail"),
  syncState: document.querySelector("#syncState"),
  themeToggle: document.querySelector("#themeToggle"),
  langToggle: document.querySelector("#langToggle"),
  urlInput: document.querySelector("#urlInput"),
  addToBatchButton: document.querySelector("#addToBatchButton"),
  batchSection: document.querySelector("#batchSection"),
  batchCount: document.querySelector("#batchCount"),
  batchList: document.querySelector("#batchList"),
  clearBatchButton: document.querySelector("#clearBatchButton"),
  labelingForm: document.querySelector("#labelingForm"),
  categorySelect: document.querySelector("#categorySelect"),
  subcategorySelect: document.querySelector("#subcategorySelect"),
  indexRelevantInput: document.querySelector("#indexRelevantInput"),
  eventLabelInput: document.querySelector("#eventLabelInput"),
  cityInput: document.querySelector("#cityInput"),
  tagOptions: document.querySelector("#tagOptions"),
  customTagInput: document.querySelector("#customTagInput"),
  addCustomTagButton: document.querySelector("#addCustomTagButton"),
  selectedTags: document.querySelector("#selectedTags"),
  notesInput: document.querySelector("#notesInput"),
  ingestButton: document.querySelector("#ingestButton"),
  ingestState: document.querySelector("#ingestState"),
  resultsSection: document.querySelector("#resultsSection"),
  resultsList: document.querySelector("#resultsList"),
};

function t(key) {
  return TRANSLATIONS[state.lang]?.[key] ?? TRANSLATIONS.en[key] ?? key;
}

async function boot() {
  initializeTheme();
  initializeLanguage();
  bindEvents();
  setSyncState("syncLoading");
  const [me, taxonomy] = await Promise.all([fetchJson("/api/me"), fetchJson("/api/taxonomy")]);
  if (me.email && me.email !== "anonymous") {
    els.reviewerEmail.textContent = me.email + (me.allowed ? "" : " (not allowed)");
  } else if (me.allowed) {
    els.reviewerEmail.textContent = t("openAccess");
  } else {
    els.reviewerEmail.textContent = t("checkingAccess");
  }
  state.taxonomy = taxonomy;
  renderTaxonomy();
  renderWorkflowTags([]);
  setSyncState("syncReady");
}

function bindEvents() {
  els.themeToggle.addEventListener("click", () => toggleTheme());
  els.langToggle.addEventListener("click", () => {
    state.lang = state.lang === "he" ? "en" : "he";
    localStorage.setItem("tfht-ingest-lang", state.lang);
    applyLang();
  });
  els.addToBatchButton.addEventListener("click", () => addToBatch());
  els.urlInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && (event.ctrlKey || event.metaKey)) {
      event.preventDefault();
      addToBatch();
    }
  });
  els.clearBatchButton.addEventListener("click", () => {
    state.batch = [];
    renderBatch();
  });
  els.labelingForm.addEventListener("submit", (event) => {
    event.preventDefault();
    ingest();
  });
  els.categorySelect.addEventListener("change", () => renderSubcategories());
  els.subcategorySelect.addEventListener("change", () => syncIndexRelevantFromSubcategory());
  els.indexRelevantInput.addEventListener("change", () => {
    if (els.indexRelevantInput.checked) {
      selectRadio("decision", "include");
    }
  });
  els.addCustomTagButton.addEventListener("click", () => addCustomTag());
  els.customTagInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      addCustomTag();
    }
  });
}

function initializeTheme() {
  const storedTheme = localStorage.getItem("tfht-ingest-theme");
  state.theme = storedTheme === "night" ? "night" : "day";
  applyTheme();
}

function toggleTheme() {
  state.theme = state.theme === "night" ? "day" : "night";
  localStorage.setItem("tfht-ingest-theme", state.theme);
  applyTheme();
}

function applyTheme() {
  document.documentElement.dataset.theme = state.theme;
  els.themeToggle.setAttribute("aria-pressed", String(state.theme === "night"));
  els.themeToggle.textContent = state.theme === "night" ? t("dayMode") : t("nightMode");
}

function initializeLanguage() {
  document.documentElement.lang = state.lang;
  document.documentElement.dir = state.lang === "he" ? "rtl" : "ltr";
  els.langToggle.textContent = t("langToggle");
}

function applyLang() {
  document.documentElement.lang = state.lang;
  document.documentElement.dir = state.lang === "he" ? "rtl" : "ltr";
  document.querySelectorAll("[data-i18n]").forEach((el) => {
    el.textContent = t(el.dataset.i18n);
  });
  document.querySelectorAll("[data-i18n-placeholder]").forEach((el) => {
    el.placeholder = t(el.dataset.i18nPlaceholder);
  });
  els.langToggle.textContent = t("langToggle");
  els.themeToggle.textContent = state.theme === "night" ? t("dayMode") : t("nightMode");
  updateIngestButtonLabel();
  renderBatch();
  if (state.taxonomy) renderTaxonomy();
  renderWorkflowTags(selectedWorkflowTags());
  updateSelectedTagSummary();
}

function setSyncState(key) {
  els.syncState.textContent = t(key);
}

function parseUrls(text) {
  return text
    .split(/[,;]+/)
    .map((u) => u.trim())
    .filter((u) => u.length > 0);
}

function addToBatch() {
  const urls = parseUrls(els.urlInput.value);
  if (urls.length === 0) return;
  const existing = new Set(state.batch);
  for (const url of urls) {
    if (!existing.has(url)) {
      state.batch.push(url);
      existing.add(url);
    }
  }
  els.urlInput.value = "";
  renderBatch();
}

function renderBatch() {
  const count = state.batch.length;
  els.batchSection.hidden = count === 0;
  els.batchCount.textContent = t("batchItems").replace("{n}", String(count));
  els.batchList.innerHTML = "";
  for (let i = 0; i < state.batch.length; i++) {
    const url = state.batch[i];
    const li = document.createElement("li");
    li.className = "batchItem";
    const urlSpan = document.createElement("span");
    urlSpan.className = "batchItemUrl";
    urlSpan.title = url;
    urlSpan.textContent = url;
    const removeBtn = document.createElement("button");
    removeBtn.type = "button";
    removeBtn.className = "batchItemRemove";
    removeBtn.textContent = t("removeBatchItem");
    removeBtn.addEventListener("click", () => {
      state.batch.splice(i, 1);
      renderBatch();
    });
    li.appendChild(urlSpan);
    li.appendChild(removeBtn);
    els.batchList.appendChild(li);
  }
  updateIngestButtonLabel();
}

function updateIngestButtonLabel() {
  const n = state.batch.length;
  if (n > 0) {
    els.ingestButton.textContent = t("ingestBtnN").replace("{n}", String(n));
  } else {
    els.ingestButton.textContent = t("ingestBtn");
  }
}

async function ingest() {
  const batchUrls = state.batch.length > 0 ? [...state.batch] : parseUrls(els.urlInput.value);
  if (batchUrls.length === 0) {
    els.ingestState.textContent = "No URLs to ingest.";
    return;
  }

  els.ingestButton.disabled = true;
  els.ingestState.textContent = t("ingesting");
  els.resultsSection.hidden = true;

  const decision = selectedRadio("decision");
  const body = {
    urls: batchUrls,
    decision,
    taxonomyCategoryId: decision === "include" ? els.categorySelect.value : "",
    taxonomySubcategoryId: decision === "include" ? els.subcategorySelect.value : "",
    indexRelevant: els.indexRelevantInput.checked,
    manualEventLabel: els.eventLabelInput.value,
    manualCity: els.cityInput.value,
    tags: selectedWorkflowTags(),
    notes: els.notesInput.value,
  };

  try {
    const data = await fetchJson("/api/ingest", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    els.ingestState.textContent = "";
    state.batch = [];
    renderBatch();
    renderResults(data);
  } catch (error) {
    els.ingestState.textContent = error.message;
  } finally {
    els.ingestButton.disabled = false;
  }
}

function renderResults(data) {
  els.resultsSection.hidden = false;
  els.resultsList.innerHTML = "";
  for (const result of data.results) {
    const li = document.createElement("li");
    li.className = `resultItem ${result.status}`;

    const statusEl = document.createElement("span");
    statusEl.className = "resultStatus";
    statusEl.textContent = result.status === "ingested" ? t("resultIngested") : t("resultError");

    const bodyEl = document.createElement("div");
    bodyEl.className = "resultBody";

    const titleEl = document.createElement("div");
    titleEl.className = "resultTitle";
    titleEl.textContent = result.title || result.url;
    titleEl.title = result.title || result.url;

    const urlEl = document.createElement("div");
    urlEl.className = "resultUrl";
    urlEl.textContent = result.url;

    bodyEl.appendChild(titleEl);
    bodyEl.appendChild(urlEl);

    if (result.status === "error" && result.error) {
      const errEl = document.createElement("div");
      errEl.className = "resultError";
      errEl.textContent = result.error;
      bodyEl.appendChild(errEl);
    }

    const fetchPill = document.createElement("span");
    fetchPill.className = "pill";
    fetchPill.textContent = result.fetchStatus || "";

    li.appendChild(statusEl);
    li.appendChild(bodyEl);
    li.appendChild(fetchPill);
    els.resultsList.appendChild(li);
  }
}

function renderTaxonomy() {
  els.categorySelect.innerHTML = state.taxonomy.categories
    .map((category) => {
      const label = state.lang === "he" ? category.labelHe : category.labelEn;
      return `<option value="${escapeAttr(category.id)}">${escapeHtml(label)}</option>`;
    })
    .join("");
  renderSubcategories();
}

function renderSubcategories() {
  const category =
    state.taxonomy.categories.find((entry) => entry.id === els.categorySelect.value) ||
    state.taxonomy.categories[0];
  els.subcategorySelect.innerHTML = category.subcategories
    .map((subcategory) => {
      const label = state.lang === "he" ? subcategory.labelHe : subcategory.labelEn;
      return `<option value="${escapeAttr(subcategory.id)}">${escapeHtml(label)}</option>`;
    })
    .join("");
  syncIndexRelevantFromSubcategory();
}

function syncIndexRelevantFromSubcategory() {
  const category = state.taxonomy.categories.find((entry) => entry.id === els.categorySelect.value);
  const subcategory = category?.subcategories.find((entry) => entry.id === els.subcategorySelect.value);
  els.indexRelevantInput.checked = Boolean(subcategory?.indexRelevant);
}

function renderWorkflowTags(selected = []) {
  const selectedSet = new Set(selected);
  const standardTagIds = new Set(WORKFLOW_TAGS.map((tag) => tag.id));
  const customTags = selected.filter((tag) => !standardTagIds.has(tag));
  const allTags = [...WORKFLOW_TAGS, ...customTags.map((tag) => ({ id: tag, label: tag, labelHe: tag }))];
  els.tagOptions.innerHTML = allTags
    .map(
      (tag) => `
        <label class="tagOption">
          <input type="checkbox" name="workflowTag" value="${escapeAttr(tag.id)}" ${selectedSet.has(tag.id) ? "checked" : ""} />
          <span>${escapeHtml(state.lang === "he" ? (tag.labelHe || tag.label) : tag.label)}</span>
        </label>
      `,
    )
    .join("");
  els.customTagInput.value = "";
  updateSelectedTagSummary();
  els.tagOptions.querySelectorAll('input[name="workflowTag"]').forEach((input) => {
    input.addEventListener("change", updateSelectedTagSummary);
  });
}

function addCustomTag() {
  const tag = normalizeTag(els.customTagInput.value);
  if (!tag) return;
  const selected = new Set(selectedWorkflowTags());
  selected.add(tag);
  renderWorkflowTags([...selected]);
}

function selectedWorkflowTags() {
  return [...els.tagOptions.querySelectorAll('input[name="workflowTag"]:checked')].map((input) => input.value);
}

function updateSelectedTagSummary() {
  const tags = selectedWorkflowTags();
  els.selectedTags.innerHTML = "";
  if (tags.length === 0) {
    const none = document.createElement("span");
    none.className = "selectedTagsNone";
    none.textContent = t("selectedTagsNone");
    els.selectedTags.appendChild(none);
    return;
  }
  for (const tag of tags) {
    const badge = document.createElement("span");
    badge.className = "tagBadge";
    badge.textContent = tag;
    const removeBtn = document.createElement("button");
    removeBtn.type = "button";
    removeBtn.className = "tagBadgeRemove";
    removeBtn.setAttribute("aria-label", `Remove ${tag}`);
    removeBtn.textContent = "×";
    removeBtn.addEventListener("click", () => {
      const selected = new Set(selectedWorkflowTags());
      selected.delete(tag);
      renderWorkflowTags([...selected]);
    });
    badge.appendChild(removeBtn);
    els.selectedTags.appendChild(badge);
  }
}

async function fetchJson(url, options) {
  const response = await fetch(url, options);
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.details || payload.error || `${response.status} ${response.statusText}`);
  }
  return payload;
}

function selectedRadio(name) {
  return document.querySelector(`input[name="${name}"]:checked`)?.value || "";
}

function selectRadio(name, value) {
  const input = document.querySelector(`input[name="${name}"][value="${value}"]`);
  if (input) input.checked = true;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function escapeAttr(value) {
  return escapeHtml(value);
}

function normalizeTag(value) {
  return String(value || "")
    .trim()
    .replaceAll("_", "-")
    .replace(/[\s]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 40);
}

boot().catch((error) => {
  setSyncState("syncError");
  console.error(error);
});

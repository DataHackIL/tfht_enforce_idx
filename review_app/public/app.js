const TRANSLATIONS = {
  en: {
    appTitle: "TFHT Review Workbench",
    nightMode: "Night mode", dayMode: "Day mode", langToggle: "HE",
    filterSearch: "Search", filterStatus: "Status", filterDomain: "Domain", filterSort: "Sort", refreshBtn: "Refresh",
    filterSearchPlaceholder: "Title, URL, source, taxonomy...",
    statusPending: "Pending review", statusAll: "All fetched", statusCandidates: "Discovery candidates",
    statusNews: "Materialized news items", statusApproved: "Approved", statusSuppressed: "Suppressed",
    sortDesc: "Highest score first", sortAsc: "Lowest score first",
    summaryShown: "shown", summaryMatching: "matching", summaryCandidates: "candidates fetched", summaryNews: "news rows fetched",
    emptyTitle: "Select a candidate", emptyBody: "Rows are sorted by the app's current likelihood score, strongest review candidates first.",
    openSource: "Open source",
    decisionLegend: "Decision", decisionInclude: "Include", decisionNeedsReview: "Needs review", decisionExclude: "Exclude", decisionInternal: "Internal only",
    taxonomyCategory: "Category", taxonomySubcategory: "Subcategory", indexRelevant: "Index relevant",
    eventLabel: "Case / event label", eventLabelPlaceholder: "Short unique case name",
    cityLabel: "City", cityPlaceholder: "Optional city",
    workflowTagsLegend: "Workflow tags", customTagPlaceholder: "Add custom tag after reviewing the list", addCustomTag: "Add tag",
    selectedTagsNone: "No workflow tags selected", selectedTagsPrefix: "Selected: ",
    notesLabel: "Review notes", notesPlaceholder: "Why this belongs, why it does not, or what needs checking",
    saveReview: "Save review", saving: "Saving...", saved: "Saved",
    bulkExclude: "Exclude — false positives", bulkClear: "Clear",
    loadMore: "Load more", noRows: "No rows loaded.", checkingAccess: "Checking access...",
    syncLoading: "Loading", syncReady: "Ready", syncError: "Error",
    scoreLabel: "Score", labelNewsItem: "news item", labelCandidate: "candidate",
    unknownSource: "unknown source", undated: "undated",
    metaUrl: "URL", metaPubDate: "Publication date", metaPubStatus: "Publication status",
    metaReviewStatus: "Review status", metaContentBasis: "Content basis", metaConfidence: "Confidence",
    metaTaxonomy: "Taxonomy", metaCandidateStatus: "Candidate status",
    noSnippet: "No snippet or summary is available.",
  },
  he: {
    appTitle: "ממשק סקירת TFHT",
    nightMode: "מצב לילה", dayMode: "מצב יום", langToggle: "EN",
    filterSearch: "חיפוש", filterStatus: "סטטוס", filterDomain: "דומיין", filterSort: "מיון", refreshBtn: "רענון",
    filterSearchPlaceholder: "כותרת, כתובת URL, מקור, טקסונומיה...",
    statusPending: "ממתין לסקירה", statusAll: "כל הפריטים", statusCandidates: "מועמדי גילוי",
    statusNews: "פריטי חדשות", statusApproved: "מאושר", statusSuppressed: "מוחסם",
    sortDesc: "ציון גבוה ראשון", sortAsc: "ציון נמוך ראשון",
    summaryShown: "מוצגים", summaryMatching: "תואמים", summaryCandidates: "מועמדים נטענו", summaryNews: "חדשות נטענו",
    emptyTitle: "בחר פריט לסקירה", emptyBody: "הפריטים ממוינים לפי ציון הדמיון של האפליקציה, המועמדים החזקים ביותר ראשון.",
    openSource: "פתח מקור",
    decisionLegend: "החלטה", decisionInclude: "כלול", decisionNeedsReview: "דרוש סקירה", decisionExclude: "החרג", decisionInternal: "פנימי בלבד",
    taxonomyCategory: "קטגוריה", taxonomySubcategory: "תת-קטגוריה", indexRelevant: "רלוונטי למדד",
    eventLabel: "תווית אירוע", eventLabelPlaceholder: "שם קצר ייחודי לתיק/אירוע",
    cityLabel: "עיר", cityPlaceholder: "עיר (אופציונלי)",
    workflowTagsLegend: "תגיות עבודה", customTagPlaceholder: "הוסף תגית מותאמת לאחר סקירת הרשימה", addCustomTag: "הוסף תגית",
    selectedTagsNone: "לא נבחרו תגיות", selectedTagsPrefix: "נבחרו: ",
    notesLabel: "הערות סקירה", notesPlaceholder: "מדוע שייך, מדוע לא, או מה דרוש בדיקה",
    saveReview: "שמור סקירה", saving: "שומר...", saved: "נשמר",
    bulkExclude: "החרג — תוצאות שגויות", bulkClear: "נקה",
    loadMore: "טען עוד", noRows: "לא נטענו שורות.", checkingAccess: "בודק גישה...",
    syncLoading: "טוען", syncReady: "מוכן", syncError: "שגיאה",
    scoreLabel: "ציון", labelNewsItem: "פריט חדשות", labelCandidate: "מועמד",
    unknownSource: "מקור לא ידוע", undated: "ללא תאריך",
    metaUrl: "כתובת URL", metaPubDate: "תאריך פרסום", metaPubStatus: "סטטוס פרסום",
    metaReviewStatus: "סטטוס סקירה", metaContentBasis: "בסיס תוכן", metaConfidence: "ביטחון",
    metaTaxonomy: "טקסונומיה", metaCandidateStatus: "סטטוס מועמד",
    noSnippet: "אין תקציר או סיכום זמין.",
  },
};

const state = {
  taxonomy: null,
  items: [],
  selected: null,
  nextOffset: 0,
  loading: false,
  theme: "day",
  bulkSelected: new Set(),
  lang: localStorage.getItem("tfht-review-lang") || "he",
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

const els = {
  reviewerEmail: document.querySelector("#reviewerEmail"),
  syncState: document.querySelector("#syncState"),
  themeToggle: document.querySelector("#themeToggle"),
  langToggle: document.querySelector("#langToggle"),
  searchInput: document.querySelector("#searchInput"),
  statusFilter: document.querySelector("#statusFilter"),
  domainFilter: document.querySelector("#domainFilter"),
  sortOrder: document.querySelector("#sortOrder"),
  refreshButton: document.querySelector("#refreshButton"),
  visibleCount: document.querySelector("#visibleCount"),
  filteredCount: document.querySelector("#filteredCount"),
  candidateCount: document.querySelector("#candidateCount"),
  newsCount: document.querySelector("#newsCount"),
  bulkBar: document.querySelector("#bulkBar"),
  bulkCount: document.querySelector("#bulkCount"),
  bulkExcludeButton: document.querySelector("#bulkExcludeButton"),
  bulkClearButton: document.querySelector("#bulkClearButton"),
  candidateList: document.querySelector("#candidateList"),
  loadMoreButton: document.querySelector("#loadMoreButton"),
  emptyState: document.querySelector("#emptyState"),
  detailView: document.querySelector("#detailView"),
  detailKind: document.querySelector("#detailKind"),
  detailTitle: document.querySelector("#detailTitle"),
  detailLink: document.querySelector("#detailLink"),
  detailSnippet: document.querySelector("#detailSnippet"),
  detailMeta: document.querySelector("#detailMeta"),
  scoreReasons: document.querySelector("#scoreReasons"),
  reviewForm: document.querySelector("#reviewForm"),
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
  saveButton: document.querySelector("#saveButton"),
  saveState: document.querySelector("#saveState"),
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
  els.reviewerEmail.textContent = me.email ? `${me.email}${me.allowed ? "" : " (not allowed)"}` : t("checkingAccess");
  state.taxonomy = taxonomy;
  renderTaxonomy();
  await refresh();
}

function bindEvents() {
  els.themeToggle.addEventListener("click", () => toggleTheme());
  els.langToggle.addEventListener("click", () => {
    state.lang = state.lang === "he" ? "en" : "he";
    localStorage.setItem("tfht-review-lang", state.lang);
    applyLang();
  });
  els.refreshButton.addEventListener("click", () => refresh());
  els.loadMoreButton.addEventListener("click", () => loadCandidates({ append: true }));
  els.searchInput.addEventListener("input", debounce(() => refresh(), 250));
  els.statusFilter.addEventListener("change", () => refresh());
  els.domainFilter.addEventListener("input", debounce(() => refresh(), 250));
  els.sortOrder.addEventListener("change", () => refresh());
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
  els.reviewForm.addEventListener("submit", saveReview);
  els.bulkExcludeButton.addEventListener("click", () => bulkExclude());
  els.bulkClearButton.addEventListener("click", () => {
    state.bulkSelected.clear();
    renderList();
  });
}

function initializeTheme() {
  const storedTheme = window.localStorage.getItem("tfht-review-theme");
  state.theme = storedTheme === "night" ? "night" : "day";
  applyTheme();
}

function toggleTheme() {
  state.theme = state.theme === "night" ? "day" : "night";
  window.localStorage.setItem("tfht-review-theme", state.theme);
  applyTheme();
}

function applyTheme() {
  document.documentElement.dataset.theme = state.theme;
  document.body.classList.toggle("theme-night", state.theme === "night");
  document.body.classList.toggle("theme-day", state.theme !== "night");
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
  if (state.taxonomy) renderTaxonomy();
  if (state.selected) {
    renderDetail(state.selected);
  } else {
    renderDetail(null);
  }
  renderList();
}

async function refresh() {
  state.items = [];
  state.selected = null;
  state.nextOffset = 0;
  state.bulkSelected.clear();
  renderList();
  renderDetail(null);
  await loadCandidates({ append: false });
}

async function loadCandidates({ append }) {
  if (state.loading) return;
  state.loading = true;
  setSyncState("syncLoading");
  els.loadMoreButton.disabled = true;

  const params = new URLSearchParams({
    limit: "250",
    offset: String(append ? state.nextOffset || 0 : 0),
    status: els.statusFilter.value,
    q: els.searchInput.value,
    domain: els.domainFilter.value,
    sort: els.sortOrder.value,
  });
  try {
    const payload = await fetchJson(`/api/candidates?${params.toString()}`);
    state.items = append ? [...state.items, ...payload.items] : payload.items;
    state.nextOffset = payload.nextOffset;
    renderList();
    renderSummary(payload);
    setSyncState("syncReady");
  } catch (error) {
    setSyncState("syncError");
    els.candidateList.innerHTML = `<p class="emptyState">${escapeHtml(error.message)}</p>`;
  } finally {
    state.loading = false;
    els.loadMoreButton.disabled = false;
  }
}

function renderSummary(payload) {
  els.visibleCount.textContent = state.items.length;
  els.filteredCount.textContent = payload.totalFiltered;
  els.candidateCount.textContent = payload.fetched.candidates;
  els.newsCount.textContent = payload.fetched.newsItems;
  els.loadMoreButton.hidden = payload.nextOffset === null;
}

function renderList() {
  if (state.items.length === 0) {
    els.candidateList.innerHTML = `<p class="emptyState">${escapeHtml(t("noRows"))}</p>`;
    updateBulkBar();
    return;
  }
  els.candidateList.innerHTML = "";
  for (const item of state.items) {
    const card = document.createElement("div");
    const isActive = state.selected?.id === item.id;
    const isBulkSelected = state.bulkSelected.has(item.id);
    card.className = `candidateCard${isActive ? " active" : ""}${isBulkSelected ? " bulkSelected" : ""}`;

    // Checkbox for bulk selection
    const checkboxWrap = document.createElement("label");
    checkboxWrap.className = "cardSelectWrap";
    checkboxWrap.title = "Select for bulk action";
    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.className = "cardSelectInput";
    checkbox.checked = isBulkSelected;
    checkbox.addEventListener("change", (event) => {
      event.stopPropagation();
      if (checkbox.checked) {
        state.bulkSelected.add(item.id);
        card.classList.add("bulkSelected");
      } else {
        state.bulkSelected.delete(item.id);
        card.classList.remove("bulkSelected");
      }
      updateBulkBar();
    });
    checkboxWrap.appendChild(checkbox);

    // Main card body — opens detail view
    const body = document.createElement("button");
    body.type = "button";
    body.className = "cardBody";
    body.innerHTML = `
      <div class="cardLine">
        <span class="pill score">${escapeHtml(t("scoreLabel"))} ${item.score}</span>
        <span class="pill">${escapeHtml(labelForKind(item.itemType))}</span>
        ${statusPill(item)}
      </div>
      <h3>${escapeHtml(item.title)}</h3>
      <div class="cardMeta">
        <span class="pill">${escapeHtml(item.sourceDomain || t("unknownSource"))}</span>
        <span class="pill">${escapeHtml(formatDate(item.publicationDatetime))}</span>
      </div>
    `;
    body.addEventListener("click", () => {
      state.selected = item;
      document.querySelectorAll(".candidateCard").forEach((c) => c.classList.remove("active"));
      card.classList.add("active");
      renderDetail(item);
    });

    card.appendChild(checkboxWrap);
    card.appendChild(body);
    els.candidateList.appendChild(card);
  }
  updateBulkBar();
}

function updateBulkBar() {
  const count = state.bulkSelected.size;
  els.bulkBar.hidden = count === 0;
  els.bulkCount.textContent = `${count} selected`;
}

async function bulkExclude() {
  const ids = [...state.bulkSelected];
  if (ids.length === 0) return;
  els.bulkExcludeButton.disabled = true;
  els.bulkExcludeButton.textContent = `Excluding ${ids.length}...`;
  const itemMap = new Map(state.items.map((item) => [item.id, item]));
  try {
    await Promise.all(
      ids.map((id) => {
        const item = itemMap.get(id);
        if (!item) return Promise.resolve();
        return fetchJson("/api/review", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            itemType: item.itemType,
            id: item.id,
            decision: "exclude",
            taxonomyCategoryId: "",
            taxonomySubcategoryId: "",
            indexRelevant: false,
            manualEventLabel: "",
            manualCity: "",
            tags: ["false-positive"],
            notes: "Bulk excluded as false positive",
          }),
        });
      }),
    );
    state.bulkSelected.clear();
    await refresh();
  } catch (error) {
    els.bulkExcludeButton.textContent = `Error: ${error.message}`;
    setTimeout(() => {
      els.bulkExcludeButton.textContent = t("bulkExclude");
      els.bulkExcludeButton.disabled = false;
    }, 3000);
    return;
  }
  els.bulkExcludeButton.disabled = false;
  els.bulkExcludeButton.textContent = t("bulkExclude");
}

function renderDetail(item) {
  if (!item) {
    els.emptyState.hidden = false;
    els.detailView.hidden = true;
    return;
  }
  els.emptyState.hidden = true;
  els.detailView.hidden = false;
  els.saveState.textContent = "";
  els.detailKind.textContent = `${labelForKind(item.itemType)} · ${item.sourceDomain || t("unknownSource")}`;
  els.detailTitle.textContent = item.title;
  els.detailLink.href = item.url || "#";
  els.detailLink.textContent = t("openSource");
  els.detailSnippet.textContent = item.snippet || t("noSnippet");
  els.detailMeta.innerHTML = metaItems(item)
    .map((entry) => `<div class="metaItem"><span>${escapeHtml(entry.label)}</span><strong>${escapeHtml(entry.value)}</strong></div>`)
    .join("");
  els.scoreReasons.innerHTML = item.scoreReasons.map((reason) => `<span class="pill">${escapeHtml(reason)}</span>`).join("");

  selectRadio("decision", decisionFromItem(item));
  els.categorySelect.value = item.taxonomyCategoryId || state.taxonomy.categories[0].id;
  renderSubcategories(item.taxonomySubcategoryId);
  if (item.taxonomyCategoryId) {
    els.indexRelevantInput.checked = Boolean(item.indexRelevant);
  } else {
    els.indexRelevantInput.checked = true;
  }
  els.eventLabelInput.value =
    item.metadata?.annotation?.manualEventLabel ?? item.metadata?.manualEventLabel ?? item.metadata?.manualStatus ?? "";
  els.cityInput.value = item.metadata?.annotation?.manualCity ?? item.metadata?.manualCity ?? "";
  renderWorkflowTags(tagsFromItem(item));
  els.notesInput.value = item.metadata?.annotation?.notes || item.metadata?.annotationNotes || "";
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
  els.selectedTags.textContent = tags.length > 0 ? `${t("selectedTagsPrefix")}${tags.join(", ")}` : t("selectedTagsNone");
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

function renderSubcategories(selectedId = "") {
  const category = state.taxonomy.categories.find((entry) => entry.id === els.categorySelect.value) || state.taxonomy.categories[0];
  els.subcategorySelect.innerHTML = category.subcategories
    .map((subcategory) => {
      const label = state.lang === "he" ? subcategory.labelHe : subcategory.labelEn;
      return `<option value="${escapeAttr(subcategory.id)}">${escapeHtml(label)}</option>`;
    })
    .join("");
  if (selectedId && category.subcategories.some((entry) => entry.id === selectedId)) {
    els.subcategorySelect.value = selectedId;
  }
  syncIndexRelevantFromSubcategory();
}

function syncIndexRelevantFromSubcategory() {
  const category = state.taxonomy.categories.find((entry) => entry.id === els.categorySelect.value);
  const subcategory = category?.subcategories.find((entry) => entry.id === els.subcategorySelect.value);
  els.indexRelevantInput.checked = Boolean(subcategory?.indexRelevant);
}

async function saveReview(event) {
  event.preventDefault();
  if (!state.selected) return;
  els.saveButton.disabled = true;
  els.saveState.textContent = t("saving");
  const decision = selectedRadio("decision");
  const body = {
    itemType: state.selected.itemType,
    id: state.selected.id,
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
    await fetchJson("/api/review", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    els.saveState.textContent = t("saved");
    await refresh();
  } catch (error) {
    els.saveState.textContent = error.message;
  } finally {
    els.saveButton.disabled = false;
  }
}

function tagsFromItem(item) {
  if (Array.isArray(item.metadata?.annotation?.tags)) return item.metadata.annotation.tags;
  if (Array.isArray(item.metadata?.topicTags)) return item.metadata.topicTags;
  return [];
}

function metaItems(item) {
  return [
    { label: t("metaUrl"), value: item.url || "" },
    { label: t("metaPubDate"), value: formatDate(item.publicationDatetime) },
    { label: t("metaPubStatus"), value: item.publicationStatus || "none" },
    { label: t("metaReviewStatus"), value: item.reviewStatus || "none" },
    { label: t("metaContentBasis"), value: item.contentBasis || "unknown" },
    { label: t("metaConfidence"), value: item.confidence ?? "none" },
    { label: t("metaTaxonomy"), value: [item.taxonomyCategoryId, item.taxonomySubcategoryId].filter(Boolean).join(" / ") || "none" },
    { label: t("metaCandidateStatus"), value: item.candidateStatus || "none" },
  ];
}

function decisionFromItem(item) {
  if (item.publicationStatus === "approved" || item.publicationStatus === "include") return "include";
  if (item.publicationStatus === "suppressed" || item.publicationStatus === "exclude") return "exclude";
  if (item.publicationStatus === "internal_only") return "internal_only";
  if (item.reviewStatus && item.reviewStatus !== "none") return "needs_review";
  return "include";
}

function statusPill(item) {
  const status = item.publicationStatus || item.reviewStatus || item.candidateStatus || "pending";
  const needsReview = item.reviewStatus && item.reviewStatus !== "none";
  const className = ["suppressed", "exclude"].includes(status) ? "danger" : needsReview ? "warning" : "";
  return `<span class="pill ${className}">${escapeHtml(status)}</span>`;
}

async function fetchJson(url, options) {
  const response = await fetch(url, options);
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.details || payload.error || `${response.status} ${response.statusText}`);
  }
  return payload;
}

function setSyncState(key) {
  els.syncState.textContent = t(key);
}

function selectedRadio(name) {
  return document.querySelector(`input[name="${name}"]:checked`)?.value || "";
}

function selectRadio(name, value) {
  const input = document.querySelector(`input[name="${name}"][value="${value}"]`);
  if (input) input.checked = true;
}

function labelForKind(kind) {
  return kind === "news_item" ? t("labelNewsItem") : t("labelCandidate");
}

function formatDate(value) {
  if (!value) return t("undated");
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toISOString().slice(0, 10);
}

function debounce(fn, wait) {
  let timeout = null;
  return (...args) => {
    window.clearTimeout(timeout);
    timeout = window.setTimeout(() => fn(...args), wait);
  };
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
    .toLowerCase()
    .replaceAll("_", "-")
    .replace(/[^a-z0-9-]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 40);
}

boot().catch((error) => {
  setSyncState("syncError");
  els.candidateList.innerHTML = `<p class="emptyState">${escapeHtml(error.message)}</p>`;
});

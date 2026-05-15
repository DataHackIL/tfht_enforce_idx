const state = {
  taxonomy: null,
  items: [],
  selected: null,
  nextOffset: 0,
  loading: false,
  theme: "day",
};

const els = {
  reviewerEmail: document.querySelector("#reviewerEmail"),
  syncState: document.querySelector("#syncState"),
  themeToggle: document.querySelector("#themeToggle"),
  searchInput: document.querySelector("#searchInput"),
  statusFilter: document.querySelector("#statusFilter"),
  domainFilter: document.querySelector("#domainFilter"),
  refreshButton: document.querySelector("#refreshButton"),
  visibleCount: document.querySelector("#visibleCount"),
  filteredCount: document.querySelector("#filteredCount"),
  candidateCount: document.querySelector("#candidateCount"),
  newsCount: document.querySelector("#newsCount"),
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
  tagsInput: document.querySelector("#tagsInput"),
  notesInput: document.querySelector("#notesInput"),
  saveButton: document.querySelector("#saveButton"),
  saveState: document.querySelector("#saveState"),
};

async function boot() {
  initializeTheme();
  bindEvents();
  setSyncState("Loading");
  const [me, taxonomy] = await Promise.all([fetchJson("/api/me"), fetchJson("/api/taxonomy")]);
  els.reviewerEmail.textContent = me.email ? `${me.email}${me.allowed ? "" : " (not allowed)"}` : "No Access identity";
  state.taxonomy = taxonomy;
  renderTaxonomy();
  await refresh();
}

function bindEvents() {
  els.themeToggle.addEventListener("click", () => toggleTheme());
  els.refreshButton.addEventListener("click", () => refresh());
  els.loadMoreButton.addEventListener("click", () => loadCandidates({ append: true }));
  els.searchInput.addEventListener("input", debounce(() => refresh(), 250));
  els.statusFilter.addEventListener("change", () => refresh());
  els.domainFilter.addEventListener("input", debounce(() => refresh(), 250));
  els.categorySelect.addEventListener("change", () => renderSubcategories());
  els.subcategorySelect.addEventListener("change", () => syncIndexRelevantFromSubcategory());
  els.reviewForm.addEventListener("submit", saveReview);
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
  els.themeToggle.textContent = state.theme === "night" ? "Day mode" : "Night mode";
}

async function refresh() {
  state.items = [];
  state.selected = null;
  state.nextOffset = 0;
  renderList();
  renderDetail(null);
  await loadCandidates({ append: false });
}

async function loadCandidates({ append }) {
  if (state.loading) return;
  state.loading = true;
  setSyncState("Loading");
  els.loadMoreButton.disabled = true;

  const params = new URLSearchParams({
    limit: "250",
    offset: String(append ? state.nextOffset || 0 : 0),
    status: els.statusFilter.value,
    q: els.searchInput.value,
    domain: els.domainFilter.value,
  });
  try {
    const payload = await fetchJson(`/api/candidates?${params.toString()}`);
    state.items = append ? [...state.items, ...payload.items] : payload.items;
    state.nextOffset = payload.nextOffset;
    renderList();
    renderSummary(payload);
    setSyncState("Ready");
  } catch (error) {
    setSyncState("Error");
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
    els.candidateList.innerHTML = '<p class="emptyState">No rows loaded.</p>';
    return;
  }
  els.candidateList.innerHTML = "";
  for (const item of state.items) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `candidateCard${state.selected?.id === item.id ? " active" : ""}`;
    button.innerHTML = `
      <div class="cardLine">
        <span class="pill score">Score ${item.score}</span>
        <span class="pill">${escapeHtml(labelForKind(item.itemType))}</span>
        ${statusPill(item)}
      </div>
      <h3>${escapeHtml(item.title)}</h3>
      <div class="cardMeta">
        <span class="pill">${escapeHtml(item.sourceDomain || "unknown source")}</span>
        <span class="pill">${escapeHtml(formatDate(item.publicationDatetime))}</span>
      </div>
    `;
    button.addEventListener("click", () => {
      state.selected = item;
      renderList();
      renderDetail(item);
    });
    els.candidateList.appendChild(button);
  }
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
  els.detailKind.textContent = `${labelForKind(item.itemType)} · ${item.sourceDomain || "unknown source"}`;
  els.detailTitle.textContent = item.title;
  els.detailLink.href = item.url || "#";
  els.detailSnippet.textContent = item.snippet || "No snippet or summary is available.";
  els.detailMeta.innerHTML = metaItems(item)
    .map((entry) => `<div class="metaItem"><span>${escapeHtml(entry.label)}</span><strong>${escapeHtml(entry.value)}</strong></div>`)
    .join("");
  els.scoreReasons.innerHTML = item.scoreReasons.map((reason) => `<span class="pill">${escapeHtml(reason)}</span>`).join("");

  selectRadio("decision", decisionFromItem(item));
  els.categorySelect.value = item.taxonomyCategoryId || state.taxonomy.categories[0].id;
  renderSubcategories(item.taxonomySubcategoryId);
  els.indexRelevantInput.checked = Boolean(item.indexRelevant);
  els.eventLabelInput.value = item.metadata?.annotation?.manualEventLabel || item.metadata?.manualStatus || "";
  els.cityInput.value = item.metadata?.annotation?.manualCity || "";
  els.tagsInput.value = Array.isArray(item.metadata?.annotation?.tags) ? item.metadata.annotation.tags.join(", ") : "";
  els.notesInput.value = item.metadata?.annotation?.notes || item.metadata?.annotationNotes || "";
}

function renderTaxonomy() {
  els.categorySelect.innerHTML = state.taxonomy.categories
    .map((category) => `<option value="${escapeAttr(category.id)}">${escapeHtml(category.labelEn)} · ${escapeHtml(category.labelHe)}</option>`)
    .join("");
  renderSubcategories();
}

function renderSubcategories(selectedId = "") {
  const category = state.taxonomy.categories.find((entry) => entry.id === els.categorySelect.value) || state.taxonomy.categories[0];
  els.subcategorySelect.innerHTML = category.subcategories
    .map(
      (subcategory) =>
        `<option value="${escapeAttr(subcategory.id)}">${escapeHtml(subcategory.labelEn)} · ${escapeHtml(subcategory.labelHe)}</option>`,
    )
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
  els.saveState.textContent = "Saving...";
  const body = {
    itemType: state.selected.itemType,
    id: state.selected.id,
    decision: selectedRadio("decision"),
    taxonomyCategoryId: els.categorySelect.value,
    taxonomySubcategoryId: els.subcategorySelect.value,
    indexRelevant: els.indexRelevantInput.checked,
    manualEventLabel: els.eventLabelInput.value,
    manualCity: els.cityInput.value,
    tags: els.tagsInput.value
      .split(",")
      .map((tag) => tag.trim())
      .filter(Boolean),
    notes: els.notesInput.value,
  };

  try {
    await fetchJson("/api/review", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    els.saveState.textContent = "Saved";
    await refresh();
  } catch (error) {
    els.saveState.textContent = error.message;
  } finally {
    els.saveButton.disabled = false;
  }
}

function metaItems(item) {
  return [
    { label: "URL", value: item.url || "" },
    { label: "Publication date", value: formatDate(item.publicationDatetime) },
    { label: "Publication status", value: item.publicationStatus || "none" },
    { label: "Review status", value: item.reviewStatus || "none" },
    { label: "Content basis", value: item.contentBasis || "unknown" },
    { label: "Confidence", value: item.confidence ?? "none" },
    { label: "Taxonomy", value: [item.taxonomyCategoryId, item.taxonomySubcategoryId].filter(Boolean).join(" / ") || "none" },
    { label: "Candidate status", value: item.candidateStatus || "none" },
  ];
}

function decisionFromItem(item) {
  if (item.publicationStatus === "approved" || item.publicationStatus === "include") return "include";
  if (item.publicationStatus === "suppressed" || item.publicationStatus === "exclude") return "exclude";
  if (item.reviewStatus && item.reviewStatus !== "none") return "needs_review";
  return "include";
}

function statusPill(item) {
  const status = item.publicationStatus || item.reviewStatus || item.candidateStatus || "pending";
  const className = ["suppressed", "exclude"].includes(status) ? "danger" : item.reviewStatus ? "warning" : "";
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

function setSyncState(value) {
  els.syncState.textContent = value;
}

function selectedRadio(name) {
  return document.querySelector(`input[name="${name}"]:checked`)?.value || "";
}

function selectRadio(name, value) {
  const input = document.querySelector(`input[name="${name}"][value="${value}"]`);
  if (input) input.checked = true;
}

function labelForKind(kind) {
  return kind === "news_item" ? "news item" : "candidate";
}

function formatDate(value) {
  if (!value) return "undated";
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

boot().catch((error) => {
  setSyncState("Error");
  els.candidateList.innerHTML = `<p class="emptyState">${escapeHtml(error.message)}</p>`;
});

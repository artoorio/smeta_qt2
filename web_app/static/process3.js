(function () {
  const core = window.ReportCore;
  if (!core) return;

  const API_BASE = window.PROCESS_API_BASE || "/api/process3";
  const EXPORT_PREFIX = window.PROCESS_EXPORT_PREFIX || "process3";

  const state = {
    mode: "single",
    file1: null,
    file2: null,
    materials: null,
    reportId: null,
    detail: null,
    summary: null,
    info: null,
    unitDiff: null,
    files: null,
    filter: "",
    summaryMode: "section",
    resourceMode: "collapsed",
    materialLink: {
      selectedRowKey: "",
      selectedRow: null,
      selectedMaterialId: null,
      selectedMaterial: null,
      binding: false,
      listFilter: "",
    },
    materialCatalog: [],
    materialCatalogLoading: false,
    materialLinks: [],
    materialLinksLoading: false,
    sort: {
      column: "",
      direction: "desc",
    },
  };

  const dom = {
    form: document.getElementById("process3-form"),
    modeBar: document.getElementById("process3-mode-bar"),
    modeInput: document.getElementById("process3-mode-input"),
    file2Wrap: document.getElementById("process3-file2-wrap"),
    materialsWrap: document.getElementById("process3-materials-wrap"),
    description: document.getElementById("process3-description"),
    status: document.getElementById("process3-status"),
    tabs: document.getElementById("process3-sheet-tabs"),
    filterBar: document.getElementById("process3-filter-bar"),
    filterInput: document.getElementById("process3-filter-input"),
    filterClear: document.getElementById("process3-filter-clear"),
    summaryBar: document.getElementById("process3-summary-view-bar"),
    summaryBySubsection: document.getElementById("process3-summary-by-subsection"),
    summaryBySection: document.getElementById("process3-summary-by-section"),
    materialFilter: document.getElementById("process3-material-filter"),
    materialSourceList: document.getElementById("process3-material-source-list"),
    detail: document.getElementById("process3-detail"),
    summary: document.getElementById("process3-summary"),
    info: document.getElementById("process3-info"),
    unitDiff: document.getElementById("process3-unit-diff"),
    files: document.getElementById("process3-files"),
    materialPanel: document.getElementById("process3-materials"),
    materialTab: document.querySelector('[data-process3-sheet-target="process3-materials"]'),
    materialClear: document.getElementById("process3-material-clear"),
    materialDeleteLinks: document.getElementById("process3-material-delete-links"),
    materialSelected: document.getElementById("process3-material-selected"),
    materialDbSelected: document.getElementById("process3-material-db-selected"),
    materialResults: document.getElementById("process3-material-results"),
    materialStatus: document.getElementById("process3-material-status"),
    materialBind: document.getElementById("process3-material-bind"),
    materialSummaryPlan: document.getElementById("process3-material-summary-plan"),
    materialSummaryFact: document.getElementById("process3-material-summary-fact"),
  };

  const sheetOrder = [
    "process3-detail",
    "process3-materials",
    "process3-summary",
    "process3-info",
    "process3-unit-diff",
    "process3-files",
  ];

  const singleLabels = {
    detail: "Смета",
    summary: "Итоги анализа",
  };

  const compareLabels = {
    detail: "Customer",
    summary: "Summary",
  };

  const modeDescriptions = {
    single: "Ниже вы увидите диаграммы, кликабельные разделы и подразделы, а также таблицы, которые можно сортировать по столбцам.",
    compare: "Ниже вы увидите диаграммы, кликабельные разделы и подразделы, таблицу сравнения и расхождения; столбцы можно сортировать.",
  };

  function showStatus(message, tone = "info") {
    if (!dom.status) return;
    dom.status.textContent = message;
    dom.status.dataset.tone = tone;
  }

  function setMode(mode) {
    const nextMode = mode === "compare" ? "compare" : "single";
    if (state.mode !== nextMode) {
      clearMaterialSelection();
    }
    state.mode = nextMode;
    if (dom.modeInput) dom.modeInput.value = state.mode;
    if (dom.file2Wrap) {
      const showCompareFile = state.mode === "compare";
      dom.file2Wrap.hidden = !showCompareFile;
      dom.file2Wrap.style.display = showCompareFile ? "" : "none";
      dom.file2Wrap.setAttribute("aria-hidden", String(!showCompareFile));
    }
    if (dom.materialsWrap) dom.materialsWrap.hidden = state.mode === "compare";
    if (dom.materialTab) dom.materialTab.hidden = state.mode !== "single";
    if (dom.summaryBar) dom.summaryBar.hidden = !state.summary;
    if (dom.filterBar) dom.filterBar.hidden = !state.detail || false;
    if (dom.tabs) dom.tabs.hidden = !state.detail;
    if (dom.summaryBySection) dom.summaryBySection.classList.toggle("active", state.summaryMode === "section");
    if (dom.summaryBySubsection) dom.summaryBySubsection.classList.toggle("active", state.summaryMode !== "section");
    if (dom.description) dom.description.textContent = modeDescriptions[state.mode] || "";

    const labels = state.mode === "compare" ? compareLabels : singleLabels;
    const detailTab = dom.tabs?.querySelector('[data-process3-sheet-target="process3-detail"]');
    const summaryTab = dom.tabs?.querySelector('[data-process3-sheet-target="process3-summary"]');
    if (detailTab) detailTab.textContent = labels.detail;
    if (summaryTab) summaryTab.textContent = labels.summary;
    const infoTab = dom.tabs?.querySelector('[data-process3-sheet-target="process3-info"]');
    const unitDiffTab = dom.tabs?.querySelector('[data-process3-sheet-target="process3-unit-diff"]');
    if (infoTab) infoTab.hidden = state.mode !== "compare";
    if (unitDiffTab) unitDiffTab.hidden = state.mode !== "compare";
    if (dom.info) dom.info.hidden = state.mode !== "compare";
    if (dom.unitDiff) dom.unitDiff.hidden = state.mode !== "compare";
    const missingButtons = document.querySelectorAll("[data-process3-export='missing'], [data-process3-export='diff']");
    missingButtons.forEach((button) => {
      if (button.dataset.process3Export === "diff") {
        button.hidden = state.mode !== "compare";
      }
    });
    renderCurrent();
    activateSheet("process3-detail");
  }

  function getVisibleSheetOrder() {
    return state.mode === "compare"
      ? sheetOrder
      : ["process3-detail", "process3-materials", "process3-summary", "process3-files"];
  }

  function activateSheet(targetId) {
    getVisibleSheetOrder().forEach((id) => {
      const panel = document.getElementById(id);
      if (panel) panel.hidden = id !== targetId;
    });
    document.querySelectorAll("[data-process3-sheet-target]").forEach((button) => {
      button.classList.toggle("active", button.dataset.process3SheetTarget === targetId);
    });
    if (dom.filterBar) {
      dom.filterBar.hidden = targetId !== "process3-detail";
    }
    if (dom.summaryBar) {
      dom.summaryBar.hidden = targetId !== "process3-summary";
    }
    if (targetId === "process3-materials") {
      requestAnimationFrame(() => renderMaterialSourceList());
      requestAnimationFrame(() => loadMaterialCatalog());
      requestAnimationFrame(() => loadMaterialLinks());
    }
    if (targetId === "process3-summary" && state.summary) {
      requestAnimationFrame(() => renderSummary());
    }
  }

  function getDetailTitle() {
    return state.mode === "compare" ? "Customer" : "Данные";
  }

  function getSummaryTitle() {
    return state.mode === "compare" ? "Summary" : "Итоги";
  }

  function getSummaryBarColumn() {
    if (!state.summary?.columns?.length) return "";
    if (state.mode === "compare") {
      return state.summary.columns.find((col) => String(col).includes("Разница") && String(col).includes("Ст-ть"))
        || state.summary.columns.find((col) => String(col).includes("Разница"))
        || state.summary.columns.find((col) => String(col).includes("Ст-ть"))
        || "";
    }
    return state.summary.columns.find((col) => String(col).includes("Ст-ть")) || "";
  }

  function rowTooltip(row) {
    if (!state.sort.column) return "";
    const section = String(row?.__meta_section_label || row?.["Раздел"] || "").trim();
    const subsection = String(row?.__meta_subsection_label || row?.["Подраздел"] || "").trim();
    if (section && subsection) {
      return `${section}\n${subsection}`;
    }
    return section || subsection;
  }

  function getCurrentSmetaFileName() {
    return String(state.file1?.name || "проект.xlsx").trim();
  }

  function materialLinkSignatureFromRow(row) {
    const fileName = getCurrentSmetaFileName();
    const position = String(row?.["№"] || row?.["Номер позиции"] || "").trim();
    const name = String(row?.["Наименование"] || "").trim();
    const code = String(row?.["Код расценки"] || "").trim();
    return [fileName, position, name, code].join("||").toLowerCase();
  }

  function materialLinkSignatureFromLink(link) {
    const fileName = String(link?.smeta_file_name || "").trim();
    const position = String(link?.smeta_position_number || "").trim();
    const name = String(link?.smeta_name || "").trim();
    const code = String(link?.smeta_code || "").trim();
    return [fileName, position, name, code].join("||").toLowerCase();
  }

  function getLinkedMaterialSignatures() {
    const signatures = new Set();
    (state.materialLinks || []).forEach((link) => {
      signatures.add(materialLinkSignatureFromLink(link));
    });
    return signatures;
  }

  function getLinkedMaterialIds() {
    const ids = new Set();
    (state.materialLinks || []).forEach((link) => {
      if (link?.material_id !== undefined && link?.material_id !== null) {
        ids.add(Number(link.material_id));
      }
    });
    return ids;
  }

  function getMaterialTotals() {
    const rows = getMaterialSourceRows();
    const catalogById = new Map((state.materialCatalog || []).map((item) => [Number(item.id), item]));
    const linksBySignature = new Map();
    (state.materialLinks || []).forEach((link) => {
      const signature = materialLinkSignatureFromLink(link);
      if (!linksBySignature.has(signature)) {
        linksBySignature.set(signature, link);
      }
    });
    let plan = 0;
    let fact = 0;
    rows.forEach((item) => {
      const row = item.row || {};
      const planCost = core.toNumber(row["Стоимость"]) ?? 0;
      plan += Number(planCost) || 0;
      const link = linksBySignature.get(materialLinkSignatureFromRow(row));
      if (!link) return;
      const material = catalogById.get(Number(link.material_id));
      if (!material) return;
      const quantity = core.toNumber(row["Количество"]) ?? 0;
      const cost = core.toNumber(material.cost) ?? 0;
      fact += (Number(quantity) || 0) * (Number(cost) || 0);
    });
    return { plan, fact };
  }

  function renderMaterialSummary() {
    if (!dom.materialSummaryPlan || !dom.materialSummaryFact) return;
    const { plan, fact } = getMaterialTotals();
    dom.materialSummaryPlan.textContent = `${plan.toLocaleString("ru-RU", { minimumFractionDigits: 2, maximumFractionDigits: 2 })} ₽`;
    dom.materialSummaryFact.textContent = `${fact.toLocaleString("ru-RU", { minimumFractionDigits: 2, maximumFractionDigits: 2 })} ₽`;
  }

  function showMaterialStatus(message, tone = "info") {
    if (!dom.materialStatus) return;
    dom.materialStatus.textContent = message;
    dom.materialStatus.dataset.tone = tone;
  }

  function getMaterialSourceRows() {
    if (!state.detail?.rows?.length) return [];
    const materialPrefixes = ["фсбц", "фссц", "тссц", "тц", "прайс", "прайслист"];
    const rows = state.detail.rows
      .filter((row) => {
        const category = String(row?.__meta_category ?? row?.["Категория"] ?? "").trim().toLowerCase();
        if (category === "материалы") return true;
        const code = String(row?.["Код расценки"] || "").trim().toLowerCase().replace(/\s+/g, "");
        if (materialPrefixes.some((prefix) => code.startsWith(prefix))) return true;
        return false;
      })
      .filter((row) => String(row?.__meta_row_type || "").trim() === "")
      .map((row, index) => {
        const cost = core.toNumber(row?.["Стоимость"]) ?? 0;
        return {
          row,
          rowKey: String(row?.__meta_row_key || row?.__meta_focus_key || index).trim(),
          cost,
          linked: getLinkedMaterialSignatures().has(materialLinkSignatureFromRow(row)),
        };
      });
    const totalCost = rows.reduce((sum, item) => sum + item.cost, 0) || 1;
    return rows
      .map((item) => ({
        ...item,
        share: (item.cost / totalCost) * 100,
      }))
      .sort((a, b) => b.cost - a.cost || String(a.row?.["Наименование"] || "").localeCompare(String(b.row?.["Наименование"] || ""), "ru"));
  }

  async function loadMaterialCatalog(force = false) {
    if (state.materialCatalogLoading) return;
    if (state.materialCatalog.length && !force) {
      renderMaterialCatalog();
      return;
    }
    state.materialCatalogLoading = true;
    renderMaterialCatalog();
    try {
      const response = await fetch("/api/materials/catalog");
      if (!response.ok) {
        throw new Error((await response.text()) || "Не удалось загрузить каталог материалов.");
      }
      const result = await response.json();
      state.materialCatalog = Array.isArray(result.rows) ? result.rows : [];
      renderMaterialCatalog();
    } catch (error) {
      state.materialCatalog = [];
      renderMaterialCatalog(error.message);
    } finally {
      state.materialCatalogLoading = false;
    }
  }

  async function loadMaterialLinks(force = false) {
    if (state.materialLinksLoading) return;
    if (state.materialLinks.length && !force) {
      renderMaterialSourceList();
      renderMaterialCatalog();
      renderMaterialSummary();
      return;
    }
    state.materialLinksLoading = true;
    try {
      const response = await fetch("/api/materials/links");
      if (!response.ok) {
        throw new Error((await response.text()) || "Не удалось загрузить связи материалов.");
      }
      const result = await response.json();
      state.materialLinks = Array.isArray(result.rows) ? result.rows : [];
    } catch (error) {
      state.materialLinks = [];
      showMaterialStatus(error.message, "warning");
    } finally {
      state.materialLinksLoading = false;
      renderMaterialSourceList();
      renderMaterialCatalog();
      renderMaterialSummary();
    }
  }

  async function deleteMaterialLinksForCurrentSmeta() {
    const fileName = getCurrentSmetaFileName();
    if (!fileName) return;
    const ok = window.confirm(`Удалить все связи для сметы "${fileName}"?`);
    if (!ok) return;
    try {
      const response = await fetch("/api/materials/links/delete", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ smeta_file_name: fileName }),
      });
      if (!response.ok) {
        throw new Error((await response.text()) || "Не удалось удалить связи.");
      }
      const result = await response.json();
      const deletedRows = Number(result.deleted_rows || 0);
      const deletedBindings = Number(result.deleted_bindings || 0);
      showMaterialStatus(`Удалено связей: ${deletedRows}. Удалено привязок: ${deletedBindings}.`, "success");
      state.materialLinks = [];
      renderMaterialSourceList();
      renderMaterialCatalog();
      renderMaterialSummary();
      if (state.materialLink.selectedRow) {
        dom.materialDbSelected.innerHTML = "<p class='summary'>Выберите материал из БД справа.</p>";
        if (dom.materialBind) dom.materialBind.disabled = true;
      }
    } catch (error) {
      showMaterialStatus(error.message, "error");
    }
  }

  function renderMaterialSourceList() {
    if (!dom.materialSourceList) return;
    if (!state.detail?.rows?.length) {
      dom.materialSourceList.innerHTML = "<p class='summary'>Сначала сформируйте смету.</p>";
      return;
    }
    const query = String(state.materialLink.listFilter || "").trim().toLowerCase();
    const rows = getMaterialSourceRows().filter((item) => {
      if (!query) return true;
      const haystack = [
        item.row?.["Наименование"],
        item.row?.["Код расценки"],
        item.row?.["Ед.изм."],
        item.row?.["Единица измерения"],
      ].map((value) => String(value ?? "").toLowerCase()).join(" ");
      return haystack.includes(query);
    });
    if (!rows.length) {
      dom.materialSourceList.innerHTML = "<p class='summary'>Материалы не найдены.</p>";
      return;
    }
    dom.materialSourceList.innerHTML = rows.map((item) => {
      const row = item.row || {};
      const isSelected = String(state.materialLink.selectedRowKey || "").trim() === String(item.rowKey || "").trim();
      const isLinked = Boolean(item.linked);
      const share = Number(item.share || 0).toLocaleString("ru-RU", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
      return `
        <article class="material-source-card ${isSelected ? "active" : ""} ${isLinked ? "linked" : ""}" data-row-key="${core.escapeText(item.rowKey || "")}">
          <div class="material-source-main">
            <strong>${core.escapeText(core.formatValue("Наименование", row["Наименование"]))}</strong>
            <span class="summary">${core.escapeText(share)}%</span>
          </div>
          <div class="material-source-meta">
            <span>${core.escapeText(core.formatValue("Код расценки", row["Код расценки"]) || "—")}</span>
            <span>${core.escapeText(core.formatValue("Ед.изм.", row["Ед.изм."] || row["Единица измерения"]) || "—")}</span>
            <span>${core.escapeText(core.formatValue("Стоимость", row["Стоимость"]) || "—")}</span>
            ${isLinked ? "<span class='linked-mark'>Связь есть</span>" : ""}
          </div>
        </article>
      `;
    }).join("");
    dom.materialSourceList.querySelectorAll(".material-source-card").forEach((card) => {
      card.addEventListener("click", () => {
        const rowKey = String(card.dataset.rowKey || "").trim();
        const source = rows.find((item) => String(item.rowKey) === rowKey);
        if (source) selectMaterialRow(source.row, source.rowKey);
      });
    });
    if (!state.materialLink.selectedRow) {
      const first = rows[0];
      if (first) {
        selectMaterialRow(first.row, first.rowKey);
      }
    }
  }

  function clearMaterialSelection() {
    state.materialLink.selectedRowKey = "";
    state.materialLink.selectedRow = null;
    state.materialLink.selectedMaterialId = null;
    state.materialLink.selectedMaterial = null;
    state.materialLink.binding = false;
    if (dom.materialResults) dom.materialResults.innerHTML = "";
    if (dom.materialSelected) dom.materialSelected.innerHTML = "<p class='summary'>Выберите строку в таблице данных.</p>";
    if (dom.materialDbSelected) dom.materialDbSelected.innerHTML = "<p class='summary'>Выберите материал из БД справа.</p>";
    if (dom.materialBind) dom.materialBind.disabled = true;
    showMaterialStatus("Выберите строку материала в таблице данных.", "info");
  }

  function materialRowSummaryHtml(row) {
    if (!row) return "<p class='summary'>Выберите строку в таблице данных.</p>";
    const fields = [
      ["Наименование", row["Наименование"]],
      ["Ед.изм.", row["Ед.изм."] || row["Единица измерения"]],
      ["Код расценки", row["Код расценки"]],
      ["Количество", row["Количество"]],
      ["Стоимость", row["Стоимость"]],
      ["Раздел", row.__meta_section_label || row["Раздел"]],
      ["Подраздел", row.__meta_subsection_label || row["Подраздел"]],
    ].filter(([, value]) => String(value ?? "").trim() !== "");
    if (!fields.length) return "<p class='summary'>Выберите строку в таблице данных.</p>";
    return `
      <div class="summary-grid">
        ${fields.map(([label, value]) => `<div><strong>${label}:</strong> ${core.escapeText(core.formatValue(label, value)).replace(/\n/g, "<br>")}</div>`).join("")}
      </div>
    `;
  }

  function inferCandidateCoefficient(candidate) {
    const selectedUnit = String(state.materialLink.selectedRow?.["Ед.изм."] || state.materialLink.selectedRow?.["Единица измерения"] || "").trim();
    if (!selectedUnit) return 1;
    const rules = Array.isArray(candidate?.rules) ? candidate.rules : [];
    const normalizedSelected = selectedUnit.toLowerCase().replace(/\s+/g, "");
    for (const rule of rules) {
      if (!rule?.active) continue;
      const source = String(rule.source_unit || "").toLowerCase().replace(/\s+/g, "");
      if (source === normalizedSelected) {
        return Number(rule.coefficient || 1) || 1;
      }
    }
    return 1;
  }

  function renderMaterialCatalog(errorMessage = "") {
    if (!dom.materialResults) return;
    if (state.materialCatalogLoading) {
      dom.materialResults.innerHTML = "<p class='summary'>Загружаем каталог материалов...</p>";
      return;
    }
    if (errorMessage) {
      dom.materialResults.innerHTML = `<p class="summary">${core.escapeText(errorMessage)}</p>`;
      return;
    }
    const catalog = Array.isArray(state.materialCatalog) ? state.materialCatalog : [];
    if (!catalog.length) {
      dom.materialResults.innerHTML = "<p class='summary'>Каталог материалов пуст.</p>";
      return;
    }
    const cardsHtml = catalog.map((item) => {
      const isSelected = Number(state.materialLink.selectedMaterialId) === Number(item.id);
      const isLinked = getLinkedMaterialIds().has(Number(item.id));
      const codes = Array.isArray(item.codes) ? item.codes.join(", ") : "";
      const aliases = Array.isArray(item.aliases) && item.aliases.length ? item.aliases.join(", ") : "—";
      const rules = Array.isArray(item.rules) ? item.rules : [];
      return `
        <article class="material-result-card ${isSelected ? "active" : ""} ${isLinked ? "linked" : ""}" data-material-id="${core.escapeText(item.id)}">
          <div class="material-result-head">
            <strong>${core.escapeText(item.name || "")}</strong>
            <span class="summary">${core.escapeText(Number(item.cost || 0).toLocaleString("ru-RU", {
              minimumFractionDigits: 2,
              maximumFractionDigits: 2,
            }))}</span>
          </div>
          <div class="material-result-meta">
            <span>Ед.изм.: ${core.escapeText(item.unit || "")}</span>
            <span>Поставщик: ${core.escapeText(item.supplier || "—")}</span>
            <span>Регион: ${core.escapeText(item.region || "—")}</span>
            <span>Источник: ${core.escapeText(item.source_name || "web")}</span>
            <span>Добавлен: ${core.escapeText(item.date_added || "—")}</span>
            <span>Коды: ${core.escapeText(codes || "—")}</span>
            <span>Алиасы: ${core.escapeText(aliases)}</span>
            <span>Правил: ${core.escapeText(String(rules.length))}</span>
            ${isLinked ? "<span class='linked-mark'>Есть связь</span>" : ""}
          </div>
        </article>
      `;
    }).join("");
    dom.materialResults.innerHTML = `
      <p class="summary">Материалов в БД: ${catalog.length}</p>
      ${cardsHtml}
    `;
    dom.materialResults.querySelectorAll(".material-result-card").forEach((card) => {
      card.addEventListener("click", () => {
        const materialId = Number(card.dataset.materialId);
        const material = catalog.find((item) => Number(item.id) === materialId);
        if (!material) return;
        state.materialLink.selectedMaterialId = material.id;
        state.materialLink.selectedMaterial = material;
        if (dom.materialDbSelected) {
          dom.materialDbSelected.innerHTML = materialRowSummaryHtml({
            "Наименование": material.name,
            "Ед.изм.": material.unit,
            "Стоимость": material.cost,
            "Наименование поставщика": material.supplier,
            "Регион поставки": material.region,
            "Код расценки": Array.isArray(material.codes) ? material.codes.join(", ") : "",
          });
        }
        if (dom.materialBind) {
          dom.materialBind.disabled = !state.materialLink.selectedRow;
        }
        renderMaterialCatalog();
      });
    });
    requestAnimationFrame(() => {
      dom.materialResults.scrollIntoView({ behavior: "smooth", block: "start" });
    });
    renderMaterialSummary();
  }

  function renderMaterialCandidates() {
    renderMaterialCatalog();
  }

  async function searchMaterialsForSelectedRow(force = false) {
    if (state.materialLink.selectedRow) {
      showMaterialStatus("Поиск отключен. Справа показан каталог материалов БД.", "info");
      await loadMaterialCatalog(force);
    }
  }

  async function fetchAllMaterialCandidates(payload) {
    return [];
  }

  async function bindSelectedMaterial() {
    if (!state.materialLink.selectedRow || !state.materialLink.selectedMaterialId) {
      showMaterialStatus("Сначала выберите строку сметы и материал БД.", "warning");
      return;
    }
    const material = state.materialLink.selectedMaterial;
    if (!material) {
      showMaterialStatus("Материал БД не найден.", "error");
      return;
    }
    const payload = {
      material_id: material.id,
      smeta_file_name: state.file1?.name || "проект.xlsx",
      smeta_position_number: String(state.materialLink.selectedRow["№"] || state.materialLink.selectedRow["Номер позиции"] || "").trim(),
      smeta_name: String(state.materialLink.selectedRow["Наименование"] || "").trim(),
      smeta_unit: String(state.materialLink.selectedRow["Ед.изм."] || state.materialLink.selectedRow["Единица измерения"] || "").trim(),
      smeta_code: String(state.materialLink.selectedRow["Код расценки"] || "").trim(),
      smeta_cost: Number(core.toNumber(state.materialLink.selectedRow["Стоимость"]) || 0),
      smeta_signature: String(state.materialLink.selectedRow.__meta_row_key || "").trim(),
      coefficient: 1,
      match_score: 0,
      source_name: `process3:${state.mode}`,
      status: "confirmed",
      note: "",
    };
    state.materialLink.binding = true;
    if (dom.materialBind) dom.materialBind.disabled = true;
    showMaterialStatus("Сохраняем связь...", "info");
    try {
      const response = await fetch("/api/materials/bind", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!response.ok) {
        throw new Error((await response.text()) || "Не удалось сохранить связь.");
      }
      const result = await response.json();
      showMaterialStatus(`Связь сохранена. ID связи: ${result.binding_id}`, "success");
      await loadMaterialLinks(true);
      renderMaterialSummary();
    } catch (error) {
      showMaterialStatus(error.message, "error");
    } finally {
      state.materialLink.binding = false;
      if (dom.materialBind) dom.materialBind.disabled = !(state.materialLink.selectedRow && state.materialLink.selectedMaterialId);
    }
  }

  function selectMaterialRow(row, rowKey) {
    if (state.mode !== "single") return;
    const rowType = String(row?.__meta_row_type || "").trim();
    if (rowType && rowType !== "") return;
    const category = String(row?.__meta_category ?? row?.["Категория"] ?? "").trim().toLowerCase();
    if (!category.startsWith("материал")) return;
    state.materialLink.selectedRowKey = rowKey || String(row?.__meta_row_key || "").trim();
    state.materialLink.selectedRow = row;
    state.materialLink.selectedMaterialId = null;
    state.materialLink.selectedMaterial = null;
    if (dom.materialSelected) {
      dom.materialSelected.innerHTML = materialRowSummaryHtml(row);
    }
    if (dom.materialDbSelected) dom.materialDbSelected.innerHTML = "<p class='summary'>Выберите материал из БД справа.</p>";
    if (dom.materialBind) dom.materialBind.disabled = !state.materialLink.selectedMaterialId;
    showMaterialStatus("Строка выбрана. Теперь выберите материал БД справа.", "info");
    activateSheet("process3-materials");
    renderMaterialSourceList();
    loadMaterialCatalog();
  }

  function renderDetail() {
    if (!state.detail) return;
    const viewData = filterDetailRows(state.detail, state.filter);
    core.renderTable(dom.detail, viewData, {
      mode: "default",
      sortState: state.sort,
      onSortChange: setSort,
      onSortReset: clearSort,
      onResourceToggle: toggleResourceMode,
      resourceMode: state.resourceMode,
      hideDividerRowsWhenSorted: true,
      rowTooltip,
      materialIndentPx: "1.33rem",
      codeWidthAdjustCh: 2,
      onRowSelect: state.mode === "single" ? selectMaterialRow : null,
      selectedRowKey: state.materialLink.selectedRowKey,
    });
  }

  function renderSummary() {
    if (!state.summary) return;
    const source = state.summaryMode === "section" ? core.buildSectionSummary(state.summary) : state.summary;
    core.renderSummaryWithBars(dom.summary, source, {
      title: getSummaryTitle(),
      barColumn: getSummaryBarColumn(),
      onFocusKey: openByFocusKey,
      mergeColumnName: state.summaryMode !== "section" ? "Раздел" : "",
    });
    if (dom.summaryBySection) {
      dom.summaryBySection.classList.toggle("active", state.summaryMode === "section");
    }
    if (dom.summaryBySubsection) {
      dom.summaryBySubsection.classList.toggle("active", state.summaryMode !== "section");
    }
  }

  function renderCurrent() {
    renderDetail();
    renderSummary();
    if (state.mode === "single") {
      renderMaterialSourceList();
    } else if (dom.materialSourceList) {
      dom.materialSourceList.innerHTML = "";
    }
    if (dom.materialPanel) {
      if (state.mode === "single") {
        if (!state.materialLink.selectedRow) {
          clearMaterialSelection();
        }
        loadMaterialCatalog();
        loadMaterialLinks();
        renderMaterialSummary();
      } else {
        clearMaterialSelection();
      }
    }
    if (state.info && dom.info) {
      core.renderTable(dom.info, state.info, { mode: "info" });
    }
    if (state.unitDiff && dom.unitDiff) {
      core.renderTable(dom.unitDiff, state.unitDiff, { mode: "unit_diff" });
    }
    if (state.files && dom.files) {
      core.renderTable(dom.files, state.files, { mode: "files" });
    }
    if (dom.tabs && state.detail) {
      dom.tabs.hidden = false;
    }
    if (dom.filterBar && state.detail) {
      dom.filterBar.hidden = false;
    }
    if (dom.summaryBar && state.summary) {
      dom.summaryBar.hidden = false;
    }
  }

  function clearSort() {
    state.sort.column = "";
    state.sort.direction = "desc";
    renderDetail();
  }

  function setSort(column) {
    const nextColumn = String(column || "");
    if (!nextColumn) {
      clearSort();
      return;
    }
    if (core.RESOURCE_METRIC_COLUMNS.has(nextColumn) && state.resourceMode === "collapsed") {
      state.resourceMode = "expanded";
    }
    if (state.sort.column === nextColumn) {
      state.sort.direction = state.sort.direction === "asc" ? "desc" : "asc";
    } else {
      state.sort.column = nextColumn;
      state.sort.direction = "desc";
    }
    if (dom.filterBar) {
      dom.filterBar.classList.toggle("compare-sort-active", Boolean(state.sort.column));
    }
    renderDetail();
  }

  function toggleResourceMode() {
    const willCollapse = state.resourceMode === "expanded";
    if (willCollapse && core.RESOURCE_METRIC_COLUMNS.has(String(state.sort.column))) {
      clearSort();
    }
    state.resourceMode = state.resourceMode === "collapsed" ? "expanded" : "collapsed";
    renderDetail();
  }

  function openByFocusKey(focusKey) {
    if (!focusKey) return;
    if (state.sort.column) {
      state.sort.column = "";
      state.sort.direction = "desc";
      renderDetail();
    }
    activateSheet("process3-detail");
    requestAnimationFrame(() => {
      const rows = Array.from(dom.detail?.querySelectorAll("tbody tr[data-focus-key]") || []);
      const match = rows.find((row) => row.dataset.focusKey === focusKey);
      if (!match) return;
      match.classList.remove("row-focus");
      match.scrollIntoView({ behavior: "smooth", block: "center" });
      requestAnimationFrame(() => match.classList.add("row-focus"));
      window.setTimeout(() => match.classList.remove("row-focus"), 2200);
    });
  }

  function filterDetailRows(data, query) {
    if (!data?.rows) return data;
    const filtered = core.filterRows(data, query);
    if (!state.sort.column) return filtered;
    const rows = filtered.rows
      .map((row, index) => ({ ...row, __meta_sort_index: index }))
      .filter((row) => row.__meta_row_type !== "divider" && row.__meta_row_type !== "subdivider")
      .sort((a, b) => core.compareNumericRows(a, b, state.sort.column, state.sort.direction));
    return { ...filtered, rows, row_count: rows.length };
  }

  async function fetchJson(endpoint, formData) {
    const response = await fetch(endpoint, { method: "POST", body: formData });
    if (!response.ok) {
      throw new Error((await response.text()) || "Сервер вернул ошибку");
    }
    return response.json();
  }

  async function handleSubmit(event) {
    event.preventDefault();
    const formData = new FormData();
    formData.append("mode", state.mode);
    if (state.file1) formData.append("file1", state.file1);
    if (state.mode === "compare" && state.file2) formData.append("file2", state.file2);
    if (state.mode === "single" && state.materials) formData.append("materials", state.materials);

    try {
      const payload = await fetchJson(API_BASE, formData);
      state.reportId = payload.report_id;
      state.detail = payload.detail;
      state.summary = payload.summary;
      state.info = payload.info;
      state.unitDiff = payload.unit_diff;
      state.files = payload.files;
      state.filter = "";
      state.summaryMode = "section";
      state.resourceMode = "collapsed";
      state.sort.column = "";
      state.sort.direction = "desc";
      state.materialLink.selectedRow = null;
      state.materialLink.selectedRowKey = "";
      state.materialLink.selectedMaterialId = null;
      state.materialLink.selectedMaterial = null;
      state.materialLink.binding = false;
      state.materialLink.listFilter = "";
      if (dom.filterInput) dom.filterInput.value = "";
      if (dom.materialFilter) dom.materialFilter.value = "";
      clearMaterialSelection();
      renderMaterialSummary();
      showStatus(`Строк: ${payload.row_count}, общая стоимость: ${Number(payload.total_cost).toLocaleString()} ₽ Без НДС и лимитированных затрат.`, "success");
      renderCurrent();
      setMode(payload.mode || state.mode);
      activateSheet("process3-summary");
    } catch (error) {
      showStatus(error.message, "error");
    }
  }

  async function handleExport(format) {
    if (!state.reportId) {
      showStatus("Сначала сформируйте отчёт.", "warning");
      return;
    }
    const formData = new FormData();
    formData.append("report_id", state.reportId);
    const response = await fetch(`${API_BASE}/export/${format}`, { method: "POST", body: formData });
    if (!response.ok) {
      showStatus((await response.text()) || "Не удалось сформировать файл.", "error");
      return;
    }
    const blob = await response.blob();
    const suggested = response.headers.get("content-disposition")?.match(/filename="?([^";]+)"?/)?.[1] || `${EXPORT_PREFIX}_${format}`;
    const anchor = document.createElement("a");
    anchor.href = URL.createObjectURL(blob);
    anchor.download = suggested;
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    setTimeout(() => URL.revokeObjectURL(anchor.href), 2000);
  }

  document.addEventListener("DOMContentLoaded", () => {
    if (!dom.form) return;

    const file1Input = dom.form.querySelector("input[name='file1']");
    const file2Input = dom.form.querySelector("input[name='file2']");
    const materialsInput = dom.form.querySelector("input[name='materials']");

    if (file1Input) {
      file1Input.addEventListener("change", (event) => {
        state.file1 = event.target.files[0] || null;
      });
    }
    if (file2Input) {
      file2Input.addEventListener("change", (event) => {
        state.file2 = event.target.files[0] || null;
      });
    }
    if (materialsInput) {
      materialsInput.addEventListener("change", (event) => {
        state.materials = event.target.files[0] || null;
      });
    }
    dom.form.addEventListener("submit", handleSubmit);

    document.querySelectorAll("[data-process3-mode]").forEach((button) => {
      button.addEventListener("click", () => {
        document.querySelectorAll("[data-process3-mode]").forEach((item) => item.classList.toggle("active", item === button));
        setMode(button.dataset.process3Mode);
      });
    });

    document.querySelectorAll("[data-process3-sheet-target]").forEach((button) => {
      button.addEventListener("click", () => activateSheet(button.dataset.process3SheetTarget));
    });

    if (dom.filterInput) {
      dom.filterInput.addEventListener("input", (event) => {
        state.filter = event.target.value;
        renderDetail();
      });
    }
    if (dom.filterClear) {
      dom.filterClear.addEventListener("click", () => {
        state.filter = "";
        if (dom.filterInput) dom.filterInput.value = "";
        renderDetail();
      });
    }
    if (dom.materialFilter) {
      dom.materialFilter.addEventListener("input", (event) => {
        state.materialLink.listFilter = event.target.value;
        renderMaterialSourceList();
      });
    }
    if (dom.summaryBySection) {
      dom.summaryBySection.addEventListener("click", () => {
        state.summaryMode = "section";
        renderSummary();
      });
    }
    if (dom.summaryBySubsection) {
      dom.summaryBySubsection.addEventListener("click", () => {
        state.summaryMode = "subsection";
        renderSummary();
      });
    }

    if (dom.materialClear) {
      dom.materialClear.addEventListener("click", () => {
        clearMaterialSelection();
        if (dom.materialFilter) dom.materialFilter.value = "";
        state.materialLink.listFilter = "";
        renderMaterialSourceList();
        renderCurrent();
        activateSheet("process3-detail");
      });
    }
    if (dom.materialDeleteLinks) {
      dom.materialDeleteLinks.addEventListener("click", () => {
        deleteMaterialLinksForCurrentSmeta();
      });
    }
    if (dom.materialBind) {
      dom.materialBind.addEventListener("click", () => {
        bindSelectedMaterial();
      });
    }
    document.querySelectorAll("[data-process3-export]").forEach((button) => {
      button.addEventListener("click", () => handleExport(button.dataset.process3Export));
    });

    setMode("single");
    if (dom.file2Wrap) {
      dom.file2Wrap.hidden = true;
      dom.file2Wrap.style.display = "none";
      dom.file2Wrap.setAttribute("aria-hidden", "true");
    }
  });
})();

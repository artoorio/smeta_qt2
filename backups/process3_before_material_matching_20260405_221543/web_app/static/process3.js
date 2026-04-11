(function () {
  const core = window.ReportCore;
  if (!core) return;

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
    status: document.getElementById("process3-status"),
    tabs: document.getElementById("process3-sheet-tabs"),
    filterBar: document.getElementById("process3-filter-bar"),
    filterInput: document.getElementById("process3-filter-input"),
    filterClear: document.getElementById("process3-filter-clear"),
    summaryBar: document.getElementById("process3-summary-view-bar"),
    summaryBySubsection: document.getElementById("process3-summary-by-subsection"),
    summaryBySection: document.getElementById("process3-summary-by-section"),
    detail: document.getElementById("process3-detail"),
    summary: document.getElementById("process3-summary"),
    info: document.getElementById("process3-info"),
    unitDiff: document.getElementById("process3-unit-diff"),
    files: document.getElementById("process3-files"),
  };

  const sheetOrder = [
    "process3-detail",
    "process3-summary",
    "process3-info",
    "process3-unit-diff",
    "process3-files",
  ];

  const singleLabels = {
    detail: "Данные анализа",
    summary: "Итоги анализа",
  };

  const compareLabels = {
    detail: "Customer",
    summary: "Summary",
  };

  function showStatus(message, tone = "info") {
    if (!dom.status) return;
    dom.status.textContent = message;
    dom.status.dataset.tone = tone;
  }

  function setMode(mode) {
    state.mode = mode === "compare" ? "compare" : "single";
    if (dom.modeInput) dom.modeInput.value = state.mode;
    if (dom.file2Wrap) {
      const showCompareFile = state.mode === "compare";
      dom.file2Wrap.hidden = !showCompareFile;
      dom.file2Wrap.style.display = showCompareFile ? "" : "none";
      dom.file2Wrap.setAttribute("aria-hidden", String(!showCompareFile));
    }
    if (dom.materialsWrap) dom.materialsWrap.hidden = state.mode === "compare";
    if (dom.summaryBar) dom.summaryBar.hidden = !state.summary;
    if (dom.filterBar) dom.filterBar.hidden = !state.detail || false;
    if (dom.tabs) dom.tabs.hidden = !state.detail;
    if (dom.summaryBySection) dom.summaryBySection.classList.toggle("active", state.summaryMode === "section");
    if (dom.summaryBySubsection) dom.summaryBySubsection.classList.toggle("active", state.summaryMode !== "section");

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
      : ["process3-detail", "process3-summary", "process3-files"];
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
      const payload = await fetchJson("/api/process3", formData);
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
      if (dom.filterInput) dom.filterInput.value = "";
      showStatus(`Строк: ${payload.row_count}, общая стоимость: ${Number(payload.total_cost).toLocaleString()} ₽`, "success");
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
    const response = await fetch(`/api/process3/export/${format}`, { method: "POST", body: formData });
    if (!response.ok) {
      showStatus((await response.text()) || "Не удалось сформировать файл.", "error");
      return;
    }
    const blob = await response.blob();
    const suggested = response.headers.get("content-disposition")?.match(/filename="?([^";]+)"?/)?.[1] || `process3_${format}`;
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

const state = {
  processFiles: [],
  materialsFile: null,
  processDetailData: null,
  processFilesData: null,
  processFilterQuery: "",
  process2File: null,
  process2MaterialsFile: null,
  process2ReportId: null,
  process2DetailData: null,
  process2SummaryData: null,
  process2InfoData: null,
  process2UnitDiffData: null,
  process2FilesData: null,
  process2FilterQuery: "",
  compareFiles: { project: null, fact: null },
  reportId: null,
  compareDetailData: null,
  compareSummaryData: null,
  compareFilterQuery: "",
  compareSort: {
    column: "",
    direction: "desc",
  },
  summaryViewMode: "subsection",
};

const dom = {
  processForm: document.getElementById("process-form"),
  processStatus: document.getElementById("process-summary"),
  processTable: document.getElementById("process-table"),
  processFiles: document.getElementById("process-files"),
  processSheetTabs: document.getElementById("process-sheet-tabs"),
  processFilterBar: document.getElementById("process-filter-bar"),
  processFilterInput: document.getElementById("process-filter-input"),
  processFilterClear: document.getElementById("process-filter-clear"),
  process2Form: document.getElementById("process2-form"),
  process2Status: document.getElementById("process2-missing"),
  process2Detail: document.getElementById("process2-detail"),
  process2Summary: document.getElementById("process2-summary"),
  process2Info: document.getElementById("process2-info"),
  process2UnitDiff: document.getElementById("process2-unit-diff"),
  process2Files: document.getElementById("process2-files"),
  process2SheetTabs: document.getElementById("process2-sheet-tabs"),
  process2FilterBar: document.getElementById("process2-filter-bar"),
  process2FilterInput: document.getElementById("process2-filter-input"),
  process2FilterClear: document.getElementById("process2-filter-clear"),
  compareForm: document.getElementById("compare-form"),
  compareMissing: document.getElementById("compare-missing"),
  compareFiles: document.getElementById("compare-files"),
  compareDetail: document.getElementById("compare-detail"),
  compareSummary: document.getElementById("compare-summary-table"),
  compareInfo: document.getElementById("compare-info"),
  compareUnitDiff: document.getElementById("compare-unit-diff"),
  compareSheetTabs: document.getElementById("compare-sheet-tabs"),
  customerFilterBar: document.getElementById("customer-filter-bar"),
  customerFilterInput: document.getElementById("customer-filter-input"),
  customerFilterClear: document.getElementById("customer-filter-clear"),
  summaryViewBar: document.getElementById("summary-view-bar"),
  summaryBySubsection: document.getElementById("summary-by-subsection"),
  summaryBySection: document.getElementById("summary-by-section"),
    compareInfoNote: document.getElementById("compare-info-note"),
    materialsTable: document.getElementById("materials-table"),
    materialsForm: document.getElementById("materials-form"),
    materialsBatchForm: document.getElementById("materials-batch-form"),
    materialsSummary: document.getElementById("materials-summary"),
  };

const compareSheetOrder = [
  "compare-detail",
  "compare-summary-table",
  "compare-info",
  "compare-unit-diff",
  "compare-files",
];

const processSheetOrder = [
  "process-table",
  "process-files",
];

const process2SheetOrder = [
  "process2-detail",
  "process2-summary",
  "process2-info",
  "process2-unit-diff",
  "process2-files",
];

function escapeText(value) {
  return String(value ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function headerIsNumeric(col) {
  return [
    "Кол-во",
    "Количество",
    "Ст-ть",
    "Стоимость",
    "Разница",
    "Материалы",
    "Общая стоимость",
    "ФОТ",
    "ЭМ",
    "НР",
    "СП",
    "ОТм",
  ].some((token) => String(col).includes(token));
}

function headerIsCode(col) {
  return ["№", "Код расценки", "Ед.изм.", "Единица измерения"].includes(String(col));
}

function isQuantityHeader(col) {
  return ["Кол-во", "Количество"].some((token) => String(col).includes(token));
}

function toNumber(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value !== "string") return null;
  const normalized = value.replace(/\s+/g, "").replace(",", ".").trim();
  if (!normalized) return null;
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

function formatValue(col, value) {
  const num = toNumber(value);
  if (num !== null && headerIsNumeric(col)) {
    if (isQuantityHeader(col)) {
      return num.toLocaleString("ru-RU", {
        maximumFractionDigits: 15,
      });
    }
    const hasFraction = Math.abs(num % 1) > 0.000001;
    return num.toLocaleString("ru-RU", {
      minimumFractionDigits: hasFraction ? 2 : 0,
      maximumFractionDigits: hasFraction ? 2 : 0,
    });
  }
  return String(value ?? "");
}

function suggestWidthCh(col, rows, options = {}) {
  const mode = options.mode || "default";
  const lines = String(col).split("\n");
  rows.slice(0, 200).forEach((row) => {
    formatValue(col, row[col])
      .split("\n")
      .forEach((line) => lines.push(line));
  });
  const maxLen = lines.reduce((acc, line) => Math.max(acc, line.length), 0);
  const compareMetricColumns = new Set([
    "Кол-во\n(Проект)",
    "Кол-во\n(Факт)",
    "Разница\n(Кол-во)",
    "Ст-ть\n(Проект)",
    "Ст-ть\n(Факт)",
    "Разница\n(Ст-ть)",
  ]);
  const resourceMetricColumns = new Set(["ФОТ", "ЭМ", "НР", "СП", "ОТм"]);
  if (col === "№") {
    return Math.max(6, Math.min(10, maxLen + 2));
  }
  if (col === "Наименование") {
    const minWidth = mode === "summary" ? 18 : 24;
    const maxWidth = mode === "summary" ? 42 : 80;
    return Math.max(minWidth, Math.min(maxWidth, Math.ceil(maxLen / 4) + 2));
  }
  if (col === "Подраздел") {
    const minWidth = mode === "summary" ? 14 : 18;
    const maxWidth = mode === "summary" ? 26 : 36;
    return Math.max(minWidth, Math.min(maxWidth, Math.ceil(maxLen / 3) + 2));
  }
  if (col === "Файл") {
    return Math.max(30, Math.min(120, maxLen + 2));
  }
  if (compareMetricColumns.has(String(col))) {
    const maxWidth = mode === "summary" ? 14 : 20;
    return Math.max(6, Math.min(maxWidth, maxLen + 2));
  }
  if (resourceMetricColumns.has(String(col))) {
    const maxWidth = mode === "summary" ? 14 : 16;
    return Math.max(13, Math.min(maxWidth, maxLen + 1));
  }
  if (headerIsNumeric(col)) {
    const maxWidth = mode === "summary" ? 14 : 20;
    return Math.max(6, Math.min(maxWidth, maxLen + 2));
  }
  if (["Ед.изм.", "Единица измерения", "Ед.изм.\n(Проект)", "Ед.изм.\n(Факт)"].includes(String(col))) {
    return Math.max(6, Math.min(14, maxLen + 2));
  }
  if (headerIsCode(col)) {
    return Math.max(8, Math.min(22, maxLen + 2));
  }
  if (mode === "summary") {
    return Math.max(10, Math.min(22, maxLen + 2));
  }
  return Math.max(12, Math.min(36, maxLen + 2));
}

  function detectRowClass(columns, row) {
    if (row.__meta_row_type === "subdivider") return "row-subdivider";
    if (row.__meta_row_type === "divider") return "row-divider";
    const values = columns.map((col) => String(row[col] ?? "").trim());
    if (values.some((value) => value.startsWith("--"))) return "row-divider";
    return "";
  }

function renderTable(container, data, title = "", options = {}) {
  if (!container) return;
  container.innerHTML = "";
  const mode = options.mode || "default";
  const sortState = options.sortState || null;
  const onSortChange = typeof options.onSortChange === "function" ? options.onSortChange : null;
  const onSortReset = typeof options.onSortReset === "function" ? options.onSortReset : null;
  const rowActions = Array.isArray(options.rowActions) ? options.rowActions : [];
  const isSortableTable = mode === "default" && sortState;
  const columns = [...(data?.columns || [])];
  if (rowActions.length) {
    columns.push("__actions");
  }

  if (title) {
    const h3 = document.createElement("h3");
    h3.className = "section-title";
    h3.textContent = title;
    container.appendChild(h3);
  }

  if (!data?.rows?.length) {
    const empty = document.createElement("p");
    empty.className = "summary";
    empty.textContent = "Нет строк для отображения.";
    container.appendChild(empty);
    return;
  }

  const table = document.createElement("table");
  table.className = "report-table";
  if (mode === "summary" || mode === "files") {
    table.classList.add("report-table-compact");
  } else {
    const colgroup = document.createElement("colgroup");
      columns.forEach((col) => {
        const colEl = document.createElement("col");
        const width = col === "__actions" ? 12 : suggestWidthCh(col, data.rows, options);
        colEl.style.width = `${width}ch`;
        colEl.style.minWidth = `${Math.max(6, width - 2)}ch`;
        colgroup.appendChild(colEl);
      });
    table.appendChild(colgroup);
  }

  const thead = document.createElement("thead");
  const headerRow = document.createElement("tr");
    columns.forEach((col) => {
      const th = document.createElement("th");
      const classes = [headerIsNumeric(col) ? "col-numeric" : "col-text"];
      if (col === "__actions") classes.push("col-actions");
      if (col === "№") classes.push("col-numeric");
    if (col === "Наименование") classes.push("col-name");
    if (col === "Файл") classes.push("no-wrap");
    if (mode === "summary" && (col === "Раздел" || col === "Подраздел")) {
      classes.push("summary-wrap-column");
    }
    if (String(col).startsWith("Разница")) {
      classes.push("sortable-diff");
    }
    if (isSortableTable && headerIsNumeric(col)) {
      classes.push("sortable-header");
      th.setAttribute("role", "button");
      th.setAttribute("tabindex", "0");
      th.title = `Нажмите, чтобы сортировать по "${col}"`;
      const sortByColumn = () => {
        if (onSortChange) onSortChange(col);
      };
      th.addEventListener("click", (event) => {
        if (event.target.closest(".sort-reset-btn")) return;
        sortByColumn();
      });
      th.addEventListener("keydown", (event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          sortByColumn();
        }
      });
    }
    if (isSortableTable && sortState.column && String(col) === sortState.column) {
      classes.push("sorted");
      th.dataset.sortDirection = sortState.direction;
    }
    th.className = classes.join(" ");
      if (col === "__actions") {
        th.textContent = "Действия";
      } else {
        const headerText = document.createElement("span");
        headerText.className = "header-label";
        headerText.innerHTML = escapeText(col).replace(/\n/g, "<br>");
        th.appendChild(headerText);
      }
      if (isSortableTable && sortState.column && String(col) === sortState.column) {
        const sortIndicator = document.createElement("span");
        sortIndicator.className = "sort-indicator";
        sortIndicator.textContent = sortState.direction === "asc" ? "▲" : "▼";
        th.appendChild(sortIndicator);
      }
    if (isSortableTable && String(col) === "Наименование" && sortState.column && onSortReset) {
      const resetButton = document.createElement("button");
      resetButton.type = "button";
      resetButton.className = "sort-reset-btn";
      resetButton.title = "Сброс сортировки";
      resetButton.setAttribute("aria-label", "Сброс сортировки");
      resetButton.textContent = "✕";
      resetButton.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        onSortReset();
      });
      th.appendChild(resetButton);
    }
    headerRow.appendChild(th);
  });
  thead.appendChild(headerRow);

  const tbody = document.createElement("tbody");
  data.rows.forEach((row) => {
    if (isSortableTable && sortState.column && (row.__meta_row_type === "divider" || row.__meta_row_type === "subdivider")) {
      return;
    }
    const tr = document.createElement("tr");
    const rowClass = detectRowClass(data.columns, row);
    if (rowClass) tr.className = rowClass;
    if (row.__meta_focus_key) {
      tr.dataset.focusKey = row.__meta_focus_key;
    }
      columns.forEach((col) => {
        const td = document.createElement("td");
        const classes = [headerIsNumeric(col) ? "col-numeric" : "col-text"];
        if (col === "__actions") classes.push("col-actions");
        const category = String(row.__meta_category ?? row["Категория"] ?? "").trim();
        if (col === "__actions") {
          td.className = classes.join(" ");
          rowActions.forEach((action) => {
            const button = document.createElement("button");
            button.type = "button";
            button.className = `action-btn ${action.className || ""}`.trim();
            button.textContent = action.label || "Действие";
            if (action.title) button.title = action.title;
            if (typeof action.onClick === "function") {
              button.addEventListener("click", (event) => {
                event.preventDefault();
                event.stopPropagation();
                action.onClick(row);
              });
            }
            td.appendChild(button);
          });
          tr.appendChild(td);
          return;
        }
        if (col === "№") classes.push("col-numeric");
        if (col === "Наименование") classes.push("col-name");
      if (col === "Наименование" && mode === "default" && category.toLowerCase().startsWith("материал")) {
        classes.push("customer-material-indent");
      }
      if (col === "Файл") classes.push("no-wrap");
      if (mode === "summary" && (col === "Раздел" || col === "Подраздел")) {
        classes.push("summary-wrap-column");
      }
      if (String(col).startsWith("Разница")) {
        const diff = toNumber(row[col]);
        if (diff !== null) classes.push(diff > 0 ? "diff-positive" : diff < 0 ? "diff-negative" : "diff-zero");
      }
      td.className = classes.join(" ");
      if (mode === "summary" && col === "Раздел" && String(row[col] ?? "").trim()) {
        const button = document.createElement("button");
        button.type = "button";
        button.className = "summary-link";
        button.textContent = formatValue(col, row[col]);
        button.dataset.focusKey = String(row.__meta_focus_key ?? "").trim();
        button.addEventListener("click", () => openCustomerByFocusKey(button.dataset.focusKey));
        td.appendChild(button);
      } else if (mode === "summary" && col === "Подраздел" && String(row[col] ?? "").trim()) {
        const button = document.createElement("button");
        button.type = "button";
        button.className = "summary-link";
        button.textContent = formatValue(col, row[col]);
        button.dataset.focusKey = String(row.__meta_focus_key ?? "").trim();
        button.addEventListener("click", () => openCustomerByFocusKey(button.dataset.focusKey));
        td.appendChild(button);
      } else {
        td.innerHTML = escapeText(formatValue(col, row[col])).replace(/\n/g, "<br>");
      }
      tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });

  table.appendChild(thead);
  table.appendChild(tbody);
  container.appendChild(table);
}

function syncSummaryRowHeights(tableA, tableB) {
  if (!tableA || !tableB) return;
  const rowsA = Array.from(tableA.querySelectorAll("tbody tr"));
  const rowsB = Array.from(tableB.querySelectorAll(".summary-bar-row"));
  const count = Math.min(rowsA.length, rowsB.length);
  for (let i = 0; i < count; i += 1) {
    rowsA[i].style.height = "";
    rowsB[i].style.height = "";
  }
  for (let i = 0; i < count; i += 1) {
    const height = Math.max(rowsA[i].offsetHeight, rowsB[i].offsetHeight);
    rowsA[i].style.height = `${height}px`;
    rowsB[i].style.height = `${height}px`;
  }
}

function renderSummaryWithBars(container, data, title = "Summary") {
  if (!container) return;
  container.innerHTML = "";

  const heading = document.createElement("h3");
  heading.className = "section-title";
  heading.textContent = title;
  container.appendChild(heading);

  if (!data?.rows?.length) {
    const empty = document.createElement("p");
    empty.className = "summary";
    empty.textContent = "Нет строк для отображения.";
    container.appendChild(empty);
    return;
  }

  const diffColumn = data.columns.find((col) => String(col).includes("Разница") && String(col).includes("Ст-ть"));
  const values = diffColumn ? data.rows.map((row) => Math.abs(toNumber(row[diffColumn]) ?? 0)) : [];
  const maxAbs = Math.max(...values, 0);

  const layout = document.createElement("div");
  layout.className = "summary-layout";

  const tablePane = document.createElement("div");
  tablePane.className = "summary-table-pane";
  const barsPane = document.createElement("div");
  barsPane.className = "summary-bars-pane summary-bars-pane-positive";

  renderTable(tablePane, data, "", { mode: "summary" });

  const barsTable = document.createElement("div");
  barsTable.className = "summary-bars-table";
  const barsBody = document.createElement("div");
  barsBody.className = "summary-bars-body";
  data.rows.forEach((row) => {
    const tr = document.createElement("div");
    tr.className = "summary-bar-row";
    const rowClass = detectRowClass(data.columns, row);
    if (rowClass) tr.classList.add(rowClass);

    const td = document.createElement("div");
    td.className = "summary-bar-cell";

    const value = diffColumn ? toNumber(row[diffColumn]) ?? 0 : 0;
    const track = document.createElement("div");
    track.className = "summary-bar-track";

    const bar = document.createElement("div");
    bar.className = "summary-bar-fill";
    const width = maxAbs > 0 ? Math.max(3, Math.round((Math.abs(value) / maxAbs) * 50)) : 0;
    if (value < 0) {
      bar.classList.add("negative");
      bar.style.left = `${50 - width}%`;
    } else {
      bar.classList.add("positive");
      bar.style.left = "50%";
    }
    bar.style.width = `${width}%`;
    track.appendChild(bar);

    td.appendChild(track);
    tr.appendChild(td);
    barsBody.appendChild(tr);
  });
  barsTable.appendChild(barsBody);
  barsPane.appendChild(barsTable);

  layout.appendChild(tablePane);
  layout.appendChild(barsPane);
  container.appendChild(layout);

  const mainTable = tablePane.querySelector("table");
  requestAnimationFrame(() => {
    const header = mainTable?.querySelector("thead");
    if (header) {
      barsPane.style.setProperty("--summary-header-height", `${header.offsetHeight}px`);
    }
    syncSummaryRowHeights(mainTable, barsBody);
  });
}

function showProcessStatus(message, tone = "info") {
  if (!dom.processStatus) return;
  dom.processStatus.textContent = message;
  dom.processStatus.dataset.tone = tone;
}

function showProcess2Status(message, tone = "info") {
  if (!dom.process2Status) return;
  dom.process2Status.textContent = message;
  dom.process2Status.dataset.tone = tone;
}

function activateProcessSheet(targetId) {
  if (!dom.processSheetTabs) return;
  processSheetOrder.forEach((id) => {
    const panel = document.getElementById(id);
    if (panel) panel.hidden = id !== targetId;
  });
  document.querySelectorAll("[data-process-sheet-target]").forEach((button) => {
    button.classList.toggle("active", button.dataset.processSheetTarget === targetId);
  });
}

function activateProcess2Sheet(targetId) {
  if (!dom.process2SheetTabs) return;
  process2SheetOrder.forEach((id) => {
    const panel = document.getElementById(id);
    if (panel) panel.hidden = id !== targetId;
  });
  document.querySelectorAll("[data-process2-sheet-target]").forEach((button) => {
    button.classList.toggle("active", button.dataset.process2SheetTarget === targetId);
  });
  if (dom.process2FilterBar) {
    dom.process2FilterBar.hidden = targetId !== "process2-detail";
  }
}

function renderMissingList(list) {
  if (!dom.compareMissing) return;
  dom.compareMissing.innerHTML = "";
  if (!list?.length) {
    dom.compareMissing.textContent = "Отсутствующих позиций не обнаружено.";
    return;
  }
  dom.compareMissing.textContent = "";
  const ul = document.createElement("ul");
  list.forEach((item) => {
    const li = document.createElement("li");
    li.textContent = item;
    ul.appendChild(li);
  });
  dom.compareMissing.appendChild(ul);
}

function activateSheet(targetId) {
  if (!dom.compareSheetTabs) return;
  compareSheetOrder.forEach((id) => {
    const panel = document.getElementById(id);
    if (panel) panel.hidden = id !== targetId;
  });
  dom.compareSheetTabs.querySelectorAll(".sheet-tab").forEach((button) => {
    button.classList.toggle("active", button.dataset.sheetTarget === targetId);
  });
  if (dom.customerFilterBar) {
    dom.customerFilterBar.hidden = targetId !== "compare-detail";
  }
  if (dom.summaryViewBar) {
    dom.summaryViewBar.hidden = targetId !== "compare-summary-table";
  }
  if (dom.compareInfoNote) {
    dom.compareInfoNote.hidden = targetId !== "compare-info";
  }
  if (targetId === "compare-summary-table" && state.compareSummaryData) {
    requestAnimationFrame(() => renderSummaryTable());
  }
}

function filterCustomerRows(data, query) {
  if (!data?.rows) return data;
  const normalized = String(query || "").trim().toLowerCase();
  if (!normalized) return data;
  const rows = data.rows.filter((row) =>
    data.columns.some((col) => String(row[col] ?? "").toLowerCase().includes(normalized))
  );
  return { ...data, rows };
}

function compareNumericRows(a, b, column, direction = "desc") {
  const av = toNumber(a?.[column]);
  const bv = toNumber(b?.[column]);
  if (av === null && bv === null) {
    return (a.__meta_sort_index ?? 0) - (b.__meta_sort_index ?? 0);
  }
  if (av === null) return 1;
  if (bv === null) return -1;
  let result = av - bv;
  if (direction === "desc") result *= -1;
  if (result !== 0) return result;
  return (a.__meta_sort_index ?? 0) - (b.__meta_sort_index ?? 0);
}

function getCompareViewData(data) {
  if (!data?.rows?.length) return data;
  const filtered = filterCustomerRows(data, state.compareFilterQuery);
  if (!state.compareSort.column) return filtered;
  const rows = filtered.rows
    .map((row, index) => ({ ...row, __meta_sort_index: index }))
    .filter((row) => row.__meta_row_type !== "divider" && row.__meta_row_type !== "subdivider")
    .sort((a, b) => compareNumericRows(a, b, state.compareSort.column, state.compareSort.direction));
  return {
    ...filtered,
    rows,
    row_count: rows.length,
  };
}

function updateCompareSortControls() {
  if (dom.customerFilterBar) {
    dom.customerFilterBar.classList.toggle("compare-sort-active", Boolean(state.compareSort.column));
  }
}

function clearCompareSort() {
  state.compareSort.column = "";
  state.compareSort.direction = "desc";
  updateCompareSortControls();
  renderCustomerTable();
}

function setCompareSort(column) {
  const nextColumn = String(column || "");
  if (!nextColumn) {
    clearCompareSort();
    return;
  }
  if (state.compareSort.column === nextColumn) {
    state.compareSort.direction = state.compareSort.direction === "asc" ? "desc" : "asc";
  } else {
    state.compareSort.column = nextColumn;
    state.compareSort.direction = "desc";
  }
  updateCompareSortControls();
  renderCustomerTable();
}

function filterProcessRows(data, query) {
  if (!data?.rows) return data;
  const normalized = String(query || "").trim().toLowerCase();
  if (!normalized) return data;
  const rows = data.rows.filter((row) =>
    data.columns.some((col) => String(row[col] ?? "").toLowerCase().includes(normalized))
  );
  return { ...data, rows };
}

function filterProcess2Rows(data, query) {
  if (!data?.rows) return data;
  const normalized = String(query || "").trim().toLowerCase();
  if (!normalized) return data;
  const rows = data.rows.filter((row) =>
    data.columns.some((col) => String(row[col] ?? "").toLowerCase().includes(normalized))
  );
  return { ...data, rows };
}

function renderProcessTable() {
  if (!state.processDetailData) return;
  renderTable(dom.processTable, filterProcessRows(state.processDetailData, state.processFilterQuery), "Обработанные строки");
}

function renderProcess2Table() {
  if (!state.process2DetailData) return;
  renderTable(dom.process2Detail, filterProcess2Rows(state.process2DetailData, state.process2FilterQuery), "Обработанные строки");
}

function renderCustomerTable() {
  if (!state.compareDetailData) return;
  const viewData = getCompareViewData(state.compareDetailData);
  renderTable(dom.compareDetail, viewData, "Customer", {
    mode: "default",
    sortState: state.compareSort,
    onSortChange: setCompareSort,
    onSortReset: clearCompareSort,
  });
  updateCompareSortControls();
}

function openCustomerByFocusKey(focusKey) {
  if (!focusKey) return;
  activateSheet("compare-detail");
  requestAnimationFrame(() => {
    const rows = Array.from(dom.compareDetail?.querySelectorAll("tbody tr[data-focus-key]") || []);
    const match = rows.find((row) => row.dataset.focusKey === focusKey);
    if (!match) return;
    match.classList.remove("row-focus");
    match.scrollIntoView({ behavior: "smooth", block: "center" });
    requestAnimationFrame(() => match.classList.add("row-focus"));
    window.setTimeout(() => match.classList.remove("row-focus"), 2200);
  });
}

function mergeSummarySectionCells(container) {
  const table = container?.querySelector("table");
  if (!table) return;
  const bodyRows = Array.from(table.querySelectorAll("tbody tr"));
  if (!bodyRows.length) return;

  let lastCell = null;
  let lastValue = "";
  let span = 1;

  bodyRows.forEach((row) => {
    const firstCell = row.querySelector("td");
    if (!firstCell) return;
    const value = firstCell.textContent.trim();
    if (lastCell && value === lastValue) {
      span += 1;
      lastCell.rowSpan = span;
      firstCell.remove();
    } else {
      lastCell = firstCell;
      lastValue = value;
      span = 1;
    }
  });
}

function buildSectionSummary(data) {
  if (!data?.rows?.length) return data;
  const numericColumns = data.columns.filter((col) => headerIsNumeric(col));
  const groups = new Map();

  data.rows.forEach((row) => {
    const section = String(row["Раздел"] ?? "").trim();
    if (!groups.has(section)) {
      const base = {};
      data.columns.forEach((col) => {
        base[col] = col === "Раздел" ? section : "";
      });
      base.__meta_focus_key = row.__meta_focus_key || "";
      groups.set(section, base);
    }
    const target = groups.get(section);
    numericColumns.forEach((col) => {
      const current = toNumber(target[col]) ?? 0;
      const incoming = toNumber(row[col]) ?? 0;
      target[col] = current + incoming;
    });
  });

  return {
    columns: data.columns.filter((col) => col !== "Подраздел"),
    rows: Array.from(groups.values()).map((row) => {
      const nextRow = { ...row };
      delete nextRow["Подраздел"];
      return nextRow;
    }),
    row_count: groups.size,
  };
}

function renderSummaryTable() {
  if (!state.compareSummaryData) return;
  const isSectionMode = state.summaryViewMode === "section";
  const source = isSectionMode ? buildSectionSummary(state.compareSummaryData) : state.compareSummaryData;
  renderSummaryWithBars(dom.compareSummary, source, "Summary");
  if (!isSectionMode) {
    const mainPane = dom.compareSummary.querySelector(".summary-table-pane");
    mergeSummarySectionCells(mainPane);
    const mainTable = mainPane?.querySelector("table");
    const barsTable = dom.compareSummary.querySelector(".summary-bars-table");
    requestAnimationFrame(() => syncSummaryRowHeights(mainTable, barsTable));
  }
  if (dom.summaryBySubsection) {
    dom.summaryBySubsection.classList.toggle("active", !isSectionMode);
  }
  if (dom.summaryBySection) {
    dom.summaryBySection.classList.toggle("active", isSectionMode);
  }
}

function showMaterialsStatus(message, tone = "info") {
  if (!dom.materialsSummary) return;
  dom.materialsSummary.textContent = message;
  dom.materialsSummary.dataset.tone = tone;
}

async function loadMaterialsView() {
  const container = dom.materialsTable;
  if (!container) return;
  container.innerHTML = "<p class='summary'>Загрузка данных...</p>";
  try {
    const response = await fetch("/api/materials");
    if (!response.ok) {
      throw new Error(await response.text() || "Не удалось загрузить данные.");
    }
    const payload = await response.json();
      if (dom.materialsSummary && payload.summary) {
        const total = Number(payload.summary.materials_sum || 0).toLocaleString("ru-RU", {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        });
        showMaterialsStatus(`Записей: ${payload.summary.rows}, суммарная стоимость материалов: ${total} ₽`, "info");
      }
      const materialsPayload = {
        columns: (payload.columns || []).filter((column) => column !== "id"),
        rows: payload.rows || [],
      };
      renderTable(container, materialsPayload, "Материалы", {
        mode: "materials",
        rowActions: [{
          label: "Удалить",
          className: "danger",
          title: "Удалить материал",
          onClick: async (row) => {
            if (!row?.id) return;
            if (!window.confirm(`Удалить запись "${row.name || row.file || row.id}"?`)) return;
            try {
              const response = await fetch("/api/materials/delete", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ id: row.id }),
              });
              if (!response.ok) {
                throw new Error(await response.text() || "Не удалось удалить запись.");
              }
              const result = await response.json();
              showMaterialsStatus(`Удалена запись из файла ${result.file || "unknown"}.`, "success");
              loadMaterialsView();
            } catch (error) {
              showMaterialsStatus(error.message, "error");
            }
          },
        }],
      });
    } catch (error) {
      container.innerHTML = `<p class='summary'>${error.message}</p>`;
    }
  }

async function handleMaterialsSubmit(event) {
  if (!dom.materialsForm) return;
  event.preventDefault();
  const formData = new FormData(dom.materialsForm);
    const payload = {
      name: String(formData.get("name") || "").trim(),
      unit: String(formData.get("unit") || "").trim(),
      cost: formData.get("cost") ? Number(formData.get("cost")) : undefined,
      supplier: String(formData.get("supplier") || "").trim(),
      region: String(formData.get("region") || "").trim(),
      price_codes: String(formData.get("priceCodes") || "").trim(),
      file_name: String(formData.get("fileName") || "web").trim() || "web",
    };
  try {
      const response = await fetch("/api/materials/add", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      throw new Error(await response.text() || "Не удалось сохранить запись.");
    }
    showMaterialsStatus("Запись добавлена в базу.", "success");
    dom.materialsForm.reset();
    loadMaterialsView();
  } catch (error) {
    showMaterialsStatus(error.message, "error");
  }
}

async function handleMaterialsBatchSubmit(event) {
  if (!dom.materialsBatchForm) return;
  event.preventDefault();
  const formData = new FormData(dom.materialsBatchForm);
  const file = formData.get("batchFile");
  if (!file || !(file instanceof File) || !file.name) {
    showMaterialsStatus("Выберите Excel или CSV-файл для импорта.", "warning");
    return;
  }
  const payload = new FormData();
  payload.append("file", file);
  payload.append("file_name", String(formData.get("fileName") || file.name).trim() || file.name);
  try {
    const response = await fetch("/api/materials/import", {
      method: "POST",
      body: payload,
    });
    if (!response.ok) {
      throw new Error(await response.text() || "Не удалось импортировать файл.");
    }
    const result = await response.json();
    showMaterialsStatus(`Импортировано строк: ${result.inserted} из файла ${result.file_name}.`, "success");
    dom.materialsBatchForm.reset();
    loadMaterialsView();
  } catch (error) {
    showMaterialsStatus(error.message, "error");
  }
}

async function fetchJson(endpoint, formData) {
  const response = await fetch(endpoint, { method: "POST", body: formData });
  if (!response.ok) {
    const content = await response.text();
    throw new Error(content || "Сервер вернул ошибку");
  }
  return response.json();
}

async function handleProcessSubmit(event) {
  event.preventDefault();
  if (!state.processFiles.length) {
    showProcessStatus("Выберите хотя бы один файл сметы.", "warning");
    return;
  }
  const formData = new FormData();
  state.processFiles.forEach((file) => formData.append("files", file));
  if (state.materialsFile) {
    formData.append("materials", state.materialsFile);
  }
  try {
    const payload = await fetchJson("/api/process", formData);
    showProcessStatus(
      `Строк: ${payload.row_count}, общая стоимость: ${Number(payload.total_cost).toLocaleString()} ₽`,
      "success"
    );
    state.processFilterQuery = "";
    if (dom.processFilterInput) {
      dom.processFilterInput.value = "";
    }
    state.processDetailData = payload.detail;
    state.processFilesData = payload.files || null;
    renderProcessTable();
    if (dom.processFiles && payload.files) {
      renderTable(dom.processFiles, payload.files, "Файлы", { mode: "files" });
    }
    activateProcessSheet("process-table");
  } catch (error) {
    showProcessStatus(error.message, "error");
  }
}

async function handleProcessExport(mode) {
  if (!state.processFiles.length) {
    showProcessStatus("Сначала выполните обработку.", "warning");
    return;
  }
  const formData = new FormData();
  state.processFiles.forEach((file) => formData.append("files", file));
  if (state.materialsFile) {
    formData.append("materials", state.materialsFile);
  }
  formData.append("mode", mode);
  const response = await fetch(`/api/process/export`, { method: "POST", body: formData });
  if (!response.ok) {
    const text = await response.text();
    showProcessStatus(text || "Не удалось сформировать файл.", "error");
    return;
  }
  const blob = await response.blob();
  const suggested = response.headers.get("content-disposition")?.match(/filename="?([^";]+)"?/)?.[1] || `processed_${mode}.xlsx`;
  const anchor = document.createElement("a");
  anchor.href = URL.createObjectURL(blob);
  anchor.download = suggested;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  setTimeout(() => URL.revokeObjectURL(anchor.href), 2000);
}

async function handleProcess2Submit(event) {
  event.preventDefault();
  const formData = new FormData();
  if (state.process2File) {
    formData.append("file", state.process2File);
  }
  if (state.process2MaterialsFile) {
    formData.append("materials", state.process2MaterialsFile);
  }
  try {
    const payload = await fetchJson("/api/process2", formData);
    showProcess2Status(
      `Строк: ${payload.row_count}, общая стоимость: ${Number(payload.total_cost).toLocaleString()} ₽`,
      "success"
    );
    state.process2FilterQuery = "";
    if (dom.process2FilterInput) {
      dom.process2FilterInput.value = "";
    }
    state.process2ReportId = payload.report_id;
    state.process2DetailData = payload.detail;
    state.process2SummaryData = payload.summary;
    state.process2InfoData = payload.info;
    state.process2UnitDiffData = payload.unit_diff;
    state.process2FilesData = payload.files || null;
    renderProcess2Table();
    if (dom.process2Summary) {
      renderTable(dom.process2Summary, payload.summary, "Summary", { mode: "summary" });
    }
    if (dom.process2Info) {
      renderTable(dom.process2Info, payload.info, "Инфо", { mode: "info" });
    }
    if (dom.process2UnitDiff) {
      renderTable(dom.process2UnitDiff, payload.unit_diff, "Отличается единица измерения", { mode: "unit_diff" });
    }
    if (dom.process2Files && payload.files) {
      renderTable(dom.process2Files, payload.files, "Файлы", { mode: "files" });
    }
    if (dom.process2SheetTabs) {
      dom.process2SheetTabs.hidden = false;
    }
    if (dom.process2FilterBar) {
      dom.process2FilterBar.hidden = false;
    }
    activateProcess2Sheet("process2-detail");
  } catch (error) {
    showProcess2Status(error.message, "error");
  }
}

async function handleProcess2Export(mode) {
  if (!state.process2ReportId) {
    showProcess2Status("Сначала сформируйте отчёт.", "warning");
    return;
  }
  const formData = new FormData();
  formData.append("report_id", state.process2ReportId);
  const response = await fetch(`/api/process2/export/${mode}`, { method: "POST", body: formData });
  if (!response.ok) {
    const text = await response.text();
    showProcess2Status(text || "Не удалось сформировать файл.", "error");
    return;
  }
  const blob = await response.blob();
  const suggested = response.headers.get("content-disposition")?.match(/filename=\"?([^\";]+)\"?/)?.[1] || `process2_${mode}.xlsx`;
  const anchor = document.createElement("a");
  anchor.href = URL.createObjectURL(blob);
  anchor.download = suggested;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  setTimeout(() => URL.revokeObjectURL(anchor.href), 2000);
}

async function handleCompareSubmit(event) {
  event.preventDefault();
  if (!dom.compareForm) return;
  const useDefaults = !state.compareFiles.project && !state.compareFiles.fact;
  if ((!state.compareFiles.project && state.compareFiles.fact) || (state.compareFiles.project && !state.compareFiles.fact)) {
    if (dom.compareMissing) {
      dom.compareMissing.textContent = "Либо загрузите оба файла, либо оставьте оба пустыми для отладочного режима.";
    }
    return;
  }
  const formData = new FormData();
  if (!useDefaults) {
    formData.append("files", state.compareFiles.project);
    formData.append("files", state.compareFiles.fact);
  }
  formData.append(
    "compare_column",
    dom.compareForm.elements["compareColumn"].value.trim() || "Наименование"
  );
  formData.append(
    "value_column",
    dom.compareForm.elements["valueColumn"].value.trim() || "Стоимость"
  );
  formData.append("extra_columns", dom.compareForm.elements["extraColumns"].value.trim());
  formData.append("subsection_column", dom.compareForm.elements["subsectionColumn"].value.trim());
  formData.append("use_defaults", useDefaults ? "true" : "false");
  try {
    const payload = await fetchJson("/api/compare", formData);
    state.reportId = payload.report_id;
    state.compareDetailData = payload.detail;
    state.compareSummaryData = payload.summary;
    renderTable(dom.compareFiles, payload.files, "Файлы", { mode: "files" });
    state.compareSort.column = "";
    state.compareSort.direction = "desc";
    renderCustomerTable();
    renderSummaryTable();
    renderTable(dom.compareInfo, payload.info, "Инфо", { mode: "info" });
    renderTable(dom.compareUnitDiff, payload.unit_diff, "Отличается единица измерения", { mode: "unit_diff" });
    renderMissingList(payload.missing);
    if (dom.compareMissing && payload.used_defaults) {
      dom.compareMissing.textContent = "Использованы отладочные файлы: проект.xlsx и факт.xlsx.";
    }
    if (dom.compareSheetTabs) {
      dom.compareSheetTabs.hidden = false;
      activateSheet("compare-detail");
    }
    if (dom.customerFilterInput) {
      dom.customerFilterInput.value = state.compareFilterQuery;
    }
  } catch (error) {
    if (dom.compareDetail) {
      dom.compareDetail.innerHTML = `<p class='summary'>${error.message}</p>`;
    }
  }
}

async function handleCompareExport(format) {
  if (!state.reportId) {
    if (dom.compareMissing) {
      dom.compareMissing.textContent = "Сначала выполните сравнение.";
    }
    return;
  }
  const formData = new FormData();
  formData.append("report_id", state.reportId);
  const response = await fetch(`/api/compare/export/${format}`, { method: "POST", body: formData });
  if (!response.ok) {
    if (dom.compareDetail) {
      dom.compareDetail.innerHTML = `<p class='summary'>${await response.text() || "Ошибка экспорта."}</p>`;
    }
    return;
  }
  const blob = await response.blob();
  const suggested = response.headers.get("content-disposition")?.match(/filename="?([^";]+)"?/)?.[1] || `compare_${format}`;
  const anchor = document.createElement("a");
  anchor.href = URL.createObjectURL(blob);
  anchor.download = suggested;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  setTimeout(() => URL.revokeObjectURL(anchor.href), 2000);
}

document.addEventListener("DOMContentLoaded", () => {
  if (dom.processForm) {
    const filesInput = dom.processForm.querySelector("input[name='files']");
    const materialsInput = dom.processForm.querySelector("input[name='materials']");
    if (filesInput) {
      filesInput.addEventListener("change", (event) => {
        state.processFiles = Array.from(event.target.files);
      });
    }
    if (materialsInput) {
      materialsInput.addEventListener("change", (event) => {
        state.materialsFile = event.target.files[0] || null;
      });
    }
    dom.processForm.addEventListener("submit", handleProcessSubmit);
  }

  document.querySelectorAll("[data-process-sheet-target]").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.preventDefault();
      activateProcessSheet(button.dataset.processSheetTarget);
    });
  });

  if (dom.processFilterInput) {
    dom.processFilterInput.addEventListener("input", (event) => {
      state.processFilterQuery = event.target.value;
      renderProcessTable();
    });
  }
  if (dom.processFilterClear) {
    dom.processFilterClear.addEventListener("click", () => {
      state.processFilterQuery = "";
      if (dom.processFilterInput) {
        dom.processFilterInput.value = "";
      }
      renderProcessTable();
    });
  }

  document.querySelectorAll("[data-export-mode]").forEach((button) => {
    button.addEventListener("click", () => handleProcessExport(button.dataset.exportMode));
  });

  if (dom.process2Form && !document.getElementById("process2-summary-view-bar")) {
    const fileInput = dom.process2Form.querySelector("input[name='file']");
    const materialsInput = dom.process2Form.querySelector("input[name='materials']");
    if (fileInput) {
      fileInput.addEventListener("change", (event) => {
        state.process2File = event.target.files[0] || null;
      });
    }
    if (materialsInput) {
      materialsInput.addEventListener("change", (event) => {
        state.process2MaterialsFile = event.target.files[0] || null;
      });
    }
    dom.process2Form.addEventListener("submit", handleProcess2Submit);
    document.querySelectorAll("[data-process2-sheet-target]").forEach((button) => {
      button.addEventListener("click", (event) => {
        event.preventDefault();
        activateProcess2Sheet(button.dataset.process2SheetTarget);
      });
    });
    if (dom.process2FilterInput) {
      dom.process2FilterInput.addEventListener("input", (event) => {
        state.process2FilterQuery = event.target.value;
        renderProcess2Table();
      });
    }
    if (dom.process2FilterClear) {
      dom.process2FilterClear.addEventListener("click", () => {
        state.process2FilterQuery = "";
        if (dom.process2FilterInput) {
          dom.process2FilterInput.value = "";
        }
        renderProcess2Table();
      });
    }
    document.querySelectorAll("[data-process2-export]").forEach((button) => {
      button.addEventListener("click", () => handleProcess2Export(button.dataset.process2Export));
    });
  }

  if (dom.compareForm) {
    const projectInput = dom.compareForm.querySelector("input[name='project']");
    const factInput = dom.compareForm.querySelector("input[name='fact']");
    if (projectInput) {
      projectInput.addEventListener("change", (event) => {
        state.compareFiles.project = event.target.files[0] || null;
      });
    }
    if (factInput) {
      factInput.addEventListener("change", (event) => {
        state.compareFiles.fact = event.target.files[0] || null;
      });
    }
    dom.compareForm.addEventListener("submit", handleCompareSubmit);
  }

  if (dom.compareSheetTabs) {
    dom.compareSheetTabs.querySelectorAll(".sheet-tab").forEach((button) => {
      button.addEventListener("click", () => activateSheet(button.dataset.sheetTarget));
    });
  }
  if (dom.summaryBySubsection) {
    dom.summaryBySubsection.addEventListener("click", () => {
      state.summaryViewMode = "subsection";
      renderSummaryTable();
    });
  }
  if (dom.summaryBySection) {
    dom.summaryBySection.addEventListener("click", () => {
      state.summaryViewMode = "section";
      renderSummaryTable();
    });
  }
  if (dom.customerFilterInput) {
    dom.customerFilterInput.addEventListener("input", (event) => {
      state.compareFilterQuery = event.target.value;
      renderCustomerTable();
    });
  }
  if (dom.customerFilterClear) {
    dom.customerFilterClear.addEventListener("click", () => {
      state.compareFilterQuery = "";
      if (dom.customerFilterInput) {
        dom.customerFilterInput.value = "";
      }
      renderCustomerTable();
    });
  }

  document.querySelectorAll("[data-compare-export]").forEach((button) => {
    button.addEventListener("click", () => handleCompareExport(button.dataset.compareExport));
  });

  if (dom.materialsForm) {
    dom.materialsForm.addEventListener("submit", handleMaterialsSubmit);
  }
  if (dom.materialsBatchForm) {
    dom.materialsBatchForm.addEventListener("submit", handleMaterialsBatchSubmit);
  }
  loadMaterialsView();
});

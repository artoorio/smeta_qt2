(function () {
  const state = {
    file: null,
    materials: null,
    reportId: null,
    detail: null,
    summary: null,
    info: null,
    unitDiff: null,
    files: null,
    filter: "",
    summaryMode: "subsection",
    resourceMode: "collapsed",
    sort: {
      column: "",
      direction: "desc",
    },
  };

  const dom = {
    form: document.getElementById("process2-form"),
    status: document.getElementById("process2-missing"),
    detail: document.getElementById("process2-detail"),
    summary: document.getElementById("process2-summary"),
    info: document.getElementById("process2-info"),
    unitDiff: document.getElementById("process2-unit-diff"),
    files: document.getElementById("process2-files"),
    tabs: document.getElementById("process2-sheet-tabs"),
    filterBar: document.getElementById("process2-filter-bar"),
    filterInput: document.getElementById("process2-filter-input"),
    filterClear: document.getElementById("process2-filter-clear"),
    sortBar: document.getElementById("process2-sort-bar"),
    sortColumn: document.getElementById("process2-sort-column"),
    sortDirection: document.getElementById("process2-sort-direction"),
    sortReset: document.getElementById("process2-sort-reset"),
    summaryBar: document.getElementById("process2-summary-view-bar"),
    summaryBySubsection: document.getElementById("process2-summary-by-subsection"),
    summaryBySection: document.getElementById("process2-summary-by-section"),
  };

  const sheetOrder = [
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
      "Вес",
      "ФОТ",
      "ЭМ",
      "НР",
      "СП",
      "ОТм",
      "ФОТ/ЭМ/НР/СП/ОТм",
    ].some((token) => String(col).includes(token));
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
      if (["Кол-во", "Количество"].some((token) => String(col).includes(token))) {
        return num.toLocaleString("ru-RU", {
          maximumFractionDigits: 15,
        });
      }
      const hasFraction = Math.abs(num % 1) > 0.000001;
      return num.toLocaleString("ru-RU", {
        minimumFractionDigits: hasFraction ? 2 : 0,
        maximumFractionDigits: hasFraction ? 2 : 2,
      });
    }
    return String(value ?? "");
  }

  function columnHasValues(rows, col) {
    return rows.some((row) => String(row?.[col] ?? "").trim() !== "");
  }

  const resourceMetricColumns = new Set(["ФОТ", "ЭМ", "НР", "СП", "ОТм"]);
  const resourceAggregateColumn = "ФОТ/ЭМ/НР/СП/ОТм";

  function isResourceColumn(col) {
    return resourceMetricColumns.has(String(col));
  }

  function resourceColumnsPresent(columns) {
    return columns.some((col) => isResourceColumn(col));
  }

  function sumResourceValue(row) {
    let total = 0;
    let hasValue = false;
    Array.from(resourceMetricColumns).forEach((col) => {
      const num = toNumber(row?.[col]);
      if (num !== null) {
        total += num;
        hasValue = true;
      }
    });
    return hasValue ? total : null;
  }

  function buildVisibleColumns(columns, resourceMode) {
    if (!columns.some((col) => isResourceColumn(col))) return columns.slice();

    if (resourceMode !== "collapsed") {
      return columns.slice();
    }

    const visible = [];
    let insertedAggregate = false;
    columns.forEach((col) => {
      if (isResourceColumn(col)) {
        if (!insertedAggregate) {
          visible.push(resourceAggregateColumn);
          insertedAggregate = true;
        }
        return;
      }
      visible.push(col);
    });
    return visible;
  }

  function isDividerDataRow(row) {
    return row?.__meta_row_type === "divider" || row?.__meta_row_type === "subdivider";
  }

  function compareNumericRows(a, b, column, direction = "desc") {
    const av = String(column) === resourceAggregateColumn ? sumResourceValue(a) : toNumber(a?.[column]);
    const bv = String(column) === resourceAggregateColumn ? sumResourceValue(b) : toNumber(b?.[column]);
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

  function getDetailViewData(data) {
    if (!data?.rows?.length) return data;
    const filtered = filterRows(data, state.filter);
    if (!state.sort.column) return filtered;
    const rows = filtered.rows
      .map((row, index) => ({ ...row, __meta_sort_index: index }))
      .filter((row) => !isDividerDataRow(row))
      .sort((a, b) => compareNumericRows(a, b, state.sort.column, state.sort.direction));
    return {
      ...filtered,
      rows,
      row_count: rows.length,
    };
  }

  function updateSortControls() {
    if (!dom.sortBar || !dom.sortColumn || !dom.sortDirection || !dom.sortReset) return;
    const hasColumns = Boolean(state.detail?.columns?.length);
    dom.sortBar.hidden = !hasColumns;
    if (!hasColumns) return;

    const columns = (state.detail.columns || []).filter((col) => headerIsNumeric(col));
    const hasResourceColumns = resourceColumnsPresent(state.detail.columns || []);
    const options = [
      { value: "", label: "Без сортировки" },
      ...columns.map((col) => ({ value: String(col), label: String(col) })),
    ];
    if (hasResourceColumns && !options.some((item) => item.value === resourceAggregateColumn)) {
      options.splice(1, 0, { value: resourceAggregateColumn, label: resourceAggregateColumn });
    }
    const current = state.sort.column;
    dom.sortColumn.innerHTML = "";
    options.forEach((item) => {
      const option = document.createElement("option");
      option.value = item.value;
      option.textContent = item.label;
      dom.sortColumn.appendChild(option);
    });
    dom.sortColumn.value = current && options.some((item) => item.value === current) ? current : "";
    dom.sortDirection.textContent = state.sort.direction === "asc" ? "↑" : "↓";
    dom.sortReset.disabled = !state.sort.column;
    dom.sortDirection.disabled = !state.sort.column;
  }

  function clearDetailSort() {
    state.sort.column = "";
    state.sort.direction = "desc";
    updateSortControls();
    renderDetail();
  }

  function setDetailSort(column) {
    const nextColumn = String(column || "");
    if (!nextColumn) {
      clearDetailSort();
      return;
    }
    if (resourceMetricColumns.has(nextColumn) && state.resourceMode === "collapsed") {
      state.resourceMode = "expanded";
    }
    if (state.sort.column === nextColumn) {
      state.sort.direction = state.sort.direction === "asc" ? "desc" : "asc";
    } else {
      state.sort.column = nextColumn;
      state.sort.direction = "desc";
    }
    updateSortControls();
    renderDetail();
  }

  function buildRowTooltip(row) {
    const section = String(row?.__meta_section_label || row?.["Раздел"] || "").trim();
    const subsection = String(row?.__meta_subsection_label || row?.["Подраздел"] || "").trim();
    if (section && subsection) {
      return `${section}\n${subsection}`;
    }
    return section || subsection;
  }

  function suggestWidthCh(col, rows, mode = "default") {
    const lines = String(col).split("\n");
    rows.slice(0, 200).forEach((row) => {
      const value = String(col) === resourceAggregateColumn ? sumResourceValue(row) : row[col];
      formatValue(col, value)
        .split("\n")
        .forEach((line) => lines.push(line));
    });
    const maxLen = lines.reduce((acc, line) => Math.max(acc, line.length), 0);
    if (String(col) === resourceAggregateColumn) {
      const maxWidth = mode === "summary" ? 14 : 16;
      return Math.max(13, Math.min(maxWidth, maxLen + 1));
    }
    if (["ФОТ", "ЭМ", "НР", "СП", "ОТм"].includes(String(col))) {
      const maxWidth = mode === "summary" ? 14 : 16;
      return Math.max(13, Math.min(maxWidth, maxLen + 1));
    }
    if (col === "№") return Math.max(6, Math.min(10, maxLen + 2));
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
    if (col === "Файл") return Math.max(30, Math.min(120, maxLen + 2));
    if (headerIsNumeric(col)) return Math.max(6, Math.min(mode === "summary" ? 14 : 20, maxLen + 2));
    if (["Ед.изм.", "Единица измерения", "Ед.изм.\n(Проект)", "Ед.изм.\n(Факт)"].includes(String(col))) {
      return Math.max(6, Math.min(14, maxLen + 2));
    }
    if (String(col) === "Код расценки") {
      return Math.max(20, Math.min(20, maxLen + 1));
    }
    if (String(col) === "Категория") {
      return Math.max(8, Math.min(22, maxLen + 2));
    }
    return Math.max(10, Math.min(mode === "summary" ? 22 : 36, maxLen + 2));
  }

  function detectRowClass(columns, row) {
    if (row.__meta_row_type === "subdivider") return "row-subdivider";
    if (row.__meta_row_type === "divider") return "row-divider";
    const values = columns.map((col) => String(row[col] ?? "").trim());
    if (values.some((value) => value.startsWith("--"))) return "row-divider";
    return "";
  }

  function filterRows(data, query) {
    if (!data?.rows) return data;
    const normalized = String(query || "").trim().toLowerCase();
    if (!normalized) return data;
    const rows = data.rows.filter((row) =>
      data.columns.some((col) => String(row[col] ?? "").toLowerCase().includes(normalized))
    );
    return { ...data, rows };
  }

  function renderTable(container, data, title = "", options = {}) {
    if (!container) return;
    container.innerHTML = "";
    const mode = options.mode || "default";

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
    const baseColumns = data.columns.filter((col) => {
      const name = String(col);
      if (name === "Вспомогательные ресурсы" || name === "Оборудование") {
        return columnHasValues(data.rows, col);
      }
      return true;
    });
    const visibleColumns = mode === "default" ? buildVisibleColumns(baseColumns, options.resourceMode || "collapsed") : baseColumns;
    const compact = mode === "summary" || mode === "files";
    if (compact) {
      table.classList.add("report-table-compact");
    } else {
      const colgroup = document.createElement("colgroup");
      visibleColumns.forEach((col) => {
        const colEl = document.createElement("col");
        const width = suggestWidthCh(col, data.rows, mode);
        colEl.style.width = `${width}ch`;
        colEl.style.minWidth = `${Math.max(6, width - 2)}ch`;
        colgroup.appendChild(colEl);
      });
      table.appendChild(colgroup);
    }

    const thead = document.createElement("thead");
    const headerRow = document.createElement("tr");
    visibleColumns.forEach((col) => {
      const th = document.createElement("th");
      th.className = headerIsNumeric(col) ? "col-numeric" : "col-text";
      if (String(col) === "Код расценки") {
        th.classList.add("code-nowrap");
      }
      const isSortableHeader = mode === "default" && (headerIsNumeric(col) || String(col) === resourceAggregateColumn);
      if (isSortableHeader) {
        th.classList.add("sortable-header");
        th.title = `Нажмите, чтобы сортировать по "${col}"`;
        th.setAttribute("role", "button");
        th.setAttribute("tabindex", "0");
        const sortByColumn = () => setDetailSort(col);
        th.addEventListener("click", (event) => {
          if (event.target.closest(".resource-toggle-btn")) return;
          sortByColumn();
        });
        th.addEventListener("keydown", (event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            sortByColumn();
          }
        });
      }
      if (mode === "default" && (String(col) === "ФОТ" || String(col) === resourceAggregateColumn)) {
        const toggle = document.createElement("button");
        toggle.type = "button";
        toggle.className = "resource-toggle-btn";
        toggle.title = state.resourceMode === "collapsed"
          ? "Показать ФОТ, ЭМ, НР, СП и ОТм"
          : "Свернуть ФОТ, ЭМ, НР, СП и ОТм";
        toggle.setAttribute("aria-label", toggle.title);
        toggle.textContent = state.resourceMode === "collapsed" ? "›" : "‹";
        toggle.addEventListener("click", (event) => {
          event.preventDefault();
          event.stopPropagation();
          const willCollapse = state.resourceMode === "expanded";
          if (willCollapse && resourceMetricColumns.has(String(state.sort.column))) {
            state.sort.column = "";
            state.sort.direction = "desc";
          }
          state.resourceMode = state.resourceMode === "collapsed" ? "expanded" : "collapsed";
          updateSortControls();
          renderDetail();
        });
        th.appendChild(toggle);
      }
      if (mode === "default" && state.sort.column && String(col) === state.sort.column) {
        th.classList.add("sorted");
        th.dataset.sortDirection = state.sort.direction;
      }
      if (mode === "summary" && (col === "Раздел" || col === "Подраздел")) {
        th.classList.add("summary-wrap-column");
      }
      const headerLabel = escapeText(col).replace(/\n/g, "<br>");
      const headerText = document.createElement("span");
      headerText.className = "header-label";
      headerText.innerHTML = headerLabel;
      th.prepend(headerText);
      if (mode === "default" && state.sort.column && String(col) === state.sort.column) {
        const sortIndicator = document.createElement("span");
        sortIndicator.className = "sort-indicator";
        sortIndicator.textContent = state.sort.direction === "asc" ? "▲" : "▼";
        th.appendChild(sortIndicator);
      }
      if (mode === "default" && String(col) === "Наименование" && state.sort.column) {
        const resetButton = document.createElement("button");
        resetButton.type = "button";
        resetButton.className = "sort-reset-btn";
        resetButton.title = "Сброс сортировки";
        resetButton.setAttribute("aria-label", "Сброс сортировки");
        resetButton.textContent = "✕";
        resetButton.addEventListener("click", (event) => {
          event.preventDefault();
          event.stopPropagation();
          clearDetailSort();
        });
        th.appendChild(resetButton);
      }
      headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);

    const tbody = document.createElement("tbody");
    data.rows.forEach((row) => {
      if (mode === "default" && state.sort.column && isDividerDataRow(row)) {
        return;
      }
      const tr = document.createElement("tr");
      const rowClass = detectRowClass(data.columns, row);
      if (rowClass) tr.className = rowClass;
      const focusKey = String(
        row.__meta_focus_key || row.__meta_subsection_focus_key || row.__meta_section_focus_key || ""
      ).trim();
      if (focusKey) {
        tr.dataset.focusKey = focusKey;
      }
      const rowTooltip = mode === "default" && state.sort.column ? buildRowTooltip(row) : "";
      if (rowTooltip) {
        tr.title = rowTooltip;
        tr.setAttribute("aria-label", rowTooltip);
      }
      visibleColumns.forEach((col) => {
        if (mode === "summary" && String(col) === "Раздел" && row.__meta_hide_section) {
          return;
        }
        const td = document.createElement("td");
        const classes = [headerIsNumeric(col) ? "col-numeric" : "col-text"];
        if (col === "№") classes.push("col-numeric");
        if (col === "Наименование") classes.push("col-name");
        if (col === "Файл") classes.push("no-wrap");
        if (String(col) === "Код расценки") classes.push("code-nowrap");
        if (resourceMetricColumns.has(String(col)) || String(col) === resourceAggregateColumn) {
          const rendered = String(formatValue(col, String(col) === resourceAggregateColumn ? sumResourceValue(row) : row[col]) ?? "");
          if (rendered.length > 12) classes.push("resource-tight");
          if (rendered.length > 15) classes.push("resource-tighter");
        }
        if (mode === "summary" && (col === "Раздел" || col === "Подраздел")) {
          classes.push("summary-wrap-column");
        }
        if (String(col).startsWith("Разница")) {
          const diff = toNumber(row[col]);
          if (diff !== null) classes.push(diff > 0 ? "diff-positive" : diff < 0 ? "diff-negative" : "diff-zero");
        }
        td.className = classes.join(" ");
        if (rowTooltip) {
          td.title = rowTooltip;
        }
        if (mode === "summary" && (String(col) === "Раздел" || String(col) === "Подраздел") && String(row[col] ?? "").trim()) {
          const sectionKey = String(row.__meta_section_focus_key || row.__meta_focus_key || "").trim();
          const subsectionKey = String(row.__meta_subsection_focus_key || row.__meta_focus_key || sectionKey).trim();
          const key = String(col) === "Раздел" ? sectionKey : (subsectionKey || sectionKey);
          const button = document.createElement("button");
          button.type = "button";
          button.className = "summary-link";
          button.textContent = formatValue(col, row[col]);
          button.dataset.focusKey = key;
          button.addEventListener("click", () => openCustomerByFocusKey(button.dataset.focusKey));
          td.appendChild(button);
        } else {
          const value = String(col) === resourceAggregateColumn ? sumResourceValue(row) : row[col];
          td.innerHTML = escapeText(formatValue(col, value)).replace(/\n/g, "<br>");
        }
        if (mode === "summary" && String(col) === "Раздел") {
          const rowSpan = Number(row.__meta_row_span || 1);
          if (rowSpan > 1) {
            td.rowSpan = rowSpan;
          }
        }
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });

    table.appendChild(thead);
    table.appendChild(tbody);
    container.appendChild(table);
  }

  function buildSectionSummary(data) {
    if (!data?.rows?.length) return data;
    const columns = data.columns.filter((col) => col !== "Подраздел");
    const numericColumns = data.columns.filter((col) => headerIsNumeric(col));
    const groups = new Map();
    data.rows.forEach((row) => {
      const sectionKey = String(row.__meta_section_uid || row.__meta_section_focus_key || row["Раздел"] || "").trim();
      const sectionLabel = String(row["Раздел"] ?? "").trim();
      if (!groups.has(sectionKey)) {
        const base = {};
        columns.forEach((col) => {
          base[col] = col === "Раздел" ? sectionLabel : "";
        });
        base.__meta_section_uid = sectionKey;
        base.__meta_section_focus_key = row.__meta_section_focus_key || row.__meta_focus_key || sectionKey;
        base.__meta_subsection_focus_key = row.__meta_subsection_focus_key || row.__meta_focus_key || "";
        base.__meta_focus_key = base.__meta_section_focus_key || base.__meta_subsection_focus_key || "";
        groups.set(sectionKey, base);
      }
      const target = groups.get(sectionKey);
      numericColumns.forEach((col) => {
        const current = toNumber(target[col]) ?? 0;
        const incoming = toNumber(row[col]) ?? 0;
        target[col] = current + incoming;
      });
    });
    return {
      columns,
      rows: Array.from(groups.values()),
      row_count: groups.size,
    };
  }

  function renderSummaryWithBars(container, data) {
    if (!container) return;
    container.innerHTML = "";

    if (!data?.rows?.length) {
      const empty = document.createElement("p");
      empty.className = "summary";
      empty.textContent = "Нет строк для отображения.";
      container.appendChild(empty);
      return;
    }

    const layout = document.createElement("div");
    layout.className = "summary-layout";

    const tablePane = document.createElement("div");
    tablePane.className = "summary-table-pane";
    renderTable(tablePane, data, "", { mode: "summary" });

    const barsPane = document.createElement("div");
    barsPane.className = "summary-bars-pane summary-bars-pane-positive";

    const weightColumn = data.columns.find((col) => String(col).includes("Вес"));
    const values = weightColumn ? data.rows.map((row) => Math.max(0, toNumber(row[weightColumn]) ?? 0)) : [];
    const maxValue = Math.max(...values, 0);

    const barsTable = document.createElement("div");
    barsTable.className = "summary-bars-table";
    const barsBody = document.createElement("div");
    barsBody.className = "summary-bars-body";

    data.rows.forEach((row) => {
      const rowDiv = document.createElement("div");
      rowDiv.className = "summary-bar-row";
      const rowClass = detectRowClass(data.columns, row);
      if (rowClass) rowDiv.classList.add(rowClass);

      const cell = document.createElement("div");
      cell.className = "summary-bar-cell";

      const track = document.createElement("div");
      track.className = "summary-bar-track";

      const fill = document.createElement("div");
      fill.className = "summary-bar-fill positive";
      const value = weightColumn ? Math.max(0, toNumber(row[weightColumn]) ?? 0) : 0;
      const width = maxValue > 0 ? Math.max(3, Math.round((value / maxValue) * 100)) : 0;
      fill.style.width = `${width}%`;
      track.appendChild(fill);
      cell.appendChild(track);
      rowDiv.appendChild(cell);
      barsBody.appendChild(rowDiv);
    });

    barsTable.appendChild(barsBody);
    barsPane.appendChild(barsTable);
    layout.appendChild(tablePane);
    layout.appendChild(barsPane);
    container.appendChild(layout);

    requestAnimationFrame(() => {
      const mainTable = tablePane.querySelector("table");
      const header = mainTable?.querySelector("thead");
      if (header) {
        barsPane.style.setProperty("--summary-header-height", `${header.offsetHeight}px`);
      }
      const tableRows = Array.from(mainTable?.querySelectorAll("tbody tr") || []);
      const barRows = Array.from(barsBody.querySelectorAll(".summary-bar-row"));
      const count = Math.min(tableRows.length, barRows.length);
      for (let i = 0; i < count; i += 1) {
        tableRows[i].style.height = "";
        barRows[i].style.height = "";
      }
      for (let i = 0; i < count; i += 1) {
        const height = Math.max(tableRows[i].offsetHeight, barRows[i].offsetHeight);
        tableRows[i].style.height = `${height}px`;
        barRows[i].style.height = `${height}px`;
      }
    });
  }

  function renderDetail() {
    if (!state.detail) return;
    const viewData = getDetailViewData(state.detail);
    renderTable(dom.detail, viewData, "Customer", { mode: "default", resourceMode: state.resourceMode });
    updateSortControls();
  }

  function renderSummary() {
    if (!state.summary) return;
    const source = state.summaryMode === "section" ? buildSectionSummary(state.summary) : state.summary;
    renderSummaryWithBars(dom.summary, source);
    if (dom.summaryBySubsection) {
      dom.summaryBySubsection.classList.toggle("active", state.summaryMode !== "section");
    }
    if (dom.summaryBySection) {
      dom.summaryBySection.classList.toggle("active", state.summaryMode === "section");
    }
  }

  function openCustomerByFocusKey(focusKey) {
    if (!focusKey) return;
    if (state.sort.column) {
      state.sort.column = "";
      state.sort.direction = "desc";
      updateSortControls();
      renderDetail();
    }
    activateSheet("process2-detail");
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

  function activateSheet(targetId) {
    sheetOrder.forEach((id) => {
      const panel = document.getElementById(id);
      if (panel) panel.hidden = id !== targetId;
    });
    document.querySelectorAll("[data-process2-sheet-target]").forEach((button) => {
      button.classList.toggle("active", button.dataset.process2SheetTarget === targetId);
    });
    if (dom.filterBar) {
      dom.filterBar.hidden = targetId !== "process2-detail";
    }
    if (dom.sortBar) {
      dom.sortBar.hidden = targetId !== "process2-detail" || !state.detail?.columns?.length;
    }
    if (dom.summaryBar) {
      dom.summaryBar.hidden = targetId !== "process2-summary";
    }
    if (targetId === "process2-summary") {
      requestAnimationFrame(renderSummary);
    }
  }

  function showStatus(message, tone = "info") {
    if (!dom.status) return;
    dom.status.textContent = message;
    dom.status.dataset.tone = tone;
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
    if (state.file) {
      formData.append("file", state.file);
    }
    if (state.materials) {
      formData.append("materials", state.materials);
    }

    try {
      const payload = await fetchJson("/api/process2", formData);
      state.reportId = payload.report_id;
      state.detail = payload.detail;
      state.summary = payload.summary;
      state.info = payload.info;
      state.unitDiff = payload.unit_diff;
      state.files = payload.files;
      state.filter = "";
      state.summaryMode = "subsection";
      state.resourceMode = "collapsed";
      state.sort.column = "";
      state.sort.direction = "desc";
      if (dom.filterInput) dom.filterInput.value = "";

      showStatus(`Строк: ${payload.row_count}, общая стоимость: ${Number(payload.total_cost).toLocaleString()} ₽`, "success");

      renderDetail();
      renderSummary();
      renderTable(dom.info, payload.info, "Инфо", { mode: "info" });
      renderTable(dom.unitDiff, payload.unit_diff, "Отличается ед. изм.", { mode: "unit_diff" });
      renderTable(dom.files, payload.files, "Файлы", { mode: "files" });

      if (dom.tabs) dom.tabs.hidden = false;
      if (dom.filterBar) dom.filterBar.hidden = false;
      if (dom.sortBar) dom.sortBar.hidden = false;
      if (dom.summaryBar) dom.summaryBar.hidden = false;
      updateSortControls();
      activateSheet("process2-detail");
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
    const response = await fetch(`/api/process2/export/${format}`, { method: "POST", body: formData });
    if (!response.ok) {
      showStatus((await response.text()) || "Не удалось сформировать файл.", "error");
      return;
    }
    const blob = await response.blob();
    const suggested = response.headers.get("content-disposition")?.match(/filename="?([^";]+)"?/)?.[1] || `process2_${format}`;
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

    const fileInput = dom.form.querySelector("input[name='file']");
    const materialsInput = dom.form.querySelector("input[name='materials']");
    if (fileInput) {
      fileInput.addEventListener("change", (event) => {
        state.file = event.target.files[0] || null;
      });
    }
    if (materialsInput) {
      materialsInput.addEventListener("change", (event) => {
        state.materials = event.target.files[0] || null;
      });
    }
    dom.form.addEventListener("submit", handleSubmit);

    document.querySelectorAll("[data-process2-sheet-target]").forEach((button) => {
      button.addEventListener("click", (event) => {
        event.preventDefault();
        activateSheet(button.dataset.process2SheetTarget);
      });
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
    if (dom.sortColumn) {
      dom.sortColumn.addEventListener("change", (event) => {
        setDetailSort(event.target.value);
      });
    }
    if (dom.sortDirection) {
      dom.sortDirection.addEventListener("click", () => {
        if (!state.sort.column) return;
        state.sort.direction = state.sort.direction === "asc" ? "desc" : "asc";
        updateSortControls();
        renderDetail();
      });
    }
    if (dom.sortReset) {
      dom.sortReset.addEventListener("click", () => {
        clearDetailSort();
      });
    }
    if (dom.summaryBySubsection) {
      dom.summaryBySubsection.addEventListener("click", () => {
        state.summaryMode = "subsection";
        renderSummary();
      });
    }
    if (dom.summaryBySection) {
      dom.summaryBySection.addEventListener("click", () => {
        state.summaryMode = "section";
        renderSummary();
      });
    }

    document.querySelectorAll("[data-process2-export]").forEach((button) => {
      button.addEventListener("click", () => handleExport(button.dataset.process2Export));
    });
  });
})();

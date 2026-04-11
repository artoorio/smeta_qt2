(function () {
  const RESOURCE_METRIC_COLUMNS = new Set(["ФОТ", "ЭМ", "НР", "СП", "ОТм"]);
  const RESOURCE_AGGREGATE_COLUMN = "ФОТ/ЭМ/НР/СП/ОТм";

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
      RESOURCE_AGGREGATE_COLUMN,
    ].some((token) => String(col).includes(token));
  }

  function headerIsCode(col) {
    return ["№", "Код расценки", "Ед.изм.", "Единица измерения"].includes(String(col));
  }

    function isQuantityHeader(col) {
      return ["Кол-во", "Количество"].some((token) => String(col).includes(token));
    }

    function isCostLikeHeader(col) {
      return headerIsNumeric(col) && !isQuantityHeader(col) && String(col) !== "№";
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
        return num.toLocaleString("ru-RU", { maximumFractionDigits: 15 });
      }
      const hasFraction = Math.abs(num % 1) > 0.000001;
      return num.toLocaleString("ru-RU", {
        minimumFractionDigits: hasFraction ? 2 : 0,
        maximumFractionDigits: hasFraction ? 2 : 0,
      });
    }
    return String(value ?? "");
  }

  function normalizeCopiedText(text) {
    return String(text ?? "").replace(/(?<=\d)[\s\u00A0\u202F](?=\d)/g, "");
  }

  function getCellCopyValue(cell) {
    if (!cell) return "";
    return String(cell.dataset.copyValue ?? cell.textContent ?? "")
      .replace(/\r?\n/g, " ")
      .trimEnd();
  }

  function buildClipboardTextFromTable(table, selection) {
    if (!table || !selection || selection.rangeCount === 0) return "";
    const range = selection.getRangeAt(0);
    const rows = Array.from(table.querySelectorAll("thead tr, tbody tr"));
    const lines = [];
    rows.forEach((row) => {
      const cells = Array.from(row.querySelectorAll("th, td"));
      const selectedCells = cells.filter((cell) => range.intersectsNode(cell));
      if (!selectedCells.length) return;
      lines.push(selectedCells.map((cell) => getCellCopyValue(cell)).join("\t"));
    });
    return lines.join("\n");
  }

  function compareNumericRows(a, b, column, direction = "desc") {
    const av = String(column) === RESOURCE_AGGREGATE_COLUMN ? sumResourceValue(a) : toNumber(a?.[column]);
    const bv = String(column) === RESOURCE_AGGREGATE_COLUMN ? sumResourceValue(b) : toNumber(b?.[column]);
    if (av === null && bv === null) return (a.__meta_sort_index ?? 0) - (b.__meta_sort_index ?? 0);
    if (av === null) return 1;
    if (bv === null) return -1;
    let result = av - bv;
    if (direction === "desc") result *= -1;
    if (result !== 0) return result;
    return (a.__meta_sort_index ?? 0) - (b.__meta_sort_index ?? 0);
  }

  function sumResourceValue(row) {
    let total = 0;
    let hasValue = false;
    Array.from(RESOURCE_METRIC_COLUMNS).forEach((col) => {
      const num = toNumber(row?.[col]);
      if (num !== null) {
        total += num;
        hasValue = true;
      }
    });
    return hasValue ? total : null;
  }

  function resourceColumnsPresent(columns) {
    return columns.some((col) => RESOURCE_METRIC_COLUMNS.has(String(col)));
  }

  function buildVisibleColumns(columns, resourceMode = "collapsed") {
    if (!resourceColumnsPresent(columns)) return columns.slice();
    if (resourceMode !== "collapsed") return columns.slice();
    const visible = [];
    let inserted = false;
    columns.forEach((col) => {
      if (RESOURCE_METRIC_COLUMNS.has(String(col))) {
        if (!inserted) {
          visible.push(RESOURCE_AGGREGATE_COLUMN);
          inserted = true;
        }
        return;
      }
      visible.push(col);
    });
    return visible;
  }

    function suggestWidthCh(col, rows, options = {}) {
      const mode = options.mode || "default";
      const lines = String(col).split("\n");
      const widthRows = isCostLikeHeader(col) ? rows : rows.slice(0, 200);
      widthRows.forEach((row) => {
        const value = String(col) === RESOURCE_AGGREGATE_COLUMN ? sumResourceValue(row) : row[col];
        formatValue(col, value).split("\n").forEach((line) => lines.push(line));
      });
      const maxLen = lines.reduce((acc, line) => Math.max(acc, line.length), 0);
    if (String(col) === RESOURCE_AGGREGATE_COLUMN || RESOURCE_METRIC_COLUMNS.has(String(col))) {
      const maxWidth = mode === "summary" ? 28 : 34;
      return Math.max(12, Math.min(maxWidth, maxLen + 4));
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
    if (headerIsNumeric(col)) {
      const maxWidth = mode === "summary" ? 14 : 20;
      return Math.max(6, Math.min(maxWidth, maxLen + 2));
    }
    if (["Ед.изм.", "Единица измерения", "Ед.изм.\n(Проект)", "Ед.изм.\n(Факт)"].includes(String(col))) {
      return Math.max(6, Math.min(14, maxLen + 2));
    }
    if (headerIsCode(col)) {
      const minWidth = Math.max(6, 18 - Number(options.codeWidthAdjustCh || 0));
      return Math.max(minWidth, Math.min(20, maxLen + 1));
    }
    return Math.max(10, Math.min(mode === "summary" ? 22 : 36, maxLen + 2));
  }

    function detectRowClass(columns, row) {
      if (row.__meta_row_type === "total") return "row-total";
      if (row.__meta_row_type === "subdivider") return "row-subdivider";
      if (row.__meta_row_type === "divider") return "row-divider";
      const values = columns.map((col) => String(row[col] ?? "").trim());
      if (values.some((value) => value.startsWith("--"))) return "row-divider";
      return "";
    }

    function buildSummaryTotalRow(data) {
      if (!data?.rows?.length) return null;
      const totalRow = {};
      data.columns.forEach((col) => {
        if (headerIsNumeric(col)) {
          totalRow[col] = data.rows.reduce((sum, row) => sum + (toNumber(row?.[col]) ?? 0), 0);
        } else {
          totalRow[col] = "";
        }
      });
      const labelColumn = data.columns.includes("Файл")
        ? "Файл"
        : (data.columns.includes("Раздел") ? "Раздел" : data.columns[0]);
      if (labelColumn) totalRow[labelColumn] = "Итого";
      totalRow.__meta_row_type = "total";
      totalRow.__meta_focus_key = "";
      totalRow.__meta_section_focus_key = "";
      totalRow.__meta_subsection_focus_key = "";
      return totalRow;
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
          base.__meta_focus_key = row.__meta_section_focus_key || row.__meta_focus_key || "";
          base.__meta_section_focus_key = row.__meta_section_focus_key || row.__meta_focus_key || "";
          base.__meta_subsection_focus_key = "";
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

    function mergeSummaryColumnCells(container, data, columnName) {
      const table = container?.querySelector("table");
      if (!table) return;
      const targetIndex = Array.isArray(data?.columns) ? data.columns.indexOf(columnName) : -1;
      if (targetIndex < 0) return;
      const bodyRows = Array.from(table.querySelectorAll("tbody tr"));
      if (!bodyRows.length) return;
      let lastCell = null;
      let lastValue = "";
      let span = 1;
      bodyRows.forEach((row) => {
        const cells = Array.from(row.querySelectorAll("td"));
        const cell = cells[targetIndex];
        if (!cell) return;
        const value = cell.textContent.trim();
        if (lastCell && value === lastValue) {
          span += 1;
          lastCell.rowSpan = span;
          cell.remove();
        } else {
          lastCell = cell;
          lastValue = value;
          span = 1;
        }
      });
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

  function renderTable(container, data, options = {}) {
    if (!container) return;
    container.innerHTML = "";
    const mode = options.mode || "default";
    const sortState = options.sortState || null;
    const onSortChange = typeof options.onSortChange === "function" ? options.onSortChange : null;
    const onSortReset = typeof options.onSortReset === "function" ? options.onSortReset : null;
    const onFocusKey = typeof options.onFocusKey === "function" ? options.onFocusKey : null;
    const rowTooltip = typeof options.rowTooltip === "function" ? options.rowTooltip : null;
    const resourceMode = options.resourceMode || "collapsed";
    const isSortableTable = mode === "default" && sortState;
    const hideDividerRowsWhenSorted = Boolean(options.hideDividerRowsWhenSorted);

    if (!data?.rows?.length) {
      const empty = document.createElement("p");
      empty.className = "summary";
      empty.textContent = "Нет строк для отображения.";
      container.appendChild(empty);
      return;
    }

    const table = document.createElement("table");
    table.className = "report-table";
    let hasResourceGroup = false;
    if (mode === "summary" || mode === "files") {
      table.classList.add("report-table-compact");
    } else {
      const baseColumns = data.columns.filter((col) => {
        const name = String(col);
        if (name === "Вспомогательные ресурсы" || name === "Оборудование") {
          return data.rows.some((row) => String(row?.[col] ?? "").trim() !== "");
        }
        return true;
      });
      hasResourceGroup = resourceColumnsPresent(baseColumns) || baseColumns.includes(RESOURCE_AGGREGATE_COLUMN);
      const visibleColumns = mode === "default" ? buildVisibleColumns(baseColumns, resourceMode) : baseColumns;
      const colgroup = document.createElement("colgroup");
      visibleColumns.forEach((col) => {
        const colEl = document.createElement("col");
        const width = suggestWidthCh(col, data.rows, options);
        colEl.style.width = `${width}ch`;
        colEl.style.minWidth = `${Math.max(6, width - 2)}ch`;
        colgroup.appendChild(colEl);
      });
      table.appendChild(colgroup);
      data = { ...data, columns: visibleColumns };
    }

    const thead = document.createElement("thead");
    const headerRow = document.createElement("tr");
    data.columns.forEach((col) => {
      const th = document.createElement("th");
      const classes = [headerIsNumeric(col) ? "col-numeric" : "col-text"];
      const isResourceColumn = String(col) === RESOURCE_AGGREGATE_COLUMN || RESOURCE_METRIC_COLUMNS.has(String(col));
      if (col === "№") classes.push("col-numeric");
      if (col === "Наименование") classes.push("col-name");
      if (col === "Файл") classes.push("no-wrap");
      if (mode === "summary" && (col === "Раздел" || col === "Подраздел")) {
        classes.push("summary-wrap-column");
      }
      if (isSortableTable && headerIsNumeric(col)) {
        classes.push("sortable-header");
        th.setAttribute("role", "button");
        th.setAttribute("tabindex", "0");
        th.title = `Нажмите, чтобы сортировать по "${col}"`;
        const sortByColumn = () => onSortChange && onSortChange(col);
        th.addEventListener("click", (event) => {
          if (event.target.closest(".sort-reset-btn") || event.target.closest(".resource-toggle-btn")) return;
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
      th.dataset.copyValue = String(col);
      const headerText = document.createElement("span");
      headerText.className = "header-label";
      headerText.innerHTML = escapeText(col).replace(/\n/g, "<br>");
      th.appendChild(headerText);
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
      if (mode === "default" && hasResourceGroup && typeof options.onResourceToggle === "function") {
        const aggregateColumnVisible = data.columns.includes(RESOURCE_AGGREGATE_COLUMN);
        const isToggleColumn = (aggregateColumnVisible && String(col) === RESOURCE_AGGREGATE_COLUMN) || (!aggregateColumnVisible && String(col) === "ФОТ");
        if (isToggleColumn) {
          const collapsed = resourceMode === "collapsed";
          th.classList.add("resource-toggle-header");
          th.title = collapsed ? "Показать ФОТ, ЭМ, НР, СП и ОТм" : "Свернуть ФОТ, ЭМ, НР, СП и ОТм";
          const toggle = document.createElement("button");
          toggle.type = "button";
          toggle.className = "resource-toggle-btn";
          toggle.title = collapsed ? "Показать ФОТ, ЭМ, НР, СП и ОТм" : "Свернуть ФОТ, ЭМ, НР, СП и ОТм";
          toggle.setAttribute("aria-label", toggle.title);
          toggle.textContent = collapsed ? "›" : "‹";
          toggle.addEventListener("click", (event) => {
            event.preventDefault();
            event.stopPropagation();
            options.onResourceToggle();
          });
          th.appendChild(toggle);
        }
      }
      headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);

    const tbody = document.createElement("tbody");
    data.rows.forEach((row) => {
      if (isSortableTable && sortState.column && hideDividerRowsWhenSorted && (row.__meta_row_type === "divider" || row.__meta_row_type === "subdivider")) {
        return;
      }
      const tr = document.createElement("tr");
      const rowClass = detectRowClass(data.columns, row);
      if (rowClass) tr.className = rowClass;
      const focusKey = String(row.__meta_focus_key || row.__meta_subsection_focus_key || row.__meta_section_focus_key || "").trim();
      if (focusKey) {
        tr.dataset.focusKey = focusKey;
      }
      const tooltip = rowTooltip ? rowTooltip(row) : "";
      if (tooltip) {
        tr.title = tooltip;
        tr.setAttribute("aria-label", tooltip);
      }
      data.columns.forEach((col) => {
        if (mode === "summary" && String(col) === "Раздел" && row.__meta_hide_section) return;
        const td = document.createElement("td");
        const classes = [headerIsNumeric(col) ? "col-numeric" : "col-text"];
        const category = String(row.__meta_category ?? row["Категория"] ?? "").trim();
        if (col === "№") classes.push("col-numeric");
        if (col === "Наименование") classes.push("col-name");
        if (col === "Наименование" && mode === "default" && category.toLowerCase().startsWith("материал")) {
          classes.push("customer-material-indent");
          td.style.setProperty("--material-indent", options.materialIndentPx || "2.6rem");
        }
        if (col === "Файл") classes.push("no-wrap");
        if (String(col).startsWith("Разница")) {
          const diff = toNumber(row[col]);
          if (diff !== null) classes.push(diff > 0 ? "diff-positive" : diff < 0 ? "diff-negative" : "diff-zero");
        }
        if (mode === "summary" && (col === "Раздел" || col === "Подраздел")) {
          classes.push("summary-wrap-column");
        }
        td.className = classes.join(" ");
        if (tooltip) {
          td.title = tooltip;
        }
        if (mode === "summary" && (String(col) === "Раздел" || String(col) === "Подраздел") && String(row[col] ?? "").trim() && onFocusKey) {
          const button = document.createElement("button");
          button.type = "button";
          button.className = "summary-link";
          button.textContent = formatValue(col, row[col]);
          button.dataset.focusKey = String(row.__meta_focus_key ?? "").trim();
          button.addEventListener("click", () => onFocusKey(button.dataset.focusKey));
          td.appendChild(button);
        } else {
          const value = String(col) === RESOURCE_AGGREGATE_COLUMN ? sumResourceValue(row) : row[col];
          const copied = headerIsNumeric(col)
            ? String(formatValue(col, value)).replace(/[\s\u00A0\u202F]/g, "")
            : String(value ?? "");
          td.dataset.copyValue = copied;
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
    table.addEventListener("copy", (event) => {
      const selection = window.getSelection?.();
      const sanitized = buildClipboardTextFromTable(table, selection);
      if (!sanitized) return;
      if (event.clipboardData) {
        event.preventDefault();
        event.clipboardData.setData("text/plain", sanitized);
      }
    });
    container.appendChild(table);
  }

    function renderSummaryWithBars(container, data, options = {}) {
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

      const barColumn = options.barColumn
        || data.columns.find((col) => String(col).includes("Разница") && String(col).includes("Ст-ть"))
        || data.columns.find((col) => String(col).includes("Ст-ть"))
        || data.columns.find((col) => headerIsNumeric(col));
      const totalRow = buildSummaryTotalRow(data);
      const rows = totalRow ? [...data.rows, totalRow] : data.rows.slice();
      const values = barColumn ? data.rows.map((row) => toNumber(row[barColumn]) ?? 0) : [];
      const maxPos = Math.max(...values.map((value) => Math.max(0, value)), 0);
      const maxNegAbs = Math.max(...values.map((value) => Math.max(0, -value)), 0);
      const zeroPosition = (maxPos + maxNegAbs) > 0 ? (maxNegAbs / (maxPos + maxNegAbs)) * 100 : 0;

    const tablePane = document.createElement("div");
    tablePane.className = "summary-table-pane";
    const barsPane = document.createElement("div");
    barsPane.className = "summary-bars-pane";
    barsPane.classList.add("summary-bars-pane-show-axis");
    barsPane.style.setProperty("--summary-zero-position", `${zeroPosition}%`);

      renderTable(tablePane, totalRow ? { ...data, rows } : data, {
        mode: "summary",
        sortState: options.sortState || null,
        onSortChange: options.onSortChange || null,
        onSortReset: options.onSortReset || null,
        onFocusKey: options.onFocusKey || null,
        rowTooltip: options.rowTooltip || null,
        hideDividerRowsWhenSorted: false,
      });
      if (options.mergeColumnName) {
        mergeSummaryColumnCells(tablePane, data, options.mergeColumnName);
      }

    const barsTable = document.createElement("div");
    barsTable.className = "summary-bars-table";
    const barsBody = document.createElement("div");
    barsBody.className = "summary-bars-body";

      rows.forEach((row) => {
        const rowDiv = document.createElement("div");
        rowDiv.className = "summary-bar-row";
        const rowClass = detectRowClass(data.columns, row);
        if (rowClass) rowDiv.classList.add(rowClass);

        const cell = document.createElement("div");
        cell.className = "summary-bar-cell";
        const track = document.createElement("div");
        track.className = "summary-bar-track";
        const value = barColumn ? (toNumber(row[barColumn]) ?? 0) : 0;
        if (row.__meta_row_type !== "total") {
          const bar = document.createElement("div");
          bar.className = "summary-bar-fill";
          if (value < 0) {
            const width = maxNegAbs > 0 ? Math.max(3, Math.round((Math.abs(value) / maxNegAbs) * zeroPosition)) : 0;
            bar.classList.add("negative");
            bar.style.right = `${100 - zeroPosition}%`;
            bar.style.width = `${width}%`;
          } else {
            const positiveSpan = 100 - zeroPosition;
            const width = maxPos > 0 ? Math.max(3, Math.round((Math.max(0, value) / maxPos) * positiveSpan)) : 0;
            bar.classList.add("positive");
            bar.style.left = `${zeroPosition}%`;
            bar.style.width = `${width}%`;
          }
          track.appendChild(bar);
        }

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
      syncSummaryRowHeights(mainTable, barsBody);
    });
  }

  window.ReportCore = {
    RESOURCE_METRIC_COLUMNS,
    RESOURCE_AGGREGATE_COLUMN,
    escapeText,
    headerIsNumeric,
    headerIsCode,
    isQuantityHeader,
    toNumber,
    formatValue,
    normalizeCopiedText,
    compareNumericRows,
    sumResourceValue,
    resourceColumnsPresent,
    buildVisibleColumns,
    suggestWidthCh,
    detectRowClass,
    filterRows,
    buildSectionSummary,
      mergeSummaryColumnCells,
    syncSummaryRowHeights,
    renderTable,
    renderSummaryWithBars,
  };
})();

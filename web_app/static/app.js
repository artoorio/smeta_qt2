const state = {
  processFiles: [],
  materialsFile: null,
  compareFiles: { project: null, fact: null },
  reportId: null,
};

const dom = {
  processForm: document.getElementById("process-form"),
  processSummary: document.getElementById("process-summary"),
  processTable: document.getElementById("process-table"),
  compareForm: document.getElementById("compare-form"),
  compareMissing: document.getElementById("compare-missing"),
  compareFiles: document.getElementById("compare-files"),
  compareDetail: document.getElementById("compare-detail"),
  compareSummary: document.getElementById("compare-summary-table"),
  compareInfo: document.getElementById("compare-info"),
  compareUnitDiff: document.getElementById("compare-unit-diff"),
  compareSheetTabs: document.getElementById("compare-sheet-tabs"),
  materialsTable: document.getElementById("materials-table"),
  materialsForm: document.getElementById("materials-form"),
  materialsSummary: document.getElementById("materials-summary"),
};

const compareSheetOrder = [
  "compare-detail",
  "compare-summary-table",
  "compare-info",
  "compare-unit-diff",
  "compare-files",
];

function escapeText(value) {
  return String(value ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function headerIsNumeric(col) {
  return ["Кол-во", "Количество", "Ст-ть", "Стоимость", "Разница", "Материалы", "Общая стоимость"].some((token) =>
    String(col).includes(token)
  );
}

function headerIsCode(col) {
  return ["№", "Код расценки", "Ед.изм.", "Единица измерения"].includes(String(col));
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
    const hasFraction = Math.abs(num % 1) > 0.000001;
    return num.toLocaleString("ru-RU", {
      minimumFractionDigits: hasFraction ? 2 : 0,
      maximumFractionDigits: hasFraction ? 2 : 0,
    });
  }
  return String(value ?? "");
}

function suggestWidthCh(col, rows) {
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
  if (col === "№") {
    return Math.max(6, Math.min(10, maxLen + 2));
  }
  if (col === "Наименование") {
    return Math.max(24, Math.min(80, Math.ceil(maxLen / 4) + 2));
  }
  if (col === "Подраздел") {
    return Math.max(18, Math.min(36, Math.ceil(maxLen / 3) + 2));
  }
  if (col === "Файл") {
    return Math.max(30, Math.min(120, maxLen + 2));
  }
  if (compareMetricColumns.has(String(col))) {
    return Math.max(6, Math.min(20, maxLen + 2));
  }
  if (headerIsNumeric(col)) {
    return Math.max(6, Math.min(20, maxLen + 2));
  }
  if (["Ед.изм.", "Единица измерения", "Ед.изм.\n(Проект)", "Ед.изм.\n(Факт)"].includes(String(col))) {
    return Math.max(6, Math.min(14, maxLen + 2));
  }
  if (headerIsCode(col)) {
    return Math.max(8, Math.min(22, maxLen + 2));
  }
  return Math.max(12, Math.min(36, maxLen + 2));
}

function detectRowClass(columns, row) {
  const values = columns.map((col) => String(row[col] ?? "").trim());
  if (values.some((value) => value.startsWith("--"))) return "row-divider";
  const category = String(row["Категория"] ?? "").trim();
  const quantity = toNumber(row["Количество"]);
  if (category === "Работа") return "row-work";
  if (category === "Материалы") return quantity !== null && quantity < 0 ? "row-material-negative" : "row-material";
  if (category === "Механизмы") return "row-machinery";
  return "";
}

function renderTable(container, data, title = "") {
  if (!container) return;
  container.innerHTML = "";
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
  const colgroup = document.createElement("colgroup");
  data.columns.forEach((col) => {
    const colEl = document.createElement("col");
    const width = suggestWidthCh(col, data.rows);
    colEl.style.width = `${width}ch`;
    colEl.style.minWidth = `${Math.max(10, width - 2)}ch`;
    colgroup.appendChild(colEl);
  });
  table.appendChild(colgroup);
  const thead = document.createElement("thead");
  const headerRow = document.createElement("tr");
  data.columns.forEach((col) => {
    const th = document.createElement("th");
    th.className = headerIsNumeric(col) ? "col-numeric" : "col-text";
    th.innerHTML = escapeText(col).replace(/\n/g, "<br>");
    headerRow.appendChild(th);
  });
  thead.appendChild(headerRow);
  const tbody = document.createElement("tbody");
  data.rows.forEach((row) => {
    const tr = document.createElement("tr");
    const rowClass = detectRowClass(data.columns, row);
    if (rowClass) tr.className = rowClass;
    data.columns.forEach((col) => {
      const td = document.createElement("td");
      const classes = [headerIsNumeric(col) ? "col-numeric" : "col-text"];
      if (col === "Наименование") classes.push("col-name");
      if (col === "Файл") classes.push("no-wrap");
      if (String(col).startsWith("Разница")) {
        const diff = toNumber(row[col]);
        if (diff !== null) classes.push(diff > 0 ? "diff-positive" : diff < 0 ? "diff-negative" : "diff-zero");
      }
      td.className = classes.join(" ");
      td.innerHTML = escapeText(formatValue(col, row[col])).replace(/\n/g, "<br>");
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(thead);
  table.appendChild(tbody);
  container.appendChild(table);
}

function showProcessStatus(message, tone = "info") {
  if (!dom.processSummary) return;
  dom.processSummary.textContent = message;
  dom.processSummary.dataset.tone = tone;
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
    renderTable(container, payload, "Материалы");
  } catch (error) {
    container.innerHTML = `<p class='summary'>${error.message}</p>`;
  }
}

async function handleMaterialsSubmit(event) {
  if (!dom.materialsForm) return;
  event.preventDefault();
  const formData = new FormData(dom.materialsForm);
  const payload = {
    file_name: formData.get("fileName") || "manual",
    code: String(formData.get("code") || "").trim(),
    name: String(formData.get("name") || "").trim(),
    unit: String(formData.get("unit") || "").trim(),
    quantity: formData.get("quantity") ? Number(formData.get("quantity")) : undefined,
    cost: formData.get("cost") ? Number(formData.get("cost")) : undefined,
    materials: formData.get("materials") ? Number(formData.get("materials")) : undefined,
    category: String(formData.get("category") || "Материалы").trim() || "Материалы",
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
    renderTable(dom.processTable, payload, "Обработанные строки");
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

async function handleCompareSubmit(event) {
  event.preventDefault();
  if (!dom.compareForm) return;
  if (!state.compareFiles.project || !state.compareFiles.fact) {
    if (dom.compareMissing) {
      dom.compareMissing.textContent = "Загрузите оба файла для сравнения.";
    }
    return;
  }
  const formData = new FormData();
  formData.append("files", state.compareFiles.project);
  formData.append("files", state.compareFiles.fact);
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
  try {
    const payload = await fetchJson("/api/compare", formData);
    state.reportId = payload.report_id;
    renderTable(dom.compareFiles, payload.files, "Файлы");
    renderTable(dom.compareDetail, payload.detail, "Customer");
    renderTable(dom.compareSummary, payload.summary, "Summary");
    renderTable(dom.compareInfo, payload.info, "Инфо");
    renderTable(dom.compareUnitDiff, payload.unit_diff, "Отличается единица измерения");
    renderMissingList(payload.missing);
    if (dom.compareSheetTabs) {
      dom.compareSheetTabs.hidden = false;
      activateSheet("compare-detail");
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
    filesInput.addEventListener("change", (event) => {
      state.processFiles = Array.from(event.target.files);
    });
    materialsInput.addEventListener("change", (event) => {
      state.materialsFile = event.target.files[0] || null;
    });
    dom.processForm.addEventListener("submit", handleProcessSubmit);
  }
  document.querySelectorAll("[data-export-mode]").forEach((button) => {
    button.addEventListener("click", () => handleProcessExport(button.dataset.exportMode));
  });

  if (dom.compareForm) {
    const projectInput = dom.compareForm.querySelector("input[name='project']");
    const factInput = dom.compareForm.querySelector("input[name='fact']");
    projectInput.addEventListener("change", (event) => {
      state.compareFiles.project = event.target.files[0] || null;
    });
    factInput.addEventListener("change", (event) => {
      state.compareFiles.fact = event.target.files[0] || null;
    });
    dom.compareForm.addEventListener("submit", handleCompareSubmit);
  }
  if (dom.compareSheetTabs) {
    dom.compareSheetTabs.querySelectorAll(".sheet-tab").forEach((button) => {
      button.addEventListener("click", () => activateSheet(button.dataset.sheetTarget));
    });
  }
  document.querySelectorAll("[data-compare-export]").forEach((button) => {
    button.addEventListener("click", () => handleCompareExport(button.dataset.compareExport));
  });
  if (dom.materialsForm) {
    dom.materialsForm.addEventListener("submit", handleMaterialsSubmit);
  }
  loadMaterialsView();
});

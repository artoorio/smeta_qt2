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
  compareDetail: document.getElementById("compare-detail"),
  compareSummary: document.getElementById("compare-summary-table"),
  materialsTable: document.getElementById("materials-table"),
  materialsForm: document.getElementById("materials-form"),
  materialsSummary: document.getElementById("materials-summary"),
};

function escapeText(value) {
  return String(value ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function renderTable(container, data) {
  if (!container) return;
  container.innerHTML = "";
  if (!data?.rows?.length) {
    container.innerHTML = "<p class='summary'>Нет строк для отображения.</p>";
    return;
  }
  const table = document.createElement("table");
  const thead = document.createElement("thead");
  const headerRow = document.createElement("tr");
  data.columns.forEach((col) => {
    const th = document.createElement("th");
    th.textContent = col;
    headerRow.appendChild(th);
  });
  thead.appendChild(headerRow);
  const tbody = document.createElement("tbody");
  data.rows.forEach((row) => {
    const tr = document.createElement("tr");
    data.columns.forEach((col) => {
      const td = document.createElement("td");
      td.textContent = escapeText(row[col]);
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
    renderTable(container, payload);
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
    renderTable(dom.processTable, payload);
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
    renderTable(dom.compareDetail, payload.detail);
    renderTable(dom.compareSummary, payload.summary);
    renderMissingList(payload.missing);
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
  document.querySelectorAll("[data-compare-export]").forEach((button) => {
    button.addEventListener("click", () => handleCompareExport(button.dataset.compareExport));
  });
  if (dom.materialsForm) {
    dom.materialsForm.addEventListener("submit", handleMaterialsSubmit);
  }
  loadMaterialsView();
});

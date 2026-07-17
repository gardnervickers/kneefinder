"use strict";

const runs = new Map();
const results = new Map();
const pending = new Map();
let selectedRunId = null;
let socket = null;
let nextRequestId = 1;

const $ = (id) => document.getElementById(id);
const svgNs = "http://www.w3.org/2000/svg";

function connect() {
  const scheme = location.protocol === "https:" ? "wss" : "ws";
  socket = new WebSocket(`${scheme}://${location.host}/api/v1/ws`);
  setConnection("Connecting", "");
  socket.addEventListener("open", () => setConnection("Live", "online"));
  socket.addEventListener("close", () => {
    setConnection("Reconnecting", "offline");
    setTimeout(connect, 1200);
  });
  socket.addEventListener("message", ({ data }) => {
    try { handleMessage(JSON.parse(data)); }
    catch (error) { showMessage(`Invalid server message: ${error.message}`, true); }
  });
}

function setConnection(text, state) {
  $("connection-text").textContent = text;
  const element = document.querySelector(".connection");
  element.classList.remove("online", "offline");
  if (state) element.classList.add(state);
  updateButtons();
}

function handleMessage(message) {
  switch (message.type) {
    case "snapshot":
      applySnapshot(message.snapshot);
      break;
    case "resync_required":
      applySnapshot(message.snapshot);
      showMessage(`Resynchronized after missing ${message.missed_events} events.`, false);
      break;
    case "event":
      applyEvent(message.event);
      break;
    case "command_accepted": {
      upsertRun(message.snapshot);
      const waiter = pending.get(message.request_id);
      if (waiter) waiter.resolve(message.snapshot);
      pending.delete(message.request_id);
      break;
    }
    case "command_rejected": {
      const waiter = message.request_id && pending.get(message.request_id);
      if (waiter) waiter.reject(new Error(message.message));
      if (message.request_id) pending.delete(message.request_id);
      showMessage(message.message, true);
      break;
    }
  }
  render();
}

function applySnapshot(snapshot) {
  runs.clear();
  results.clear();
  for (const run of snapshot.runs) runs.set(run.run_id, run);
  for (const result of snapshot.results) results.set(result.run_id, result.phases);
  if (selectedRunId === null || !runs.has(selectedRunId)) {
    selectedRunId = latestRunId();
  }
  render();
}

function applyEvent(event) {
  if (event.event === "run_configured" || event.event === "run_configuration_updated" || event.event === "run_state_changed") {
    upsertRun(event.snapshot);
    if (selectedRunId === null) selectedRunId = event.snapshot.run_id;
  } else if (event.event === "phase_stats") {
    const phases = results.get(event.run_id) || [];
    const observation = { phase_id: event.phase_id, report: event.report };
    const index = phases.findIndex((phase) => phase.phase_id === event.phase_id);
    if (index >= 0) phases[index] = observation; else phases.push(observation);
    results.set(event.run_id, phases);
  }
}

function upsertRun(snapshot) {
  const current = runs.get(snapshot.run_id);
  if (!current || snapshot.revision >= current.revision) runs.set(snapshot.run_id, snapshot);
}

function latestRunId() {
  const ids = [...runs.keys()];
  return ids.length ? Math.max(...ids) : null;
}

function sendCommand(command) {
  if (!socket || socket.readyState !== WebSocket.OPEN) return Promise.reject(new Error("The server is not connected."));
  const requestId = String(nextRequestId++);
  return new Promise((resolve, reject) => {
    pending.set(requestId, { resolve, reject });
    socket.send(JSON.stringify({ type: "command", request_id: requestId, command }));
  });
}

function parseVariant(line) {
  let variant = line.trim();
  let weight = 1;
  const at = variant.lastIndexOf("@");
  if (at >= 0) {
    weight = Number(variant.slice(at + 1));
    variant = variant.slice(0, at);
  }
  if (!Number.isFinite(weight) || weight <= 0) throw new Error(`Invalid weight in ${line}`);
  const colon = variant.indexOf(":");
  const name = (colon < 0 ? variant : variant.slice(0, colon)).trim();
  if (!name) throw new Error("Operation names cannot be empty.");
  const arguments_ = {};
  if (colon >= 0) {
    for (const assignment of variant.slice(colon + 1).split(",")) {
      const equals = assignment.indexOf("=");
      if (equals < 1) throw new Error(`Invalid argument assignment ${assignment}`);
      const key = assignment.slice(0, equals).trim();
      let value = assignment.slice(equals + 1);
      if (Object.hasOwn(arguments_, key)) throw new Error(`Duplicate argument ${key}`);
      if (value.startsWith("str:")) value = value.slice(4);
      else if (/^-?\d+$/.test(value)) value = Number(value);
      arguments_[key] = value;
    }
  }
  return { name, weight, arguments: arguments_ };
}

function readConfig() {
  const operationLines = $("operations").value.split("\n").map((line) => line.trim()).filter(Boolean);
  const operations = operationLines.length
    ? { selection: "selected", operations: operationLines.map(parseVariant) }
    : { selection: "adapter_defaults" };
  const levels = $("levels").value.split(",").map((value) => value.trim()).filter(Boolean).map(Number);
  if (levels.some((level) => !Number.isFinite(level) || level <= 0)) throw new Error("Explicit levels must be positive numbers.");
  const program = $("adapter-program").value.trim();
  const config = {
    preset: $("preset").value,
    strategy: $("strategy").value,
    phases: {
      warmup_ms: numberValue("warmup"),
      measurement_ms: numberValue("measurement"),
      recovery_ms: numberValue("recovery"),
      repetitions: 1,
    },
    load: {
      initial_rate: numberValue("initial-rate"),
      maximum_rate: numberValue("maximum-rate"),
      growth_factor: numberValue("growth-factor"),
      explicit_levels: levels,
      cycles: numberValue("cycles"),
    },
    workload: { operations },
    output_directory: $("output-directory").value.trim() || "results",
    adapter: program ? {
      program,
      arguments: $("adapter-args").value.split("\n").map((line) => line.trim()).filter(Boolean),
    } : null,
  };
  if (config.phases.measurement_ms < 1) throw new Error("Measurement time must be positive.");
  if (config.load.initial_rate <= 0 || config.load.maximum_rate < config.load.initial_rate) throw new Error("Check the initial and maximum rates.");
  if (config.load.growth_factor <= 1) throw new Error("Growth factor must exceed one.");
  return config;
}

function numberValue(id) {
  const value = Number($(id).value);
  if (!Number.isFinite(value)) throw new Error(`${id} must be a number.`);
  return value;
}

async function applyConfiguration() {
  try {
    const config = readConfig();
    const run = selectedRun();
    const command = run && run.state.state === "configured"
      ? { command: "update_configured", run_id: run.run_id, config }
      : { command: "configure", config };
    const snapshot = await sendCommand(command);
    selectedRunId = snapshot.run_id;
    showMessage(run && run.state.state === "configured" ? "Configuration updated." : "Run configured.", false);
  } catch (error) { showMessage(error.message, true); }
}

async function startRun() {
  const run = selectedRun();
  if (!run) return;
  try {
    await sendCommand({ command: "start", run_id: run.run_id });
    showMessage("Start requested.", false);
  } catch (error) { showMessage(error.message, true); }
}

async function stopRun() {
  const run = selectedRun();
  if (!run) return;
  try {
    await sendCommand({ command: "stop", run_id: run.run_id });
    showMessage("Graceful stop requested.", false);
  } catch (error) { showMessage(error.message, true); }
}

function selectedRun() { return selectedRunId === null ? null : runs.get(selectedRunId); }

function showMessage(message, error) {
  $("form-message").textContent = message;
  $("form-message").classList.toggle("error", error);
}

function render() {
  renderRuns();
  renderMetrics();
  renderCharts();
  renderErrorCodes();
  renderVariants();
  updateButtons();
}

function renderRuns() {
  const container = $("runs");
  container.replaceChildren();
  const ordered = [...runs.values()].sort((a, b) => b.run_id - a.run_id);
  if (!ordered.length) {
    const empty = document.createElement("p"); empty.className = "empty"; empty.textContent = "No runs configured."; container.append(empty); return;
  }
  for (const run of ordered) {
    const button = document.createElement("button");
    button.className = `run-row${run.run_id === selectedRunId ? " selected" : ""}`;
    const info = document.createElement("span");
    const title = document.createElement("strong"); title.textContent = `Run ${run.run_id}`;
    const detail = document.createElement("small"); detail.textContent = `${run.config.load.initial_rate} → ${run.config.load.maximum_rate} ops/s`;
    info.append(title, detail);
    const state = document.createElement("span"); state.className = "state-pill"; state.textContent = stateName(run.state);
    button.append(info, state);
    button.addEventListener("click", () => { selectedRunId = run.run_id; loadForm(run.config); render(); });
    container.append(button);
  }
}

function loadForm(config) {
  $("preset").value = config.preset;
  $("strategy").value = config.strategy;
  $("initial-rate").value = config.load.initial_rate;
  $("maximum-rate").value = config.load.maximum_rate;
  $("growth-factor").value = config.load.growth_factor;
  $("cycles").value = config.load.cycles;
  $("levels").value = config.load.explicit_levels.join(", ");
  $("warmup").value = config.phases.warmup_ms;
  $("measurement").value = config.phases.measurement_ms;
  $("recovery").value = config.phases.recovery_ms;
  const selection = config.workload.operations;
  $("operations").value = selection.selection === "selected" ? selection.operations.map(formatConfiguredVariant).join("\n") : "";
  $("adapter-program").value = config.adapter?.program || "";
  $("adapter-args").value = config.adapter?.arguments?.join("\n") || "";
  $("output-directory").value = config.output_directory;
}

function formatConfiguredVariant(variant) {
  const args = Object.entries(variant.arguments).map(([name, value]) => `${name}=${typeof value === "string" && /^-?\d+$/.test(value) ? `str:${value}` : value}`).join(",");
  return `${variant.name}${args ? `:${args}` : ""}@${variant.weight}`;
}

function renderMetrics() {
  const run = selectedRun();
  const phases = run ? (results.get(run.run_id) || []) : [];
  const knee = run?.state?.state === "completed" ? run.state.outcome.knee : null;
  const latest = latestPhase(phases);
  const stats = latest?.report.stats.overall;
  const unsuccessful = stats ? stats.failed + stats.timed_out : 0;
  const unsuccessfulRate = stats?.attempts ? unsuccessful / stats.attempts : 0;
  $("selected-run").textContent = run ? `Run ${run.run_id} · r${run.revision}` : "New run";
  $("metric-state").textContent = run ? stateName(run.state) : "—";
  $("metric-stage").textContent = run ? stageName(run.state) : "Waiting for a run";
  $("metric-knee").textContent = knee ? formatRate(knee.offered_rate) : "—";
  $("metric-recommended").textContent = knee ? formatRate(knee.recommended_operating_rate) : "—";
  $("metric-error-rate").textContent = stats ? formatPercent(unsuccessfulRate) : "—";
  $("metric-error-detail").textContent = stats ? `${stats.failed} errors · ${stats.timed_out} timeouts` : "errors + timeouts";
  $("metric-errors").classList.toggle("warning", unsuccessful > 0);
  $("metric-phases").textContent = String(phases.length);
}

function stateName(state) { return state.state.replaceAll("_", " "); }
function stageName(state) {
  if (state.state === "measuring") return `Stage: ${state.stage}`;
  if (state.state === "completed") return state.outcome.classification.replaceAll("_", " ");
  return state.state === "configured" ? "Ready to start" : stateName(state);
}
function formatRate(rate) { return Number(rate).toLocaleString(undefined, { maximumFractionDigits: 1 }); }
function formatPercent(rate) { return `${(rate * 100).toFixed(rate >= .1 ? 1 : 2)}%`; }
function latestPhase(phases) { return phases.length ? phases.reduce((a, b) => a.phase_id > b.phase_id ? a : b) : null; }
function failureRate(phase, field) {
  const stats = phase.report.stats.overall;
  return stats.attempts ? stats[field] / stats.attempts : 0;
}

function renderCharts() {
  const run = selectedRun();
  const phases = run ? [...(results.get(run.run_id) || [])].sort((a, b) => a.report.offered_rate - b.report.offered_rate) : [];
  const knee = run?.state?.state === "completed" ? run.state.outcome.knee?.offered_rate : null;
  drawChart("throughput-chart", phases, [
    { name: "Ideal", color: "#5c8df6", value: (phase) => phase.report.offered_rate },
    { name: "Goodput", color: "#55d9d2", value: (phase) => phase.report.goodput_rate },
  ], knee, "ops/s");
  drawChart("latency-chart", phases, [
    { name: "p50", color: "#5c8df6", value: (phase) => nsMs(phase.report.stats.overall.client_latency_ns.p50) },
    { name: "p95", color: "#55d9d2", value: (phase) => nsMs(phase.report.stats.overall.client_latency_ns.p95) },
    { name: "p99", color: "#f06b75", value: (phase) => nsMs(phase.report.stats.overall.client_latency_ns.p99) },
  ], knee, "ms");
  drawChart("reliability-chart", phases, [
    { name: "Errors", color: "#f06b75", value: (phase) => failureRate(phase, "failed") * 100 },
    { name: "Timeouts", color: "#f3b562", value: (phase) => failureRate(phase, "timed_out") * 100 },
  ], knee, "%");
}

function drawChart(id, points, series, knee, unit) {
  const container = $(id);
  container.replaceChildren();
  if (!points.length) { container.className = "chart empty-chart"; container.textContent = "Measurements will stream here."; return; }
  container.className = "chart";
  const width = 920, height = 250, left = 58, right = 18, top = 14, bottom = 34;
  const xMax = Math.max(...points.map((point) => point.report.offered_rate), knee || 0) * 1.04 || 1;
  const yMax = Math.max(...points.flatMap((point) => series.map((line) => line.value(point) || 0))) * 1.12 || 1;
  const x = (value) => left + value / xMax * (width - left - right);
  const y = (value) => height - bottom - value / yMax * (height - top - bottom);
  const svg = svgElement("svg", { viewBox: `0 0 ${width} ${height}`, role: "img", "aria-label": `${unit} by offered load` });
  for (let tick = 0; tick <= 4; tick++) {
    const gy = top + tick * (height - top - bottom) / 4;
    svg.append(svgElement("line", { x1: left, y1: gy, x2: width - right, y2: gy, class: "grid" }));
    const value = yMax * (4 - tick) / 4;
    const label = svgElement("text", { x: left - 8, y: gy + 4, "text-anchor": "end" }); label.textContent = `${formatCompact(value)} ${unit}`; svg.append(label);
  }
  svg.append(svgElement("line", { x1: left, y1: height - bottom, x2: width - right, y2: height - bottom, class: "axis" }));
  for (const line of series) {
    const path = points.map((point, index) => `${index ? "L" : "M"}${x(point.report.offered_rate)},${y(line.value(point) || 0)}`).join(" ");
    svg.append(svgElement("path", { d: path, fill: "none", stroke: line.color, "stroke-width": 2.2, "stroke-linejoin": "round" }));
    for (const point of points) svg.append(svgElement("circle", { cx: x(point.report.offered_rate), cy: y(line.value(point) || 0), r: 3, fill: line.color }));
  }
  if (knee) {
    svg.append(svgElement("line", { x1: x(knee), y1: top, x2: x(knee), y2: height - bottom, class: "knee-line" }));
    const label = svgElement("text", { x: x(knee) + 5, y: top + 10 }); label.textContent = `knee ${formatCompact(knee)}`; svg.append(label);
  }
  for (let tick = 0; tick <= 4; tick++) {
    const value = xMax * tick / 4;
    const label = svgElement("text", { x: x(value), y: height - 10, "text-anchor": "middle" }); label.textContent = formatCompact(value); svg.append(label);
  }
  container.append(svg);
}

function svgElement(name, attributes) {
  const element = document.createElementNS(svgNs, name);
  for (const [key, value] of Object.entries(attributes)) element.setAttribute(key, value);
  return element;
}
function formatCompact(value) { return value >= 1000 ? `${(value / 1000).toFixed(value >= 10000 ? 0 : 1)}k` : value.toFixed(value >= 100 ? 0 : 1); }
function nsMs(value) { return value == null ? 0 : value / 1e6; }

function renderErrorCodes() {
  const container = $("error-codes");
  container.replaceChildren();
  const phases = selectedRunId === null ? [] : (results.get(selectedRunId) || []);
  const stats = latestPhase(phases)?.report.stats.overall;
  const codes = stats?.errors_by_code || [];
  if (!codes.length && !stats?.timed_out) {
    const healthy = document.createElement("span"); healthy.className = "healthy"; healthy.textContent = "No reported errors"; container.append(healthy); return;
  }
  for (const entry of codes) {
    const chip = document.createElement("span"); chip.className = "error-code"; chip.textContent = `${entry.code || "uncategorized"}: ${entry.count}`; container.append(chip);
  }
  if (stats.timed_out) {
    const chip = document.createElement("span"); chip.className = "error-code"; chip.textContent = `timeout: ${stats.timed_out}`; container.append(chip);
  }
}

function renderVariants() {
  const body = $("variants");
  body.replaceChildren();
  const phases = selectedRunId === null ? [] : (results.get(selectedRunId) || []);
  const latest = latestPhase(phases);
  $("variant-phase").textContent = latest ? `Phase ${latest.phase_id} · ${formatRate(latest.report.offered_rate)} ops/s` : "Latest phase";
  if (!latest?.report.stats.variants.length) {
    const row = document.createElement("tr"); const cell = document.createElement("td"); cell.colSpan = 8; cell.className = "empty"; cell.textContent = "No measurements yet."; row.append(cell); body.append(row); return;
  }
  for (const entry of latest.report.stats.variants) {
    const stats = entry.stats;
    const row = document.createElement("tr");
    const values = [formatVariant(entry.variant), stats.attempts, stats.successful, stats.failed, stats.timed_out, formatMs(stats.client_latency_ns.p50), formatMs(stats.client_latency_ns.p95), formatMs(stats.client_latency_ns.p99)];
    for (const value of values) { const cell = document.createElement("td"); cell.textContent = value; row.append(cell); }
    body.append(row);
  }
}

function formatVariant(variant) {
  const args = Object.entries(variant.arguments).map(([name, value]) => `${name}=${value}`).join(",");
  return `${variant.operation}${args ? `(${args})` : ""}`;
}
function formatMs(value) { return value == null ? "—" : `${(value / 1e6).toFixed(2)} ms`; }

function updateButtons() {
  const connected = socket?.readyState === WebSocket.OPEN;
  const run = selectedRun();
  const state = run?.state?.state;
  $("apply").disabled = !connected;
  $("start").disabled = !connected || state !== "configured";
  $("stop").disabled = !connected || !["starting", "measuring"].includes(state);
}

$("apply").addEventListener("click", applyConfiguration);
$("start").addEventListener("click", startRun);
$("stop").addEventListener("click", stopRun);
connect();
render();

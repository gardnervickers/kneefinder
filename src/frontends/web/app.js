"use strict";

const runs = new Map();
const results = new Map();
const decisions = new Map();
const phaseProgress = new Map();
const pending = new Map();
let selectedRunId = null;
let socket = null;
let nextRequestId = 1;
let configuredVariants = [];
let formDirty = false;
let queryInFlight = false;
let configurationSyncTimer = null;
let configurationSyncPending = false;
let runnerMode = null;
let selectedStrategy = null;
let selectedPreset = null;

const $ = (id) => document.getElementById(id);
const svgNs = "http://www.w3.org/2000/svg";
const strategyDescriptions = Object.freeze({
  adaptive: "Establishes a stable baseline, grows load geometrically until saturation is bracketed, then refines with geometric midpoints.",
  sweep: "Runs each configured load level from low to high for a predictable capacity curve.",
  "up-down": "Runs load upward and then downward to reveal hysteresis: different behavior while load is falling.",
});
const presetDefinitions = Object.freeze({
  quick: {
    label: "Quick",
    strategies: ["adaptive", "sweep"],
    description: "Fast feedback for smoke tests and early capacity exploration.",
    values: { warmup: 2000, measurement: 10000, recovery: 2000, repetitions: 1, cycles: 1 },
  },
  careful: {
    label: "Careful",
    strategies: ["adaptive", "sweep"],
    description: "Longer phases and repeated observations for more stable estimates.",
    values: { warmup: 10000, measurement: 30000, recovery: 10000, repetitions: 3, cycles: 1 },
  },
  hysteresis: {
    label: "Hysteresis",
    strategies: ["up-down"],
    description: "Three slow up/down cycles with extra recovery to expose path-dependent behavior.",
    values: { warmup: 10000, measurement: 20000, recovery: 15000, repetitions: 1, cycles: 3 },
  },
});

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
  decisions.clear();
  phaseProgress.clear();
  for (const run of snapshot.runs) runs.set(run.run_id, run);
  for (const result of snapshot.results) {
    results.set(result.run_id, result.phases);
    decisions.set(result.run_id, result.decisions || []);
    if (result.progress) phaseProgress.set(result.run_id, result.progress);
  }
  let selectionChanged = false;
  if (selectedRunId === null || !runs.has(selectedRunId)) {
    selectedRunId = latestRunId();
    selectionChanged = true;
  }
  if (selectionChanged && selectedRunId !== null) loadForm(runs.get(selectedRunId));
  render();
}

function applyEvent(event) {
  if (event.event === "run_configured"
      || event.event === "run_configuration_updated"
      || event.event === "run_preparation_changed"
      || event.event === "run_state_changed") {
    upsertRun(event.snapshot);
    if (selectedRunId === null) selectedRunId = event.snapshot.run_id;
  } else if (event.event === "phase_stats") {
    const phases = results.get(event.run_id) || [];
    const observation = { phase_id: event.phase_id, report: event.report };
    const index = phases.findIndex((phase) => phase.phase_id === event.phase_id);
    if (index >= 0) phases[index] = observation; else phases.push(observation);
    results.set(event.run_id, phases);
  } else if (event.event === "strategy_decision") {
    const runDecisions = decisions.get(event.run_id) || [];
    runDecisions.push(event.decision);
    decisions.set(event.run_id, runDecisions);
  } else if (event.event === "phase_progress") {
    phaseProgress.set(event.run_id, event.progress);
  }
  if (event.event === "run_state_changed"
      && ["completed", "stopped", "failed"].includes(event.snapshot.state.state)) {
    phaseProgress.delete(event.snapshot.run_id);
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

function readConfig() {
  const selected = readStructuredOperations();
  const operations = selected.length
    ? { selection: "selected", operations: selected }
    : { selection: "adapter_defaults" };
  const levels = $("levels").value.split(",").map((value) => value.trim()).filter(Boolean).map(Number);
  if (levels.some((level) => !Number.isFinite(level) || level <= 0)) throw new Error("Explicit levels must be positive numbers.");
  if (!runnerMode) throw new Error("Choose a runner mode.");
  if (!selectedStrategy) throw new Error("Choose a workload strategy.");
  const agents = [];
  const program = $("adapter-program").value.trim();
  if (runnerMode === "adapter" && program) {
    agents.push({
      id: "local-0",
      transport: {
        kind: "subprocess",
        command: {
          program,
          arguments: $("adapter-args").value.split("\n").map((line) => line.trim()).filter(Boolean),
        },
      },
    });
  }
  if (runnerMode === "remote") {
    agents.push(...$("agent-endpoints").value.split("\n")
      .map((line) => line.trim()).filter(Boolean).map(parseAgentEndpoint));
  }
  if (!agents.length) throw new Error(
    runnerMode === "adapter" ? "Enter an adapter executable." : "Enter at least one remote agent.",
  );
  if (new Set(agents.map((agent) => agent.id)).size !== agents.length) throw new Error("Agent IDs must be unique.");
  const strategy = selectedStrategy;
  const config = {
    preset: selectedPreset || (strategy === "up-down" ? "hysteresis" : "quick"),
    strategy,
    phases: {
      warmup_ms: numberValue("warmup"),
      measurement_ms: numberValue("measurement"),
      recovery_ms: numberValue("recovery"),
      repetitions: numberValue("repetitions"),
    },
    load: {
      initial_rate: numberValue("initial-rate"),
      maximum_rate: numberValue("maximum-rate"),
      growth_factor: numberValue("growth-factor"),
      explicit_levels: strategy === "adaptive" ? [] : levels,
      cycles: strategy === "adaptive" ? 1 : numberValue("cycles"),
    },
    analysis: {
      latency_slo_ms: optionalNumberValue("latency-slo"),
      maximum_unsuccessful_rate: optionalNumberValue("unsuccessful-slo"),
      safety_factor: numberValue("safety-factor"),
      bootstrap_samples: numberValue("bootstrap-samples"),
      bootstrap_seed: numberValue("bootstrap-seed"),
    },
    workload: { operations },
    output_directory: $("output-directory").value.trim() || "results",
    agents,
  };
  if (config.phases.measurement_ms < 1) throw new Error("Measurement time must be positive.");
  if (!Number.isSafeInteger(config.phases.repetitions) || config.phases.repetitions < 1) {
    throw new Error("Attempts must be a positive integer.");
  }
  if (!Number.isSafeInteger(config.load.cycles) || config.load.cycles < 1) {
    throw new Error("Cycles must be a positive integer.");
  }
  if (!(config.analysis.safety_factor > 0 && config.analysis.safety_factor <= 1)) {
    throw new Error("Safety factor must be in (0, 1].");
  }
  if (!Number.isSafeInteger(config.analysis.bootstrap_samples)
      || config.analysis.bootstrap_samples < 1) {
    throw new Error("Bootstrap samples must be a positive integer.");
  }
  if (!Number.isSafeInteger(config.analysis.bootstrap_seed)
      || config.analysis.bootstrap_seed < 1) {
    throw new Error("Bootstrap seed must be a positive safe integer.");
  }
  if (config.analysis.maximum_unsuccessful_rate !== null
      && !(config.analysis.maximum_unsuccessful_rate >= 0
        && config.analysis.maximum_unsuccessful_rate <= 1)) {
    throw new Error("Maximum bad rate must be in [0, 1].");
  }
  if (config.load.initial_rate <= 0 || config.load.maximum_rate < config.load.initial_rate) throw new Error("Check the initial and maximum rates.");
  if (config.load.growth_factor <= 1) throw new Error("Growth factor must exceed one.");
  return config;
}

function readStructuredOperations() {
  if (!configuredVariants.length) return [];
  const catalog = selectedCatalog();
  const descriptors = new Map(catalog.map((operation) => [operation.name, operation]));
  const seen = new Set();
  return configuredVariants.map((variant) => {
    const descriptor = descriptors.get(variant.name);
    const weight = Number(variant.weight);
    if (!Number.isFinite(weight) || weight <= 0) {
      throw new Error(`${variant.name} must have a positive finite weight.`);
    }
    const arguments_ = {};
    if (descriptor) {
      for (const argument of descriptor.arguments) {
        if (!Object.hasOwn(variant.arguments, argument.name)) {
          if (argument.required) throw new Error(`${variant.name} requires ${argument.name}.`);
          continue;
        }
        const value = variant.arguments[argument.name];
        if (argument.kind === "integer" && !Number.isSafeInteger(value)) {
          throw new Error(`${variant.name}.${argument.name} must be an integer.`);
        }
        if (argument.kind === "string" && typeof value !== "string") {
          throw new Error(`${variant.name}.${argument.name} must be text.`);
        }
        if (argument.kind === "enum"
            && (typeof value !== "string" || !argument.values.includes(value))) {
          throw new Error(
            `${variant.name}.${argument.name} must be one of: ${argument.values.join(", ")}.`,
          );
        }
        arguments_[argument.name] = value;
      }
      const unknown = Object.keys(variant.arguments).find(
        (name) => !descriptor.arguments.some((argument) => argument.name === name),
      );
      if (unknown) throw new Error(`${variant.name} does not advertise argument ${unknown}.`);
    } else {
      for (const [name, value] of Object.entries(variant.arguments)) {
        if (!(typeof value === "string" || Number.isSafeInteger(value))) {
          throw new Error(`${variant.name}.${name} must be text or an integer.`);
        }
        arguments_[name] = value;
      }
    }
    const key = `${variant.name}\u0000${JSON.stringify(
      Object.entries(arguments_).sort(([left], [right]) => left.localeCompare(right)),
    )}`;
    if (seen.has(key)) throw new Error(`Duplicate configured variant: ${formatVariantName(variant.name, arguments_)}.`);
    seen.add(key);
    return { name: variant.name, weight, arguments: arguments_ };
  });
}

function optionalNumberValue(id) {
  const value = $(id).value.trim();
  return value === "" ? null : Number(value);
}

function parseAgentEndpoint(value) {
  const match = value.match(/^([A-Za-z0-9_-]+)=tcp:\/\/(\S+):(\d+)$/);
  if (!match || Number(match[3]) < 1 || Number(match[3]) > 65535) {
    throw new Error(`Invalid agent endpoint ${value}; use ID=tcp://HOST:PORT.`);
  }
  return { id: match[1], transport: { kind: "tcp", address: `${match[2]}:${match[3]}` } };
}

function numberValue(id) {
  const value = Number($(id).value);
  if (!Number.isFinite(value)) throw new Error(`${id} must be a number.`);
  return value;
}

async function persistConfiguration() {
  const config = readConfig();
  const run = selectedRun();
  const updated = Boolean(run && run.state.state === "configured");
  const command = updated
    ? { command: "update_configured", run_id: run.run_id, config }
    : { command: "configure", config };
  const snapshot = await sendCommand(command);
  selectedRunId = snapshot.run_id;
  return snapshot;
}

function scheduleConfigurationSync(delay = 650) {
  if (configurationSyncTimer !== null) clearTimeout(configurationSyncTimer);
  configurationSyncTimer = setTimeout(() => {
    configurationSyncTimer = null;
    synchronizeConfiguration();
  }, delay);
}

function markConfigurationDirty() {
  formDirty = true;
  updateButtons();
  updateConfigurationFlow();
  if (configurationReady()) scheduleConfigurationSync();
}

async function synchronizeConfiguration() {
  if (queryInFlight) {
    configurationSyncPending = true;
    return;
  }
  queryInFlight = true;
  updateButtons();
  showMessage("Saving configuration…", false);
  try {
    const snapshot = await persistConfiguration();
    let current = snapshot;
    if (snapshot.preparation.status !== "ready") {
      showMessage("Connecting to configured agents…", false);
      current = await sendCommand({ command: "prepare_agents", run_id: snapshot.run_id });
    }
    loadConfiguredVariants(current.config);
    formDirty = false;
    const catalog = current.preparation.catalog;
    showMessage(
      `Ready: ${catalog.agents.length} agent${catalog.agents.length === 1 ? "" : "s"} connected, ${catalog.operations.length} operation${catalog.operations.length === 1 ? "" : "s"} discovered.`,
      false,
    );
  } catch (error) {
    showMessage(`Could not prepare agents: ${error.message}`, true);
  } finally {
    queryInFlight = false;
    render();
    if (configurationSyncPending) {
      configurationSyncPending = false;
      scheduleConfigurationSync(0);
    }
  }
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
  renderWorkloadEditor();
  renderRunProgress();
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
    button.addEventListener("click", () => { selectedRunId = run.run_id; loadForm(run); render(); });
    container.append(button);
  }
}

function loadForm(run) {
  const config = run.config;
  selectedPreset = config.preset;
  selectedStrategy = config.strategy;
  if (!presetDefinitions[selectedPreset]?.strategies.includes(selectedStrategy)) {
    selectedPreset = selectedStrategy === "up-down" ? "hysteresis" : "quick";
  }
  $("initial-rate").value = config.load.initial_rate;
  $("maximum-rate").value = config.load.maximum_rate;
  $("growth-factor").value = config.load.growth_factor;
  $("cycles").value = config.load.cycles;
  $("repetitions").value = config.phases.repetitions;
  $("levels").value = config.load.explicit_levels.join(", ");
  $("warmup").value = config.phases.warmup_ms;
  $("measurement").value = config.phases.measurement_ms;
  $("recovery").value = config.phases.recovery_ms;
  const analysis = config.analysis || {};
  $("latency-slo").value = analysis.latency_slo_ms ?? "";
  $("unsuccessful-slo").value = analysis.maximum_unsuccessful_rate ?? "";
  $("safety-factor").value = analysis.safety_factor ?? 0.8;
  $("bootstrap-samples").value = analysis.bootstrap_samples ?? 400;
  $("bootstrap-seed").value = analysis.bootstrap_seed ?? 1263420741;
  loadConfiguredVariants(config);
  const subprocess = config.agents.find((agent) => agent.transport.kind === "subprocess");
  const remoteAgents = config.agents.filter((agent) => agent.transport.kind === "tcp");
  runnerMode = subprocess && remoteAgents.length ? null : (subprocess ? "adapter" : (remoteAgents.length ? "remote" : null));
  $("adapter-program").value = subprocess?.transport.command.program || "";
  $("adapter-args").value = subprocess?.transport.command.arguments?.join("\n") || "";
  $("agent-endpoints").value = config.agents
    .filter((agent) => agent.transport.kind === "tcp")
    .map((agent) => `${agent.id}=tcp://${agent.transport.address}`)
    .join("\n");
  $("output-directory").value = config.output_directory;
  formDirty = false;
  updateConfigurationFlow();
  if (subprocess && remoteAgents.length) {
    showMessage("This older run mixes colocated and remote agents. Choose one runner mode before editing it.", true);
  }
}

function loadConfiguredVariants(config) {
  const selection = config.workload.operations;
  configuredVariants = selection.selection === "selected"
    ? selection.operations.map((variant) => structuredClone(variant))
    : [];
}

function runnerConfigured() {
  if (runnerMode === "adapter") return $("adapter-program").value.trim() !== "";
  if (runnerMode === "remote") return $("agent-endpoints").value.trim() !== "";
  return false;
}

function configurationReady() {
  return runnerConfigured() && selectedStrategy !== null && selectedPreset !== null;
}

function relevantPresets() {
  return Object.entries(presetDefinitions)
    .filter(([, definition]) => definition.strategies.includes(selectedStrategy));
}

function applyPreset(name) {
  const preset = presetDefinitions[name];
  if (!preset || !preset.strategies.includes(selectedStrategy)) return;
  selectedPreset = name;
  for (const [field, value] of Object.entries(preset.values)) $(field).value = value;
  updateConfigurationFlow();
}

function renderPresetChoices() {
  const container = $("preset-choices");
  container.replaceChildren();
  for (const [name, preset] of relevantPresets()) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `choice-card${selectedPreset === name ? " selected" : ""}`;
    button.dataset.preset = name;
    button.setAttribute("aria-pressed", String(selectedPreset === name));
    button.title = preset.description;
    const label = document.createElement("strong");
    label.textContent = preset.label;
    const description = document.createElement("span");
    description.textContent = preset.description;
    button.append(label, description);
    button.addEventListener("click", () => {
      applyPreset(name);
      markConfigurationDirty();
    });
    container.append(button);
  }
}

function updateConfigurationFlow() {
  for (const button of document.querySelectorAll("[data-runner-mode]")) {
    const selected = button.dataset.runnerMode === runnerMode;
    button.classList.toggle("selected", selected);
    button.setAttribute("aria-pressed", String(selected));
  }
  $("adapter-config").hidden = runnerMode !== "adapter";
  $("remote-config").hidden = runnerMode !== "remote";
  $("runner-step").classList.toggle("complete", runnerConfigured());
  $("runner-step-status").textContent = !runnerMode
    ? "Choose one"
    : (runnerConfigured() ? (runnerMode === "adapter" ? "Executable set" : "Agents set") : "Details needed");

  const runnerChosen = runnerMode !== null;
  $("strategy-step").classList.toggle("locked", !runnerChosen);
  $("strategy-step").classList.toggle("active", runnerChosen && !selectedStrategy);
  $("strategy-step").classList.toggle("complete", selectedStrategy !== null);
  $("strategy-choices").hidden = !runnerChosen;
  $("strategy-step-status").textContent = selectedStrategy
    ? selectedStrategy.replace("up-down", "Up / down")
    : (runnerChosen ? "Choose one" : "Runner first");
  for (const button of document.querySelectorAll("[data-strategy]")) {
    const selected = button.dataset.strategy === selectedStrategy;
    button.classList.toggle("selected", selected);
    button.setAttribute("aria-pressed", String(selected));
  }

  const description = strategyDescriptions[selectedStrategy] || "";
  $("strategy-help").textContent = description;
  $("strategy-help").hidden = !description;
  const strategyChosen = selectedStrategy !== null;
  $("parameters-step").classList.toggle("locked", !strategyChosen);
  $("parameters-step").classList.toggle("active", strategyChosen);
  $("parameters-step-status").textContent = strategyChosen ? "Review" : "Strategy first";
  $("parameter-fields").hidden = !strategyChosen;
  const adaptive = selectedStrategy === "adaptive";
  $("levels-field").hidden = adaptive;
  $("cycles-field").hidden = adaptive;
  $("levels").disabled = adaptive;
  $("cycles").disabled = adaptive;
  $("repetitions-label").textContent = adaptive ? "Stability attempts" : "Repetitions";
  renderPresetChoices();

  const ready = configurationReady();
  $("operations-step").classList.toggle("locked", !ready);
  $("operations-step").classList.toggle("active", ready);
  $("operation-fields").hidden = !ready;
  $("operations-step-status").textContent = ready ? "Discovering" : "Configure first";
}

function selectedCatalog() {
  const preparation = selectedRun()?.preparation;
  return preparation?.status === "ready" ? preparation.catalog.operations : [];
}

function variantFromDescriptor(operation) {
  const arguments_ = {};
  for (const argument of operation.arguments || []) {
    if (argument.default !== null && argument.default !== undefined) {
      arguments_[argument.name] = argument.default;
    }
  }
  return {
    name: operation.name,
    weight: operation.default_weight,
    arguments: arguments_,
  };
}

function renderWorkloadEditor() {
  const run = selectedRun();
  const preparation = run?.preparation || { status: "unprepared" };
  const status = $("agent-query-status");
  const catalog = preparation.status === "ready" ? preparation.catalog : null;
  if (queryInFlight || preparation.status === "preparing") {
    $("operations-step-status").textContent = "Connecting";
    status.className = "catalog-status preparing";
    status.textContent = "Coordinator is opening and querying every configured agent…";
  } else if (preparation.status === "failed") {
    $("operations-step-status").textContent = "Needs attention";
    status.className = "catalog-status failed";
    status.textContent = preparation.message;
  } else if (catalog) {
    $("operations-step-status").textContent = "Ready";
    status.className = "catalog-status ready";
    const adapter = `${catalog.adapter.name}${catalog.adapter.version ? ` ${catalog.adapter.version}` : ""}`;
    status.textContent = `${adapter} · ${catalog.agents.length} agent${catalog.agents.length === 1 ? "" : "s"} ready · ${catalog.operations.length} operation${catalog.operations.length === 1 ? "" : "s"} share one schema`;
  } else {
    $("operations-step-status").textContent = configurationReady() ? "Waiting" : "Configure first";
    status.className = "catalog-status";
    status.textContent = run?.config.agents.length
      ? "Agent endpoints changed; waiting to connect automatically."
      : "Enter an adapter or remote agent to connect automatically.";
  }

  const operations = [...(catalog?.operations || [])].sort((left, right) => {
    if (left.enabled_by_default !== right.enabled_by_default) {
      return left.enabled_by_default ? -1 : 1;
    }
    return left.name.localeCompare(right.name);
  });
  const catalogContainer = $("operation-catalog");
  catalogContainer.replaceChildren();
  if (!operations.length) {
    const empty = document.createElement("p");
    empty.className = "empty";
    empty.textContent = "Operation schemas will appear here.";
    catalogContainer.append(empty);
  }
  for (const operation of operations) {
    const card = document.createElement("article");
    card.className = `operation-card${operation.enabled_by_default ? "" : " opt-in"}`;
    const heading = document.createElement("div");
    heading.className = "operation-heading";
    const identity = document.createElement("div");
    const name = document.createElement("strong");
    name.textContent = operation.name;
    const badges = document.createElement("span");
    badges.className = "operation-badges";
    const kind = document.createElement("span");
    kind.className = `operation-kind ${operation.kind}`;
    kind.textContent = operation.kind;
    badges.append(kind);
    if (operation.enabled_by_default) {
      const defaultBadge = document.createElement("span");
      defaultBadge.className = "default-badge";
      defaultBadge.textContent = "safe default";
      badges.append(defaultBadge);
    } else {
      const optInBadge = document.createElement("span");
      optInBadge.className = "opt-in-badge";
      optInBadge.textContent = "explicit opt-in";
      badges.append(optInBadge);
    }
    identity.append(name, badges);
    const add = document.createElement("button");
    add.type = "button";
    add.textContent = operation.enabled_by_default ? "Add variant" : "Add explicitly";
    add.addEventListener("click", () => {
      configuredVariants.push(variantFromDescriptor(operation));
      markConfigurationDirty();
      renderWorkloadEditor();
    });
    heading.append(identity, add);
    card.append(heading);
    if (operation.description) {
      const description = document.createElement("p");
      description.textContent = operation.description;
      card.append(description);
    }
    if (operation.arguments?.length) {
      const arguments_ = document.createElement("p");
      arguments_.className = "argument-summary";
      arguments_.textContent = operation.arguments
        .map((argument) => {
          const choices = argument.kind === "enum" ? ` [${argument.values.join(" | ")}]` : "";
          return `${argument.name}: ${argument.kind}${choices}${argument.required ? " required" : ""}`;
        })
        .join(" · ");
      card.append(arguments_);
    }
    catalogContainer.append(card);
  }

  const variants = $("configured-workload");
  variants.replaceChildren();
  $("variant-count").textContent = configuredVariants.length
    ? `${configuredVariants.length} variant${configuredVariants.length === 1 ? "" : "s"}`
    : "Adapter defaults";
  if (!configuredVariants.length) {
    const empty = document.createElement("p");
    empty.className = "empty";
    empty.textContent = catalog
      ? "Safe adapter defaults are active. Editing the form materializes them as variants."
      : "Safe adapter defaults will be selected after discovery.";
    variants.append(empty);
    return;
  }

  const descriptors = new Map(operations.map((operation) => [operation.name, operation]));
  configuredVariants.forEach((variant, index) => {
    const descriptor = descriptors.get(variant.name);
    const card = document.createElement("article");
    card.className = "configured-variant";
    const heading = document.createElement("div");
    heading.className = "configured-variant-heading";
    const title = document.createElement("strong");
    title.textContent = descriptor
      ? formatVariantName(variant.name, variant.arguments)
      : `${variant.name} · catalog unavailable`;
    const remove = document.createElement("button");
    remove.type = "button";
    remove.className = "remove-variant";
    remove.textContent = "Remove";
    remove.addEventListener("click", () => {
      configuredVariants.splice(index, 1);
      markConfigurationDirty();
      renderWorkloadEditor();
    });
    heading.append(title, remove);
    card.append(heading);

    const fields = document.createElement("div");
    fields.className = "variant-fields";
    for (const argument of descriptor?.arguments || []) {
      const label = document.createElement("label");
      label.textContent = `${argument.name} · ${argument.kind}${argument.required ? " · required" : ""}`;
      const input = document.createElement(argument.kind === "enum" ? "select" : "input");
      if (argument.kind === "enum") {
        if (!argument.required || !Object.hasOwn(variant.arguments, argument.name)) {
          const placeholder = document.createElement("option");
          placeholder.value = "";
          placeholder.textContent = argument.required ? "Select a value" : "Unset";
          placeholder.disabled = argument.required;
          placeholder.selected = true;
          input.append(placeholder);
        }
        for (const value of argument.values) {
          const option = document.createElement("option");
          option.value = value;
          option.textContent = value;
          input.append(option);
        }
      } else {
        input.type = argument.kind === "integer" ? "number" : "text";
        if (argument.kind === "integer") input.step = "1";
        input.placeholder = argument.default === null || argument.default === undefined
          ? (argument.required ? "required" : "optional")
          : String(argument.default);
      }
      if (Object.hasOwn(variant.arguments, argument.name)) {
        input.value = String(variant.arguments[argument.name]);
      }
      const updateArgument = () => {
        if (input.value === "" && !argument.required) {
          delete variant.arguments[argument.name];
        } else if (argument.kind === "integer") {
          if (input.value === "") delete variant.arguments[argument.name];
          else variant.arguments[argument.name] = Number(input.value);
        } else {
          variant.arguments[argument.name] = input.value;
        }
        title.textContent = formatVariantName(variant.name, variant.arguments);
        markConfigurationDirty();
      };
      input.addEventListener(argument.kind === "enum" ? "change" : "input", updateArgument);
      label.append(input);
      fields.append(label);
    }
    const weight = document.createElement("label");
    weight.textContent = "Relative weight";
    const weightInput = document.createElement("input");
    weightInput.type = "number";
    weightInput.min = "0.000001";
    weightInput.step = "any";
    weightInput.value = variant.weight;
    weightInput.addEventListener("input", () => {
      variant.weight = Number(weightInput.value);
      markConfigurationDirty();
    });
    weight.append(weightInput);
    fields.append(weight);
    card.append(fields);
    variants.append(card);
  });
}

function formatVariantName(name, arguments_) {
  const values = Object.entries(arguments_)
    .map(([argument, value]) => `${argument}=${typeof value === "string" ? JSON.stringify(value) : value}`)
    .join(", ");
  return `${name}${values ? `(${values})` : ""}`;
}

function renderRunProgress() {
  const run = selectedRun();
  const track = $("run-progress-track");
  const fill = $("run-progress-fill");
  if (!run) {
    $("run-progress-title").textContent = "No run selected";
    $("run-progress-status").textContent = "Ready to configure";
    fill.style.width = "0%";
    track.setAttribute("aria-valuenow", "0");
    track.setAttribute("aria-valuetext", "No run selected");
    $("run-progress-detail").textContent = "Phase activity will appear here.";
    return;
  }

  const progress = progressForRun(run);
  $("run-progress-title").textContent = `Run ${run.run_id}`;
  $("run-progress-status").textContent = progress.label;
  $("run-progress-detail").textContent = progress.detail || "Waiting for phase activity.";
  fill.style.width = `${progress.percent}%`;
  track.setAttribute("aria-valuenow", String(Math.round(progress.percent)));
  track.setAttribute("aria-valuetext", progress.label);
}

function progressForRun(run) {
  const state = run.state.state;
  const completed = (results.get(run.run_id) || []).length;
  const planned = plannedPhaseCount(run.config);
  const active = phaseProgress.get(run.run_id);
  const measuredPercent = planned ? Math.min(96, completed / planned * 100) : 0;
  if (state === "configured") return { percent: 0, label: "Ready to start", detail: "No workload traffic yet." };
  if (state === "starting") return { percent: 3, label: "Starting workload agents", detail: "Preparing the execution session." };
  if (state === "completed") return { percent: 100, label: `Complete · ${completed} measurements`, detail: "Analysis and validation complete." };
  if (state === "stopped") return { percent: measuredPercent, label: `Stopped · ${completed} measurements`, detail: "Partial results were preserved." };
  if (state === "failed") return { percent: measuredPercent, label: `Failed · ${completed} measurements`, detail: run.state.message };

  if (state === "measuring" || state === "stopping") {
    const stopping = state === "stopping";
    if (active) {
      const segmentFraction = active.planned_ms
        ? Math.min(1, active.elapsed_ms / active.planned_ms)
        : 0;
      const phaseFraction = progressWithinPhase(run.config, active.segment, segmentFraction);
      const percent = active.planned_phases
        ? Math.min(99, ((active.phase_id - 1) + phaseFraction) / active.planned_phases * 100)
        : segmentFraction * 100;
      const phaseCount = active.planned_phases ? ` of ${active.planned_phases}` : "";
      const segment = active.segment[0].toUpperCase() + active.segment.slice(1);
      const label = stopping
        ? `Phase ${active.phase_id}${phaseCount} · stopping`
        : `Phase ${active.phase_id}${phaseCount} · ${segment} · ${formatRate(active.offered_rate)} ops/s`;
      const time = `${formatDuration(active.elapsed_ms)} / ${formatDuration(active.planned_ms)}`;
      const activity = active.segment === "recovery"
        ? "idle recovery"
        : `${active.scheduled.toLocaleString()} scheduled · ${active.reported.toLocaleString()} reported · ${active.awaiting_results.toLocaleString()} awaiting results`;
      const remaining = estimatedRemaining(run.config, active, segmentFraction);
      const estimate = remaining === null ? "" : ` · ~${formatDuration(remaining)} remaining`;
      return { percent, label, detail: `${time} · ${activity}${estimate}` };
    }
    if (planned) {
      const activePhase = Math.min(completed + 1, planned);
      const label = stopping
        ? `${completed} of ${planned} measurements complete · stopping`
        : `Measurement ${activePhase} of ${planned} · ${completed} complete`;
      return { percent: Math.max(5, measuredPercent), label, detail: "Waiting for phase activity." };
    }
    const stage = state === "measuring" ? run.state.stage : run.state.interrupted_stage;
    const stagePercent = { baseline: 10, discovery: 35, refinement: 65, validation: 88 }[stage] || 5;
    const label = `${completed} measurements complete · ${stopping ? "stopping" : stageName(run.state)}`;
    return { percent: Math.max(measuredPercent, stagePercent), label, detail: "Waiting for phase activity." };
  }

  return { percent: measuredPercent, label: stateName(run.state), detail: "Waiting for phase activity." };
}

function progressWithinPhase(config, segment, fraction) {
  const warmup = config.phases.warmup_ms;
  const measurement = config.phases.measurement_ms;
  const recovery = config.phases.recovery_ms;
  const total = warmup + measurement + recovery;
  if (!total) return fraction;
  if (segment === "warmup") return warmup * fraction / total;
  if (segment === "measurement") return (warmup + measurement * fraction) / total;
  return (warmup + measurement + recovery * fraction) / total;
}

function estimatedRemaining(config, active, segmentFraction) {
  if (!active.planned_phases) return null;
  const warmup = config.phases.warmup_ms;
  const measurement = config.phases.measurement_ms;
  const recovery = config.phases.recovery_ms;
  const total = warmup + measurement + recovery;
  const consumed = progressWithinPhase(config, active.segment, segmentFraction) * total;
  return Math.max(0, (active.planned_phases - active.phase_id) * total + total - consumed);
}

function formatDuration(milliseconds) {
  if (milliseconds < 1000) return `${milliseconds}ms`;
  const seconds = milliseconds / 1000;
  if (seconds < 60) return `${seconds.toFixed(seconds < 10 ? 1 : 0)}s`;
  const minutes = Math.floor(seconds / 60);
  return `${minutes}m ${Math.round(seconds % 60)}s`;
}

function plannedPhaseCount(config) {
  if (config.strategy === "adaptive") return 0;
  let levels = config.load.explicit_levels.length;
  if (!levels) {
    let rate = config.load.initial_rate;
    while (rate < config.load.maximum_rate && levels < 10_000) {
      levels += 1;
      rate *= config.load.growth_factor;
    }
    levels += 1;
  }
  if (config.strategy === "up-down" && levels > 1) levels = levels * 2 - 1;
  return levels * config.load.cycles * config.phases.repetitions;
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
  $("metric-knee-detail").textContent = knee
    ? `${formatRate(knee.lower_bound)}–${formatRate(knee.upper_bound)} confidence interval`
    : (run && ["starting", "measuring", "stopping"].includes(run.state.state)
      ? "available after final validation"
      : "offered operations / sec");
  const sloMaximum = run?.state?.state === "completed" ? run.state.outcome.slo_maximum_rate : null;
  $("metric-recommended-detail").textContent = sloMaximum == null
    ? "safety-adjusted lower bound"
    : `SLO maximum ${formatRate(sloMaximum)}`;
  const agents = run?.config.agents || [];
  const tcpAgents = agents.filter((agent) => agent.transport.kind === "tcp").length;
  const colocatedAgents = agents.length - tcpAgents;
  $("metric-agents").textContent = run ? String(agents.length) : "—";
  $("metric-agent-detail").textContent = run
    ? [
      run.preparation?.status === "ready" ? "connected" : run.preparation?.status || "unprepared",
      `${tcpAgents} TCP`,
      `${colocatedAgents} colocated`,
    ].filter((entry) => !entry.startsWith("0 ")).join(" · ") || "No agents"
    : "workload clients";
  $("metric-error-rate").textContent = stats ? formatPercent(unsuccessfulRate) : "—";
  $("metric-error-detail").textContent = stats ? `${stats.failed} errors · ${stats.timed_out} timeouts` : "errors + timeouts";
  $("metric-errors").classList.toggle("warning", unsuccessful > 0);
  $("metric-phases").textContent = String(phases.length);
}

function stateName(state) { return state.state.replaceAll("_", " "); }
function stageName(state) {
  if (state.state === "measuring") {
    const latest = selectedRunId === null ? null : (decisions.get(selectedRunId) || []).at(-1);
    return latest
      ? `${latest.stage}: ${latest.action} ${formatRate(latest.offered_rate)} ops/s`
      : `Stage: ${state.stage}`;
  }
  if (state.state === "completed") return state.outcome.classification.replaceAll("_", " ");
  if (state.state === "configured") {
    return selectedRun()?.preparation?.status === "ready" ? "Agents ready" : "Connecting to agents";
  }
  return stateName(state);
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
  const timeline = run ? [...(results.get(run.run_id) || [])].sort((a, b) => a.phase_id - b.phase_id) : [];
  const phases = [...timeline].sort((a, b) => a.report.offered_rate - b.report.offered_rate);
  const knee = run?.state?.state === "completed" ? run.state.outcome.knee?.offered_rate : null;
  drawTimeline("timeline-chart", timeline);
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

function drawTimeline(id, phases) {
  const container = $(id);
  container.replaceChildren();
  if (!phases.length) {
    container.className = "chart timeline-chart empty-chart";
    container.textContent = "Completed phases will stream here.";
    return;
  }
  container.className = "chart timeline-chart";
  const width = 920, height = 270, left = 58, right = 58, top = 18, bottom = 48;
  const plotWidth = width - left - right;
  const plotHeight = height - top - bottom;
  const goodputMax = Math.max(...phases.map((phase) => phase.report.goodput_rate || 0)) * 1.12 || 1;
  const latencyMax = Math.max(...phases.map((phase) => nsMs(phase.report.stats.overall.client_latency_ns.p95))) * 1.12 || 1;
  const x = (index) => phases.length === 1 ? left + plotWidth / 2 : left + index / (phases.length - 1) * plotWidth;
  const goodputY = (value) => height - bottom - value / goodputMax * plotHeight;
  const latencyY = (value) => height - bottom - value / latencyMax * plotHeight;
  const svg = svgElement("svg", {
    viewBox: `0 0 ${width} ${height}`,
    role: "img",
    "aria-label": "Goodput and p95 client latency by completed phase",
  });
  for (let tick = 0; tick <= 4; tick++) {
    const gy = top + tick * plotHeight / 4;
    svg.append(svgElement("line", { x1: left, y1: gy, x2: width - right, y2: gy, class: "grid" }));
    const goodputLabel = svgElement("text", { x: left - 8, y: gy + 4, "text-anchor": "end", class: "timeline-goodput-axis" });
    goodputLabel.textContent = `${formatCompact(goodputMax * (4 - tick) / 4)} ops/s`;
    svg.append(goodputLabel);
    const latencyLabel = svgElement("text", { x: width - right + 8, y: gy + 4, "text-anchor": "start", class: "timeline-latency-axis" });
    latencyLabel.textContent = `${formatCompact(latencyMax * (4 - tick) / 4)} ms`;
    svg.append(latencyLabel);
  }
  svg.append(svgElement("line", { x1: left, y1: height - bottom, x2: width - right, y2: height - bottom, class: "axis" }));
  const series = [
    { color: "#55d9d2", value: (phase) => phase.report.goodput_rate || 0, y: goodputY, name: "Goodput", unit: "ops/s" },
    { color: "#f3b562", value: (phase) => nsMs(phase.report.stats.overall.client_latency_ns.p95), y: latencyY, name: "p95 latency", unit: "ms" },
  ];
  for (const line of series) {
    const path = phases.map((phase, index) => `${index ? "L" : "M"}${x(index)},${line.y(line.value(phase))}`).join(" ");
    svg.append(svgElement("path", { d: path, fill: "none", stroke: line.color, "stroke-width": 2.2, "stroke-linejoin": "round" }));
    phases.forEach((phase, index) => {
      const point = svgElement("circle", { cx: x(index), cy: line.y(line.value(phase)), r: 3.5, fill: line.color });
      const title = svgElement("title", {});
      title.textContent = `Phase ${phase.phase_id} · offered ${formatRate(phase.report.offered_rate)} ops/s · ${line.name} ${formatRate(line.value(phase))} ${line.unit}`;
      point.append(title);
      svg.append(point);
    });
  }
  const labelEvery = Math.max(1, Math.ceil(phases.length / 8));
  phases.forEach((phase, index) => {
    if (index % labelEvery !== 0 && index !== phases.length - 1) return;
    const phaseLabel = svgElement("text", { x: x(index), y: height - 25, "text-anchor": "middle", class: "timeline-phase-label" });
    phaseLabel.textContent = `P${phase.phase_id}`;
    svg.append(phaseLabel);
    const rateLabel = svgElement("text", { x: x(index), y: height - 10, "text-anchor": "middle", class: "timeline-rate-label" });
    rateLabel.textContent = `${formatCompact(phase.report.offered_rate)}/s`;
    svg.append(rateLabel);
  });
  container.append(svg);
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
  const prepared = run?.preparation?.status === "ready";
  const activeRun = [...runs.values()].find((candidate) =>
    ["starting", "measuring", "stopping"].includes(candidate.state?.state)
  );
  const anotherRunActive = Boolean(activeRun && activeRun.run_id !== run?.run_id);
  $("start").disabled = !connected || anotherRunActive || state !== "configured" || !prepared || formDirty || queryInFlight;
  $("stop").disabled = !connected || !["starting", "measuring"].includes(state);
}

$("start").addEventListener("click", startRun);
$("stop").addEventListener("click", stopRun);
for (const button of document.querySelectorAll("[data-runner-mode]")) {
  button.addEventListener("click", () => {
    runnerMode = button.dataset.runnerMode;
    markConfigurationDirty();
  });
}
for (const button of document.querySelectorAll("[data-strategy]")) {
  button.addEventListener("click", () => {
    const strategy = button.dataset.strategy;
    if (strategy === selectedStrategy) return;
    selectedStrategy = strategy;
    const presets = relevantPresets();
    applyPreset(presets[0][0]);
    markConfigurationDirty();
  });
}
$("config-form").addEventListener("submit", (event) => event.preventDefault());
$("config-form").addEventListener("input", markConfigurationDirty);
updateConfigurationFlow();
connect();
render();

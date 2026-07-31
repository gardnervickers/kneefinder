//! Durable, schema-versioned run artifacts shared by every frontend.

use std::{
    collections::BTreeMap,
    fmt, fs,
    fs::{File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    agent::{AgentDescriptor, CohortReady},
    config::RunConfig,
    measurement::{RunClassification, RunState},
    protocol::{AdapterIdentity, PROTOCOL_VERSION, PhaseId, RunId},
    stats::PhaseReport,
    strategy::StrategyDecision,
};

pub const ARTIFACT_SCHEMA_VERSION: u16 = 1;
static NEXT_TEMPORARY_FILE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactSummary {
    #[serde(default)]
    pub schema_version: u16,
    #[serde(default)]
    pub protocol_version: u16,
    #[serde(default)]
    pub tool: ToolIdentity,
    #[serde(default = "default_run_id")]
    pub run_id: RunId,
    #[serde(default)]
    pub created_unix_ms: u64,
    #[serde(default)]
    pub updated_unix_ms: u64,
    #[serde(default)]
    pub adapter: Option<AdapterIdentity>,
    #[serde(default)]
    pub agents: Vec<AgentDescriptor>,
    #[serde(default)]
    pub environment: EnvironmentMetadata,
    #[serde(default)]
    pub config: Value,
    #[serde(default = "configured_state")]
    pub state: RunState,
    #[serde(default)]
    pub phases: Vec<ArtifactPhase>,
    #[serde(default)]
    pub decisions: Vec<StrategyDecision>,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub redactions: Vec<String>,
    #[serde(default)]
    pub last_record_sequence: u64,
    #[serde(default)]
    pub recovered_from_incremental_records: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolIdentity {
    pub name: String,
    pub version: String,
}

impl Default for ToolIdentity {
    fn default() -> Self {
        Self {
            name: env!("CARGO_PKG_NAME").into(),
            version: env!("CARGO_PKG_VERSION").into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EnvironmentMetadata {
    pub operating_system: String,
    pub architecture: String,
    pub process_id: u32,
    pub available_parallelism: Option<usize>,
}

impl EnvironmentMetadata {
    fn capture() -> Self {
        Self {
            operating_system: std::env::consts::OS.into(),
            architecture: std::env::consts::ARCH.into(),
            process_id: std::process::id(),
            available_parallelism: std::thread::available_parallelism().ok().map(usize::from),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactPhase {
    pub phase_id: PhaseId,
    pub report: PhaseReport,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct PersistedConfig {
    #[serde(default)]
    schema_version: u16,
    #[serde(default)]
    protocol_version: u16,
    #[serde(default = "default_run_id")]
    run_id: RunId,
    #[serde(default)]
    config: Value,
    #[serde(default)]
    redactions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ArtifactRecord {
    schema_version: u16,
    sequence: u64,
    observed_unix_ms: u64,
    #[serde(flatten)]
    value: ArtifactRecordValue,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "record", rename_all = "snake_case")]
enum ArtifactRecordValue {
    Phase {
        phase_id: PhaseId,
        report: Box<PhaseReport>,
    },
    StrategyDecision {
        decision: StrategyDecision,
    },
}

pub struct ArtifactWriter {
    directory: PathBuf,
    measurements: BufWriter<File>,
    summary: ArtifactSummary,
    next_sequence: u64,
}

impl ArtifactWriter {
    pub fn create(
        config: &RunConfig,
        run_id: RunId,
        catalog: &CohortReady,
    ) -> Result<Self, ArtifactError> {
        let directory = unique_run_directory(&config.output_directory, run_id)?;
        fs::create_dir_all(&directory).map_err(ArtifactError::io)?;
        let (redacted_config, redactions) = redacted_config(config)?;
        let now = unix_time_ms();
        let summary = ArtifactSummary {
            schema_version: ARTIFACT_SCHEMA_VERSION,
            protocol_version: PROTOCOL_VERSION,
            tool: ToolIdentity::default(),
            run_id,
            created_unix_ms: now,
            updated_unix_ms: now,
            adapter: Some(catalog.adapter.clone()),
            agents: catalog.agents.clone(),
            environment: EnvironmentMetadata::capture(),
            config: redacted_config.clone(),
            state: RunState::Configured,
            phases: Vec::new(),
            decisions: Vec::new(),
            warnings: Vec::new(),
            redactions: redactions.clone(),
            last_record_sequence: 0,
            recovered_from_incremental_records: false,
        };
        let persisted_config = PersistedConfig {
            schema_version: ARTIFACT_SCHEMA_VERSION,
            protocol_version: PROTOCOL_VERSION,
            run_id,
            config: redacted_config,
            redactions,
        };
        atomic_json(&directory.join("config.json"), &persisted_config)?;
        atomic_bytes(&directory.join("adapter.log"), b"")?;
        atomic_json(&directory.join("summary.json"), &summary)?;
        atomic_bytes(
            &directory.join("report.svg"),
            render_svg(&summary).as_bytes(),
        )?;
        let measurements = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(directory.join("measurements.ndjson"))
                .map_err(ArtifactError::io)?,
        );
        Ok(Self {
            directory,
            measurements,
            summary,
            next_sequence: 1,
        })
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }

    pub fn update_state(&mut self, state: RunState) -> Result<(), ArtifactError> {
        self.summary.state = state;
        self.summary.updated_unix_ms = unix_time_ms();
        self.summary.warnings = warnings_for_state(&self.summary.state);
        self.write_summary_and_report()
    }

    pub fn record_phase(
        &mut self,
        phase_id: PhaseId,
        report: PhaseReport,
    ) -> Result<(), ArtifactError> {
        self.append_record(ArtifactRecordValue::Phase {
            phase_id,
            report: Box::new(report.clone()),
        })?;
        upsert_phase(&mut self.summary.phases, ArtifactPhase { phase_id, report });
        self.write_summary_and_report()
    }

    pub fn record_decision(&mut self, decision: StrategyDecision) -> Result<(), ArtifactError> {
        self.append_record(ArtifactRecordValue::StrategyDecision {
            decision: decision.clone(),
        })?;
        upsert_decision(&mut self.summary.decisions, decision);
        self.write_summary_and_report()
    }

    pub fn write_diagnostics(
        &mut self,
        diagnostics: &BTreeMap<String, Vec<String>>,
    ) -> Result<(), ArtifactError> {
        let mut body = String::new();
        for (agent, lines) in diagnostics {
            body.push_str(&format!("[{agent}]\n"));
            for line in lines {
                body.push_str(line);
                body.push('\n');
            }
        }
        atomic_bytes(&self.directory.join("adapter.log"), body.as_bytes())
    }

    pub fn finalize(&mut self, state: RunState) -> Result<(), ArtifactError> {
        self.measurements.flush().map_err(ArtifactError::io)?;
        self.measurements
            .get_ref()
            .sync_data()
            .map_err(ArtifactError::io)?;
        self.update_state(state)
    }

    fn append_record(&mut self, value: ArtifactRecordValue) -> Result<(), ArtifactError> {
        let record = ArtifactRecord {
            schema_version: ARTIFACT_SCHEMA_VERSION,
            sequence: self.next_sequence,
            observed_unix_ms: unix_time_ms(),
            value,
        };
        serde_json::to_writer(&mut self.measurements, &record).map_err(ArtifactError::json)?;
        self.measurements
            .write_all(b"\n")
            .map_err(ArtifactError::io)?;
        self.measurements.flush().map_err(ArtifactError::io)?;
        self.measurements
            .get_ref()
            .sync_data()
            .map_err(ArtifactError::io)?;
        self.summary.last_record_sequence = record.sequence;
        self.summary.updated_unix_ms = record.observed_unix_ms;
        self.next_sequence = self.next_sequence.saturating_add(1);
        Ok(())
    }

    fn write_summary_and_report(&self) -> Result<(), ArtifactError> {
        atomic_json(&self.directory.join("summary.json"), &self.summary)?;
        atomic_bytes(
            &self.directory.join("report.svg"),
            render_svg(&self.summary).as_bytes(),
        )
    }
}

pub fn load_artifact(path: &Path) -> Result<ArtifactSummary, ArtifactError> {
    let (directory, summary_path) = artifact_paths(path);
    let mut summary = if summary_path.exists() {
        let file = File::open(&summary_path).map_err(ArtifactError::io)?;
        serde_json::from_reader(file).map_err(ArtifactError::json)?
    } else {
        recover_from_config(&directory)?
    };
    if summary.schema_version > ARTIFACT_SCHEMA_VERSION {
        return Err(ArtifactError::UnsupportedSchema(summary.schema_version));
    }
    let records = read_records(&directory.join("measurements.ndjson"))?;
    let mut recovered = false;
    for record in records {
        if record.sequence <= summary.last_record_sequence {
            continue;
        }
        match record.value {
            ArtifactRecordValue::Phase { phase_id, report } => {
                upsert_phase(
                    &mut summary.phases,
                    ArtifactPhase {
                        phase_id,
                        report: *report,
                    },
                );
            }
            ArtifactRecordValue::StrategyDecision { decision } => {
                upsert_decision(&mut summary.decisions, decision);
            }
        }
        summary.last_record_sequence = record.sequence;
        summary.updated_unix_ms = summary.updated_unix_ms.max(record.observed_unix_ms);
        recovered = true;
    }
    summary.recovered_from_incremental_records |= recovered;
    Ok(summary)
}

pub fn render_artifact(path: &Path, output: Option<&Path>) -> Result<PathBuf, ArtifactError> {
    let summary = load_artifact(path)?;
    let (directory, _) = artifact_paths(path);
    let destination = output
        .map(Path::to_path_buf)
        .unwrap_or_else(|| directory.join("report.svg"));
    atomic_bytes(&destination, render_svg(&summary).as_bytes())?;
    Ok(destination)
}

pub fn human_summary(summary: &ArtifactSummary) -> String {
    let mut lines = vec![
        format!("run: {}", summary.run_id.0),
        format!("state: {}", state_label(&summary.state)),
        format!("measurements: {}", summary.phases.len()),
        format!(
            "adapter: {}",
            summary
                .adapter
                .as_ref()
                .map(|adapter| adapter.name.as_str())
                .unwrap_or("unknown")
        ),
    ];
    if let RunState::Completed { outcome } = &summary.state {
        lines.push(format!("classification: {:?}", outcome.classification));
        if let Some(knee) = &outcome.knee {
            lines.push(format!(
                "knee: {:.1} ops/s ({:.1}-{:.1})",
                knee.offered_rate, knee.lower_bound, knee.upper_bound
            ));
            lines.push(format!(
                "recommended: {:.1} ops/s",
                knee.recommended_operating_rate
            ));
        }
    }
    if summary.recovered_from_incremental_records {
        lines.push("recovery: incorporated incremental records newer than summary.json".into());
    }
    if !summary.warnings.is_empty() {
        lines.push(format!("warnings: {}", summary.warnings.join("; ")));
    }
    lines.join("\n")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ArtifactExitStatus {
    Completed = 0,
    Stopped = 2,
    Invalid = 3,
    Failed = 4,
}

impl ArtifactExitStatus {
    pub fn code(self) -> u8 {
        self as u8
    }
}

pub fn exit_status(state: &RunState) -> ArtifactExitStatus {
    match state {
        RunState::Completed { outcome }
            if matches!(
                outcome.classification,
                RunClassification::GeneratorSaturated | RunClassification::UnstableMeasurement
            ) =>
        {
            ArtifactExitStatus::Invalid
        }
        RunState::Completed { .. } => ArtifactExitStatus::Completed,
        RunState::Stopped => ArtifactExitStatus::Stopped,
        RunState::Failed { .. } => ArtifactExitStatus::Failed,
        _ => ArtifactExitStatus::Invalid,
    }
}

fn recover_from_config(directory: &Path) -> Result<ArtifactSummary, ArtifactError> {
    let path = directory.join("config.json");
    let persisted: PersistedConfig =
        serde_json::from_reader(File::open(&path).map_err(ArtifactError::io)?)
            .map_err(ArtifactError::json)?;
    Ok(ArtifactSummary {
        schema_version: persisted.schema_version,
        protocol_version: persisted.protocol_version,
        tool: ToolIdentity::default(),
        run_id: persisted.run_id,
        created_unix_ms: 0,
        updated_unix_ms: 0,
        adapter: None,
        agents: Vec::new(),
        environment: EnvironmentMetadata::default(),
        config: persisted.config,
        state: RunState::Configured,
        phases: Vec::new(),
        decisions: Vec::new(),
        warnings: vec!["summary.json was missing; recovered available incremental records".into()],
        redactions: persisted.redactions,
        last_record_sequence: 0,
        recovered_from_incremental_records: true,
    })
}

fn read_records(path: &Path) -> Result<Vec<ArtifactRecord>, ArtifactError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(ArtifactError::io(error)),
    };
    let mut records = Vec::new();
    for line in BufReader::new(file).lines() {
        let line = line.map_err(ArtifactError::io)?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str(&line) {
            Ok(record) => records.push(record),
            Err(_) => break,
        }
    }
    Ok(records)
}

fn artifact_paths(path: &Path) -> (PathBuf, PathBuf) {
    if path.is_dir() {
        (path.to_path_buf(), path.join("summary.json"))
    } else {
        (
            path.parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf(),
            path.to_path_buf(),
        )
    }
}

fn redacted_config(config: &RunConfig) -> Result<(Value, Vec<String>), ArtifactError> {
    let mut value = serde_json::to_value(config).map_err(ArtifactError::json)?;
    let mut redacted = false;
    if let Some(agents) = value.get_mut("agents").and_then(Value::as_array_mut) {
        for agent in agents {
            let Some(transport) = agent.get_mut("transport") else {
                continue;
            };
            if transport.get("kind").and_then(Value::as_str) == Some("subprocess")
                && let Some(command) = transport.get_mut("command")
            {
                *command = serde_json::json!({
                    "program": "<redacted>",
                    "arguments": []
                });
                redacted = true;
            }
        }
    }
    let redactions = if redacted {
        vec!["agents[].transport.command".into()]
    } else {
        Vec::new()
    };
    Ok((value, redactions))
}

fn unique_run_directory(base: &Path, run_id: RunId) -> Result<PathBuf, ArtifactError> {
    fs::create_dir_all(base).map_err(ArtifactError::io)?;
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    for suffix in 0..1_000_u16 {
        let name = if suffix == 0 {
            format!("run-{stamp}-{}", run_id.0)
        } else {
            format!("run-{stamp}-{}-{suffix}", run_id.0)
        };
        let candidate = base.join(name);
        match fs::create_dir(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(ArtifactError::io(error)),
        }
    }
    Err(ArtifactError::Message(
        "could not allocate a unique run artifact directory".into(),
    ))
}

fn atomic_json(path: &Path, value: &impl Serialize) -> Result<(), ArtifactError> {
    let bytes = serde_json::to_vec_pretty(value).map_err(ArtifactError::json)?;
    atomic_bytes(path, &bytes)
}

fn atomic_bytes(path: &Path, bytes: &[u8]) -> Result<(), ArtifactError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(ArtifactError::io)?;
    let temporary = parent.join(format!(
        ".{}.tmp-{}-{}",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("artifact"),
        std::process::id(),
        NEXT_TEMPORARY_FILE.fetch_add(1, Ordering::Relaxed)
    ));
    {
        let mut file = BufWriter::new(File::create(&temporary).map_err(ArtifactError::io)?);
        file.write_all(bytes).map_err(ArtifactError::io)?;
        file.flush().map_err(ArtifactError::io)?;
        file.get_ref().sync_all().map_err(ArtifactError::io)?;
    }
    fs::rename(&temporary, path).map_err(ArtifactError::io)
}

fn upsert_phase(phases: &mut Vec<ArtifactPhase>, phase: ArtifactPhase) {
    if let Some(existing) = phases
        .iter_mut()
        .find(|existing| existing.phase_id == phase.phase_id)
    {
        *existing = phase;
    } else {
        phases.push(phase);
        phases.sort_by_key(|phase| phase.phase_id.0);
    }
}

fn upsert_decision(decisions: &mut Vec<StrategyDecision>, decision: StrategyDecision) {
    if let Some(existing) = decisions
        .iter_mut()
        .find(|existing| existing.sequence == decision.sequence)
    {
        *existing = decision;
    } else {
        decisions.push(decision);
        decisions.sort_by_key(|decision| decision.sequence);
    }
}

fn warnings_for_state(state: &RunState) -> Vec<String> {
    match state {
        RunState::Completed { outcome } => outcome.warnings.clone(),
        RunState::Failed { message } => vec![message.clone()],
        RunState::Stopped => vec!["run was stopped before completion".into()],
        _ => Vec::new(),
    }
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn render_svg(summary: &ArtifactSummary) -> String {
    let width = 960.0;
    let height = 540.0;
    let left = 82.0;
    let right = 30.0;
    let top = 100.0;
    let bottom = 72.0;
    let plot_width = width - left - right;
    let plot_height = height - top - bottom;
    let max_x = summary
        .phases
        .iter()
        .map(|phase| phase.report.offered_rate)
        .fold(1.0_f64, f64::max);
    let max_y = summary
        .phases
        .iter()
        .flat_map(|phase| [phase.report.offered_rate, phase.report.goodput_rate])
        .fold(1.0_f64, f64::max);
    let points = summary
        .phases
        .iter()
        .map(|phase| {
            let x = left + phase.report.offered_rate / max_x * plot_width;
            let y = top + plot_height - phase.report.goodput_rate / max_y * plot_height;
            format!("{x:.1},{y:.1}")
        })
        .collect::<Vec<_>>()
        .join(" ");
    let knee = match &summary.state {
        RunState::Completed { outcome } => outcome.knee.as_ref().map(|knee| {
            let x = left + knee.offered_rate / max_x * plot_width;
            format!(
                r##"<line x1="{x:.1}" y1="{top:.1}" x2="{x:.1}" y2="{:.1}" stroke="#f6b94a" stroke-width="2" stroke-dasharray="6 5"/><text x="{:.1}" y="{:.1}" fill="#f6b94a" font-size="14">knee {:.1}</text>"##,
                top + plot_height,
                x + 8.0,
                top + 20.0,
                knee.offered_rate
            )
        }),
        _ => None,
    }
    .unwrap_or_default();
    format!(
        r##"<svg xmlns="http://www.w3.org/2000/svg" width="960" height="540" viewBox="0 0 960 540" role="img" aria-labelledby="title description">
<title id="title">Kneefinder run {}</title><desc id="description">Throughput curve with {} measured phases; state {}</desc>
<rect width="960" height="540" fill="#081017"/><text x="40" y="42" fill="#62ddd5" font-family="sans-serif" font-size="14" letter-spacing="2">KNEEFINDER</text>
<text x="40" y="74" fill="#edf7fb" font-family="sans-serif" font-size="24">Run {} - {}</text>
<line x1="{left}" y1="{:.1}" x2="{:.1}" y2="{:.1}" stroke="#40515e"/><line x1="{left}" y1="{top}" x2="{left}" y2="{:.1}" stroke="#40515e"/>
<line x1="{left}" y1="{:.1}" x2="{:.1}" y2="{top}" stroke="#5288ff" stroke-width="2" opacity=".8"/>
<polyline points="{}" fill="none" stroke="#59d9d1" stroke-width="4" stroke-linejoin="round" stroke-linecap="round"/>{}
<text x="{:.1}" y="512" text-anchor="middle" fill="#98aab6" font-family="sans-serif" font-size="14">Offered operations / second</text>
<text x="22" y="{:.1}" transform="rotate(-90 22 {:.1})" text-anchor="middle" fill="#98aab6" font-family="sans-serif" font-size="14">Goodput operations / second</text>
</svg>"##,
        summary.run_id.0,
        summary.phases.len(),
        xml_escape(state_label(&summary.state)),
        summary.run_id.0,
        xml_escape(state_label(&summary.state)),
        top + plot_height,
        left + plot_width,
        top + plot_height,
        top + plot_height,
        top + plot_height,
        left + plot_width,
        points,
        knee,
        left + plot_width / 2.0,
        top + plot_height / 2.0,
        top + plot_height / 2.0,
    )
}

fn state_label(state: &RunState) -> &'static str {
    match state {
        RunState::Configured => "configured",
        RunState::Starting => "starting",
        RunState::Measuring { .. } => "measuring",
        RunState::Stopping { .. } => "stopping",
        RunState::Completed { .. } => "completed",
        RunState::Stopped => "stopped",
        RunState::Failed { .. } => "failed",
    }
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn default_run_id() -> RunId {
    RunId(0)
}

fn configured_state() -> RunState {
    RunState::Configured
}

#[derive(Debug)]
pub enum ArtifactError {
    Io(String),
    Json(serde_json::Error),
    UnsupportedSchema(u16),
    Message(String),
}

impl ArtifactError {
    fn io(error: io::Error) -> Self {
        Self::Io(error.to_string())
    }

    fn json(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}

impl fmt::Display for ArtifactError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(message) | Self::Message(message) => formatter.write_str(message),
            Self::Json(error) => error.fmt(formatter),
            Self::UnsupportedSchema(version) => write!(
                formatter,
                "artifact schema {version} is newer than supported schema {ARTIFACT_SCHEMA_VERSION}"
            ),
        }
    }
}

impl std::error::Error for ArtifactError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Json(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        agent::{AgentDescriptor, AgentId, AgentInstanceId, AgentPlacement},
        config::{
            AdapterCommand, AgentEndpointConfig, AgentTransportConfig, AnalysisConfig, LoadConfig,
            OperationSelection, PhaseConfig, Preset, Strategy, WorkloadConfig,
        },
        measurement::{RunClassification, RunOutcome},
        protocol::{Capabilities, LoadModel},
        stats::{PhaseQuality, summarize_results},
    };

    static NEXT_TEST_DIRECTORY: AtomicU64 = AtomicU64::new(1);

    fn test_directory(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "kneefinder-artifact-{name}-{}-{}",
            std::process::id(),
            NEXT_TEST_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn config(output_directory: PathBuf) -> RunConfig {
        RunConfig {
            preset: Preset::Quick,
            strategy: Strategy::Sweep,
            phases: PhaseConfig {
                warmup_ms: 0,
                measurement_ms: 1_000,
                recovery_ms: 0,
                repetitions: 1,
            },
            load: LoadConfig {
                initial_rate: 100.0,
                maximum_rate: 200.0,
                growth_factor: 2.0,
                explicit_levels: vec![100.0, 200.0],
                cycles: 1,
            },
            analysis: AnalysisConfig::default(),
            workload: WorkloadConfig {
                operations: OperationSelection::AdapterDefaults,
            },
            output_directory,
            agents: vec![AgentEndpointConfig {
                id: "local-0".into(),
                transport: AgentTransportConfig::Subprocess {
                    command: AdapterCommand {
                        program: "./secret-adapter".into(),
                        arguments: vec!["--token".into(), "very-secret".into()],
                    },
                },
            }],
        }
    }

    fn catalog() -> CohortReady {
        CohortReady {
            agents: vec![AgentDescriptor {
                id: AgentId("local-0".into()),
                instance_id: AgentInstanceId("local-0-test".into()),
                placement: AgentPlacement::Colocated,
            }],
            adapter: AdapterIdentity {
                name: "fixture".into(),
                version: Some("1.0".into()),
            },
            capabilities: Capabilities {
                scheduled_operations: true,
                adapter_managed_phases: false,
                load_models: vec![LoadModel::OpenLoop],
                max_batch_size: None,
            },
            operations: Vec::new(),
        }
    }

    fn report(rate: f64) -> PhaseReport {
        PhaseReport {
            offered_rate: rate,
            goodput_rate: rate - 1.0,
            elapsed_ns: 1_000_000_000,
            in_flight_high_water: 2,
            stats: summarize_results(&[]).unwrap(),
            quality: PhaseQuality::default(),
        }
    }

    #[test]
    fn schema_round_trip_creates_required_redacted_artifacts() {
        let base = test_directory("round-trip");
        let mut writer = ArtifactWriter::create(&config(base.clone()), RunId(7), &catalog())
            .expect("artifact writer should start");
        writer
            .record_phase(PhaseId(1), report(100.0))
            .expect("phase should persist");
        writer
            .write_diagnostics(&BTreeMap::from([(
                "local-0".into(),
                vec!["adapter ready".into()],
            )]))
            .expect("diagnostics should persist");
        writer
            .finalize(RunState::Completed {
                outcome: RunOutcome {
                    classification: RunClassification::NoKneeObserved,
                    knee: None,
                    slo_maximum_rate: None,
                    analysis: None,
                    warnings: vec!["no knee".into()],
                },
            })
            .expect("summary should finalize");
        let directory = writer.directory().to_path_buf();
        drop(writer);

        for name in [
            "summary.json",
            "config.json",
            "measurements.ndjson",
            "report.svg",
            "adapter.log",
        ] {
            assert!(directory.join(name).is_file(), "missing {name}");
        }
        let loaded = load_artifact(&directory).expect("artifact should round trip");
        assert_eq!(loaded.schema_version, ARTIFACT_SCHEMA_VERSION);
        assert_eq!(loaded.protocol_version, PROTOCOL_VERSION);
        assert_eq!(loaded.phases.len(), 1);
        assert_eq!(loaded.warnings, ["no knee"]);
        let config_text = fs::read_to_string(directory.join("config.json")).unwrap();
        assert!(!config_text.contains("very-secret"));
        assert!(!config_text.contains("secret-adapter"));
        assert!(config_text.contains("<redacted>"));
        assert!(
            fs::read_to_string(directory.join("adapter.log"))
                .unwrap()
                .contains("adapter ready")
        );
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn newer_incremental_records_recover_past_a_stale_summary_and_truncated_tail() {
        let base = test_directory("recovery");
        let mut writer = ArtifactWriter::create(&config(base.clone()), RunId(8), &catalog())
            .expect("artifact writer should start");
        let stale = writer.summary.clone();
        writer.record_phase(PhaseId(1), report(150.0)).unwrap();
        let directory = writer.directory().to_path_buf();
        atomic_json(&directory.join("summary.json"), &stale).unwrap();
        writer.measurements.write_all(b"{\"record\":").unwrap();
        writer.measurements.flush().unwrap();
        drop(writer);

        let recovered = load_artifact(&directory).expect("stale summary should recover");
        assert_eq!(recovered.phases.len(), 1);
        assert_eq!(recovered.phases[0].report.offered_rate, 150.0);
        assert!(recovered.recovered_from_incremental_records);
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn schema_zero_summary_fixture_remains_readable() {
        let base = test_directory("compatibility");
        fs::create_dir_all(&base).unwrap();
        atomic_bytes(
            &base.join("summary.json"),
            include_bytes!("../tests/fixtures/artifact-summary-v0.json"),
        )
        .unwrap();

        let loaded = load_artifact(&base).expect("schema zero fixture should load");
        assert_eq!(loaded.schema_version, 0);
        assert_eq!(loaded.run_id, RunId(12));
        assert_eq!(loaded.state, RunState::Stopped);
        assert!(loaded.phases.is_empty());
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn render_regenerates_svg_from_a_summary_path() {
        let base = test_directory("render");
        let writer = ArtifactWriter::create(&config(base.clone()), RunId(9), &catalog()).unwrap();
        let directory = writer.directory().to_path_buf();
        drop(writer);
        let output = directory.join("regenerated.svg");

        assert_eq!(
            render_artifact(&directory.join("summary.json"), Some(&output)).unwrap(),
            output
        );
        let svg = fs::read_to_string(&output).unwrap();
        assert!(svg.contains("<svg"));
        assert!(svg.contains("Kneefinder run 9"));
        fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn terminal_states_have_stable_exit_statuses() {
        let outcome = |classification| RunState::Completed {
            outcome: RunOutcome {
                classification,
                knee: None,
                slo_maximum_rate: None,
                analysis: None,
                warnings: Vec::new(),
            },
        };
        assert_eq!(
            exit_status(&outcome(RunClassification::NoKneeObserved)).code(),
            0
        );
        assert_eq!(
            exit_status(&outcome(RunClassification::GeneratorSaturated)).code(),
            3
        );
        assert_eq!(exit_status(&RunState::Stopped).code(), 2);
        assert_eq!(
            exit_status(&RunState::Failed {
                message: "failed".into()
            })
            .code(),
            4
        );
    }
}

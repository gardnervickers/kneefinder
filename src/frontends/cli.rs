//! Command-line parsing and resolution into engine configuration.

#[cfg(feature = "web")]
use std::net::SocketAddr;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    num::NonZeroU32,
    path::PathBuf,
};

use clap::{Args, Parser, Subcommand, ValueEnum};

use crate::{
    config::{
        AdapterCommand, AgentEndpointConfig, AgentTransportConfig, AnalysisConfig, HumanDuration,
        LoadConfig, OperationSelection, PhaseConfig, Preset, RunConfig, Strategy,
        WeightedOperation, WorkloadConfig,
    },
    engine::{EngineCommand, EngineError, EngineEvent, EngineHandle},
    measurement::RunState,
    protocol::ArgumentValue,
};

use super::Frontend;
#[cfg(feature = "web")]
use super::web::{WebFrontend, WebFrontendError};

#[derive(Debug, Parser)]
#[command(
    name = "kneefinder",
    version,
    about = "Find throughput and latency knees in arbitrary systems",
    propagate_version = true
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run an experiment using a user-provided adapter process.
    Run(Box<RunArgs>),
    /// Serve the browser GUI and versioned control API.
    #[cfg(feature = "web")]
    Serve(ServeArgs),
}

#[cfg(feature = "web")]
#[derive(Debug, Args)]
pub struct ServeArgs {
    /// Address on which the web server listens.
    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: SocketAddr,

    /// Permit unauthenticated control API access from non-loopback interfaces.
    #[arg(long)]
    allow_remote: bool,
}

#[derive(Debug, Args)]
pub struct RunArgs {
    /// Apply a useful group of defaults. Explicit options override the preset.
    #[arg(long, value_enum, default_value_t = PresetArg::Quick)]
    preset: PresetArg,

    /// Choose how load levels are visited.
    #[arg(long, value_enum)]
    strategy: Option<StrategyArg>,

    /// Traffic duration excluded after each load-level transition.
    #[arg(long, value_name = "DURATION")]
    warmup: Option<HumanDuration>,

    /// Traffic duration included in each measurement.
    #[arg(long, value_name = "DURATION")]
    measurement: Option<HumanDuration>,

    /// Idle duration used where the strategy requests recovery.
    #[arg(long, value_name = "DURATION")]
    recovery: Option<HumanDuration>,

    /// Number of measurements collected at each selected load.
    #[arg(long)]
    repetitions: Option<NonZeroU32>,

    /// First offered load in operations per second.
    #[arg(long, value_name = "OPS_PER_SECOND")]
    initial_rate: Option<f64>,

    /// Highest offered load kneefinder may attempt.
    #[arg(long, value_name = "OPS_PER_SECOND")]
    maximum_rate: Option<f64>,

    /// Multiplicative step used to discover or generate load levels.
    #[arg(long, value_name = "FACTOR")]
    growth_factor: Option<f64>,

    /// Explicit comma-delimited load levels for sweep or up-down strategies.
    #[arg(long, value_delimiter = ',', value_name = "RATE,...")]
    levels: Vec<f64>,

    /// Number of complete traversals, primarily for up-down experiments.
    #[arg(long)]
    cycles: Option<NonZeroU32>,

    /// Optional p95 client-latency SLO in milliseconds.
    #[arg(long, value_name = "MILLISECONDS")]
    latency_slo_ms: Option<f64>,

    /// Optional maximum combined error and timeout rate in [0, 1].
    #[arg(long, value_name = "RATE")]
    maximum_unsuccessful_rate: Option<f64>,

    /// Safety multiplier applied to the conservative knee lower bound.
    #[arg(long, default_value_t = 0.8)]
    safety_factor: f64,

    /// Number of deterministic time-bucket bootstrap samples.
    #[arg(long, default_value_t = 400)]
    bootstrap_samples: u32,

    /// Deterministic bootstrap seed recorded in the result.
    #[arg(long, default_value_t = 0x4b4e_4545)]
    bootstrap_seed: u64,

    /// Add a weighted operation variant as NAME[:ARG=VALUE,...][@WEIGHT].
    #[arg(long = "operation", value_name = "VARIANT")]
    operations: Vec<String>,

    /// Explicitly include every operation advertised by the adapter.
    #[arg(long, conflicts_with = "operations")]
    all_operations: bool,

    /// Directory in which run artifacts will be written.
    #[arg(long, default_value = "results")]
    output: PathBuf,

    /// Print the fully resolved JSON configuration without starting a run.
    #[arg(long)]
    pub print_config: bool,

    /// Connect to a remote workload agent. May be repeated.
    #[arg(
        long = "agent-endpoint",
        value_name = "ID=tcp://HOST:PORT",
        action = clap::ArgAction::Append
    )]
    agent_endpoints: Vec<String>,

    /// Colocated adapter executable followed by its arguments. Must appear after `--`.
    #[arg(last = true, value_name = "ADAPTER [ARG]...")]
    adapter: Vec<String>,
}

impl RunArgs {
    pub fn resolve(&self) -> Result<RunConfig, String> {
        let defaults = PresetDefaults::for_preset(self.preset);
        let strategy = self.strategy.unwrap_or(defaults.strategy);
        let warmup_ms = self.warmup.unwrap_or(defaults.warmup).as_millis();
        let measurement_ms = self.measurement.unwrap_or(defaults.measurement).as_millis();
        let recovery_ms = self.recovery.unwrap_or(defaults.recovery).as_millis();
        let repetitions = self
            .repetitions
            .map_or(defaults.repetitions, NonZeroU32::get);
        let cycles = self.cycles.map_or(defaults.cycles, NonZeroU32::get);
        let initial_rate = self.initial_rate.unwrap_or(100.0);
        let maximum_rate = self.maximum_rate.unwrap_or(10_000.0);
        let growth_factor = self.growth_factor.unwrap_or(1.5);
        let analysis = AnalysisConfig {
            latency_slo_ms: self.latency_slo_ms,
            maximum_unsuccessful_rate: self.maximum_unsuccessful_rate,
            safety_factor: self.safety_factor,
            bootstrap_samples: self.bootstrap_samples,
            bootstrap_seed: self.bootstrap_seed,
        };
        analysis.validate()?;

        if measurement_ms == 0 {
            return Err("measurement duration must be greater than zero".into());
        }
        if !initial_rate.is_finite() || initial_rate <= 0.0 {
            return Err("initial rate must be a positive finite number".into());
        }
        if !maximum_rate.is_finite() || maximum_rate < initial_rate {
            return Err("maximum rate must be finite and at least the initial rate".into());
        }
        if !growth_factor.is_finite() || growth_factor <= 1.0 {
            return Err("growth factor must be finite and greater than one".into());
        }
        if self
            .levels
            .iter()
            .any(|level| !level.is_finite() || *level <= 0.0)
        {
            return Err("every explicit load level must be positive and finite".into());
        }
        if strategy == StrategyArg::Adaptive && !self.levels.is_empty() {
            return Err("--levels cannot be used with the adaptive strategy".into());
        }

        let mut agents = self
            .agent_endpoints
            .iter()
            .map(|endpoint| parse_agent_endpoint(endpoint))
            .collect::<Result<Vec<_>, _>>()?;
        if let Some((program, arguments)) = self.adapter.split_first() {
            agents.push(AgentEndpointConfig {
                id: "local-0".into(),
                transport: AgentTransportConfig::Subprocess {
                    command: AdapterCommand {
                        program: program.clone(),
                        arguments: arguments.to_vec(),
                    },
                },
            });
        }
        let mut agent_ids = BTreeSet::new();
        for agent in &agents {
            if !agent_ids.insert(&agent.id) {
                return Err(format!(
                    "agent id {:?} is configured more than once",
                    agent.id
                ));
            }
        }
        if agents.is_empty() && !self.print_config {
            return Err(
                "at least one agent is required: use --agent-endpoint or a command after `--`"
                    .into(),
            );
        }
        let operations = if self.all_operations {
            OperationSelection::All
        } else if self.operations.is_empty() {
            OperationSelection::AdapterDefaults
        } else {
            OperationSelection::Selected {
                operations: self
                    .operations
                    .iter()
                    .map(|operation| parse_operation(operation))
                    .collect::<Result<_, _>>()?,
            }
        };
        Ok(RunConfig {
            preset: self.preset.into(),
            strategy: strategy.into(),
            phases: PhaseConfig {
                warmup_ms,
                measurement_ms,
                recovery_ms,
                repetitions,
            },
            load: LoadConfig {
                initial_rate,
                maximum_rate,
                growth_factor,
                explicit_levels: self.levels.clone(),
                cycles,
            },
            analysis,
            workload: WorkloadConfig { operations },
            output_directory: self.output.clone(),
            agents,
        })
    }
}

fn parse_agent_endpoint(value: &str) -> Result<AgentEndpointConfig, String> {
    let (id, endpoint) = value
        .split_once('=')
        .ok_or_else(|| format!("agent endpoint {value:?} must be ID=tcp://HOST:PORT"))?;
    if id.is_empty()
        || !id
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        return Err(format!(
            "agent id {id:?} must contain only ASCII letters, digits, '-' or '_'"
        ));
    }
    let address = endpoint
        .strip_prefix("tcp://")
        .ok_or_else(|| format!("agent endpoint {value:?} must use tcp://"))?;
    let (host, port) = address
        .rsplit_once(':')
        .ok_or_else(|| format!("agent endpoint {value:?} must include a TCP port"))?;
    let port = port
        .parse::<u16>()
        .map_err(|_| format!("agent endpoint {value:?} has an invalid TCP port"))?;
    if host.is_empty() || port == 0 || address.chars().any(char::is_whitespace) {
        return Err(format!(
            "agent endpoint {value:?} is not a valid TCP address"
        ));
    }

    Ok(AgentEndpointConfig {
        id: id.into(),
        transport: AgentTransportConfig::Tcp {
            address: address.into(),
        },
    })
}

fn parse_operation(value: &str) -> Result<WeightedOperation, String> {
    let (variant, weight) = if let Some((variant, weight)) = value.rsplit_once('@') {
        let weight = weight
            .parse::<f64>()
            .map_err(|_| format!("invalid weight in operation variant {value:?}"))?;
        (variant, weight)
    } else if !value.contains(':') {
        match value.split_once('=') {
            Some((name, weight)) => {
                let weight = weight
                    .parse::<f64>()
                    .map_err(|_| format!("invalid weight in operation {value:?}"))?;
                (name, weight)
            }
            None => (value, 1.0),
        }
    } else {
        (value, 1.0)
    };
    let (name, arguments) = match variant.split_once(':') {
        Some((name, arguments)) => {
            let mut parsed = BTreeMap::new();
            for assignment in arguments.split(',') {
                let (argument, value) = assignment.split_once('=').ok_or_else(|| {
                    format!("argument {assignment:?} in operation {name:?} must contain '='")
                })?;
                if argument.is_empty() {
                    return Err(format!("operation {name:?} has an empty argument name"));
                }
                if parsed
                    .insert(argument.into(), parse_argument_value(value))
                    .is_some()
                {
                    return Err(format!(
                        "operation {name:?} provides argument {argument:?} more than once"
                    ));
                }
            }
            (name, parsed)
        }
        None => (variant, BTreeMap::new()),
    };
    if name.is_empty() {
        return Err("operation name must not be empty".into());
    }
    if !weight.is_finite() || weight <= 0.0 {
        return Err(format!(
            "operation {name:?} must have a positive finite weight"
        ));
    }
    Ok(WeightedOperation {
        name: name.into(),
        weight,
        arguments,
    })
}

fn parse_argument_value(value: &str) -> ArgumentValue {
    if let Some(value) = value.strip_prefix("str:") {
        ArgumentValue::String(value.into())
    } else if let Ok(value) = value.parse::<i64>() {
        ArgumentValue::Integer(value)
    } else {
        ArgumentValue::String(value.into())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum PresetArg {
    Quick,
    Careful,
    Hysteresis,
}

impl From<PresetArg> for Preset {
    fn from(value: PresetArg) -> Self {
        match value {
            PresetArg::Quick => Self::Quick,
            PresetArg::Careful => Self::Careful,
            PresetArg::Hysteresis => Self::Hysteresis,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum StrategyArg {
    Adaptive,
    Sweep,
    UpDown,
}

impl From<StrategyArg> for Strategy {
    fn from(value: StrategyArg) -> Self {
        match value {
            StrategyArg::Adaptive => Self::Adaptive,
            StrategyArg::Sweep => Self::Sweep,
            StrategyArg::UpDown => Self::UpDown,
        }
    }
}

struct PresetDefaults {
    strategy: StrategyArg,
    warmup: HumanDuration,
    measurement: HumanDuration,
    recovery: HumanDuration,
    repetitions: u32,
    cycles: u32,
}

impl PresetDefaults {
    fn for_preset(preset: PresetArg) -> Self {
        let duration = |value: &str| -> HumanDuration {
            value.parse().expect("static preset duration must be valid")
        };
        match preset {
            PresetArg::Quick => Self {
                strategy: StrategyArg::Adaptive,
                warmup: duration("2s"),
                measurement: duration("10s"),
                recovery: duration("2s"),
                repetitions: 1,
                cycles: 1,
            },
            PresetArg::Careful => Self {
                strategy: StrategyArg::Adaptive,
                warmup: duration("10s"),
                measurement: duration("30s"),
                recovery: duration("10s"),
                repetitions: 3,
                cycles: 1,
            },
            PresetArg::Hysteresis => Self {
                strategy: StrategyArg::UpDown,
                warmup: duration("10s"),
                measurement: duration("20s"),
                recovery: duration("15s"),
                repetitions: 1,
                cycles: 3,
            },
        }
    }
}

pub struct CliFrontend {
    cli: Cli,
}

impl CliFrontend {
    pub fn new(cli: Cli) -> Self {
        Self { cli }
    }
}

impl Frontend for CliFrontend {
    type Error = CliFrontendError;

    fn run(self, engine: EngineHandle) -> Result<(), Self::Error> {
        match self.cli.command {
            Command::Run(arguments) => {
                let config = arguments.resolve().map_err(CliFrontendError::Config)?;
                let events = engine.subscribe();
                let snapshot = engine.execute(EngineCommand::Configure {
                    config: Box::new(config),
                })?;

                if arguments.print_config {
                    println!("{}", serde_json::to_string_pretty(&snapshot.config)?);
                    return Ok(());
                }

                engine.execute(EngineCommand::PrepareAgents {
                    run_id: snapshot.run_id,
                })?;
                engine.execute(EngineCommand::Start {
                    run_id: snapshot.run_id,
                })?;
                loop {
                    match events
                        .recv()
                        .map_err(|_| CliFrontendError::EventStreamClosed)?
                    {
                        EngineEvent::PhaseStats {
                            run_id,
                            phase_id,
                            report,
                        } if run_id == snapshot.run_id => {
                            eprintln!(
                                "phase {}: offered {:.1} ops/s, goodput {:.1} ops/s, p95 {}",
                                phase_id.0,
                                report.offered_rate,
                                report.goodput_rate,
                                report.stats.overall.client_latency_ns.p95.map_or_else(
                                    || "n/a".into(),
                                    |value| format!("{:.2} ms", value as f64 / 1_000_000.0)
                                )
                            );
                        }
                        EngineEvent::RunStateChanged {
                            snapshot: completed,
                            ..
                        } if completed.run_id == snapshot.run_id
                            && completed.state.is_terminal() =>
                        {
                            match &completed.state {
                                RunState::Failed { message } => {
                                    return Err(CliFrontendError::RunFailed(message.clone()));
                                }
                                _ => {
                                    println!("{}", serde_json::to_string_pretty(&completed)?);
                                    return Ok(());
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            #[cfg(feature = "web")]
            Command::Serve(arguments) => {
                WebFrontend::new(arguments.bind, arguments.allow_remote).run(engine)?;
                Ok(())
            }
        }
    }
}

#[derive(Debug)]
pub enum CliFrontendError {
    Config(String),
    Engine(EngineError),
    Serialization(serde_json::Error),
    #[cfg(feature = "web")]
    Web(WebFrontendError),
    EventStreamClosed,
    RunFailed(String),
}

impl fmt::Display for CliFrontendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Config(message) => formatter.write_str(message),
            Self::Engine(error) => error.fmt(formatter),
            Self::Serialization(error) => error.fmt(formatter),
            #[cfg(feature = "web")]
            Self::Web(error) => error.fmt(formatter),
            Self::EventStreamClosed => {
                formatter.write_str("the engine event stream closed before the run completed")
            }
            Self::RunFailed(message) => write!(formatter, "run failed: {message}"),
        }
    }
}

impl std::error::Error for CliFrontendError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Engine(error) => Some(error),
            Self::Serialization(error) => Some(error),
            #[cfg(feature = "web")]
            Self::Web(error) => Some(error),
            _ => None,
        }
    }
}

impl From<EngineError> for CliFrontendError {
    fn from(value: EngineError) -> Self {
        Self::Engine(value)
    }
}

impl From<serde_json::Error> for CliFrontendError {
    fn from(value: serde_json::Error) -> Self {
        Self::Serialization(value)
    }
}

#[cfg(feature = "web")]
impl From<WebFrontendError> for CliFrontendError {
    fn from(value: WebFrontendError) -> Self {
        Self::Web(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(arguments: &[&str]) -> RunArgs {
        match Cli::try_parse_from(arguments).unwrap().command {
            Command::Run(arguments) => *arguments,
            #[cfg(feature = "web")]
            Command::Serve(_) => panic!("test helper expected the run command"),
        }
    }

    #[test]
    fn quick_preset_resolves_with_an_adapter() {
        let arguments = parse(&["kneefinder", "run", "--", "./adapter", "--verbose"]);
        let config = arguments.resolve().unwrap();

        assert_eq!(config.strategy, Strategy::Adaptive);
        assert_eq!(config.phases.warmup_ms, 2_000);
        assert_eq!(config.phases.measurement_ms, 10_000);
        assert_eq!(config.analysis, AnalysisConfig::default());
        assert_eq!(config.agents.len(), 1);
        assert_eq!(config.agents[0].id, "local-0");
        assert_eq!(
            config.agents[0].transport,
            AgentTransportConfig::Subprocess {
                command: AdapterCommand {
                    program: "./adapter".into(),
                    arguments: vec!["--verbose".into()],
                }
            }
        );
    }

    #[test]
    fn explicit_options_override_hysteresis_preset() {
        let arguments = parse(&[
            "kneefinder",
            "run",
            "--preset",
            "hysteresis",
            "--warmup",
            "3s",
            "--levels",
            "100,200,400",
            "--cycles",
            "2",
            "--",
            "./adapter",
        ]);
        let config = arguments.resolve().unwrap();

        assert_eq!(config.strategy, Strategy::UpDown);
        assert_eq!(config.phases.warmup_ms, 3_000);
        assert_eq!(config.load.explicit_levels, vec![100.0, 200.0, 400.0]);
        assert_eq!(config.load.cycles, 2);
    }

    #[test]
    fn print_config_does_not_require_an_adapter() {
        let arguments = parse(&["kneefinder", "run", "--print-config"]);
        assert!(arguments.resolve().unwrap().agents.is_empty());
    }

    #[test]
    fn remote_and_colocated_agents_can_be_combined() {
        let arguments = parse(&[
            "kneefinder",
            "run",
            "--agent-endpoint",
            "east=tcp://loadgen.example:9000",
            "--agent-endpoint",
            "west=tcp://[::1]:9001",
            "--",
            "./adapter",
        ]);
        let config = arguments.resolve().unwrap();

        assert_eq!(config.agents.len(), 3);
        assert_eq!(config.agents[0].id, "east");
        assert_eq!(
            config.agents[0].transport,
            AgentTransportConfig::Tcp {
                address: "loadgen.example:9000".into()
            }
        );
        assert_eq!(config.agents[1].id, "west");
        assert_eq!(config.agents[2].id, "local-0");
    }

    #[test]
    fn duplicate_agent_ids_are_rejected() {
        let arguments = parse(&[
            "kneefinder",
            "run",
            "--agent-endpoint",
            "local-0=tcp://127.0.0.1:9000",
            "--",
            "./adapter",
        ]);

        assert!(arguments.resolve().unwrap_err().contains("more than once"));
    }

    #[test]
    fn invalid_strategy_combination_is_rejected() {
        let arguments = parse(&[
            "kneefinder",
            "run",
            "--strategy",
            "adaptive",
            "--levels",
            "100,200",
            "--print-config",
        ]);
        assert!(arguments.resolve().is_err());
    }

    #[test]
    fn analysis_slos_and_bootstrap_are_resolved() {
        let arguments = parse(&[
            "kneefinder",
            "run",
            "--latency-slo-ms",
            "25",
            "--maximum-unsuccessful-rate",
            "0.01",
            "--safety-factor",
            "0.75",
            "--bootstrap-samples",
            "200",
            "--bootstrap-seed",
            "42",
            "--print-config",
        ]);
        let config = arguments.resolve().unwrap();

        assert_eq!(config.analysis.latency_slo_ms, Some(25.0));
        assert_eq!(config.analysis.maximum_unsuccessful_rate, Some(0.01));
        assert_eq!(config.analysis.safety_factor, 0.75);
        assert_eq!(config.analysis.bootstrap_samples, 200);
        assert_eq!(config.analysis.bootstrap_seed, 42);
    }

    #[test]
    fn operation_mix_is_resolved_for_every_frontend() {
        let arguments = parse(&[
            "kneefinder",
            "run",
            "--operation",
            "read:key=42@9",
            "--operation",
            "write:value=str:123@1",
            "--print-config",
        ]);
        let config = arguments.resolve().unwrap();

        assert_eq!(
            config.workload.operations,
            OperationSelection::Selected {
                operations: vec![
                    WeightedOperation {
                        name: "read".into(),
                        weight: 9.0,
                        arguments: BTreeMap::from([("key".into(), ArgumentValue::Integer(42),)]),
                    },
                    WeightedOperation {
                        name: "write".into(),
                        weight: 1.0,
                        arguments: BTreeMap::from([(
                            "value".into(),
                            ArgumentValue::String("123".into()),
                        )]),
                    },
                ]
            }
        );
    }
}

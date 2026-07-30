//! Resolved, frontend-independent experiment configuration.

use std::{collections::BTreeMap, fmt, path::PathBuf, str::FromStr, time::Duration};

use serde::{Deserialize, Serialize};

use crate::protocol::ArgumentValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Preset {
    Quick,
    Careful,
    Hysteresis,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Strategy {
    Adaptive,
    Sweep,
    UpDown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunConfig {
    pub preset: Preset,
    pub strategy: Strategy,
    pub phases: PhaseConfig,
    pub load: LoadConfig,
    #[serde(default)]
    pub analysis: AnalysisConfig,
    pub workload: WorkloadConfig,
    pub output_directory: PathBuf,
    pub agents: Vec<AgentEndpointConfig>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct AnalysisConfig {
    /// Optional p95 client-latency service-level objective.
    pub latency_slo_ms: Option<f64>,
    /// Optional maximum combined error and timeout rate in the range `[0, 1]`.
    pub maximum_unsuccessful_rate: Option<f64>,
    /// Multiplier applied to the conservative knee lower bound.
    pub safety_factor: f64,
    /// Deterministic time-bucket bootstrap iterations.
    pub bootstrap_samples: u32,
    /// Seed recorded with the fit for reproducibility.
    pub bootstrap_seed: u64,
}

impl Default for AnalysisConfig {
    fn default() -> Self {
        Self {
            latency_slo_ms: None,
            maximum_unsuccessful_rate: None,
            safety_factor: 0.80,
            bootstrap_samples: 400,
            bootstrap_seed: 0x4b4e_4545,
        }
    }
}

impl AnalysisConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !self.safety_factor.is_finite() || self.safety_factor <= 0.0 || self.safety_factor > 1.0
        {
            return Err("analysis safety factor must be finite and in (0, 1]".into());
        }
        if self.bootstrap_samples == 0 {
            return Err("analysis bootstrap sample count must be greater than zero".into());
        }
        if self
            .latency_slo_ms
            .is_some_and(|value| !value.is_finite() || value <= 0.0)
        {
            return Err("latency SLO must be a positive finite number".into());
        }
        if self
            .maximum_unsuccessful_rate
            .is_some_and(|value| !value.is_finite() || !(0.0..=1.0).contains(&value))
        {
            return Err("maximum unsuccessful rate must be finite and in [0, 1]".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkloadConfig {
    pub operations: OperationSelection,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "selection", rename_all = "snake_case")]
pub enum OperationSelection {
    /// Use the operations and weights the adapter marks as defaults.
    AdapterDefaults,
    /// Use every advertised operation. This must be explicitly requested.
    All,
    /// Use only the named operations with the supplied relative weights.
    Selected { operations: Vec<WeightedOperation> },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WeightedOperation {
    pub name: String,
    pub weight: f64,
    #[serde(default)]
    pub arguments: BTreeMap<String, ArgumentValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseConfig {
    pub warmup_ms: u64,
    pub measurement_ms: u64,
    pub recovery_ms: u64,
    pub repetitions: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LoadConfig {
    pub initial_rate: f64,
    pub maximum_rate: f64,
    pub growth_factor: f64,
    pub explicit_levels: Vec<f64>,
    pub cycles: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterCommand {
    pub program: String,
    pub arguments: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentEndpointConfig {
    /// Stable identity used for deterministic schedule assignment and attribution.
    pub id: String,
    pub transport: AgentTransportConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentTransportConfig {
    /// Coordinator-owned colocated agent using the adapter protocol over stdio.
    Subprocess { command: AdapterCommand },
    /// Coordinator-initiated persistent TCP connection to an explicit endpoint.
    Tcp { address: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HumanDuration(Duration);

impl HumanDuration {
    pub fn as_millis(self) -> u64 {
        self.0.as_millis().min(u64::MAX as u128) as u64
    }
}

impl FromStr for HumanDuration {
    type Err = ParseDurationError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let (number, multiplier) = if let Some(number) = input.strip_suffix("ms") {
            (number, 1_u64)
        } else if let Some(number) = input.strip_suffix('s') {
            (number, 1_000)
        } else if let Some(number) = input.strip_suffix('m') {
            (number, 60_000)
        } else if let Some(number) = input.strip_suffix('h') {
            (number, 3_600_000)
        } else {
            return Err(ParseDurationError(input.into()));
        };

        let value = number
            .parse::<u64>()
            .ok()
            .and_then(|value| value.checked_mul(multiplier))
            .ok_or_else(|| ParseDurationError(input.into()))?;
        Ok(Self(Duration::from_millis(value)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseDurationError(String);

impl fmt::Display for ParseDurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid duration {:?}; use an integer followed by ms, s, m, or h",
            self.0
        )
    }
}

impl std::error::Error for ParseDurationError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_human_durations() {
        assert_eq!("250ms".parse::<HumanDuration>().unwrap().as_millis(), 250);
        assert_eq!("5s".parse::<HumanDuration>().unwrap().as_millis(), 5_000);
        assert_eq!("2m".parse::<HumanDuration>().unwrap().as_millis(), 120_000);
        assert!("5".parse::<HumanDuration>().is_err());
    }
}

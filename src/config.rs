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
    pub workload: WorkloadConfig,
    pub output_directory: PathBuf,
    pub adapter: Option<AdapterCommand>,
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

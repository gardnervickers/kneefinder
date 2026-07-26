//! Versioned messages exchanged with a workload adapter.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub const PROTOCOL_VERSION: u16 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RunId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PhaseId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OperationId(pub u64);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControllerMessage {
    Initialize {
        protocol_version: u16,
        run_id: RunId,
        #[serde(default)]
        config: Value,
    },
    Schedule {
        phase_id: PhaseId,
        phase_start_unix_ns: u64,
        operations: Vec<ScheduledOperation>,
    },
    RunPhase {
        phase_id: PhaseId,
        warmup_ms: u64,
        duration_ms: u64,
        timeout_ms: u64,
        load: Load,
        #[serde(default)]
        parameters: Value,
    },
    CancelPhase {
        phase_id: PhaseId,
    },
    Shutdown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AdapterMessage {
    Ready {
        protocol_version: u16,
        identity: AdapterIdentity,
        capabilities: Capabilities,
        #[serde(default)]
        operations: Vec<OperationDescriptor>,
    },
    Results {
        phase_id: PhaseId,
        operations: Vec<OperationResult>,
    },
    PhaseComplete {
        phase_id: PhaseId,
        result: PhaseResult,
    },
    Error {
        phase_id: Option<PhaseId>,
        code: String,
        message: String,
        retryable: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterIdentity {
    pub name: String,
    pub version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    pub scheduled_operations: bool,
    pub adapter_managed_phases: bool,
    #[serde(default)]
    pub load_models: Vec<LoadModel>,
    pub max_batch_size: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OperationDescriptor {
    /// Stable machine-readable name referenced by scheduled operations.
    pub name: String,
    pub description: Option<String>,
    pub kind: OperationKind,
    pub enabled_by_default: bool,
    pub default_weight: f64,
    #[serde(default)]
    pub arguments: Vec<OperationArgument>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationArgument {
    pub name: String,
    pub description: Option<String>,
    pub kind: ArgumentKind,
    pub required: bool,
    pub default: Option<ArgumentValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArgumentKind {
    Integer,
    String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ArgumentValue {
    Integer(i64),
    String(String),
}

impl ArgumentValue {
    pub fn kind(&self) -> ArgumentKind {
        match self {
            Self::Integer(_) => ArgumentKind::Integer,
            Self::String(_) => ArgumentKind::String,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    Read,
    Write,
    Administrative,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoadModel {
    OpenLoop,
    ClosedLoop,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "model", rename_all = "snake_case")]
pub enum Load {
    OpenLoop { requests_per_second: f64 },
    ClosedLoop { concurrency: u32 },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScheduledOperation {
    pub id: OperationId,
    pub operation: String,
    /// Intended start relative to the phase start.
    pub start_offset_ns: u64,
    #[serde(default)]
    pub arguments: BTreeMap<String, ArgumentValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationResult {
    pub id: OperationId,
    pub operation: String,
    #[serde(default)]
    pub arguments: BTreeMap<String, ArgumentValue>,
    pub intended_start_offset_ns: u64,
    pub actual_start_offset_ns: u64,
    pub client_latency_ns: u64,
    pub status: OperationStatus,
}

impl OperationResult {
    pub fn dispatch_lag_ns(&self) -> u64 {
        self.actual_start_offset_ns
            .saturating_sub(self.intended_start_offset_ns)
    }

    pub fn total_latency_ns(&self) -> u64 {
        self.dispatch_lag_ns()
            .saturating_add(self.client_latency_ns)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum OperationStatus {
    Ok,
    Error { code: Option<String> },
    Timeout,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseResult {
    pub offered: u64,
    pub started: u64,
    pub completed: u64,
    pub successful: u64,
    pub failed: u64,
    pub timed_out: u64,
    pub elapsed_ns: u64,
    pub in_flight_high_water: u64,
    pub client_latency: EncodedHistogram,
    pub total_latency: EncodedHistogram,
    pub dispatch_lag: EncodedHistogram,
    #[serde(default)]
    pub time_buckets: Vec<TimeBucket>,
    #[serde(default)]
    pub per_operation: Vec<OperationPhaseResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationPhaseResult {
    pub operation: String,
    #[serde(default)]
    pub arguments: BTreeMap<String, ArgumentValue>,
    pub offered: u64,
    pub started: u64,
    pub completed: u64,
    pub successful: u64,
    pub failed: u64,
    pub timed_out: u64,
    pub client_latency: EncodedHistogram,
    pub total_latency: EncodedHistogram,
    pub dispatch_lag: EncodedHistogram,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncodedHistogram {
    pub encoding: String,
    pub data: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeBucket {
    pub start_offset_ns: u64,
    pub duration_ns: u64,
    pub offered: u64,
    pub successful: u64,
    pub failed: u64,
    pub timed_out: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn controller_message_round_trips_as_tagged_json() {
        let message = ControllerMessage::Schedule {
            phase_id: PhaseId(3),
            phase_start_unix_ns: 42,
            operations: vec![ScheduledOperation {
                id: OperationId(9),
                operation: "lookup".into(),
                start_offset_ns: 1_000,
                arguments: BTreeMap::from([(
                    "key".into(),
                    ArgumentValue::String("example".into()),
                )]),
            }],
        };

        let json = serde_json::to_string(&message).unwrap();
        assert!(json.contains(r#""type":"schedule""#));
        assert_eq!(
            serde_json::from_str::<ControllerMessage>(&json).unwrap(),
            message
        );
    }

    #[test]
    fn operation_result_separates_dispatch_and_client_latency() {
        let result = OperationResult {
            id: OperationId(1),
            operation: "lookup".into(),
            arguments: BTreeMap::new(),
            intended_start_offset_ns: 1_000,
            actual_start_offset_ns: 1_250,
            client_latency_ns: 750,
            status: OperationStatus::Ok,
        };

        assert_eq!(result.dispatch_lag_ns(), 250);
        assert_eq!(result.total_latency_ns(), 1_000);
    }
}

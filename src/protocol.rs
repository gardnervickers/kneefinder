//! Versioned messages exchanged with a workload adapter.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub use crate::histogram::{EncodedHistogram, HistogramEncoding, HistogramSpec};

pub const PROTOCOL_VERSION: u16 = 4;

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
    PreparePhase {
        phase_id: PhaseId,
        request: ManagedPhaseRequest,
    },
    StartPhase {
        phase_id: PhaseId,
        phase_start_unix_ns: u64,
    },
    CancelPhase {
        phase_id: PhaseId,
        /// Coordinator-owned absolute cutoff for a managed phase. `None`
        /// requests immediate cleanup of a prepared phase that never started.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cancel_at_unix_ns: Option<u64>,
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
    PhaseReady {
        phase_id: PhaseId,
    },
    PhaseStarted {
        phase_id: PhaseId,
    },
    PhaseComplete {
        phase_id: PhaseId,
        completion: PhaseCompletion,
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
    pub adapter_managed_phases: bool,
    #[serde(default)]
    pub load_models: Vec<LoadModel>,
    #[serde(default)]
    pub histogram_encodings: Vec<HistogramEncoding>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OperationDescriptor {
    /// Stable machine-readable name referenced by bound operation variants.
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
    /// Ordered choices for an enum argument. Empty for integer and free-form
    /// string arguments.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<String>,
    pub required: bool,
    pub default: Option<ArgumentValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArgumentKind {
    Integer,
    String,
    Enum,
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
pub struct ManagedPhaseRequest {
    pub warmup_ns: u64,
    pub measurement_ns: u64,
    pub operation_timeout_ns: u64,
    pub load: Load,
    pub operations: Vec<ManagedOperation>,
    pub bucket_count: u16,
    pub histogram: HistogramSpec,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ManagedOperation {
    pub operation: String,
    #[serde(default)]
    pub arguments: BTreeMap<String, ArgumentValue>,
    pub weight: f64,
    pub shard_index: u32,
    pub shard_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhaseCompletion {
    Completed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OperationInvocation {
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
    pub successful_in_window: u64,
    pub failed: u64,
    pub timed_out: u64,
    #[serde(default)]
    pub errors_by_code: Vec<PhaseErrorCount>,
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
    pub successful_in_window: u64,
    pub failed: u64,
    pub timed_out: u64,
    #[serde(default)]
    pub errors_by_code: Vec<PhaseErrorCount>,
    pub client_latency: EncodedHistogram,
    pub total_latency: EncodedHistogram,
    pub dispatch_lag: EncodedHistogram,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseErrorCount {
    pub code: Option<String>,
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeBucket {
    pub start_offset_ns: u64,
    pub duration_ns: u64,
    pub offered: u64,
    pub started: u64,
    pub completed: u64,
    pub successful: u64,
    pub failed: u64,
    pub timed_out: u64,
    pub in_flight_high_water: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn controller_message_round_trips_as_tagged_json() {
        let message = ControllerMessage::PreparePhase {
            phase_id: PhaseId(3),
            request: ManagedPhaseRequest {
                warmup_ns: 0,
                measurement_ns: 1_000,
                operation_timeout_ns: 100,
                load: Load::OpenLoop {
                    requests_per_second: 10.0,
                },
                operations: vec![ManagedOperation {
                    operation: "lookup".into(),
                    arguments: BTreeMap::from([(
                        "key".into(),
                        ArgumentValue::String("example".into()),
                    )]),
                    weight: 1.0,
                    shard_index: 0,
                    shard_count: 1,
                }],
                bucket_count: 1,
                histogram: HistogramSpec {
                    lowest_discernible_ns: 1,
                    highest_trackable_ns: 1_000,
                    significant_figures: 3,
                },
            },
        };

        let json = serde_json::to_string(&message).unwrap();
        assert!(json.contains(r#""type":"prepare_phase""#));
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

    #[test]
    fn managed_phase_messages_round_trip_with_typed_contracts() {
        let message = ControllerMessage::PreparePhase {
            phase_id: PhaseId(4),
            request: ManagedPhaseRequest {
                warmup_ns: 10,
                measurement_ns: 100,
                operation_timeout_ns: 20,
                load: Load::OpenLoop {
                    requests_per_second: 1_000.0,
                },
                operations: vec![ManagedOperation {
                    operation: "lookup".into(),
                    arguments: BTreeMap::from([("key".into(), ArgumentValue::Integer(1))]),
                    weight: 1.0,
                    shard_index: 0,
                    shard_count: 2,
                }],
                bucket_count: 5,
                histogram: HistogramSpec {
                    lowest_discernible_ns: 1,
                    highest_trackable_ns: 1_000_000,
                    significant_figures: 3,
                },
            },
        };

        let json = serde_json::to_string(&message).unwrap();
        assert!(json.contains(r#""type":"prepare_phase""#));
        assert!(json.contains(r#""shard_count":2"#));
        assert_eq!(
            serde_json::from_str::<ControllerMessage>(&json).unwrap(),
            message
        );
    }

    #[test]
    fn enum_argument_descriptor_round_trips_with_ordered_values() {
        let argument = OperationArgument {
            name: "size".into(),
            description: Some("payload size".into()),
            kind: ArgumentKind::Enum,
            values: vec!["small".into(), "large".into()],
            required: true,
            default: Some(ArgumentValue::String("small".into())),
        };

        let json = serde_json::to_string(&argument).unwrap();
        assert!(json.contains(r#""kind":"enum""#));
        assert!(json.contains(r#""values":["small","large"]"#));
        assert_eq!(
            serde_json::from_str::<OperationArgument>(&json).unwrap(),
            argument
        );
    }
}

//! Pure run-lifecycle transitions shared by every frontend.

use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MeasurementStage {
    Baseline,
    Discovery,
    Refinement,
    Validation,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum RunState {
    Configured,
    Starting,
    Measuring {
        stage: MeasurementStage,
    },
    Stopping {
        interrupted_stage: Option<MeasurementStage>,
    },
    Completed {
        outcome: RunOutcome,
    },
    Stopped,
    Failed {
        message: String,
    },
}

impl RunState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed { .. } | Self::Stopped | Self::Failed { .. }
        )
    }

    /// Applies one lifecycle event without performing any I/O.
    pub fn transition(self, event: RunEvent) -> Result<Self, TransitionError> {
        use MeasurementStage::*;
        use RunEvent::*;

        let next = match (&self, &event) {
            (Self::Configured, StartRequested) => Self::Starting,
            (Self::Starting, AdapterReady) => Self::Measuring { stage: Baseline },
            (Self::Measuring { stage: Baseline }, BaselineEstablished) => {
                Self::Measuring { stage: Discovery }
            }
            (Self::Measuring { stage: Discovery }, SaturationBracketed) => {
                Self::Measuring { stage: Refinement }
            }
            (Self::Measuring { stage: Refinement }, BracketRefined) => {
                Self::Measuring { stage: Validation }
            }
            (Self::Measuring { .. }, AnalysisStarted) => Self::Measuring { stage: Validation },
            (Self::Measuring { stage: Validation }, CandidateValidated { outcome }) => {
                Self::Completed {
                    outcome: outcome.clone(),
                }
            }
            (Self::Measuring { .. }, RunCompleted { outcome }) => Self::Completed {
                outcome: outcome.clone(),
            },
            (Self::Starting, StopRequested) => Self::Stopping {
                interrupted_stage: None,
            },
            (Self::Measuring { stage }, StopRequested) => Self::Stopping {
                interrupted_stage: Some(*stage),
            },
            (Self::Stopping { .. }, AdapterStopped) => Self::Stopped,
            (state, Failed { message }) if !state.is_terminal() => Self::Failed {
                message: message.clone(),
            },
            _ => {
                return Err(TransitionError {
                    state: Box::new(self),
                    event: Box::new(event),
                });
            }
        };

        Ok(next)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum RunEvent {
    StartRequested,
    AdapterReady,
    BaselineEstablished,
    SaturationBracketed,
    BracketRefined,
    AnalysisStarted,
    CandidateValidated {
        outcome: RunOutcome,
    },
    /// Completes a fixed execution whose analysis does not require traversing
    /// the adaptive bracket/refinement lifecycle.
    RunCompleted {
        outcome: RunOutcome,
    },
    StopRequested,
    AdapterStopped,
    Failed {
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunOutcome {
    pub classification: RunClassification,
    pub knee: Option<KneeEstimate>,
    pub slo_maximum_rate: Option<f64>,
    #[serde(default)]
    pub analysis: Option<Box<crate::analysis::KneeAnalysis>>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunClassification {
    TargetSaturated,
    GeneratorSaturated,
    SloExceeded,
    UnstableMeasurement,
    NoKneeObserved,
    MaximumLoadReached,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KneeEstimate {
    pub offered_rate: f64,
    pub lower_bound: f64,
    pub upper_bound: f64,
    pub recommended_operating_rate: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TransitionError {
    pub state: Box<RunState>,
    pub event: Box<RunEvent>,
}

impl fmt::Display for TransitionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "event {:?} is invalid while run is in state {:?}",
            self.event, self.state
        )
    }
}

impl std::error::Error for TransitionError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn validated_outcome() -> RunOutcome {
        RunOutcome {
            classification: RunClassification::TargetSaturated,
            knee: Some(KneeEstimate {
                offered_rate: 18_700.0,
                lower_bound: 17_900.0,
                upper_bound: 19_600.0,
                recommended_operating_rate: 15_900.0,
            }),
            slo_maximum_rate: None,
            analysis: None,
            warnings: Vec::new(),
        }
    }

    #[test]
    fn happy_path_reaches_completed() {
        let state = RunState::Configured
            .transition(RunEvent::StartRequested)
            .unwrap()
            .transition(RunEvent::AdapterReady)
            .unwrap()
            .transition(RunEvent::BaselineEstablished)
            .unwrap()
            .transition(RunEvent::SaturationBracketed)
            .unwrap()
            .transition(RunEvent::BracketRefined)
            .unwrap()
            .transition(RunEvent::CandidateValidated {
                outcome: validated_outcome(),
            })
            .unwrap();

        assert!(matches!(state, RunState::Completed { .. }));
        assert!(state.is_terminal());
    }

    #[test]
    fn fixed_plan_enters_validation_before_completion() {
        let state = RunState::Configured
            .transition(RunEvent::StartRequested)
            .unwrap()
            .transition(RunEvent::AdapterReady)
            .unwrap()
            .transition(RunEvent::AnalysisStarted)
            .unwrap();
        assert_eq!(
            state,
            RunState::Measuring {
                stage: MeasurementStage::Validation
            }
        );
        assert!(matches!(
            state
                .transition(RunEvent::CandidateValidated {
                    outcome: validated_outcome(),
                })
                .unwrap(),
            RunState::Completed { .. }
        ));
    }

    #[test]
    fn active_run_can_stop_gracefully() {
        let state = RunState::Measuring {
            stage: MeasurementStage::Discovery,
        }
        .transition(RunEvent::StopRequested)
        .unwrap();

        assert_eq!(
            state,
            RunState::Stopping {
                interrupted_stage: Some(MeasurementStage::Discovery)
            }
        );
        assert_eq!(
            state.transition(RunEvent::AdapterStopped).unwrap(),
            RunState::Stopped
        );
    }

    #[test]
    fn invalid_transition_preserves_context() {
        let error = RunState::Configured
            .transition(RunEvent::AdapterReady)
            .unwrap_err();

        assert_eq!(*error.state, RunState::Configured);
        assert_eq!(*error.event, RunEvent::AdapterReady);
    }

    #[test]
    fn a_failure_terminates_an_active_run() {
        let state = RunState::Measuring {
            stage: MeasurementStage::Baseline,
        }
        .transition(RunEvent::Failed {
            message: "adapter exited".into(),
        })
        .unwrap();

        assert_eq!(
            state,
            RunState::Failed {
                message: "adapter exited".into()
            }
        );
    }
}

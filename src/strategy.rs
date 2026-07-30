//! Pure load-traversal and phase-quality decisions.

use serde::{Deserialize, Serialize};

use crate::{
    analysis::AnalysisTermination,
    config::{RunConfig, Strategy},
    measurement::{MeasurementStage, RunClassification, RunEvent, RunOutcome},
    stats::PhaseReport,
};

const SATURATION_EFFICIENCY: f64 = 0.90;
const SATURATION_LATENCY_MULTIPLIER: f64 = 2.0;
const SATURATION_UNSUCCESSFUL_RATE: f64 = 0.01;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyAction {
    Select,
    Accept,
    Repeat,
    Reject,
    Recover,
    Complete,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StrategyDecision {
    pub sequence: u64,
    pub stage: MeasurementStage,
    pub action: StrategyAction,
    pub offered_rate: f64,
    pub next_rate: Option<f64>,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PhaseRequest {
    pub rate: f64,
    pub stage: MeasurementStage,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ObservationOutcome {
    Continue {
        request: PhaseRequest,
        lifecycle_event: Option<RunEvent>,
    },
    Complete {
        outcome: RunOutcome,
        lifecycle_events: Vec<RunEvent>,
    },
    Analyze {
        termination: AnalysisTermination,
        lifecycle_events: Vec<RunEvent>,
    },
}

pub struct AdaptiveStrategy {
    maximum_rate: f64,
    growth_factor: f64,
    maximum_repeats: u32,
    maximum_refinements: u32,
    refinement_ratio: f64,
    current: PhaseRequest,
    repeats: u32,
    refinements: u32,
    baseline_latency_p95: Option<u64>,
    last_healthy_rate: Option<f64>,
    saturated_rate: Option<f64>,
}

impl AdaptiveStrategy {
    pub fn new(config: &RunConfig) -> Self {
        Self {
            maximum_rate: config.load.maximum_rate,
            growth_factor: config.load.growth_factor,
            maximum_repeats: config.phases.repetitions.saturating_sub(1),
            maximum_refinements: 8,
            refinement_ratio: 1.10,
            current: PhaseRequest {
                rate: config.load.initial_rate,
                stage: MeasurementStage::Baseline,
            },
            repeats: 0,
            refinements: 0,
            baseline_latency_p95: None,
            last_healthy_rate: None,
            saturated_rate: None,
        }
    }

    pub fn initial_request(&self) -> PhaseRequest {
        self.current
    }

    pub fn observe(
        &mut self,
        report: &PhaseReport,
        generator_saturated: bool,
    ) -> ObservationOutcome {
        if generator_saturated {
            return ObservationOutcome::Complete {
                outcome: outcome(
                    RunClassification::GeneratorSaturated,
                    "dispatch lag exceeded the generator validity threshold",
                ),
                lifecycle_events: Vec::new(),
            };
        }
        if !report.quality.stationary {
            if self.repeats < self.maximum_repeats {
                self.repeats += 1;
                return ObservationOutcome::Continue {
                    request: self.current,
                    lifecycle_event: None,
                };
            }
            return ObservationOutcome::Complete {
                outcome: outcome(
                    RunClassification::UnstableMeasurement,
                    report
                        .quality
                        .reason
                        .as_deref()
                        .unwrap_or("phase remained non-stationary after its repeat budget"),
                ),
                lifecycle_events: Vec::new(),
            };
        }
        self.repeats = 0;

        match self.current.stage {
            MeasurementStage::Baseline => {
                self.baseline_latency_p95 = report.stats.overall.client_latency_ns.p95;
                self.last_healthy_rate = Some(self.current.rate);
                let next = (self.current.rate * self.growth_factor).min(self.maximum_rate);
                if next <= self.current.rate {
                    return ObservationOutcome::Analyze {
                        termination: AnalysisTermination::MaximumLoadReached,
                        lifecycle_events: vec![
                            RunEvent::BaselineEstablished,
                            RunEvent::AnalysisStarted,
                        ],
                    };
                }
                self.current = PhaseRequest {
                    rate: next,
                    stage: MeasurementStage::Discovery,
                };
                ObservationOutcome::Continue {
                    request: self.current,
                    lifecycle_event: Some(RunEvent::BaselineEstablished),
                }
            }
            MeasurementStage::Discovery => {
                if target_saturated(report, self.baseline_latency_p95) {
                    self.saturated_rate = Some(self.current.rate);
                    self.begin_refinement()
                } else {
                    self.last_healthy_rate = Some(self.current.rate);
                    if self.current.rate >= self.maximum_rate {
                        ObservationOutcome::Analyze {
                            termination: AnalysisTermination::MaximumLoadReached,
                            lifecycle_events: vec![RunEvent::AnalysisStarted],
                        }
                    } else {
                        self.current.rate =
                            (self.current.rate * self.growth_factor).min(self.maximum_rate);
                        ObservationOutcome::Continue {
                            request: self.current,
                            lifecycle_event: None,
                        }
                    }
                }
            }
            MeasurementStage::Refinement => {
                if target_saturated(report, self.baseline_latency_p95) {
                    self.saturated_rate = Some(self.current.rate);
                } else {
                    self.last_healthy_rate = Some(self.current.rate);
                }
                self.refinements += 1;
                let lower = self
                    .last_healthy_rate
                    .expect("refinement has a healthy bound");
                let upper = self
                    .saturated_rate
                    .expect("refinement has a saturated bound");
                if upper / lower <= self.refinement_ratio
                    || self.refinements >= self.maximum_refinements
                {
                    ObservationOutcome::Analyze {
                        termination: AnalysisTermination::BracketRefined,
                        lifecycle_events: vec![RunEvent::BracketRefined],
                    }
                } else {
                    self.current.rate = geometric_midpoint(lower, upper);
                    ObservationOutcome::Continue {
                        request: self.current,
                        lifecycle_event: None,
                    }
                }
            }
            MeasurementStage::Validation => unreachable!("validation belongs to the fitter"),
        }
    }

    fn begin_refinement(&mut self) -> ObservationOutcome {
        let lower = self
            .last_healthy_rate
            .expect("discovery begins after a baseline");
        let upper = self.saturated_rate.expect("saturation was just observed");
        self.current = PhaseRequest {
            rate: geometric_midpoint(lower, upper),
            stage: MeasurementStage::Refinement,
        };
        ObservationOutcome::Continue {
            request: self.current,
            lifecycle_event: Some(RunEvent::SaturationBracketed),
        }
    }
}

pub fn target_saturated(report: &PhaseReport, baseline_latency_p95: Option<u64>) -> bool {
    let efficiency = report.goodput_rate / report.offered_rate;
    let latency_increased = baseline_latency_p95
        .zip(report.stats.overall.client_latency_ns.p95)
        .is_some_and(|(baseline, current)| {
            current as f64 >= baseline.max(1) as f64 * SATURATION_LATENCY_MULTIPLIER
        });
    efficiency < SATURATION_EFFICIENCY
        && (latency_increased
            || report.stats.overall.unsuccessful_rate() >= SATURATION_UNSUCCESSFUL_RATE)
}

pub fn geometric_midpoint(lower: f64, upper: f64) -> f64 {
    (lower * upper).sqrt()
}

pub fn fixed_stage(strategy: Strategy) -> MeasurementStage {
    match strategy {
        Strategy::Adaptive => MeasurementStage::Baseline,
        Strategy::Sweep | Strategy::UpDown => MeasurementStage::Discovery,
    }
}

fn outcome(classification: RunClassification, warning: &str) -> RunOutcome {
    RunOutcome {
        classification,
        knee: None,
        slo_maximum_rate: None,
        analysis: None,
        warnings: vec![warning.into()],
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, path::PathBuf};

    use super::*;
    use crate::{
        config::{
            LoadConfig, OperationSelection, PhaseConfig, Preset, WeightedOperation, WorkloadConfig,
        },
        stats::{DistributionStats, PhaseQuality, SampleStats, StatsReport},
    };

    fn config(maximum_rate: f64, repetitions: u32) -> RunConfig {
        RunConfig {
            preset: Preset::Quick,
            strategy: Strategy::Adaptive,
            phases: PhaseConfig {
                warmup_ms: 0,
                measurement_ms: 1_000,
                recovery_ms: 0,
                repetitions,
            },
            load: LoadConfig {
                initial_rate: 100.0,
                maximum_rate,
                growth_factor: 2.0,
                explicit_levels: Vec::new(),
                cycles: 1,
            },
            analysis: Default::default(),
            workload: WorkloadConfig {
                operations: OperationSelection::Selected {
                    operations: vec![WeightedOperation {
                        name: "read".into(),
                        weight: 1.0,
                        arguments: BTreeMap::new(),
                    }],
                },
            },
            output_directory: PathBuf::from("results"),
            agents: Vec::new(),
        }
    }

    fn report(rate: f64, goodput: f64, p95: u64, stationary: bool) -> PhaseReport {
        let distribution = DistributionStats {
            samples: 100,
            min: Some(p95),
            p50: Some(p95),
            p95: Some(p95),
            p99: Some(p95),
            max: Some(p95),
        };
        PhaseReport {
            offered_rate: rate,
            goodput_rate: goodput,
            elapsed_ns: 1_000_000_000,
            in_flight_high_water: 1,
            stats: StatsReport {
                overall: SampleStats {
                    attempts: rate as u64,
                    successful: goodput as u64,
                    failed: 0,
                    timed_out: 0,
                    errors_by_code: Vec::new(),
                    client_latency_ns: distribution.clone(),
                    total_latency_ns: distribution.clone(),
                    dispatch_lag_ns: distribution,
                },
                variants: Vec::new(),
            },
            quality: PhaseQuality {
                stationary,
                reason: (!stationary).then(|| "bucket drift".into()),
                buckets: Vec::new(),
            },
        }
    }

    #[test]
    fn geometric_refinement_is_multiplicative() {
        assert_eq!(geometric_midpoint(100.0, 400.0), 200.0);
    }

    #[test]
    fn linear_system_reaches_maximum_without_fabricating_a_knee() {
        let mut strategy = AdaptiveStrategy::new(&config(400.0, 1));
        assert!(matches!(
            strategy.observe(&report(100.0, 100.0, 10, true), false),
            ObservationOutcome::Continue {
                request: PhaseRequest { rate: 200.0, .. },
                ..
            }
        ));
        assert!(matches!(
            strategy.observe(&report(200.0, 200.0, 10, true), false),
            ObservationOutcome::Continue {
                request: PhaseRequest { rate: 400.0, .. },
                ..
            }
        ));
        assert!(matches!(
            strategy.observe(&report(400.0, 400.0, 10, true), false),
            ObservationOutcome::Analyze {
                termination: AnalysisTermination::MaximumLoadReached,
                ..
            }
        ));
    }

    #[test]
    fn obvious_queue_knee_is_bracketed_and_refined() {
        let mut strategy = AdaptiveStrategy::new(&config(800.0, 1));
        let _ = strategy.observe(&report(100.0, 100.0, 10, true), false);
        let _ = strategy.observe(&report(200.0, 200.0, 11, true), false);
        let mut outcome = strategy.observe(&report(400.0, 280.0, 40, true), false);
        for _ in 0..10 {
            let ObservationOutcome::Continue { request, .. } = outcome else {
                break;
            };
            let saturated = request.rate >= 290.0;
            outcome = strategy.observe(
                &report(
                    request.rate,
                    if saturated { 280.0 } else { request.rate },
                    if saturated { 40 } else { 11 },
                    true,
                ),
                false,
            );
        }
        assert!(matches!(
            outcome,
            ObservationOutcome::Analyze {
                termination: AnalysisTermination::BracketRefined,
                ..
            }
        ));
    }

    #[test]
    fn unstable_phase_repeats_then_exhausts_its_budget() {
        let mut strategy = AdaptiveStrategy::new(&config(400.0, 2));
        assert!(matches!(
            strategy.observe(&report(100.0, 90.0, 10, false), false),
            ObservationOutcome::Continue {
                request: PhaseRequest { rate: 100.0, .. },
                ..
            }
        ));
        assert!(matches!(
            strategy.observe(&report(100.0, 90.0, 10, false), false),
            ObservationOutcome::Complete {
                outcome: RunOutcome {
                    classification: RunClassification::UnstableMeasurement,
                    ..
                },
                ..
            }
        ));
    }

    #[test]
    fn generator_saturation_stops_before_target_classification() {
        let mut strategy = AdaptiveStrategy::new(&config(400.0, 1));
        assert!(matches!(
            strategy.observe(&report(100.0, 80.0, 40, true), true),
            ObservationOutcome::Complete {
                outcome: RunOutcome {
                    classification: RunClassification::GeneratorSaturated,
                    ..
                },
                ..
            }
        ));
    }
}

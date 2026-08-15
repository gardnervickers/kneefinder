//! Frontend-neutral execution of prepared workload cohorts.

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use crate::{
    adapter_session::ScheduleCompletion,
    agent::{AgentCohort, CohortError, CohortReady},
    analysis::{AnalysisTermination, analyze},
    config::{OperationSelection, RunConfig, Strategy, WeightedOperation},
    measurement::{PhaseProgress, PhaseSegment, RunClassification, RunEvent, RunOutcome},
    protocol::{
        HistogramEncoding, HistogramSpec, Load, LoadModel, ManagedOperation, ManagedPhaseRequest,
        PhaseId,
    },
    stats::{
        MeasurementBucket, OperationVariant, PhaseAccumulator, PhaseAggregationPlan, PhaseQuality,
        PhaseReport, StatsError,
    },
    strategy::{
        AdaptiveStrategy, ObservationOutcome, StrategyAction, StrategyDecision, fixed_stage,
    },
};

const NANOS_PER_SECOND: f64 = 1_000_000_000.0;

#[derive(Debug, Clone)]
pub struct ExecutorOptions {
    /// Maximum number of measured phases in one fixed plan.
    pub maximum_phases: usize,
    /// Lead time allowing every agent to receive a phase start before it begins.
    pub schedule_lead_time: Duration,
    /// Dispatch-lag fraction that invalidates target-capacity conclusions.
    pub dispatch_lag_fraction: f64,
    /// Absolute floor for generator-saturation dispatch lag.
    pub minimum_dispatch_lag: Duration,
    /// Number of equal measurement buckets used for stationarity decisions.
    pub stationarity_buckets: usize,
    /// Maximum relative deviation in bucket goodput before a phase is unstable.
    pub stationarity_tolerance: f64,
    /// Minimum attempts required before bucket stationarity is meaningful.
    pub minimum_stationarity_samples: u64,
}

impl Default for ExecutorOptions {
    fn default() -> Self {
        Self {
            maximum_phases: 10_000,
            schedule_lead_time: Duration::from_millis(25),
            dispatch_lag_fraction: 0.01,
            minimum_dispatch_lag: Duration::from_millis(10),
            stationarity_buckets: 5,
            stationarity_tolerance: 0.30,
            minimum_stationarity_samples: 20,
        }
    }
}

pub trait ExecutionSink {
    fn record_run_event(&mut self, event: RunEvent) -> Result<(), String>;
    fn record_phase_stats(&mut self, phase_id: PhaseId, report: PhaseReport) -> Result<(), String>;
    fn record_strategy_decision(&mut self, decision: StrategyDecision) -> Result<(), String>;
    fn record_phase_progress(&mut self, progress: PhaseProgress) -> Result<(), String>;
}

#[derive(Debug, Clone, Copy)]
struct ProgressContext {
    phase_id: PhaseId,
    planned_phases: Option<u64>,
    offered_rate: f64,
}

pub struct RunExecutor {
    options: ExecutorOptions,
}

impl RunExecutor {
    pub fn new(options: ExecutorOptions) -> Self {
        Self { options }
    }

    pub fn execute(
        &self,
        config: &RunConfig,
        catalog: &CohortReady,
        cohort: &mut AgentCohort,
        stop: &Arc<AtomicBool>,
        sink: &mut impl ExecutionSink,
    ) -> Result<ExecutorCompletion, ExecutorError> {
        validate_capabilities(catalog)?;
        let rates = configured_rates(config, self.options.maximum_phases)?;
        let operations = concrete_operations(config)?;

        sink.record_run_event(RunEvent::AdapterReady)
            .map_err(ExecutorError::Sink)?;
        let mut next_wire_phase_id = 1_u64;
        if config.strategy == Strategy::Adaptive {
            self.execute_adaptive(
                config,
                cohort,
                stop,
                sink,
                operations,
                &mut next_wire_phase_id,
            )
        } else {
            self.execute_fixed(
                config,
                &rates,
                cohort,
                stop,
                sink,
                operations,
                &mut next_wire_phase_id,
            )
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_fixed(
        &self,
        config: &RunConfig,
        rates: &[f64],
        cohort: &mut AgentCohort,
        stop: &Arc<AtomicBool>,
        sink: &mut impl ExecutionSink,
        operations: &[WeightedOperation],
        next_wire_phase_id: &mut u64,
    ) -> Result<ExecutorCompletion, ExecutorError> {
        let mut sequence = 1_u64;
        let mut measured_phases = 0_u64;
        let mut reports = Vec::with_capacity(rates.len());
        for (index, rate) in rates.iter().copied().enumerate() {
            let progress = ProgressContext {
                phase_id: PhaseId(index as u64 + 1),
                planned_phases: Some(rates.len() as u64),
                offered_rate: rate,
            };
            publish_decision(
                sink,
                &mut sequence,
                fixed_stage(config.strategy),
                StrategyAction::Select,
                rate,
                Some(rate),
                "selected by the configured fixed traversal",
            )?;
            let Some(report) = self.measure_phase(
                config,
                cohort,
                stop,
                sink,
                operations,
                next_wire_phase_id,
                rate,
                progress,
            )?
            else {
                return Ok(ExecutorCompletion::Stopped);
            };
            measured_phases += 1;
            let phase_id = PhaseId(measured_phases);
            sink.record_phase_stats(phase_id, report.clone())
                .map_err(ExecutorError::Sink)?;
            reports.push(report.clone());
            publish_decision(
                sink,
                &mut sequence,
                fixed_stage(config.strategy),
                if report.quality.stationary {
                    StrategyAction::Accept
                } else {
                    StrategyAction::Reject
                },
                rate,
                None,
                report.quality.reason.as_deref().unwrap_or("phase accepted"),
            )?;
            if config.phases.recovery_ms > 0 {
                publish_decision(
                    sink,
                    &mut sequence,
                    fixed_stage(config.strategy),
                    StrategyAction::Recover,
                    rate,
                    None,
                    &format!(
                        "waiting {} ms for recovery before the next phase",
                        config.phases.recovery_ms
                    ),
                )?;
            }
            if recover_or_stop(config, stop, sink, progress)? {
                return Ok(ExecutorCompletion::Stopped);
            }
        }
        sink.record_run_event(RunEvent::AnalysisStarted)
            .map_err(ExecutorError::Sink)?;
        let outcome = analyze(
            &reports,
            &config.analysis,
            AnalysisTermination::CompletedPlan,
        );
        sink.record_run_event(RunEvent::CandidateValidated {
            outcome: outcome.clone(),
        })
        .map_err(ExecutorError::Sink)?;
        Ok(ExecutorCompletion::Completed(outcome))
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_adaptive(
        &self,
        config: &RunConfig,
        cohort: &mut AgentCohort,
        stop: &Arc<AtomicBool>,
        sink: &mut impl ExecutionSink,
        operations: &[WeightedOperation],
        next_wire_phase_id: &mut u64,
    ) -> Result<ExecutorCompletion, ExecutorError> {
        let mut strategy = AdaptiveStrategy::new(config);
        let mut request = strategy.initial_request();
        let mut sequence = 1_u64;
        let mut measured_phases = 0_usize;
        let mut reports = Vec::new();
        loop {
            if measured_phases >= self.options.maximum_phases {
                return Ok(ExecutorCompletion::Completed(RunOutcome {
                    classification: RunClassification::UnstableMeasurement,
                    knee: None,
                    slo_maximum_rate: None,
                    analysis: None,
                    warnings: vec!["adaptive phase budget was exhausted".into()],
                }));
            }
            publish_decision(
                sink,
                &mut sequence,
                request.stage,
                StrategyAction::Select,
                request.rate,
                Some(request.rate),
                "selected by adaptive traversal",
            )?;
            let Some(report) = self.measure_phase(
                config,
                cohort,
                stop,
                sink,
                operations,
                next_wire_phase_id,
                request.rate,
                ProgressContext {
                    phase_id: PhaseId(measured_phases as u64 + 1),
                    planned_phases: None,
                    offered_rate: request.rate,
                },
            )?
            else {
                return Ok(ExecutorCompletion::Stopped);
            };
            measured_phases += 1;
            let generator_saturated =
                dispatch_lag_invalid(&report, config.phases.measurement_ms, &self.options);
            sink.record_phase_stats(PhaseId(measured_phases as u64), report.clone())
                .map_err(ExecutorError::Sink)?;
            reports.push(report.clone());
            let completed_request = request;
            let observation = strategy.observe(&report, generator_saturated);
            match observation {
                ObservationOutcome::Continue {
                    request: next,
                    lifecycle_event,
                } => {
                    let repeating = next == request;
                    publish_decision(
                        sink,
                        &mut sequence,
                        request.stage,
                        if repeating {
                            StrategyAction::Repeat
                        } else {
                            StrategyAction::Accept
                        },
                        request.rate,
                        Some(next.rate),
                        if repeating {
                            report
                                .quality
                                .reason
                                .as_deref()
                                .unwrap_or("phase was non-stationary")
                        } else {
                            "phase accepted and traversal advanced"
                        },
                    )?;
                    if let Some(event) = lifecycle_event {
                        sink.record_run_event(event).map_err(ExecutorError::Sink)?;
                    }
                    request = next;
                }
                ObservationOutcome::Complete {
                    outcome,
                    lifecycle_events,
                } => {
                    let action = if matches!(
                        outcome.classification,
                        RunClassification::GeneratorSaturated
                            | RunClassification::UnstableMeasurement
                    ) {
                        StrategyAction::Reject
                    } else {
                        StrategyAction::Complete
                    };
                    publish_decision(
                        sink,
                        &mut sequence,
                        request.stage,
                        action,
                        request.rate,
                        None,
                        outcome
                            .warnings
                            .first()
                            .map(String::as_str)
                            .unwrap_or("adaptive traversal completed"),
                    )?;
                    for event in lifecycle_events {
                        sink.record_run_event(event).map_err(ExecutorError::Sink)?;
                    }
                    return Ok(ExecutorCompletion::Completed(outcome));
                }
                ObservationOutcome::Analyze {
                    termination,
                    lifecycle_events,
                } => {
                    for event in lifecycle_events {
                        sink.record_run_event(event).map_err(ExecutorError::Sink)?;
                    }
                    let outcome = analyze(&reports, &config.analysis, termination);
                    publish_decision(
                        sink,
                        &mut sequence,
                        request.stage,
                        StrategyAction::Complete,
                        request.rate,
                        None,
                        outcome
                            .warnings
                            .first()
                            .map(String::as_str)
                            .unwrap_or("statistical analysis completed"),
                    )?;
                    sink.record_run_event(RunEvent::CandidateValidated {
                        outcome: outcome.clone(),
                    })
                    .map_err(ExecutorError::Sink)?;
                    return Ok(ExecutorCompletion::Completed(outcome));
                }
            }
            if config.phases.recovery_ms > 0 {
                publish_decision(
                    sink,
                    &mut sequence,
                    completed_request.stage,
                    StrategyAction::Recover,
                    completed_request.rate,
                    None,
                    &format!(
                        "waiting {} ms for recovery before the next phase",
                        config.phases.recovery_ms
                    ),
                )?;
            }
            if recover_or_stop(
                config,
                stop,
                sink,
                ProgressContext {
                    phase_id: PhaseId(measured_phases as u64),
                    planned_phases: None,
                    offered_rate: completed_request.rate,
                },
            )? {
                return Ok(ExecutorCompletion::Stopped);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn measure_phase(
        &self,
        config: &RunConfig,
        cohort: &mut AgentCohort,
        stop: &Arc<AtomicBool>,
        sink: &mut impl ExecutionSink,
        operations: &[WeightedOperation],
        next_wire_phase_id: &mut u64,
        rate: f64,
        progress: ProgressContext,
    ) -> Result<Option<PhaseReport>, ExecutorError> {
        if stop.load(Ordering::Acquire) {
            return Ok(None);
        }
        let plan = aggregation_plan(config, operations, &self.options)?;
        self.measure_managed_phase(
            config,
            cohort,
            stop,
            sink,
            operations,
            next_wire_phase_id,
            rate,
            progress,
            plan,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn measure_managed_phase(
        &self,
        config: &RunConfig,
        cohort: &mut AgentCohort,
        stop: &Arc<AtomicBool>,
        sink: &mut impl ExecutionSink,
        operations: &[WeightedOperation],
        next_wire_phase_id: &mut u64,
        rate: f64,
        progress: ProgressContext,
        plan: PhaseAggregationPlan,
    ) -> Result<Option<PhaseReport>, ExecutorError> {
        let warmup = Duration::from_millis(config.phases.warmup_ms);
        let measurement = Duration::from_millis(config.phases.measurement_ms);
        if !warmup.is_zero() {
            publish_progress(sink, progress, PhaseSegment::Warmup, 0, warmup, 0, 0)?;
        }
        let phase_id = PhaseId(*next_wire_phase_id);
        *next_wire_phase_id = next_wire_phase_id
            .checked_add(1)
            .ok_or(ExecutorError::PhaseIdExhausted)?;
        let request = ManagedPhaseRequest {
            warmup_ns: duration_ns(warmup),
            measurement_ns: plan.measurement_ns,
            operation_timeout_ns: plan.measurement_ns,
            load: Load::OpenLoop {
                requests_per_second: rate,
            },
            operations: operations
                .iter()
                .map(|operation| ManagedOperation {
                    operation: operation.name.clone(),
                    arguments: operation.arguments.clone(),
                    weight: operation.weight,
                    shard_index: 0,
                    shard_count: 1,
                })
                .collect(),
            bucket_count: plan.bucket_count,
            histogram: plan.histogram,
        };
        let outcome = cohort.execute_managed_phase(
            phase_id,
            self.options.schedule_lead_time,
            request,
            stop,
        )?;

        if !warmup.is_zero() {
            publish_progress(
                sink,
                progress,
                PhaseSegment::Warmup,
                duration_ns(warmup),
                warmup,
                0,
                0,
            )?;
        }
        let mut accumulator = PhaseAccumulator::new(plan)?;
        let mut elapsed_ns = None;
        for agent in outcome.agents {
            if matches!(
                agent.outcome.completion,
                ScheduleCompletion::Cancelled { .. }
            ) {
                stop.store(true, Ordering::Release);
            }
            let Some(result) = agent.outcome.result else {
                if stop.load(Ordering::Acquire) {
                    return Ok(None);
                }
                return Err(ExecutorError::InvalidManagedPhase(format!(
                    "agent {} did not return a terminal aggregate",
                    agent.agent.id
                )));
            };
            if agent.outcome.completion == ScheduleCompletion::Completed
                && result.started != result.completed
            {
                return Err(ExecutorError::InvalidManagedPhase(format!(
                    "agent {} completed the phase with {} started calls but {} terminal observations",
                    agent.agent.id, result.started, result.completed
                )));
            }
            elapsed_ns.get_or_insert(result.elapsed_ns);
            accumulator.merge_phase_result(&result)?;
        }
        let elapsed_ns = elapsed_ns.ok_or_else(|| {
            ExecutorError::InvalidManagedPhase("cohort returned no agent aggregates".into())
        })?;
        let mut report = accumulator.finish_at(rate, elapsed_ns)?;
        report.quality = phase_quality(
            &report.quality.buckets,
            report.stats.overall.attempts,
            &self.options,
        );
        publish_progress(
            sink,
            progress,
            PhaseSegment::Measurement,
            elapsed_ns,
            measurement,
            report.offered_count,
            report.completed_count,
        )?;
        Ok(Some(report))
    }
}

impl Default for RunExecutor {
    fn default() -> Self {
        Self::new(ExecutorOptions::default())
    }
}

fn concrete_operations(config: &RunConfig) -> Result<&[WeightedOperation], ExecutorError> {
    match &config.workload.operations {
        OperationSelection::Selected { operations }
            if !operations.is_empty()
                && operations
                    .iter()
                    .all(|operation| operation.weight.is_finite() && operation.weight > 0.0)
                && operations
                    .iter()
                    .map(|operation| operation.weight)
                    .sum::<f64>()
                    .is_finite() =>
        {
            Ok(operations)
        }
        _ => Err(ExecutorError::InvalidConfiguration(
            "prepared run must contain concrete variants with positive finite weights".into(),
        )),
    }
}

fn validate_capabilities(catalog: &CohortReady) -> Result<(), ExecutorError> {
    if !catalog.capabilities.adapter_managed_phases {
        return Err(ExecutorError::UnsupportedCapability(
            "adapter does not support managed phases".into(),
        ));
    }
    if !catalog
        .capabilities
        .load_models
        .contains(&LoadModel::OpenLoop)
    {
        return Err(ExecutorError::UnsupportedCapability(
            "managed execution requires open-loop load support".into(),
        ));
    }
    if !catalog
        .capabilities
        .histogram_encodings
        .contains(&HistogramEncoding::HdrV2Base64)
    {
        return Err(ExecutorError::UnsupportedCapability(
            "managed execution requires hdr_v2_base64 histograms".into(),
        ));
    }
    Ok(())
}

fn aggregation_plan(
    config: &RunConfig,
    operations: &[WeightedOperation],
    options: &ExecutorOptions,
) -> Result<PhaseAggregationPlan, ExecutorError> {
    let measurement_ns = config
        .phases
        .measurement_ms
        .checked_mul(1_000_000)
        .ok_or_else(|| {
            ExecutorError::InvalidConfiguration(
                "measurement duration cannot be represented in nanoseconds".into(),
            )
        })?;
    let bucket_count = u16::try_from(options.stationarity_buckets.max(1)).map_err(|_| {
        ExecutorError::InvalidConfiguration(
            "stationarity bucket count cannot be represented by the adapter protocol".into(),
        )
    })?;
    let highest_trackable_ns = measurement_ns.checked_mul(2).ok_or_else(|| {
        ExecutorError::InvalidConfiguration(
            "measurement and operation-timeout range cannot be represented in nanoseconds".into(),
        )
    })?;
    Ok(PhaseAggregationPlan {
        measurement_ns,
        bucket_count,
        histogram: HistogramSpec {
            lowest_discernible_ns: 1,
            highest_trackable_ns,
            significant_figures: 3,
        },
        variants: operations
            .iter()
            .map(|operation| OperationVariant {
                operation: operation.name.clone(),
                arguments: operation.arguments.clone(),
            })
            .collect(),
    })
}

pub fn configured_rates(
    config: &RunConfig,
    maximum_phases: usize,
) -> Result<Vec<f64>, ExecutorError> {
    if config.phases.measurement_ms == 0 {
        return Err(ExecutorError::InvalidConfiguration(
            "measurement duration must be greater than zero".into(),
        ));
    }
    if config.phases.repetitions == 0 {
        return Err(ExecutorError::InvalidConfiguration(
            "phase repetitions must be greater than zero".into(),
        ));
    }
    if config.load.cycles == 0 {
        return Err(ExecutorError::InvalidConfiguration(
            "load cycles must be greater than zero".into(),
        ));
    }
    if config.strategy == Strategy::Adaptive && !config.load.explicit_levels.is_empty() {
        return Err(ExecutorError::InvalidConfiguration(
            "explicit load levels cannot be used with adaptive traversal".into(),
        ));
    }
    if config.strategy == Strategy::Adaptive && config.load.cycles != 1 {
        return Err(ExecutorError::InvalidConfiguration(
            "adaptive traversal uses one discovery/refinement cycle".into(),
        ));
    }
    if !config.load.initial_rate.is_finite() || config.load.initial_rate <= 0.0 {
        return Err(ExecutorError::InvalidConfiguration(
            "initial rate must be a positive finite number".into(),
        ));
    }
    if !config.load.maximum_rate.is_finite() || config.load.maximum_rate < config.load.initial_rate
    {
        return Err(ExecutorError::InvalidConfiguration(
            "maximum rate must be finite and at least the initial rate".into(),
        ));
    }
    if !config.load.growth_factor.is_finite() || config.load.growth_factor <= 1.0 {
        return Err(ExecutorError::InvalidConfiguration(
            "growth factor must be finite and greater than one".into(),
        ));
    }

    let ascending = if config.load.explicit_levels.is_empty() {
        let mut levels = Vec::new();
        let mut rate = config.load.initial_rate;
        while rate < config.load.maximum_rate {
            levels.push(rate);
            if levels.len() >= maximum_phases {
                return Err(ExecutorError::InvalidConfiguration(format!(
                    "configured load plan exceeds the phase limit of {maximum_phases}"
                )));
            }
            rate *= config.load.growth_factor;
        }
        levels.push(config.load.maximum_rate);
        levels
    } else {
        if config
            .load
            .explicit_levels
            .iter()
            .any(|rate| !rate.is_finite() || *rate <= 0.0)
        {
            return Err(ExecutorError::InvalidConfiguration(
                "explicit load levels must be positive finite numbers".into(),
            ));
        }
        config.load.explicit_levels.clone()
    };

    if config.strategy == Strategy::Adaptive {
        return Ok(ascending);
    }

    let mut cycle = ascending.clone();
    if config.strategy == Strategy::UpDown && ascending.len() > 1 {
        cycle.extend(ascending.iter().rev().skip(1).copied());
    }
    let mut rates = Vec::new();
    for _ in 0..config.load.cycles {
        for rate in &cycle {
            for _ in 0..config.phases.repetitions {
                rates.push(*rate);
                if rates.len() > maximum_phases {
                    return Err(ExecutorError::InvalidConfiguration(format!(
                        "configured load plan exceeds the phase limit of {maximum_phases}"
                    )));
                }
            }
        }
    }
    Ok(rates)
}

fn dispatch_lag_invalid(
    report: &PhaseReport,
    measurement_ms: u64,
    options: &ExecutorOptions,
) -> bool {
    if report.started_count < report.offered_count
        || report.offered_count.saturating_add(1)
            < expected_offer_count(report.offered_rate, report.elapsed_ns)
    {
        return true;
    }
    let threshold = options
        .minimum_dispatch_lag
        .as_nanos()
        .min(u64::MAX as u128) as u64;
    let fractional =
        (measurement_ms as f64 * 1_000_000.0 * options.dispatch_lag_fraction).round() as u64;
    report
        .stats
        .overall
        .dispatch_lag_ns
        .p99
        .is_some_and(|lag| lag > threshold.max(fractional))
}

fn expected_offer_count(offered_rate: f64, elapsed_ns: u64) -> u64 {
    (offered_rate * elapsed_ns as f64 / NANOS_PER_SECOND)
        .ceil()
        .clamp(0.0, u64::MAX as f64) as u64
}

fn phase_quality(
    buckets: &[MeasurementBucket],
    attempts: u64,
    options: &ExecutorOptions,
) -> PhaseQuality {
    let buckets = buckets.to_vec();
    if attempts < options.minimum_stationarity_samples || buckets.len() < 2 {
        return PhaseQuality {
            stationary: true,
            reason: Some("too few samples for a stationarity rejection".into()),
            buckets,
        };
    }

    let mean = buckets
        .iter()
        .map(|bucket| bucket.goodput_rate)
        .sum::<f64>()
        / buckets.len() as f64;
    let maximum_deviation = buckets
        .iter()
        .map(|bucket| (bucket.goodput_rate - mean).abs())
        .fold(0.0_f64, f64::max);
    let relative_deviation = if mean > 0.0 {
        maximum_deviation / mean
    } else {
        f64::INFINITY
    };
    let stationary = relative_deviation <= options.stationarity_tolerance;
    PhaseQuality {
        stationary,
        reason: Some(if stationary {
            format!(
                "bucket goodput deviation {:.1}% was within the {:.1}% limit",
                relative_deviation * 100.0,
                options.stationarity_tolerance * 100.0
            )
        } else {
            format!(
                "bucket goodput deviation {:.1}% exceeded the {:.1}% limit",
                relative_deviation * 100.0,
                options.stationarity_tolerance * 100.0
            )
        }),
        buckets,
    }
}

#[allow(clippy::too_many_arguments)]
fn publish_decision(
    sink: &mut impl ExecutionSink,
    sequence: &mut u64,
    stage: crate::measurement::MeasurementStage,
    action: StrategyAction,
    offered_rate: f64,
    next_rate: Option<f64>,
    reason: &str,
) -> Result<(), ExecutorError> {
    sink.record_strategy_decision(StrategyDecision {
        sequence: *sequence,
        stage,
        action,
        offered_rate,
        next_rate,
        reason: reason.into(),
    })
    .map_err(ExecutorError::Sink)?;
    *sequence = sequence.saturating_add(1);
    Ok(())
}

fn recover_or_stop(
    config: &RunConfig,
    stop: &Arc<AtomicBool>,
    sink: &mut impl ExecutionSink,
    progress: ProgressContext,
) -> Result<bool, ExecutorError> {
    let duration = Duration::from_millis(config.phases.recovery_ms);
    if duration.is_zero() {
        return Ok(stop.load(Ordering::Acquire));
    }
    let started = std::time::Instant::now();
    publish_progress(sink, progress, PhaseSegment::Recovery, 0, duration, 0, 0)?;
    loop {
        if stop.load(Ordering::Acquire) {
            return Ok(true);
        }
        let elapsed = started.elapsed().min(duration);
        publish_progress(
            sink,
            progress,
            PhaseSegment::Recovery,
            elapsed.as_nanos().min(u64::MAX as u128) as u64,
            duration,
            0,
            0,
        )?;
        if elapsed >= duration {
            return Ok(false);
        }
        thread::sleep((duration - elapsed).min(Duration::from_millis(250)));
    }
}

fn publish_progress(
    sink: &mut impl ExecutionSink,
    context: ProgressContext,
    segment: PhaseSegment,
    elapsed_ns: u64,
    planned: Duration,
    scheduled: u64,
    reported: u64,
) -> Result<(), ExecutorError> {
    sink.record_phase_progress(PhaseProgress {
        phase_id: context.phase_id,
        planned_phases: context.planned_phases,
        offered_rate: context.offered_rate,
        segment,
        elapsed_ms: elapsed_ns / 1_000_000,
        planned_ms: planned.as_millis().min(u64::MAX as u128) as u64,
        scheduled,
        reported,
        awaiting_results: scheduled.saturating_sub(reported),
    })
    .map_err(ExecutorError::Sink)
}

fn duration_ns(duration: Duration) -> u64 {
    duration.as_nanos().min(u64::MAX as u128) as u64
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExecutorCompletion {
    Completed(RunOutcome),
    Stopped,
}

#[derive(Debug)]
pub enum ExecutorError {
    InvalidConfiguration(String),
    InvalidManagedPhase(String),
    UnsupportedCapability(String),
    Cohort(CohortError),
    Stats(StatsError),
    Sink(String),
    PhaseIdExhausted,
}

impl From<CohortError> for ExecutorError {
    fn from(value: CohortError) -> Self {
        Self::Cohort(value)
    }
}

impl From<StatsError> for ExecutorError {
    fn from(value: StatsError) -> Self {
        Self::Stats(value)
    }
}

impl fmt::Display for ExecutorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfiguration(message) => {
                write!(formatter, "invalid execution configuration: {message}")
            }
            Self::InvalidManagedPhase(message) => {
                write!(formatter, "invalid adapter-managed phase: {message}")
            }
            Self::UnsupportedCapability(message) => formatter.write_str(message),
            Self::Cohort(error) => error.fmt(formatter),
            Self::Stats(error) => error.fmt(formatter),
            Self::Sink(message) => write!(formatter, "failed to publish executor event: {message}"),
            Self::PhaseIdExhausted => formatter.write_str("phase identifier exhausted"),
        }
    }
}

impl std::error::Error for ExecutorError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Cohort(error) => Some(error),
            Self::Stats(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        path::PathBuf,
        sync::{Arc, Mutex},
    };

    use serde_json::Value;

    use super::*;
    use crate::{
        adapter_session::{AdapterReady, ManagedPhaseOutcome},
        agent::{
            AgentDescriptor, AgentError, AgentId, AgentInstanceId, AgentPlacement, AgentReady,
            WorkloadAgent,
        },
        config::{LoadConfig, PhaseConfig, Preset, Strategy, WorkloadConfig},
        protocol::{
            AdapterIdentity, ArgumentValue, Capabilities, HistogramEncoding, Load, LoadModel,
            ManagedPhaseRequest, OperationDescriptor, OperationId, OperationKind, OperationResult,
            OperationStatus,
        },
        stats::{PhaseAccumulator, PhaseAggregationPlan, PhaseReport},
    };

    struct FakeManagedAgent {
        descriptor: AgentDescriptor,
        prepared: Arc<Mutex<Option<ManagedPhaseRequest>>>,
        omit_terminal_observation: bool,
        interrupt_after_first: bool,
    }

    impl WorkloadAgent for FakeManagedAgent {
        fn descriptor(&self) -> &AgentDescriptor {
            &self.descriptor
        }

        fn initialize(
            &mut self,
            _run_id: crate::protocol::RunId,
            _config: Value,
        ) -> Result<AgentReady, AgentError> {
            let mut adapter = ready();
            adapter.capabilities.adapter_managed_phases = true;
            adapter.capabilities.histogram_encodings = vec![HistogramEncoding::HdrV2Base64];
            Ok(AgentReady {
                agent: self.descriptor.clone(),
                adapter,
            })
        }

        fn prepare_managed_phase(
            &mut self,
            _phase_id: PhaseId,
            request: ManagedPhaseRequest,
        ) -> Result<(), AgentError> {
            *self.prepared.lock().unwrap() = Some(request);
            Ok(())
        }

        fn start_managed_phase_interruptible(
            &mut self,
            _phase_id: PhaseId,
            _phase_start_unix_ns: u64,
            stop: &AtomicBool,
        ) -> Result<ManagedPhaseOutcome, AgentError> {
            let request = self.prepared.lock().unwrap().clone().unwrap();
            let variants = request
                .operations
                .iter()
                .map(|operation| OperationVariant {
                    operation: operation.operation.clone(),
                    arguments: operation.arguments.clone(),
                })
                .collect::<Vec<_>>();
            let mut accumulator = PhaseAccumulator::new(PhaseAggregationPlan {
                measurement_ns: request.measurement_ns,
                bucket_count: request.bucket_count,
                histogram: request.histogram,
                variants,
            })
            .unwrap();
            let Load::OpenLoop {
                requests_per_second,
            } = request.load
            else {
                unreachable!()
            };
            let count = (requests_per_second * request.measurement_ns as f64 / NANOS_PER_SECOND)
                .floor() as u64;
            let count = if self.interrupt_after_first {
                count.min(1)
            } else {
                count
            };
            let operation = &request.operations[0];
            for id in 0..count {
                accumulator
                    .record(&OperationResult {
                        id: OperationId(id + 1),
                        operation: operation.operation.clone(),
                        arguments: operation.arguments.clone(),
                        intended_start_offset_ns: (id as f64 * NANOS_PER_SECOND
                            / requests_per_second)
                            .round() as u64,
                        actual_start_offset_ns: (id as f64 * NANOS_PER_SECOND / requests_per_second)
                            .round() as u64,
                        client_latency_ns: 1,
                        status: OperationStatus::Ok,
                    })
                    .unwrap();
            }
            let mut result = if self.interrupt_after_first {
                stop.store(true, Ordering::Release);
                accumulator.to_phase_result_at(1).unwrap()
            } else {
                accumulator.to_phase_result().unwrap()
            };
            if self.omit_terminal_observation {
                result.completed = result.completed.saturating_sub(1);
            }
            Ok(ManagedPhaseOutcome {
                result: Some(result),
                completion: if self.interrupt_after_first {
                    ScheduleCompletion::Cancelled { forced: false }
                } else {
                    ScheduleCompletion::Completed
                },
            })
        }

        fn cancel(&mut self, _phase_id: PhaseId) -> Result<(), AgentError> {
            Ok(())
        }

        fn disconnect(&mut self) -> Result<(), AgentError> {
            Ok(())
        }

        fn shutdown(&mut self) -> Result<(), AgentError> {
            Ok(())
        }

        fn diagnostics(&self) -> Vec<String> {
            Vec::new()
        }
    }

    #[derive(Default)]
    struct RecordingSink {
        events: Vec<RunEvent>,
        phases: Vec<(PhaseId, PhaseReport)>,
        decisions: Vec<StrategyDecision>,
        progress: Vec<PhaseProgress>,
    }

    impl ExecutionSink for RecordingSink {
        fn record_run_event(&mut self, event: RunEvent) -> Result<(), String> {
            self.events.push(event);
            Ok(())
        }

        fn record_phase_stats(
            &mut self,
            phase_id: PhaseId,
            report: PhaseReport,
        ) -> Result<(), String> {
            self.phases.push((phase_id, report));
            Ok(())
        }

        fn record_strategy_decision(&mut self, decision: StrategyDecision) -> Result<(), String> {
            self.decisions.push(decision);
            Ok(())
        }

        fn record_phase_progress(&mut self, progress: PhaseProgress) -> Result<(), String> {
            self.progress.push(progress);
            Ok(())
        }
    }

    fn ready() -> crate::adapter_session::AdapterReady {
        AdapterReady {
            identity: AdapterIdentity {
                name: "fake".into(),
                version: None,
            },
            capabilities: Capabilities {
                adapter_managed_phases: true,
                load_models: vec![LoadModel::OpenLoop],
                histogram_encodings: vec![HistogramEncoding::HdrV2Base64],
            },
            operations: vec![OperationDescriptor {
                name: "read".into(),
                description: None,
                kind: OperationKind::Read,
                enabled_by_default: true,
                default_weight: 1.0,
                arguments: Vec::new(),
            }],
        }
    }

    fn config() -> RunConfig {
        RunConfig {
            preset: Preset::Quick,
            strategy: Strategy::Sweep,
            phases: PhaseConfig {
                warmup_ms: 0,
                measurement_ms: 20,
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
            analysis: Default::default(),
            workload: WorkloadConfig {
                operations: OperationSelection::Selected {
                    operations: vec![
                        WeightedOperation {
                            name: "read".into(),
                            weight: 3.0,
                            arguments: BTreeMap::from([("key".into(), ArgumentValue::Integer(0))]),
                        },
                        WeightedOperation {
                            name: "read".into(),
                            weight: 1.0,
                            arguments: BTreeMap::from([("key".into(), ArgumentValue::Integer(1))]),
                        },
                    ],
                },
            },
            output_directory: PathBuf::from("results"),
            agents: Vec::new(),
        }
    }

    fn managed_cohort(
        prepared: Arc<Mutex<Option<ManagedPhaseRequest>>>,
    ) -> (AgentCohort, CohortReady) {
        managed_cohort_with_behavior(prepared, false, false)
    }

    fn managed_cohort_with_terminal_omission(
        prepared: Arc<Mutex<Option<ManagedPhaseRequest>>>,
        omit_terminal_observation: bool,
    ) -> (AgentCohort, CohortReady) {
        managed_cohort_with_behavior(prepared, omit_terminal_observation, false)
    }

    fn managed_cohort_with_behavior(
        prepared: Arc<Mutex<Option<ManagedPhaseRequest>>>,
        omit_terminal_observation: bool,
        interrupt_after_first: bool,
    ) -> (AgentCohort, CohortReady) {
        let descriptor = AgentDescriptor {
            id: AgentId("managed-0".into()),
            instance_id: AgentInstanceId("managed-0-instance".into()),
            placement: AgentPlacement::Colocated,
        };
        let mut cohort = AgentCohort::new(vec![Box::new(FakeManagedAgent {
            descriptor,
            prepared,
            omit_terminal_observation,
            interrupt_after_first,
        })])
        .unwrap();
        let catalog = cohort
            .initialize(crate::protocol::RunId(1), Value::Null)
            .unwrap();
        (cohort, catalog)
    }

    #[test]
    fn fixed_sweep_reports_every_managed_phase() {
        let prepared = Arc::new(Mutex::new(None));
        let (mut cohort, catalog) = managed_cohort(Arc::clone(&prepared));
        let mut sink = RecordingSink::default();
        let completion = RunExecutor::default()
            .execute(
                &config(),
                &catalog,
                &mut cohort,
                &Arc::new(AtomicBool::new(false)),
                &mut sink,
            )
            .unwrap();

        assert_eq!(sink.phases.len(), 2);
        assert!(prepared.lock().unwrap().is_some());
        assert!(matches!(sink.events.first(), Some(RunEvent::AdapterReady)));
        assert!(sink.progress.iter().any(|progress| {
            progress.phase_id == PhaseId(1)
                && progress.planned_phases == Some(2)
                && progress.segment == PhaseSegment::Measurement
                && progress.elapsed_ms == progress.planned_ms
        }));
        assert!(matches!(
            completion,
            ExecutorCompletion::Completed(RunOutcome {
                classification: RunClassification::NoKneeObserved,
                ..
            })
        ));
    }

    #[test]
    fn executor_uses_one_phase_request_and_shared_report_semantics() {
        let prepared = Arc::new(Mutex::new(None));
        let (mut cohort, catalog) = managed_cohort(Arc::clone(&prepared));
        let mut config = config();
        config.load.maximum_rate = 100.0;
        config.load.explicit_levels = vec![100.0];
        let mut sink = RecordingSink::default();

        let completion = RunExecutor::default()
            .execute(
                &config,
                &catalog,
                &mut cohort,
                &Arc::new(AtomicBool::new(false)),
                &mut sink,
            )
            .unwrap();

        let request = prepared.lock().unwrap().clone().unwrap();
        assert_eq!(request.warmup_ns, 0);
        assert_eq!(request.measurement_ns, 20_000_000);
        assert_eq!(request.operations[0].shard_index, 0);
        assert_eq!(request.operations[0].shard_count, 1);
        assert_eq!(sink.phases.len(), 1);
        assert_eq!(sink.phases[0].1.offered_count, 2);
        assert_eq!(sink.phases[0].1.started_count, 2);
        assert_eq!(sink.phases[0].1.completed_count, 2);
        assert_eq!(sink.phases[0].1.goodput_rate, 100.0);
        assert!(matches!(
            completion,
            ExecutorCompletion::Completed(RunOutcome {
                classification: RunClassification::NoKneeObserved,
                ..
            })
        ));
    }

    #[test]
    fn completed_managed_phase_requires_a_terminal_observation_for_every_started_call() {
        let prepared = Arc::new(Mutex::new(None));
        let (mut cohort, catalog) =
            managed_cohort_with_terminal_omission(Arc::clone(&prepared), true);
        let mut config = config();
        config.load.maximum_rate = 100.0;
        config.load.explicit_levels = vec![100.0];
        let mut sink = RecordingSink::default();

        let error = RunExecutor::default()
            .execute(
                &config,
                &catalog,
                &mut cohort,
                &Arc::new(AtomicBool::new(false)),
                &mut sink,
            )
            .unwrap_err();

        assert!(matches!(
            error,
            ExecutorError::InvalidManagedPhase(message)
                if message.contains("terminal observations")
        ));
    }

    #[test]
    fn progress_reports_warmup_measurement_and_recovery_segments() {
        let prepared = Arc::new(Mutex::new(None));
        let (mut cohort, catalog) = managed_cohort(prepared);
        let mut config = config();
        config.phases.warmup_ms = 5;
        config.phases.recovery_ms = 5;
        config.load.maximum_rate = 100.0;
        config.load.explicit_levels = vec![100.0];
        let mut sink = RecordingSink::default();

        RunExecutor::default()
            .execute(
                &config,
                &catalog,
                &mut cohort,
                &Arc::new(AtomicBool::new(false)),
                &mut sink,
            )
            .unwrap();

        for segment in [
            PhaseSegment::Warmup,
            PhaseSegment::Measurement,
            PhaseSegment::Recovery,
        ] {
            assert!(sink.progress.iter().any(|progress| {
                progress.phase_id == PhaseId(1)
                    && progress.segment == segment
                    && progress.elapsed_ms == progress.planned_ms
            }));
        }
    }

    #[test]
    fn up_down_plan_honors_repetitions_and_cycles() {
        let mut config = config();
        config.strategy = Strategy::UpDown;
        config.load.explicit_levels = vec![100.0, 200.0, 300.0];
        config.load.cycles = 2;
        config.phases.repetitions = 2;

        assert_eq!(
            configured_rates(&config, 100).unwrap(),
            [
                100.0, 100.0, 200.0, 200.0, 300.0, 300.0, 200.0, 200.0, 100.0, 100.0, 100.0, 100.0,
                200.0, 200.0, 300.0, 300.0, 200.0, 200.0, 100.0, 100.0,
            ]
        );
    }

    #[test]
    fn interrupted_measurement_publishes_the_results_received_before_stop() {
        let prepared = Arc::new(Mutex::new(None));
        let (mut cohort, catalog) = managed_cohort_with_behavior(prepared, false, true);
        let stop = Arc::new(AtomicBool::new(false));
        let mut sink = RecordingSink::default();
        let mut config = config();
        config.load.maximum_rate = 100.0;
        config.load.explicit_levels = vec![100.0];

        let completion = RunExecutor::default()
            .execute(&config, &catalog, &mut cohort, &stop, &mut sink)
            .unwrap();

        assert_eq!(completion, ExecutorCompletion::Stopped);
        assert_eq!(sink.phases.len(), 1);
        assert_eq!(sink.phases[0].1.stats.overall.attempts, 1);
        assert_eq!(sink.phases[0].1.stats.overall.successful, 1);
    }

    #[test]
    fn stop_before_start_disconnects_without_scheduling() {
        let prepared = Arc::new(Mutex::new(None));
        let (mut cohort, catalog) = managed_cohort(Arc::clone(&prepared));
        let stop = Arc::new(AtomicBool::new(true));
        let mut sink = RecordingSink::default();
        let completion = RunExecutor::default()
            .execute(&config(), &catalog, &mut cohort, &stop, &mut sink)
            .unwrap();

        assert!(prepared.lock().unwrap().is_none());
        assert_eq!(completion, ExecutorCompletion::Stopped);
    }

    #[test]
    fn bucket_drift_marks_a_phase_non_stationary() {
        let buckets = (0..5)
            .map(|index| MeasurementBucket {
                start_offset_ns: index * 200,
                duration_ns: 200,
                attempts: 4,
                successful: if index == 0 { 20 } else { 0 },
                failed: 0,
                timed_out: 0,
                goodput_rate: if index == 0 { 100_000_000.0 } else { 0.0 },
            })
            .collect::<Vec<_>>();
        let quality = phase_quality(&buckets, 20, &ExecutorOptions::default());

        assert!(!quality.stationary);
        assert_eq!(quality.buckets.len(), 5);
        assert!(
            quality
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("exceeded"))
        );
    }
}

//! Frontend-neutral execution of prepared workload cohorts.

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::{
    adapter_session::ScheduleCompletion,
    agent::{AgentCohort, CohortError, CohortReady},
    config::{OperationSelection, RunConfig, Strategy, WeightedOperation},
    measurement::{RunClassification, RunEvent, RunOutcome},
    protocol::{OperationId, OperationResult, OperationStatus, PhaseId, ScheduledOperation},
    stats::{MeasurementBucket, PhaseQuality, PhaseReport, StatsError, summarize_results},
    strategy::{
        AdaptiveStrategy, ObservationOutcome, StrategyAction, StrategyDecision, fixed_stage,
    },
};

const NANOS_PER_SECOND: f64 = 1_000_000_000.0;

#[derive(Debug, Clone)]
pub struct ExecutorOptions {
    /// Maximum global schedule submitted in one coordinator round trip.
    pub maximum_batch_size: usize,
    /// Maximum retained operation results used to summarize one phase.
    pub maximum_results_per_phase: usize,
    /// Maximum number of measured phases in one fixed plan.
    pub maximum_phases: usize,
    /// Maximum intended duration represented by one schedule batch.
    pub schedule_horizon: Duration,
    /// Lead time allowing every agent to receive a schedule before it begins.
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
            maximum_batch_size: 4_096,
            maximum_results_per_phase: 1_000_000,
            maximum_phases: 10_000,
            schedule_horizon: Duration::from_secs(1),
            schedule_lead_time: Duration::from_millis(25),
            dispatch_lag_fraction: 0.10,
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
        validate_operation_budget(config, &rates, &self.options)?;
        let batch_limit = catalog
            .capabilities
            .max_batch_size
            .map_or(self.options.maximum_batch_size, |limit| {
                self.options.maximum_batch_size.min(limit as usize)
            });
        if batch_limit == 0 {
            return Err(ExecutorError::InvalidConfiguration(
                "adapter maximum batch size must be greater than zero".into(),
            ));
        }

        sink.record_run_event(RunEvent::AdapterReady)
            .map_err(ExecutorError::Sink)?;
        let mut next_wire_phase_id = 1_u64;
        let mut next_operation_id = 1_u64;
        if config.strategy == Strategy::Adaptive {
            self.execute_adaptive(
                config,
                cohort,
                stop,
                sink,
                operations,
                batch_limit,
                &mut next_wire_phase_id,
                &mut next_operation_id,
            )
        } else {
            self.execute_fixed(
                config,
                &rates,
                cohort,
                stop,
                sink,
                operations,
                batch_limit,
                &mut next_wire_phase_id,
                &mut next_operation_id,
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
        batch_limit: usize,
        next_wire_phase_id: &mut u64,
        next_operation_id: &mut u64,
    ) -> Result<ExecutorCompletion, ExecutorError> {
        let mut generator_saturated = false;
        let mut unstable_measurement = false;
        let mut sequence = 1_u64;
        let mut measured_phases = 0_u64;
        for rate in rates.iter().copied() {
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
                operations,
                batch_limit,
                next_wire_phase_id,
                next_operation_id,
                rate,
            )?
            else {
                return Ok(ExecutorCompletion::Stopped);
            };
            generator_saturated |=
                dispatch_lag_invalid(&report, config.phases.measurement_ms, &self.options);
            unstable_measurement |= !report.quality.stationary;
            measured_phases += 1;
            let phase_id = PhaseId(measured_phases);
            sink.record_phase_stats(phase_id, report.clone())
                .map_err(ExecutorError::Sink)?;
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
            if recover_or_stop(config, stop) {
                return Ok(ExecutorCompletion::Stopped);
            }
        }
        let outcome = if generator_saturated {
            RunOutcome {
                classification: RunClassification::GeneratorSaturated,
                knee: None,
                slo_maximum_rate: None,
                warnings: vec![
                    "dispatch lag exceeded the executor validity threshold; no target knee was reported"
                        .into(),
                ],
            }
        } else if unstable_measurement {
            RunOutcome {
                classification: RunClassification::UnstableMeasurement,
                knee: None,
                slo_maximum_rate: None,
                warnings: vec![
                    "one or more fixed phases failed the bucket stationarity check".into(),
                ],
            }
        } else {
            RunOutcome {
                classification: RunClassification::MaximumLoadReached,
                knee: None,
                slo_maximum_rate: None,
                warnings: vec![
                    "fixed execution completed; statistical knee fitting is tracked by issue #4"
                        .into(),
                ],
            }
        };
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
        batch_limit: usize,
        next_wire_phase_id: &mut u64,
        next_operation_id: &mut u64,
    ) -> Result<ExecutorCompletion, ExecutorError> {
        let mut strategy = AdaptiveStrategy::new(config);
        let mut request = strategy.initial_request();
        let mut sequence = 1_u64;
        let mut measured_phases = 0_usize;
        loop {
            if measured_phases >= self.options.maximum_phases {
                return Ok(ExecutorCompletion::Completed(RunOutcome {
                    classification: RunClassification::UnstableMeasurement,
                    knee: None,
                    slo_maximum_rate: None,
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
                operations,
                batch_limit,
                next_wire_phase_id,
                next_operation_id,
                request.rate,
            )?
            else {
                return Ok(ExecutorCompletion::Stopped);
            };
            measured_phases += 1;
            let generator_saturated =
                dispatch_lag_invalid(&report, config.phases.measurement_ms, &self.options);
            sink.record_phase_stats(PhaseId(measured_phases as u64), report.clone())
                .map_err(ExecutorError::Sink)?;
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
            if recover_or_stop(config, stop) {
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
        operations: &[WeightedOperation],
        batch_limit: usize,
        next_wire_phase_id: &mut u64,
        next_operation_id: &mut u64,
        rate: f64,
    ) -> Result<Option<PhaseReport>, ExecutorError> {
        if stop.load(Ordering::Acquire) {
            return Ok(None);
        }
        if config.phases.warmup_ms > 0 {
            self.run_interval(
                cohort,
                stop,
                next_wire_phase_id,
                next_operation_id,
                rate,
                Duration::from_millis(config.phases.warmup_ms),
                operations,
                batch_limit,
                false,
            )?;
            if stop.load(Ordering::Acquire) {
                return Ok(None);
            }
        }
        let measured = self.run_interval(
            cohort,
            stop,
            next_wire_phase_id,
            next_operation_id,
            rate,
            Duration::from_millis(config.phases.measurement_ms),
            operations,
            batch_limit,
            true,
        )?;
        if measured.elapsed_ns == 0 {
            return Ok(None);
        }
        let quality = phase_quality(&measured, &self.options);
        Ok(Some(PhaseReport {
            offered_rate: rate,
            goodput_rate: measured.successful_in_window as f64 * NANOS_PER_SECOND
                / measured.elapsed_ns as f64,
            elapsed_ns: measured.elapsed_ns,
            stats: summarize_results(&measured.results)?,
            quality,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    fn run_interval(
        &self,
        cohort: &mut AgentCohort,
        stop: &Arc<AtomicBool>,
        next_wire_phase_id: &mut u64,
        next_operation_id: &mut u64,
        offered_rate: f64,
        duration: Duration,
        operations: &[WeightedOperation],
        batch_limit: usize,
        retain_results: bool,
    ) -> Result<MeasuredInterval, ExecutorError> {
        let mut remaining_ns = duration.as_nanos().min(u64::MAX as u128) as u64;
        let mut elapsed_ns = 0_u64;
        let mut operation_budget = 0.0_f64;
        let mut successful_in_window = 0_u64;
        let mut results = Vec::new();
        let total_weight = operations.iter().map(|operation| operation.weight).sum();
        let mut scheduler = SmoothWeightedScheduler::new(operations, total_weight);
        let phase_start = unix_now_ns().saturating_add(
            self.options
                .schedule_lead_time
                .as_nanos()
                .min(u64::MAX as u128) as u64,
        );

        while remaining_ns > 0 && !stop.load(Ordering::Acquire) {
            let horizon_ns = self
                .options
                .schedule_horizon
                .as_nanos()
                .min(u64::MAX as u128) as u64;
            let batch_limited_ns =
                ((batch_limit as f64 / offered_rate) * NANOS_PER_SECOND).floor() as u64;
            let chunk_ns = remaining_ns
                .min(horizon_ns.max(1))
                .min(batch_limited_ns.max(1));
            operation_budget += offered_rate * chunk_ns as f64 / NANOS_PER_SECOND;
            let operation_count = (operation_budget.floor() as usize).min(batch_limit);
            operation_budget -= operation_count as f64;

            if operation_count == 0 {
                if sleep_until_or_stop(stop, Duration::from_nanos(chunk_ns)) {
                    break;
                }
            } else {
                let mut scheduled = Vec::with_capacity(operation_count);
                for index in 0..operation_count {
                    let variant = scheduler.next();
                    let id = OperationId(*next_operation_id);
                    *next_operation_id = next_operation_id
                        .checked_add(1)
                        .ok_or(ExecutorError::OperationIdExhausted)?;
                    scheduled.push(ScheduledOperation {
                        id,
                        operation: variant.name.clone(),
                        start_offset_ns: elapsed_ns.saturating_add(
                            (index as f64 * NANOS_PER_SECOND / offered_rate).round() as u64,
                        ),
                        arguments: variant.arguments.clone(),
                    });
                }
                let phase_id = PhaseId(*next_wire_phase_id);
                *next_wire_phase_id = next_wire_phase_id
                    .checked_add(1)
                    .ok_or(ExecutorError::PhaseIdExhausted)?;
                let batch = cohort.execute_schedule(phase_id, phase_start, scheduled, stop)?;
                let interrupted = matches!(batch.completion, ScheduleCompletion::Cancelled { .. });
                let batch_results = batch.into_operations();
                let completed_offset_ns = batch_results
                    .iter()
                    .map(|result| {
                        result
                            .actual_start_offset_ns
                            .saturating_add(result.client_latency_ns)
                    })
                    .max()
                    .unwrap_or(0);
                if retain_results {
                    successful_in_window = successful_in_window.saturating_add(
                        batch_results
                            .iter()
                            .filter(|result| successful_within(result, duration))
                            .count() as u64,
                    );
                    results.extend(batch_results);
                }
                if interrupted {
                    elapsed_ns = elapsed_ns.max(
                        unix_now_ns()
                            .saturating_sub(phase_start)
                            .max(completed_offset_ns)
                            .min(duration.as_nanos().min(u64::MAX as u128) as u64),
                    );
                    break;
                }
            }
            elapsed_ns = elapsed_ns.saturating_add(chunk_ns);
            remaining_ns -= chunk_ns;
        }

        Ok(MeasuredInterval {
            elapsed_ns,
            successful_in_window,
            results,
        })
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
    if !catalog.capabilities.scheduled_operations {
        return Err(ExecutorError::UnsupportedCapability(
            "adapter does not support coordinator-scheduled operations".into(),
        ));
    }
    Ok(())
}

fn validate_operation_budget(
    config: &RunConfig,
    rates: &[f64],
    options: &ExecutorOptions,
) -> Result<(), ExecutorError> {
    let duration_seconds = config.phases.measurement_ms as f64 / 1_000.0;
    if rates
        .iter()
        .any(|rate| rate * duration_seconds > options.maximum_results_per_phase as f64)
    {
        return Err(ExecutorError::InvalidConfiguration(format!(
            "a measurement phase exceeds the retained result limit of {} operations",
            options.maximum_results_per_phase
        )));
    }
    Ok(())
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

fn phase_quality(measured: &MeasuredInterval, options: &ExecutorOptions) -> PhaseQuality {
    let bucket_count = options.stationarity_buckets.max(1);
    let bucket_duration = measured.elapsed_ns.div_ceil(bucket_count as u64).max(1);
    let mut buckets = (0..bucket_count)
        .map(|index| MeasurementBucket {
            start_offset_ns: index as u64 * bucket_duration,
            duration_ns: bucket_duration.min(
                measured
                    .elapsed_ns
                    .saturating_sub(index as u64 * bucket_duration),
            ),
            attempts: 0,
            successful: 0,
            failed: 0,
            timed_out: 0,
            goodput_rate: 0.0,
        })
        .filter(|bucket| bucket.duration_ns > 0)
        .collect::<Vec<_>>();

    for result in &measured.results {
        let last_bucket = buckets.len().saturating_sub(1);
        let attempt_index = (result.intended_start_offset_ns / bucket_duration) as usize;
        let Some(attempt_bucket) = buckets.get_mut(attempt_index.min(last_bucket)) else {
            continue;
        };
        attempt_bucket.attempts += 1;
        let completion_offset = result
            .actual_start_offset_ns
            .saturating_add(result.client_latency_ns);
        if completion_offset > measured.elapsed_ns {
            continue;
        }
        let completion_index = (completion_offset / bucket_duration) as usize;
        let Some(completion_bucket) = buckets.get_mut(completion_index.min(last_bucket)) else {
            continue;
        };
        match result.status {
            OperationStatus::Ok => completion_bucket.successful += 1,
            OperationStatus::Error { .. } => completion_bucket.failed += 1,
            OperationStatus::Timeout => completion_bucket.timed_out += 1,
        }
    }
    for bucket in &mut buckets {
        bucket.goodput_rate =
            bucket.successful as f64 * NANOS_PER_SECOND / bucket.duration_ns as f64;
    }

    if (measured.results.len() as u64) < options.minimum_stationarity_samples || buckets.len() < 2 {
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

fn recover_or_stop(config: &RunConfig, stop: &Arc<AtomicBool>) -> bool {
    config.phases.recovery_ms > 0
        && sleep_until_or_stop(stop, Duration::from_millis(config.phases.recovery_ms))
}

fn successful_within(result: &OperationResult, interval_duration: Duration) -> bool {
    let interval_ns = interval_duration.as_nanos().min(u64::MAX as u128) as u64;
    matches!(result.status, OperationStatus::Ok)
        && result
            .actual_start_offset_ns
            .saturating_add(result.client_latency_ns)
            <= interval_ns
}

fn sleep_until_or_stop(stop: &Arc<AtomicBool>, duration: Duration) -> bool {
    let deadline = std::time::Instant::now() + duration;
    loop {
        if stop.load(Ordering::Acquire) {
            return true;
        }
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return false;
        }
        thread::sleep(remaining.min(Duration::from_millis(25)));
    }
}

fn unix_now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

struct MeasuredInterval {
    elapsed_ns: u64,
    successful_in_window: u64,
    results: Vec<OperationResult>,
}

struct SmoothWeightedScheduler<'a> {
    variants: &'a [WeightedOperation],
    scores: Vec<f64>,
    total_weight: f64,
}

impl<'a> SmoothWeightedScheduler<'a> {
    fn new(variants: &'a [WeightedOperation], total_weight: f64) -> Self {
        Self {
            variants,
            scores: vec![0.0; variants.len()],
            total_weight,
        }
    }

    fn next(&mut self) -> &'a WeightedOperation {
        for (score, variant) in self.scores.iter_mut().zip(self.variants) {
            *score += variant.weight;
        }
        let selected = self
            .scores
            .iter()
            .enumerate()
            .max_by(|(_, left), (_, right)| left.total_cmp(right))
            .map(|(index, _)| index)
            .expect("prepared workloads contain at least one variant");
        self.scores[selected] -= self.total_weight;
        &self.variants[selected]
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExecutorCompletion {
    Completed(RunOutcome),
    Stopped,
}

#[derive(Debug)]
pub enum ExecutorError {
    InvalidConfiguration(String),
    UnsupportedCapability(String),
    Cohort(CohortError),
    Stats(StatsError),
    Sink(String),
    OperationIdExhausted,
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
            Self::UnsupportedCapability(message) => formatter.write_str(message),
            Self::Cohort(error) => error.fmt(formatter),
            Self::Stats(error) => error.fmt(formatter),
            Self::Sink(message) => write!(formatter, "failed to publish executor event: {message}"),
            Self::OperationIdExhausted => formatter.write_str("operation identifier exhausted"),
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
        adapter_session::AdapterReady,
        agent::{
            AgentDescriptor, AgentError, AgentId, AgentInstanceId, AgentPlacement, AgentReady,
            WorkloadAgent,
        },
        config::{LoadConfig, PhaseConfig, Preset, Strategy, WorkloadConfig},
        protocol::{
            AdapterIdentity, ArgumentValue, Capabilities, LoadModel, OperationDescriptor,
            OperationKind,
        },
        stats::PhaseReport,
    };

    struct FakeAgent {
        descriptor: AgentDescriptor,
        assignments: Arc<Mutex<Vec<Vec<ScheduledOperation>>>>,
        interrupt_after_first: bool,
    }

    impl WorkloadAgent for FakeAgent {
        fn descriptor(&self) -> &AgentDescriptor {
            &self.descriptor
        }

        fn initialize(
            &mut self,
            _run_id: crate::protocol::RunId,
            _config: Value,
        ) -> Result<AgentReady, AgentError> {
            Ok(AgentReady {
                agent: self.descriptor.clone(),
                adapter: ready(),
            })
        }

        fn execute_schedule(
            &mut self,
            _phase_id: PhaseId,
            _phase_start_unix_ns: u64,
            operations: Vec<ScheduledOperation>,
            stop: &AtomicBool,
        ) -> Result<crate::adapter_session::ScheduleOutcome, AgentError> {
            self.assignments.lock().unwrap().push(operations.clone());
            let completion = if self.interrupt_after_first {
                stop.store(true, Ordering::Release);
                ScheduleCompletion::Cancelled { forced: false }
            } else {
                ScheduleCompletion::Completed
            };
            Ok(crate::adapter_session::ScheduleOutcome {
                operations: operations
                    .into_iter()
                    .take(if self.interrupt_after_first {
                        1
                    } else {
                        usize::MAX
                    })
                    .map(|operation| OperationResult {
                        id: operation.id,
                        operation: operation.operation,
                        arguments: operation.arguments,
                        intended_start_offset_ns: operation.start_offset_ns,
                        actual_start_offset_ns: operation.start_offset_ns,
                        client_latency_ns: 1,
                        status: OperationStatus::Ok,
                    })
                    .collect(),
                completion,
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
    }

    fn ready() -> crate::adapter_session::AdapterReady {
        AdapterReady {
            identity: AdapterIdentity {
                name: "fake".into(),
                version: None,
            },
            capabilities: Capabilities {
                scheduled_operations: true,
                adapter_managed_phases: false,
                load_models: vec![LoadModel::OpenLoop],
                max_batch_size: Some(4),
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

    fn cohort(assignments: Arc<Mutex<Vec<Vec<ScheduledOperation>>>>) -> (AgentCohort, CohortReady) {
        cohort_with_interrupt(assignments, false)
    }

    fn cohort_with_interrupt(
        assignments: Arc<Mutex<Vec<Vec<ScheduledOperation>>>>,
        interrupt_after_first: bool,
    ) -> (AgentCohort, CohortReady) {
        let descriptor = AgentDescriptor {
            id: AgentId("fake-0".into()),
            instance_id: AgentInstanceId("fake-0-instance".into()),
            placement: AgentPlacement::Colocated,
        };
        let mut cohort = AgentCohort::new(vec![Box::new(FakeAgent {
            descriptor,
            assignments,
            interrupt_after_first,
        })])
        .unwrap();
        let catalog = cohort
            .initialize(crate::protocol::RunId(1), Value::Null)
            .unwrap();
        (cohort, catalog)
    }

    #[test]
    fn fixed_sweep_batches_work_and_reports_every_phase() {
        let assignments = Arc::new(Mutex::new(Vec::new()));
        let (mut cohort, catalog) = cohort(Arc::clone(&assignments));
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
        assert!(
            assignments
                .lock()
                .unwrap()
                .iter()
                .all(|batch| batch.len() <= 4)
        );
        assert!(matches!(sink.events.first(), Some(RunEvent::AdapterReady)));
        assert!(matches!(
            completion,
            ExecutorCompletion::Completed(RunOutcome {
                classification: RunClassification::MaximumLoadReached,
                ..
            })
        ));
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
    fn batches_keep_offsets_relative_to_the_whole_measurement_phase() {
        let assignments = Arc::new(Mutex::new(Vec::new()));
        let (mut cohort, catalog) = cohort(Arc::clone(&assignments));
        let mut config = config();
        config.load.initial_rate = 500.0;
        config.load.maximum_rate = 500.0;
        config.load.explicit_levels = vec![500.0];
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

        let offsets = assignments
            .lock()
            .unwrap()
            .iter()
            .flatten()
            .map(|operation| operation.start_offset_ns)
            .collect::<Vec<_>>();
        assert_eq!(
            offsets,
            [
                0, 2_000_000, 4_000_000, 6_000_000, 8_000_000, 10_000_000, 12_000_000, 14_000_000,
                16_000_000, 18_000_000,
            ]
        );
        assert_eq!(sink.phases[0].1.goodput_rate, 500.0);
    }

    #[test]
    fn interrupted_measurement_publishes_the_results_received_before_stop() {
        let assignments = Arc::new(Mutex::new(Vec::new()));
        let (mut cohort, catalog) = cohort_with_interrupt(Arc::clone(&assignments), true);
        let stop = Arc::new(AtomicBool::new(false));
        let mut sink = RecordingSink::default();

        let completion = RunExecutor::default()
            .execute(&config(), &catalog, &mut cohort, &stop, &mut sink)
            .unwrap();

        assert_eq!(completion, ExecutorCompletion::Stopped);
        assert_eq!(sink.phases.len(), 1);
        assert_eq!(sink.phases[0].1.stats.overall.attempts, 1);
        assert_eq!(sink.phases[0].1.stats.overall.successful, 1);
    }

    #[test]
    fn stop_before_start_disconnects_without_scheduling() {
        let assignments = Arc::new(Mutex::new(Vec::new()));
        let (mut cohort, catalog) = cohort(Arc::clone(&assignments));
        let stop = Arc::new(AtomicBool::new(true));
        let mut sink = RecordingSink::default();
        let completion = RunExecutor::default()
            .execute(&config(), &catalog, &mut cohort, &stop, &mut sink)
            .unwrap();

        assert!(assignments.lock().unwrap().is_empty());
        assert_eq!(completion, ExecutorCompletion::Stopped);
    }

    #[test]
    fn bucket_drift_marks_a_phase_non_stationary() {
        let results = (0..20)
            .map(|id| OperationResult {
                id: OperationId(id + 1),
                operation: "read".into(),
                arguments: BTreeMap::new(),
                intended_start_offset_ns: id,
                actual_start_offset_ns: id,
                client_latency_ns: 1,
                status: OperationStatus::Ok,
            })
            .collect();
        let quality = phase_quality(
            &MeasuredInterval {
                elapsed_ns: 1_000,
                successful_in_window: 20,
                results,
            },
            &ExecutorOptions::default(),
        );

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

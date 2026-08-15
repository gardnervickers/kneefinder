//! Statistics grouped by fully bound operation variants.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use serde::{Deserialize, Serialize};

use crate::{
    histogram::{
        EncodedHistogram, HistogramDecodeLimits, HistogramError, HistogramSpec, LatencyHistogram,
    },
    protocol::{
        ArgumentValue, OperationPhaseResult, OperationResult, OperationStatus, PhaseErrorCount,
        PhaseResult, TimeBucket,
    },
};

pub const DEFAULT_MAX_VARIANTS: usize = 1_024;
pub const DEFAULT_MAX_ERROR_CODES: usize = 256;
pub const DEFAULT_MAX_ERROR_CODE_BYTES: usize = 128;
pub const DEFAULT_MAX_BUCKETS: usize = 256;
pub const DEFAULT_MAX_TOTAL_ENCODED_HISTOGRAM_BYTES: usize = 12 * 1024 * 1024;
pub const DEFAULT_MAX_TOTAL_HISTOGRAM_CELLS: usize = 8_000_000;

/// One graphable point produced by a completed measurement phase.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PhaseReport {
    pub offered_rate: f64,
    pub goodput_rate: f64,
    pub elapsed_ns: u64,
    #[serde(default)]
    pub offered_count: u64,
    #[serde(default)]
    pub started_count: u64,
    #[serde(default)]
    pub completed_count: u64,
    #[serde(default)]
    pub successful_in_window: u64,
    #[serde(default)]
    pub in_flight_high_water: u64,
    pub stats: StatsReport,
    #[serde(default)]
    pub quality: PhaseQuality,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PhaseQuality {
    pub stationary: bool,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub buckets: Vec<MeasurementBucket>,
}

impl Default for PhaseQuality {
    fn default() -> Self {
        Self {
            stationary: true,
            reason: None,
            buckets: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MeasurementBucket {
    pub start_offset_ns: u64,
    pub duration_ns: u64,
    pub attempts: u64,
    pub successful: u64,
    pub failed: u64,
    pub timed_out: u64,
    pub goodput_rate: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OperationVariant {
    pub operation: String,
    pub arguments: BTreeMap<String, ArgumentValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatsReport {
    pub overall: SampleStats,
    pub variants: Vec<VariantStats>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VariantStats {
    pub variant: OperationVariant,
    pub stats: SampleStats,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SampleStats {
    pub attempts: u64,
    pub successful: u64,
    pub failed: u64,
    pub timed_out: u64,
    #[serde(default)]
    pub errors_by_code: Vec<ErrorCount>,
    pub client_latency_ns: DistributionStats,
    pub total_latency_ns: DistributionStats,
    pub dispatch_lag_ns: DistributionStats,
}

impl SampleStats {
    pub fn error_rate(&self) -> f64 {
        ratio(self.failed, self.attempts)
    }

    pub fn timeout_rate(&self) -> f64 {
        ratio(self.timed_out, self.attempts)
    }

    pub fn unsuccessful_rate(&self) -> f64 {
        ratio(self.failed.saturating_add(self.timed_out), self.attempts)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorCount {
    /// Adapter-provided stable code, or `None` for an uncategorized error.
    pub code: Option<String>,
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DistributionStats {
    pub samples: u64,
    pub min: Option<u64>,
    pub p50: Option<u64>,
    pub p95: Option<u64>,
    pub p99: Option<u64>,
    pub max: Option<u64>,
}

/// Immutable aggregation contract shared by local observations and wire summaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhaseAggregationPlan {
    pub measurement_ns: u64,
    pub bucket_count: u16,
    pub histogram: HistogramSpec,
    pub variants: Vec<OperationVariant>,
}

/// Resource limits for aggregate state and untrusted adapter summaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggregationLimits {
    pub maximum_variants: usize,
    pub maximum_buckets: usize,
    pub maximum_error_codes: usize,
    pub maximum_error_code_bytes: usize,
    pub maximum_total_encoded_histogram_bytes: usize,
    pub maximum_total_histogram_cells: usize,
    pub histogram_decode: HistogramDecodeLimits,
}

impl Default for AggregationLimits {
    fn default() -> Self {
        Self {
            maximum_variants: DEFAULT_MAX_VARIANTS,
            maximum_buckets: DEFAULT_MAX_BUCKETS,
            maximum_error_codes: DEFAULT_MAX_ERROR_CODES,
            maximum_error_code_bytes: DEFAULT_MAX_ERROR_CODE_BYTES,
            maximum_total_encoded_histogram_bytes: DEFAULT_MAX_TOTAL_ENCODED_HISTOGRAM_BYTES,
            maximum_total_histogram_cells: DEFAULT_MAX_TOTAL_HISTOGRAM_CELLS,
            histogram_decode: HistogramDecodeLimits::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AggregationMode {
    Empty,
    LocalObservations,
    Managed,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct SampleCounts {
    offered: u64,
    started: u64,
    completed: u64,
    successful: u64,
    successful_in_window: u64,
    failed: u64,
    timed_out: u64,
}

impl SampleCounts {
    fn validate(&self, context: &str) -> Result<(), StatsError> {
        let outcomes = checked_sum(
            [self.successful, self.failed, self.timed_out],
            "terminal outcome counts",
        )?;
        if outcomes != self.completed {
            return Err(StatsError::InvalidPhase(format!(
                "{context} successful, failed, and timed-out counts sum to {outcomes}, not completed count {}",
                self.completed
            )));
        }
        if self.completed > self.started || self.started > self.offered {
            return Err(StatsError::InvalidPhase(format!(
                "{context} requires completed <= started <= offered, got {} <= {} <= {}",
                self.completed, self.started, self.offered
            )));
        }
        if self.successful_in_window > self.successful {
            return Err(StatsError::InvalidPhase(format!(
                "{context} in-window successes exceed all successes"
            )));
        }
        Ok(())
    }

    fn checked_add(&self, other: &Self) -> Result<Self, StatsError> {
        Ok(Self {
            offered: checked_add(self.offered, other.offered, "offered count")?,
            started: checked_add(self.started, other.started, "started count")?,
            completed: checked_add(self.completed, other.completed, "completed count")?,
            successful: checked_add(self.successful, other.successful, "successful count")?,
            successful_in_window: checked_add(
                self.successful_in_window,
                other.successful_in_window,
                "in-window successful count",
            )?,
            failed: checked_add(self.failed, other.failed, "failed count")?,
            timed_out: checked_add(self.timed_out, other.timed_out, "timed-out count")?,
        })
    }
}

#[derive(Debug, Clone)]
struct SampleAccumulator {
    counts: SampleCounts,
    errors_by_code: BTreeMap<Option<String>, u64>,
    client_latency: LatencyHistogram,
    total_latency: LatencyHistogram,
    dispatch_lag: LatencyHistogram,
}

impl SampleAccumulator {
    fn new(spec: HistogramSpec) -> Result<Self, StatsError> {
        Ok(Self {
            counts: SampleCounts::default(),
            errors_by_code: BTreeMap::new(),
            client_latency: LatencyHistogram::new(spec).map_err(StatsError::histogram)?,
            total_latency: LatencyHistogram::new(spec).map_err(StatsError::histogram)?,
            dispatch_lag: LatencyHistogram::new(spec).map_err(StatsError::histogram)?,
        })
    }

    fn validate_record(
        &self,
        result: &OperationResult,
        completion_offset_ns: u64,
        limits: &AggregationLimits,
    ) -> Result<(), StatsError> {
        if result.actual_start_offset_ns < result.intended_start_offset_ns {
            return Err(StatsError::InvalidPhase(format!(
                "operation {} starts before its intended deadline",
                result.id.0
            )));
        }
        let highest = self.client_latency.spec().highest_trackable_ns;
        for (name, value) in [
            ("client latency", result.client_latency_ns),
            ("total latency", result.total_latency_ns()),
            ("dispatch lag", result.dispatch_lag_ns()),
        ] {
            if value > highest {
                return Err(StatsError::InvalidPhase(format!(
                    "operation {} {name} of {value} ns exceeds histogram maximum {highest} ns",
                    result.id.0
                )));
            }
        }
        self.counts
            .completed
            .checked_add(1)
            .ok_or(StatsError::CounterOverflow("completed count"))?;
        if let OperationStatus::Error { code } = &result.status {
            validate_error_code(code.as_deref(), limits)?;
            let new_code = !self.errors_by_code.contains_key(code);
            if new_code && self.errors_by_code.len() >= limits.maximum_error_codes {
                return Err(StatsError::TooManyErrorCodes {
                    maximum: limits.maximum_error_codes,
                });
            }
        }
        let _ = completion_offset_ns;
        Ok(())
    }

    fn validate_offer(&self) -> Result<(), StatsError> {
        self.counts
            .offered
            .checked_add(1)
            .ok_or(StatsError::CounterOverflow("offered count"))?;
        Ok(())
    }

    fn record_offer(&mut self) {
        self.counts.offered += 1;
    }

    fn record(
        &mut self,
        result: &OperationResult,
        successful_in_window: bool,
    ) -> Result<(), StatsError> {
        self.counts.offered += 1;
        self.counts.started += 1;
        self.counts.completed += 1;
        match &result.status {
            OperationStatus::Ok => {
                self.counts.successful += 1;
                self.counts.successful_in_window += u64::from(successful_in_window);
            }
            OperationStatus::Error { code } => {
                self.counts.failed += 1;
                *self.errors_by_code.entry(code.clone()).or_default() += 1;
            }
            OperationStatus::Timeout => self.counts.timed_out += 1,
        }
        self.client_latency
            .record(result.client_latency_ns)
            .map_err(StatsError::histogram)?;
        self.total_latency
            .record(result.total_latency_ns())
            .map_err(StatsError::histogram)?;
        self.dispatch_lag
            .record(result.dispatch_lag_ns())
            .map_err(StatsError::histogram)
    }

    fn checked_merge(
        &mut self,
        other: &Self,
        limits: &AggregationLimits,
    ) -> Result<(), StatsError> {
        self.counts = self.counts.checked_add(&other.counts)?;
        for (code, count) in &other.errors_by_code {
            validate_error_code(code.as_deref(), limits)?;
            if !self.errors_by_code.contains_key(code)
                && self.errors_by_code.len() >= limits.maximum_error_codes
            {
                return Err(StatsError::TooManyErrorCodes {
                    maximum: limits.maximum_error_codes,
                });
            }
            let current = self.errors_by_code.entry(code.clone()).or_default();
            *current = checked_add(*current, *count, "error-code count")?;
        }
        self.client_latency
            .merge(&other.client_latency)
            .map_err(StatsError::histogram)?;
        self.total_latency
            .merge(&other.total_latency)
            .map_err(StatsError::histogram)?;
        self.dispatch_lag
            .merge(&other.dispatch_lag)
            .map_err(StatsError::histogram)
    }

    fn to_stats(&self) -> Result<SampleStats, StatsError> {
        Ok(SampleStats {
            attempts: self.counts.completed,
            successful: self.counts.successful,
            failed: self.counts.failed,
            timed_out: self.counts.timed_out,
            errors_by_code: self
                .errors_by_code
                .iter()
                .map(|(code, count)| ErrorCount {
                    code: code.clone(),
                    count: *count,
                })
                .collect(),
            client_latency_ns: distribution(&self.client_latency)?,
            total_latency_ns: distribution(&self.total_latency)?,
            dispatch_lag_ns: distribution(&self.dispatch_lag)?,
        })
    }

    fn encoded_histograms(
        &self,
    ) -> Result<(EncodedHistogram, EncodedHistogram, EncodedHistogram), StatsError> {
        Ok((
            self.client_latency
                .encode()
                .map_err(StatsError::histogram)?,
            self.total_latency.encode().map_err(StatsError::histogram)?,
            self.dispatch_lag.encode().map_err(StatsError::histogram)?,
        ))
    }
}

/// Bounded phase aggregator used by both execution modes.
#[derive(Debug, Clone)]
pub struct PhaseAccumulator {
    plan: PhaseAggregationPlan,
    limits: AggregationLimits,
    overall: SampleAccumulator,
    variants: BTreeMap<OperationVariant, SampleAccumulator>,
    buckets: Vec<TimeBucket>,
    in_flight_high_water: u64,
    managed_elapsed_ns: Option<u64>,
    mode: AggregationMode,
}

impl PhaseAccumulator {
    pub fn new(plan: PhaseAggregationPlan) -> Result<Self, StatsError> {
        Self::new_with_limits(plan, AggregationLimits::default())
    }

    pub fn new_with_limits(
        plan: PhaseAggregationPlan,
        limits: AggregationLimits,
    ) -> Result<Self, StatsError> {
        validate_plan(&plan, &limits)?;
        let probe = LatencyHistogram::new(plan.histogram).map_err(StatsError::histogram)?;
        let histogram_count = plan
            .variants
            .len()
            .checked_add(1)
            .and_then(|samples| samples.checked_mul(3))
            .ok_or(StatsError::ResourceLimit("histogram count overflow"))?;
        let total_cells = probe
            .distinct_values()
            .checked_mul(histogram_count)
            .ok_or(StatsError::ResourceLimit("histogram cell count overflow"))?;
        if total_cells > limits.maximum_total_histogram_cells {
            return Err(StatsError::ResourceLimit(
                "phase histograms exceed the configured counter-cell budget",
            ));
        }

        let variants = plan
            .variants
            .iter()
            .cloned()
            .map(|variant| Ok((variant, SampleAccumulator::new(plan.histogram)?)))
            .collect::<Result<_, StatsError>>()?;
        let buckets = planned_buckets(plan.measurement_ns, plan.bucket_count);
        let overall = SampleAccumulator::new(plan.histogram)?;
        Ok(Self {
            plan,
            limits,
            overall,
            variants,
            buckets,
            in_flight_high_water: 0,
            managed_elapsed_ns: None,
            mode: AggregationMode::Empty,
        })
    }

    pub fn plan(&self) -> &PhaseAggregationPlan {
        &self.plan
    }

    pub fn record(&mut self, result: &OperationResult) -> Result<(), StatsError> {
        self.record_inner(result, true)
    }

    /// Record an intended offer that the generator did not start.
    pub fn record_unstarted_offer(
        &mut self,
        variant: &OperationVariant,
        intended_start_offset_ns: u64,
    ) -> Result<(), StatsError> {
        if self.mode == AggregationMode::Managed {
            return Err(StatsError::MixedAggregationModes);
        }
        let bucket = bucket_index(
            intended_start_offset_ns,
            self.plan.measurement_ns,
            self.plan.bucket_count,
        )
        .filter(|_| intended_start_offset_ns < self.plan.measurement_ns)
        .ok_or_else(|| {
            StatsError::InvalidPhase(format!(
                "unstarted offer at {intended_start_offset_ns} ns lies outside the measurement interval"
            ))
        })?;
        let variant_sample = self
            .variants
            .get(variant)
            .ok_or_else(|| StatsError::UnknownVariant(variant.clone()))?;
        self.overall.validate_offer()?;
        variant_sample.validate_offer()?;
        self.buckets[bucket]
            .offered
            .checked_add(1)
            .ok_or(StatsError::CounterOverflow("bucket offered count"))?;

        self.overall.record_offer();
        self.variants
            .get_mut(variant)
            .expect("variant existence was validated before recording")
            .record_offer();
        self.buckets[bucket].offered += 1;
        self.mode = AggregationMode::LocalObservations;
        Ok(())
    }

    pub fn record_batch(&mut self, results: &[OperationResult]) -> Result<(), StatsError> {
        if self.mode == AggregationMode::Managed {
            return Err(StatsError::MixedAggregationModes);
        }
        for result in results {
            self.record_inner(result, false)?;
        }
        self.update_batch_high_water(results)?;
        self.mode = AggregationMode::LocalObservations;
        Ok(())
    }

    fn record_inner(
        &mut self,
        result: &OperationResult,
        update_high_water: bool,
    ) -> Result<(), StatsError> {
        if self.mode == AggregationMode::Managed {
            return Err(StatsError::MixedAggregationModes);
        }
        if result.intended_start_offset_ns >= self.plan.measurement_ns {
            return Err(StatsError::InvalidPhase(format!(
                "operation {} intended start lies outside the measurement interval",
                result.id.0
            )));
        }
        let completion_offset_ns = result
            .actual_start_offset_ns
            .checked_add(result.client_latency_ns)
            .ok_or(StatsError::CounterOverflow("operation completion offset"))?;
        let variant = OperationVariant {
            operation: result.operation.clone(),
            arguments: result.arguments.clone(),
        };
        let variant_sample = self
            .variants
            .get(&variant)
            .ok_or_else(|| StatsError::UnknownVariant(variant.clone()))?;
        self.overall
            .validate_record(result, completion_offset_ns, &self.limits)?;
        variant_sample.validate_record(result, completion_offset_ns, &self.limits)?;

        let successful_in_window = matches!(result.status, OperationStatus::Ok)
            && completion_offset_ns <= self.plan.measurement_ns;
        self.overall.record(result, successful_in_window)?;
        self.variants
            .get_mut(&variant)
            .expect("variant existence was validated before recording")
            .record(result, successful_in_window)?;

        let offered_bucket = bucket_index(
            result.intended_start_offset_ns,
            self.plan.measurement_ns,
            self.plan.bucket_count,
        )
        .expect("the intended start was validated inside the measurement interval");
        self.buckets[offered_bucket].offered += 1;
        if let Some(started_bucket) = bucket_index(
            result.actual_start_offset_ns,
            self.plan.measurement_ns,
            self.plan.bucket_count,
        ) {
            self.buckets[started_bucket].started += 1;
        }
        if let Some(completed_bucket) = bucket_index(
            completion_offset_ns,
            self.plan.measurement_ns,
            self.plan.bucket_count,
        ) {
            let bucket = &mut self.buckets[completed_bucket];
            bucket.completed += 1;
            match result.status {
                OperationStatus::Ok => bucket.successful += 1,
                OperationStatus::Error { .. } => bucket.failed += 1,
                OperationStatus::Timeout => bucket.timed_out += 1,
            }
        }
        if update_high_water {
            self.in_flight_high_water = self.in_flight_high_water.max(1);
            for bucket in &mut self.buckets {
                if intervals_overlap(
                    result.actual_start_offset_ns,
                    completion_offset_ns,
                    bucket.start_offset_ns,
                    bucket.start_offset_ns.saturating_add(bucket.duration_ns),
                ) {
                    bucket.in_flight_high_water = bucket.in_flight_high_water.max(1);
                }
            }
        }
        self.mode = AggregationMode::LocalObservations;
        Ok(())
    }

    pub fn merge(&mut self, result: &PhaseResult) -> Result<(), StatsError> {
        self.merge_phase_result(result)
    }

    pub fn merge_phase_result(&mut self, result: &PhaseResult) -> Result<(), StatsError> {
        if self.mode == AggregationMode::LocalObservations {
            return Err(StatsError::MixedAggregationModes);
        }
        let contribution = self.decode_contribution(result)?;
        let mut candidate = self.clone();
        candidate.apply_contribution(&contribution)?;
        candidate.mode = AggregationMode::Managed;
        *self = candidate;
        Ok(())
    }

    pub fn finish(self, offered_rate: f64) -> Result<PhaseReport, StatsError> {
        let elapsed_ns = self.plan.measurement_ns;
        self.finish_at(offered_rate, elapsed_ns)
    }

    /// Finalize a complete or interrupted phase at its actual measured duration.
    pub fn finish_at(self, offered_rate: f64, elapsed_ns: u64) -> Result<PhaseReport, StatsError> {
        if !offered_rate.is_finite() || offered_rate <= 0.0 {
            return Err(StatsError::InvalidPhase(
                "offered rate must be positive and finite".into(),
            ));
        }
        validate_elapsed(elapsed_ns, self.plan.measurement_ns)?;
        if let Some(managed_elapsed_ns) = self.managed_elapsed_ns
            && elapsed_ns != managed_elapsed_ns
        {
            return Err(StatsError::InvalidPhase(format!(
                "managed phase contributions report {managed_elapsed_ns} ns elapsed, not requested final elapsed {elapsed_ns} ns"
            )));
        }
        validate_trailing_empty_buckets(&self.buckets, elapsed_ns)?;
        let buckets = self
            .buckets
            .iter()
            .filter(|bucket| bucket.start_offset_ns < elapsed_ns)
            .map(|bucket| MeasurementBucket {
                start_offset_ns: bucket.start_offset_ns,
                duration_ns: bucket
                    .duration_ns
                    .min(elapsed_ns.saturating_sub(bucket.start_offset_ns)),
                attempts: bucket.offered,
                successful: bucket.successful,
                failed: bucket.failed,
                timed_out: bucket.timed_out,
                goodput_rate: bucket.successful as f64 * 1_000_000_000.0
                    / bucket.duration_ns as f64,
            })
            .collect();
        let variants = self
            .plan
            .variants
            .iter()
            .map(|variant| {
                Ok(VariantStats {
                    variant: variant.clone(),
                    stats: self
                        .variants
                        .get(variant)
                        .expect("the accumulator owns every planned variant")
                        .to_stats()?,
                })
            })
            .collect::<Result<_, StatsError>>()?;
        Ok(PhaseReport {
            offered_rate,
            goodput_rate: self.overall.counts.successful_in_window as f64 * 1_000_000_000.0
                / elapsed_ns as f64,
            elapsed_ns,
            offered_count: self.overall.counts.offered,
            started_count: self.overall.counts.started,
            completed_count: self.overall.counts.completed,
            successful_in_window: self.overall.counts.successful_in_window,
            in_flight_high_water: self.in_flight_high_water,
            stats: StatsReport {
                overall: self.overall.to_stats()?,
                variants,
            },
            quality: PhaseQuality {
                stationary: true,
                reason: None,
                buckets,
            },
        })
    }

    pub fn to_phase_result(&self) -> Result<PhaseResult, StatsError> {
        self.to_phase_result_at(self.plan.measurement_ns)
    }

    /// Encode a complete or interrupted phase using the planned bucket topology.
    pub fn to_phase_result_at(&self, elapsed_ns: u64) -> Result<PhaseResult, StatsError> {
        validate_elapsed(elapsed_ns, self.plan.measurement_ns)?;
        if let Some(managed_elapsed_ns) = self.managed_elapsed_ns
            && elapsed_ns != managed_elapsed_ns
        {
            return Err(StatsError::InvalidPhase(format!(
                "managed phase contributions report {managed_elapsed_ns} ns elapsed, not requested encoded elapsed {elapsed_ns} ns"
            )));
        }
        validate_trailing_empty_buckets(&self.buckets, elapsed_ns)?;
        let (client_latency, total_latency, dispatch_lag) = self.overall.encoded_histograms()?;
        let mut per_operation = Vec::with_capacity(self.plan.variants.len());
        for variant in &self.plan.variants {
            let sample = self
                .variants
                .get(variant)
                .expect("the accumulator owns every planned variant");
            let (variant_client, variant_total, variant_dispatch) = sample.encoded_histograms()?;
            per_operation.push(OperationPhaseResult {
                operation: variant.operation.clone(),
                arguments: variant.arguments.clone(),
                offered: sample.counts.offered,
                started: sample.counts.started,
                completed: sample.counts.completed,
                successful: sample.counts.successful,
                successful_in_window: sample.counts.successful_in_window,
                failed: sample.counts.failed,
                timed_out: sample.counts.timed_out,
                errors_by_code: wire_error_counts(&sample.errors_by_code),
                client_latency: variant_client,
                total_latency: variant_total,
                dispatch_lag: variant_dispatch,
            });
        }
        let result = PhaseResult {
            offered: self.overall.counts.offered,
            started: self.overall.counts.started,
            completed: self.overall.counts.completed,
            successful: self.overall.counts.successful,
            successful_in_window: self.overall.counts.successful_in_window,
            failed: self.overall.counts.failed,
            timed_out: self.overall.counts.timed_out,
            errors_by_code: wire_error_counts(&self.overall.errors_by_code),
            elapsed_ns,
            in_flight_high_water: self.in_flight_high_water,
            client_latency,
            total_latency,
            dispatch_lag,
            time_buckets: self.buckets.clone(),
            per_operation,
        };
        validate_total_encoded_size(&result, &self.limits)?;
        Ok(result)
    }

    fn update_batch_high_water(&mut self, results: &[OperationResult]) -> Result<(), StatsError> {
        let phase_high_water = high_water_in_interval(results, 0, self.plan.measurement_ns)?;
        self.in_flight_high_water = self.in_flight_high_water.max(phase_high_water);
        for bucket in &mut self.buckets {
            let high_water = high_water_in_interval(
                results,
                bucket.start_offset_ns,
                bucket.start_offset_ns.saturating_add(bucket.duration_ns),
            )?;
            bucket.in_flight_high_water = bucket.in_flight_high_water.max(high_water);
        }
        Ok(())
    }

    fn decode_contribution(&self, result: &PhaseResult) -> Result<DecodedContribution, StatsError> {
        validate_total_encoded_size(result, &self.limits)?;
        validate_elapsed(result.elapsed_ns, self.plan.measurement_ns)?;
        let overall = decode_sample(
            SampleCounts {
                offered: result.offered,
                started: result.started,
                completed: result.completed,
                successful: result.successful,
                successful_in_window: result.successful_in_window,
                failed: result.failed,
                timed_out: result.timed_out,
            },
            &result.errors_by_code,
            &result.client_latency,
            &result.total_latency,
            &result.dispatch_lag,
            "overall phase",
            &self.plan,
            &self.limits,
        )?;
        validate_buckets(result, &self.plan)?;

        if result.per_operation.len() != self.plan.variants.len() {
            return Err(StatsError::InvalidPhase(format!(
                "phase contains {} per-variant results; {} were planned",
                result.per_operation.len(),
                self.plan.variants.len()
            )));
        }
        let mut variants = BTreeMap::new();
        for operation in &result.per_operation {
            let variant = OperationVariant {
                operation: operation.operation.clone(),
                arguments: operation.arguments.clone(),
            };
            if !self.variants.contains_key(&variant) {
                return Err(StatsError::UnknownVariant(variant));
            }
            let sample = decode_sample(
                SampleCounts {
                    offered: operation.offered,
                    started: operation.started,
                    completed: operation.completed,
                    successful: operation.successful,
                    successful_in_window: operation.successful_in_window,
                    failed: operation.failed,
                    timed_out: operation.timed_out,
                },
                &operation.errors_by_code,
                &operation.client_latency,
                &operation.total_latency,
                &operation.dispatch_lag,
                "operation variant",
                &self.plan,
                &self.limits,
            )?;
            if variants.insert(variant.clone(), sample).is_some() {
                return Err(StatsError::DuplicateVariant(variant));
            }
        }

        let mut derived = SampleAccumulator::new(self.plan.histogram)?;
        for variant in &self.plan.variants {
            let sample = variants
                .get(variant)
                .ok_or_else(|| StatsError::MissingVariant(variant.clone()))?;
            derived.checked_merge(sample, &self.limits)?;
        }
        if derived.counts != overall.counts
            || derived.errors_by_code != overall.errors_by_code
            || !derived.client_latency.equivalent(&overall.client_latency)
            || !derived.total_latency.equivalent(&overall.total_latency)
            || !derived.dispatch_lag.equivalent(&overall.dispatch_lag)
        {
            return Err(StatsError::InvalidPhase(
                "overall counts, errors, or histograms do not equal the per-variant aggregate"
                    .into(),
            ));
        }
        if result.in_flight_high_water
            < result
                .time_buckets
                .iter()
                .map(|bucket| bucket.in_flight_high_water)
                .max()
                .unwrap_or(0)
        {
            return Err(StatsError::InvalidPhase(
                "phase in-flight high-water mark is below a bucket high-water mark".into(),
            ));
        }
        Ok(DecodedContribution {
            overall,
            variants,
            buckets: result.time_buckets.clone(),
            in_flight_high_water: result.in_flight_high_water,
            elapsed_ns: result.elapsed_ns,
        })
    }

    fn apply_contribution(&mut self, contribution: &DecodedContribution) -> Result<(), StatsError> {
        if let Some(elapsed_ns) = self.managed_elapsed_ns
            && elapsed_ns != contribution.elapsed_ns
        {
            return Err(StatsError::InvalidPhase(format!(
                "managed agent elapsed time {} ns differs from the cohort elapsed time {elapsed_ns} ns",
                contribution.elapsed_ns
            )));
        }
        self.overall
            .checked_merge(&contribution.overall, &self.limits)?;
        for (variant, sample) in &contribution.variants {
            self.variants
                .get_mut(variant)
                .expect("managed variants were validated against the plan")
                .checked_merge(sample, &self.limits)?;
        }
        for (target, source) in self.buckets.iter_mut().zip(&contribution.buckets) {
            target.offered = checked_add(target.offered, source.offered, "bucket offered count")?;
            target.started = checked_add(target.started, source.started, "bucket started count")?;
            target.completed =
                checked_add(target.completed, source.completed, "bucket completed count")?;
            target.successful = checked_add(
                target.successful,
                source.successful,
                "bucket successful count",
            )?;
            target.failed = checked_add(target.failed, source.failed, "bucket failed count")?;
            target.timed_out =
                checked_add(target.timed_out, source.timed_out, "bucket timed-out count")?;
            target.in_flight_high_water = checked_add(
                target.in_flight_high_water,
                source.in_flight_high_water,
                "bucket in-flight high-water mark",
            )?;
        }
        self.in_flight_high_water = checked_add(
            self.in_flight_high_water,
            contribution.in_flight_high_water,
            "phase in-flight high-water mark",
        )?;
        self.managed_elapsed_ns = Some(contribution.elapsed_ns);
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct DecodedContribution {
    overall: SampleAccumulator,
    variants: BTreeMap<OperationVariant, SampleAccumulator>,
    buckets: Vec<TimeBucket>,
    in_flight_high_water: u64,
    elapsed_ns: u64,
}

#[allow(clippy::too_many_arguments)]
fn decode_sample(
    counts: SampleCounts,
    errors: &[PhaseErrorCount],
    client_latency: &EncodedHistogram,
    total_latency: &EncodedHistogram,
    dispatch_lag: &EncodedHistogram,
    context: &str,
    plan: &PhaseAggregationPlan,
    limits: &AggregationLimits,
) -> Result<SampleAccumulator, StatsError> {
    counts.validate(context)?;
    let errors_by_code = validate_wire_errors(errors, counts.failed, limits, context)?;
    let client_latency =
        LatencyHistogram::decode(client_latency, plan.histogram, limits.histogram_decode)
            .map_err(StatsError::histogram)?;
    let total_latency =
        LatencyHistogram::decode(total_latency, plan.histogram, limits.histogram_decode)
            .map_err(StatsError::histogram)?;
    let dispatch_lag =
        LatencyHistogram::decode(dispatch_lag, plan.histogram, limits.histogram_decode)
            .map_err(StatsError::histogram)?;
    for (name, samples) in [
        ("client-latency", client_latency.len()),
        ("total-latency", total_latency.len()),
        ("dispatch-lag", dispatch_lag.len()),
    ] {
        if samples != counts.completed {
            return Err(StatsError::InvalidPhase(format!(
                "{context} {name} histogram has {samples} samples, not completed count {}",
                counts.completed
            )));
        }
    }
    Ok(SampleAccumulator {
        counts,
        errors_by_code,
        client_latency,
        total_latency,
        dispatch_lag,
    })
}

fn validate_plan(
    plan: &PhaseAggregationPlan,
    limits: &AggregationLimits,
) -> Result<(), StatsError> {
    if plan.measurement_ns == 0 {
        return Err(StatsError::InvalidPlan(
            "measurement duration must be greater than zero".into(),
        ));
    }
    let bucket_count = usize::from(plan.bucket_count);
    if bucket_count == 0
        || bucket_count > limits.maximum_buckets
        || u64::from(plan.bucket_count) > plan.measurement_ns
    {
        return Err(StatsError::InvalidPlan(format!(
            "bucket count must be in 1..={} and no greater than the measurement duration in nanoseconds",
            limits.maximum_buckets
        )));
    }
    if plan.variants.is_empty() {
        return Err(StatsError::InvalidPlan(
            "at least one operation variant is required".into(),
        ));
    }
    if plan.variants.len() > limits.maximum_variants {
        return Err(StatsError::TooManyVariants {
            maximum: limits.maximum_variants,
        });
    }
    let unique = plan.variants.iter().cloned().collect::<BTreeSet<_>>();
    if unique.len() != plan.variants.len() {
        return Err(StatsError::InvalidPlan(
            "operation variants must be unique".into(),
        ));
    }
    LatencyHistogram::new(plan.histogram).map_err(StatsError::histogram)?;
    Ok(())
}

fn planned_buckets(measurement_ns: u64, bucket_count: u16) -> Vec<TimeBucket> {
    let width = measurement_ns.div_ceil(u64::from(bucket_count));
    (0..bucket_count)
        .map(|index| {
            let start = u64::from(index) * width;
            TimeBucket {
                start_offset_ns: start,
                duration_ns: width.min(measurement_ns.saturating_sub(start)),
                offered: 0,
                started: 0,
                completed: 0,
                successful: 0,
                failed: 0,
                timed_out: 0,
                in_flight_high_water: 0,
            }
        })
        .collect()
}

fn bucket_index(offset_ns: u64, measurement_ns: u64, bucket_count: u16) -> Option<usize> {
    if offset_ns > measurement_ns {
        return None;
    }
    let width = measurement_ns.div_ceil(u64::from(bucket_count));
    let last = usize::from(bucket_count) - 1;
    Some(((offset_ns / width) as usize).min(last))
}

fn validate_elapsed(elapsed_ns: u64, measurement_ns: u64) -> Result<(), StatsError> {
    if elapsed_ns == 0 || elapsed_ns > measurement_ns {
        return Err(StatsError::InvalidPhase(format!(
            "elapsed time must be in 1..={measurement_ns} ns, got {elapsed_ns} ns"
        )));
    }
    Ok(())
}

fn validate_trailing_empty_buckets(
    buckets: &[TimeBucket],
    elapsed_ns: u64,
) -> Result<(), StatsError> {
    for (index, bucket) in buckets.iter().enumerate() {
        if bucket.start_offset_ns >= elapsed_ns
            && (bucket.offered != 0
                || bucket.started != 0
                || bucket.completed != 0
                || bucket.successful != 0
                || bucket.failed != 0
                || bucket.timed_out != 0
                || bucket.in_flight_high_water != 0)
        {
            return Err(StatsError::InvalidPhase(format!(
                "time bucket {index} begins after the partial phase ended but is not empty"
            )));
        }
    }
    Ok(())
}

fn validate_buckets(result: &PhaseResult, plan: &PhaseAggregationPlan) -> Result<(), StatsError> {
    let expected = planned_buckets(plan.measurement_ns, plan.bucket_count);
    if result.time_buckets.len() != expected.len() {
        return Err(StatsError::InvalidPhase(format!(
            "phase has {} time buckets; {} were planned",
            result.time_buckets.len(),
            expected.len()
        )));
    }
    let mut offered = 0_u64;
    let mut started = 0_u64;
    let mut completed = 0_u64;
    let mut successful = 0_u64;
    let mut failed = 0_u64;
    let mut timed_out = 0_u64;
    for (index, (actual, expected)) in result.time_buckets.iter().zip(expected).enumerate() {
        if actual.start_offset_ns != expected.start_offset_ns
            || actual.duration_ns != expected.duration_ns
        {
            return Err(StatsError::InvalidPhase(format!(
                "time bucket {index} does not match the coordinator-owned boundary"
            )));
        }
        let outcomes = checked_sum(
            [actual.successful, actual.failed, actual.timed_out],
            "bucket outcome counts",
        )?;
        if outcomes != actual.completed {
            return Err(StatsError::InvalidPhase(format!(
                "time bucket {index} terminal outcomes do not equal its completed count"
            )));
        }
        offered = checked_add(offered, actual.offered, "bucket offered total")?;
        started = checked_add(started, actual.started, "bucket started total")?;
        completed = checked_add(completed, actual.completed, "bucket completed total")?;
        successful = checked_add(successful, actual.successful, "bucket successful total")?;
        failed = checked_add(failed, actual.failed, "bucket failed total")?;
        timed_out = checked_add(timed_out, actual.timed_out, "bucket timed-out total")?;
    }
    validate_trailing_empty_buckets(&result.time_buckets, result.elapsed_ns)?;
    if offered != result.offered
        || started > result.started
        || successful != result.successful_in_window
        || completed != checked_sum([successful, failed, timed_out], "bucket outcomes")?
        || completed > result.completed
        || failed > result.failed
        || timed_out > result.timed_out
    {
        return Err(StatsError::InvalidPhase(
            "time-bucket counts do not reconcile with phase counts".into(),
        ));
    }
    Ok(())
}

fn validate_wire_errors(
    errors: &[PhaseErrorCount],
    failed: u64,
    limits: &AggregationLimits,
    context: &str,
) -> Result<BTreeMap<Option<String>, u64>, StatsError> {
    if errors.len() > limits.maximum_error_codes {
        return Err(StatsError::TooManyErrorCodes {
            maximum: limits.maximum_error_codes,
        });
    }
    let mut result = BTreeMap::new();
    let mut total = 0_u64;
    for error in errors {
        validate_error_code(error.code.as_deref(), limits)?;
        if result.insert(error.code.clone(), error.count).is_some() {
            return Err(StatsError::InvalidPhase(format!(
                "{context} contains duplicate error codes"
            )));
        }
        total = checked_add(total, error.count, "error-code total")?;
    }
    if total != failed {
        return Err(StatsError::InvalidPhase(format!(
            "{context} error-code counts sum to {total}, not failed count {failed}"
        )));
    }
    Ok(result)
}

fn validate_error_code(code: Option<&str>, limits: &AggregationLimits) -> Result<(), StatsError> {
    if let Some(code) = code {
        if code.is_empty() {
            return Err(StatsError::InvalidPhase(
                "error codes cannot be empty strings".into(),
            ));
        }
        if code.len() > limits.maximum_error_code_bytes {
            return Err(StatsError::ErrorCodeTooLong {
                actual: code.len(),
                maximum: limits.maximum_error_code_bytes,
            });
        }
    }
    Ok(())
}

fn validate_total_encoded_size(
    result: &PhaseResult,
    limits: &AggregationLimits,
) -> Result<(), StatsError> {
    let mut histograms = std::iter::once(&result.client_latency)
        .chain(std::iter::once(&result.total_latency))
        .chain(std::iter::once(&result.dispatch_lag))
        .chain(result.per_operation.iter().flat_map(|operation| {
            [
                &operation.client_latency,
                &operation.total_latency,
                &operation.dispatch_lag,
            ]
        }));
    let total = histograms.try_fold(0_usize, |total, histogram| {
        total
            .checked_add(histogram.data.len())
            .ok_or(StatsError::ResourceLimit(
                "encoded histogram byte count overflow",
            ))
    })?;
    if total > limits.maximum_total_encoded_histogram_bytes {
        return Err(StatsError::ResourceLimit(
            "encoded histograms exceed the configured aggregate byte limit",
        ));
    }
    Ok(())
}

fn wire_error_counts(errors: &BTreeMap<Option<String>, u64>) -> Vec<PhaseErrorCount> {
    errors
        .iter()
        .map(|(code, count)| PhaseErrorCount {
            code: code.clone(),
            count: *count,
        })
        .collect()
}

fn distribution(histogram: &LatencyHistogram) -> Result<DistributionStats, StatsError> {
    Ok(DistributionStats {
        samples: histogram.len(),
        min: histogram.min(),
        p50: histogram
            .value_at_quantile(0.50)
            .map_err(StatsError::histogram)?,
        p95: histogram
            .value_at_quantile(0.95)
            .map_err(StatsError::histogram)?,
        p99: histogram
            .value_at_quantile(0.99)
            .map_err(StatsError::histogram)?,
        max: histogram.max(),
    })
}

fn high_water_in_interval(
    results: &[OperationResult],
    interval_start: u64,
    interval_end: u64,
) -> Result<u64, StatsError> {
    let mut events = Vec::with_capacity(results.len().saturating_mul(2));
    for result in results {
        let completion = result
            .actual_start_offset_ns
            .checked_add(result.client_latency_ns)
            .ok_or(StatsError::CounterOverflow("operation completion offset"))?;
        if !intervals_overlap(
            result.actual_start_offset_ns,
            completion,
            interval_start,
            interval_end,
        ) {
            continue;
        }
        events.push((result.actual_start_offset_ns.max(interval_start), 1_i8));
        events.push((completion.min(interval_end), -1_i8));
    }
    events.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    let mut current = 0_u64;
    let mut maximum = 0_u64;
    for (_, delta) in events {
        if delta > 0 {
            current = current
                .checked_add(1)
                .ok_or(StatsError::CounterOverflow("in-flight count"))?;
            maximum = maximum.max(current);
        } else {
            current = current.saturating_sub(1);
        }
    }
    Ok(maximum)
}

fn intervals_overlap(left_start: u64, left_end: u64, right_start: u64, right_end: u64) -> bool {
    left_start < right_end && right_start < left_end
}

fn checked_add(left: u64, right: u64, name: &'static str) -> Result<u64, StatsError> {
    left.checked_add(right)
        .ok_or(StatsError::CounterOverflow(name))
}

fn checked_sum(
    values: impl IntoIterator<Item = u64>,
    name: &'static str,
) -> Result<u64, StatsError> {
    values
        .into_iter()
        .try_fold(0_u64, |total, value| checked_add(total, value, name))
}

pub fn summarize_results(results: &[OperationResult]) -> Result<StatsReport, StatsError> {
    summarize_results_with_limit(results, DEFAULT_MAX_VARIANTS)
}

pub fn summarize_results_with_limit(
    results: &[OperationResult],
    maximum_variants: usize,
) -> Result<StatsReport, StatsError> {
    let mut grouped = BTreeMap::<OperationVariant, Vec<&OperationResult>>::new();
    for result in results {
        grouped
            .entry(OperationVariant {
                operation: result.operation.clone(),
                arguments: result.arguments.clone(),
            })
            .or_default()
            .push(result);
        if grouped.len() > maximum_variants {
            return Err(StatsError::TooManyVariants {
                maximum: maximum_variants,
            });
        }
    }

    Ok(StatsReport {
        overall: summarize_samples(results.iter()),
        variants: grouped
            .into_iter()
            .map(|(variant, samples)| VariantStats {
                variant,
                stats: summarize_samples(samples),
            })
            .collect(),
    })
}

fn summarize_samples<'a>(samples: impl IntoIterator<Item = &'a OperationResult>) -> SampleStats {
    let samples: Vec<_> = samples.into_iter().collect();
    let mut errors_by_code = BTreeMap::<Option<String>, u64>::new();
    let successful = samples
        .iter()
        .filter(|sample| matches!(sample.status, OperationStatus::Ok))
        .count() as u64;
    let failed = samples
        .iter()
        .filter(|sample| matches!(sample.status, OperationStatus::Error { .. }))
        .count() as u64;
    let timed_out = samples
        .iter()
        .filter(|sample| matches!(sample.status, OperationStatus::Timeout))
        .count() as u64;
    for sample in &samples {
        if let OperationStatus::Error { code } = &sample.status {
            *errors_by_code.entry(code.clone()).or_default() += 1;
        }
    }

    SampleStats {
        attempts: samples.len() as u64,
        successful,
        failed,
        timed_out,
        errors_by_code: errors_by_code
            .into_iter()
            .map(|(code, count)| ErrorCount { code, count })
            .collect(),
        client_latency_ns: summarize_distribution(
            samples.iter().map(|sample| sample.client_latency_ns),
        ),
        total_latency_ns: summarize_distribution(
            samples.iter().map(|sample| sample.total_latency_ns()),
        ),
        dispatch_lag_ns: summarize_distribution(
            samples.iter().map(|sample| sample.dispatch_lag_ns()),
        ),
    }
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn summarize_distribution(values: impl IntoIterator<Item = u64>) -> DistributionStats {
    let mut values: Vec<_> = values.into_iter().collect();
    values.sort_unstable();
    DistributionStats {
        samples: values.len() as u64,
        min: values.first().copied(),
        p50: percentile(&values, 0.50),
        p95: percentile(&values, 0.95),
        p99: percentile(&values, 0.99),
        max: values.last().copied(),
    }
}

fn percentile(sorted_values: &[u64], quantile: f64) -> Option<u64> {
    if sorted_values.is_empty() {
        return None;
    }
    let index = ((sorted_values.len() - 1) as f64 * quantile).round() as usize;
    sorted_values.get(index).copied()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatsError {
    TooManyVariants { maximum: usize },
    TooManyErrorCodes { maximum: usize },
    ErrorCodeTooLong { actual: usize, maximum: usize },
    InvalidPlan(String),
    InvalidPhase(String),
    UnknownVariant(OperationVariant),
    MissingVariant(OperationVariant),
    DuplicateVariant(OperationVariant),
    CounterOverflow(&'static str),
    Histogram(String),
    ResourceLimit(&'static str),
    MixedAggregationModes,
}

impl StatsError {
    fn histogram(error: HistogramError) -> Self {
        Self::Histogram(error.to_string())
    }
}

impl fmt::Display for StatsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyVariants { maximum } => write!(
                formatter,
                "operation result cardinality exceeds the configured limit of {maximum} variants"
            ),
            Self::TooManyErrorCodes { maximum } => write!(
                formatter,
                "phase error-code cardinality exceeds the configured limit of {maximum} codes"
            ),
            Self::ErrorCodeTooLong { actual, maximum } => write!(
                formatter,
                "error code is {actual} bytes, exceeding the configured limit of {maximum} bytes"
            ),
            Self::InvalidPlan(message) => write!(formatter, "invalid aggregation plan: {message}"),
            Self::InvalidPhase(message) => write!(formatter, "invalid phase result: {message}"),
            Self::UnknownVariant(variant) => {
                write!(
                    formatter,
                    "phase contains unknown operation variant {variant:?}"
                )
            }
            Self::MissingVariant(variant) => {
                write!(formatter, "phase omits operation variant {variant:?}")
            }
            Self::DuplicateVariant(variant) => {
                write!(formatter, "phase repeats operation variant {variant:?}")
            }
            Self::CounterOverflow(name) => write!(formatter, "{name} overflow"),
            Self::Histogram(message) => write!(formatter, "invalid histogram: {message}"),
            Self::ResourceLimit(message) => formatter.write_str(message),
            Self::MixedAggregationModes => formatter
                .write_str("local observations and wire summaries cannot be mixed in one phase"),
        }
    }
}

impl std::error::Error for StatsError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{histogram::HistogramEncoding, protocol::OperationId};

    fn result(
        id: u64,
        operation: &str,
        arguments: BTreeMap<String, ArgumentValue>,
        latency_ns: u64,
    ) -> OperationResult {
        OperationResult {
            id: OperationId(id),
            operation: operation.into(),
            arguments,
            intended_start_offset_ns: id * 100,
            actual_start_offset_ns: id * 100 + 10,
            client_latency_ns: latency_ns,
            status: OperationStatus::Ok,
        }
    }

    fn result_with_status(id: u64, status: OperationStatus) -> OperationResult {
        let mut result = result(id, "read", BTreeMap::new(), 10);
        result.status = status;
        result
    }

    fn aggregation_plan(measurement_ns: u64) -> PhaseAggregationPlan {
        PhaseAggregationPlan {
            measurement_ns,
            bucket_count: 2,
            histogram: HistogramSpec {
                lowest_discernible_ns: 1,
                highest_trackable_ns: 10_000,
                significant_figures: 3,
            },
            variants: vec![OperationVariant {
                operation: "read".into(),
                arguments: BTreeMap::new(),
            }],
        }
    }

    fn observed_result(id: u64, intended: u64, actual: u64, latency: u64) -> OperationResult {
        OperationResult {
            id: OperationId(id),
            operation: "read".into(),
            arguments: BTreeMap::new(),
            intended_start_offset_ns: intended,
            actual_start_offset_ns: actual,
            client_latency_ns: latency,
            status: OperationStatus::Ok,
        }
    }

    #[test]
    fn reports_each_bound_argument_variant_separately() {
        let key_zero = BTreeMap::from([("key".into(), ArgumentValue::Integer(0))]);
        let key_one = BTreeMap::from([("key".into(), ArgumentValue::Integer(1))]);
        let results = vec![
            result(1, "read", key_zero.clone(), 10),
            result(2, "read", key_zero, 20),
            result(3, "read", key_one, 100),
        ];

        let report = summarize_results(&results).unwrap();

        assert_eq!(report.overall.attempts, 3);
        assert_eq!(report.variants.len(), 2);
        assert_eq!(report.variants[0].stats.attempts, 2);
        assert_eq!(report.variants[0].stats.client_latency_ns.p95, Some(20));
        assert_eq!(report.variants[1].stats.client_latency_ns.p95, Some(100));
    }

    #[test]
    fn cardinality_limit_prevents_unbounded_series() {
        let results = vec![
            result(
                1,
                "read",
                BTreeMap::from([("key".into(), ArgumentValue::Integer(0))]),
                10,
            ),
            result(
                2,
                "read",
                BTreeMap::from([("key".into(), ArgumentValue::Integer(1))]),
                10,
            ),
        ];

        assert_eq!(
            summarize_results_with_limit(&results, 1),
            Err(StatsError::TooManyVariants { maximum: 1 })
        );
    }

    #[test]
    fn reports_error_rates_timeouts_and_codes() {
        let results = vec![
            result_with_status(1, OperationStatus::Ok),
            result_with_status(
                2,
                OperationStatus::Error {
                    code: Some("overloaded".into()),
                },
            ),
            result_with_status(
                3,
                OperationStatus::Error {
                    code: Some("overloaded".into()),
                },
            ),
            result_with_status(4, OperationStatus::Error { code: None }),
            result_with_status(5, OperationStatus::Timeout),
        ];

        let stats = summarize_results(&results).unwrap().overall;
        assert_eq!(stats.failed, 3);
        assert_eq!(stats.timed_out, 1);
        assert_eq!(stats.error_rate(), 0.6);
        assert_eq!(stats.timeout_rate(), 0.2);
        assert_eq!(stats.unsuccessful_rate(), 0.8);
        assert_eq!(
            stats.errors_by_code,
            vec![
                ErrorCount {
                    code: None,
                    count: 1,
                },
                ErrorCount {
                    code: Some("overloaded".into()),
                    count: 2,
                },
            ]
        );
    }

    #[test]
    fn local_observations_and_wire_aggregates_have_report_parity() {
        let plan = aggregation_plan(1_000);
        let results = vec![
            observed_result(1, 100, 110, 20),
            observed_result(2, 600, 620, 30),
        ];
        let mut local = PhaseAccumulator::new(plan.clone()).unwrap();
        local.record_batch(&results).unwrap();
        let phase_result = local.to_phase_result().unwrap();
        let expected = local.finish(2_000.0).unwrap();

        let mut managed = PhaseAccumulator::new(plan).unwrap();
        managed.merge_phase_result(&phase_result).unwrap();
        let actual = managed.finish(2_000.0).unwrap();

        assert_eq!(actual.offered_count, expected.offered_count);
        assert_eq!(actual.successful_in_window, expected.successful_in_window);
        assert_eq!(actual.stats, expected.stats);
        assert_eq!(actual.quality.buckets, expected.quality.buckets);
    }

    #[test]
    fn late_success_is_retained_but_excluded_from_goodput() {
        let plan = aggregation_plan(100);
        let mut accumulator = PhaseAccumulator::new(plan).unwrap();
        accumulator.record(&observed_result(1, 80, 90, 20)).unwrap();

        let report = accumulator.finish(10_000.0).unwrap();

        assert_eq!(report.stats.overall.successful, 1);
        assert_eq!(report.stats.overall.client_latency_ns.samples, 1);
        assert_eq!(report.successful_in_window, 0);
        assert_eq!(report.goodput_rate, 0.0);
    }

    #[test]
    fn malformed_managed_histogram_is_rejected_without_mutation() {
        let plan = aggregation_plan(1_000);
        let mut source = PhaseAccumulator::new(plan.clone()).unwrap();
        source.record(&observed_result(1, 100, 110, 20)).unwrap();
        let valid = source.to_phase_result().unwrap();
        let mut malformed = valid.clone();
        malformed.client_latency = EncodedHistogram {
            encoding: HistogramEncoding::HdrV2Base64,
            data: "%%%".into(),
        };
        let mut managed = PhaseAccumulator::new(plan).unwrap();

        assert!(matches!(
            managed.merge_phase_result(&malformed),
            Err(StatsError::Histogram(_))
        ));
        managed.merge_phase_result(&valid).unwrap();
        assert_eq!(managed.finish(1_000.0).unwrap().completed_count, 1);
    }

    #[test]
    fn managed_agent_summaries_merge_by_variant_and_bucket() {
        let plan = aggregation_plan(1_000);
        let mut first = PhaseAccumulator::new(plan.clone()).unwrap();
        first.record(&observed_result(1, 100, 110, 20)).unwrap();
        let mut second = PhaseAccumulator::new(plan.clone()).unwrap();
        second.record(&observed_result(2, 600, 620, 30)).unwrap();

        let mut cohort = PhaseAccumulator::new(plan).unwrap();
        cohort.merge(&first.to_phase_result().unwrap()).unwrap();
        cohort.merge(&second.to_phase_result().unwrap()).unwrap();
        let report = cohort.finish(2_000.0).unwrap();

        assert_eq!(report.offered_count, 2);
        assert_eq!(report.started_count, 2);
        assert_eq!(report.completed_count, 2);
        assert_eq!(report.stats.variants[0].stats.attempts, 2);
        assert_eq!(report.quality.buckets[0].attempts, 1);
        assert_eq!(report.quality.buckets[1].attempts, 1);
    }

    #[test]
    fn local_partial_finish_uses_actual_elapsed_and_trims_empty_buckets() {
        let plan = aggregation_plan(1_000);
        let mut accumulator = PhaseAccumulator::new(plan).unwrap();
        accumulator
            .record(&observed_result(1, 100, 110, 20))
            .unwrap();

        let report = accumulator.finish_at(1_000.0, 400).unwrap();

        assert_eq!(report.elapsed_ns, 400);
        assert_eq!(report.goodput_rate, 2_500_000.0);
        assert_eq!(report.quality.buckets.len(), 1);
        assert_eq!(report.quality.buckets[0].duration_ns, 400);
    }

    #[test]
    fn partial_managed_agents_must_report_the_same_elapsed_time() {
        let plan = aggregation_plan(1_000);
        let mut source = PhaseAccumulator::new(plan.clone()).unwrap();
        source.record(&observed_result(1, 100, 110, 20)).unwrap();
        let elapsed_400 = source.to_phase_result_at(400).unwrap();
        let elapsed_300 = source.to_phase_result_at(300).unwrap();
        let mut cohort = PhaseAccumulator::new(plan).unwrap();
        cohort.merge(&elapsed_400).unwrap();

        assert!(matches!(
            cohort.merge(&elapsed_300),
            Err(StatsError::InvalidPhase(_))
        ));
        let report = cohort.finish_at(1_000.0, 400).unwrap();
        assert_eq!(report.completed_count, 1);
        assert_eq!(report.elapsed_ns, 400);
    }

    #[test]
    fn interrupted_managed_phase_may_have_started_work_still_in_flight() {
        let plan = aggregation_plan(1_000);
        let mut source = PhaseAccumulator::new(plan.clone()).unwrap();
        source.record(&observed_result(1, 100, 110, 20)).unwrap();
        let mut partial = source.to_phase_result_at(400).unwrap();
        partial.offered = 2;
        partial.started = 2;
        partial.time_buckets[0].offered = 2;
        partial.time_buckets[0].started = 2;
        partial.per_operation[0].offered = 2;
        partial.per_operation[0].started = 2;

        let mut managed = PhaseAccumulator::new(plan).unwrap();
        managed.merge(&partial).unwrap();
        let report = managed.finish_at(1_000.0, 400).unwrap();

        assert_eq!(report.offered_count, 2);
        assert_eq!(report.started_count, 2);
        assert_eq!(report.completed_count, 1);
    }

    #[test]
    fn partial_elapsed_time_is_bounded_by_the_plan() {
        let plan = aggregation_plan(1_000);
        assert!(matches!(
            PhaseAccumulator::new(plan.clone())
                .unwrap()
                .finish_at(1_000.0, 0),
            Err(StatsError::InvalidPhase(_))
        ));
        assert!(matches!(
            PhaseAccumulator::new(plan)
                .unwrap()
                .finish_at(1_000.0, 1_001),
            Err(StatsError::InvalidPhase(_))
        ));
    }

    #[test]
    fn late_start_need_not_appear_in_a_measurement_bucket() {
        let plan = aggregation_plan(1_000);
        let late = observed_result(1, 100, 1_100, 20);
        let mut source = PhaseAccumulator::new(plan.clone()).unwrap();
        source.record(&late).unwrap();
        let phase_result = source.to_phase_result().unwrap();
        assert_eq!(phase_result.started, 1);
        assert_eq!(
            phase_result
                .time_buckets
                .iter()
                .map(|bucket| bucket.started)
                .sum::<u64>(),
            0
        );

        let mut managed = PhaseAccumulator::new(plan).unwrap();
        managed.merge(&phase_result).unwrap();
        let report = managed.finish(1_000.0).unwrap();
        assert_eq!(report.started_count, 1);
        assert_eq!(report.completed_count, 1);
        assert_eq!(report.successful_in_window, 0);
    }

    #[test]
    fn unstarted_offer_has_no_terminal_or_latency_sample() {
        let plan = aggregation_plan(1_000);
        let variant = plan.variants[0].clone();
        let mut source = PhaseAccumulator::new(plan.clone()).unwrap();
        source.record(&observed_result(1, 100, 110, 20)).unwrap();
        source.record_unstarted_offer(&variant, 600).unwrap();
        let phase_result = source.to_phase_result().unwrap();

        assert_eq!(phase_result.offered, 2);
        assert_eq!(phase_result.started, 1);
        assert_eq!(phase_result.completed, 1);
        assert_eq!(phase_result.time_buckets[0].offered, 1);
        assert_eq!(phase_result.time_buckets[1].offered, 1);
        assert_eq!(phase_result.per_operation[0].offered, 2);
        assert_eq!(phase_result.per_operation[0].completed, 1);

        let mut managed = PhaseAccumulator::new(plan).unwrap();
        managed.merge(&phase_result).unwrap();
        let report = managed.finish(2_000.0).unwrap();
        assert_eq!(report.offered_count, 2);
        assert_eq!(report.started_count, 1);
        assert_eq!(report.completed_count, 1);
        assert_eq!(report.stats.overall.attempts, 1);
        assert_eq!(report.stats.overall.client_latency_ns.samples, 1);
        assert_eq!(report.quality.buckets[1].attempts, 1);
    }

    #[test]
    fn unstarted_offer_validates_variant_and_intended_range() {
        let plan = aggregation_plan(1_000);
        let variant = plan.variants[0].clone();
        let unknown = OperationVariant {
            operation: "unknown".into(),
            arguments: BTreeMap::new(),
        };
        let mut accumulator = PhaseAccumulator::new(plan).unwrap();

        assert!(matches!(
            accumulator.record_unstarted_offer(&unknown, 100),
            Err(StatsError::UnknownVariant(_))
        ));
        assert!(matches!(
            accumulator.record_unstarted_offer(&variant, 1_000),
            Err(StatsError::InvalidPhase(_))
        ));
        assert_eq!(accumulator.finish(1_000.0).unwrap().offered_count, 0);
    }
}

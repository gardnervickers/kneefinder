//! Reproducible knee fitting, uncertainty estimation, and candidate validation.

use serde::{Deserialize, Serialize};

use crate::{
    config::AnalysisConfig,
    measurement::{KneeEstimate, RunClassification, RunOutcome},
    stats::PhaseReport,
};

const MINIMUM_POINTS: usize = 5;
const MINIMUM_UNIQUE_RATES: usize = 4;
const MINIMUM_MODEL_IMPROVEMENT: f64 = 0.20;
const MINIMUM_SLOPE_REDUCTION: f64 = 0.50;
const LATENCY_VALIDATION_MULTIPLIER: f64 = 1.50;
const IN_FLIGHT_VALIDATION_MULTIPLIER: f64 = 2.0;
const RELIABILITY_VALIDATION_INCREASE: f64 = 0.005;
const MINIMUM_DISPATCH_LAG_NS: u64 = 10_000_000;
const DISPATCH_LAG_FRACTION: f64 = 0.10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnalysisTermination {
    /// A complete fixed traversal supplied the analysis range.
    CompletedPlan,
    /// Adaptive traversal reached its configured maximum without a bracket.
    MaximumLoadReached,
    /// Adaptive traversal formed and refined a healthy/saturated bracket.
    BracketRefined,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KneeAnalysis {
    pub method: String,
    pub observations: usize,
    pub unique_rates: usize,
    pub sparse_phases: usize,
    pub failed_attempts: u64,
    pub timed_out_attempts: u64,
    pub null_model: LinearFit,
    pub segmented_model: SegmentedFit,
    pub model_improvement: f64,
    pub required_model_improvement: f64,
    pub slope_reduction: f64,
    pub required_slope_reduction: f64,
    pub bootstrap: BootstrapSummary,
    pub validation: CandidateValidation,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LinearFit {
    pub intercept: f64,
    pub slope: f64,
    pub sum_squared_error: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentedFit {
    pub intercept: f64,
    pub breakpoint: f64,
    pub pre_knee_slope: f64,
    pub post_knee_slope: f64,
    pub sum_squared_error: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BootstrapSummary {
    pub seed: u64,
    pub requested_samples: u32,
    pub valid_samples: u32,
    pub confidence_level: f64,
    pub lower_bound: Option<f64>,
    pub upper_bound: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CandidateValidation {
    pub client_latency_ratio: Option<f64>,
    pub total_latency_ratio: Option<f64>,
    pub error_rate_increase: f64,
    pub timeout_rate_increase: f64,
    pub unsuccessful_rate_increase: f64,
    pub in_flight_ratio: Option<f64>,
    pub dispatch_lag_valid: bool,
    pub target_signal_present: bool,
}

pub fn analyze(
    reports: &[PhaseReport],
    config: &AnalysisConfig,
    termination: AnalysisTermination,
) -> RunOutcome {
    let mut warnings = Vec::new();
    if let Err(message) = config.validate() {
        return outcome(
            RunClassification::UnstableMeasurement,
            None,
            None,
            None,
            vec![message],
        );
    }
    if reports.iter().any(|report| !report.quality.stationary) {
        return outcome(
            RunClassification::UnstableMeasurement,
            None,
            slo_maximum(reports, config),
            None,
            vec!["at least one phase failed its stationarity check".into()],
        );
    }
    if reports.iter().any(dispatch_lag_invalid) {
        return outcome(
            RunClassification::GeneratorSaturated,
            None,
            slo_maximum(reports, config),
            None,
            vec!["dispatch lag invalidated target-knee analysis; the generator saturated".into()],
        );
    }

    let points = reports
        .iter()
        .filter(|report| {
            report.offered_rate.is_finite()
                && report.offered_rate > 0.0
                && report.goodput_rate.is_finite()
                && report.stats.overall.attempts > 0
        })
        .map(|report| Point {
            x: report.offered_rate,
            y: report.goodput_rate,
        })
        .collect::<Vec<_>>();
    let unique_rates = unique_rate_count(&points);
    let sparse_phases = reports.len().saturating_sub(points.len());
    if sparse_phases > 0 {
        warnings.push(format!(
            "{sparse_phases} sparse phase(s) had no attempts or non-finite rates and could not enter the regression"
        ));
    }
    let slo_maximum_rate = slo_maximum(reports, config);
    if points.len() < MINIMUM_POINTS || unique_rates < MINIMUM_UNIQUE_RATES {
        warnings.push(format!(
            "knee fitting requires at least {MINIMUM_POINTS} observations across {MINIMUM_UNIQUE_RATES} distinct rates"
        ));
        return outcome(
            sparse_classification(termination),
            None,
            slo_maximum_rate,
            None,
            warnings,
        );
    }

    let Some(null_model) = fit_line(&points) else {
        return outcome(
            RunClassification::UnstableMeasurement,
            None,
            slo_maximum_rate,
            None,
            vec!["the single-line model was numerically singular".into()],
        );
    };
    let Some(segmented_model) = best_segmented_fit(&points) else {
        return outcome(
            no_knee_classification(termination),
            None,
            slo_maximum_rate,
            None,
            vec!["no numerically valid internal segmented breakpoint was available".into()],
        );
    };
    let model_improvement = improvement(
        null_model.sum_squared_error,
        segmented_model.sum_squared_error,
    );
    let slope_reduction = if segmented_model.pre_knee_slope > 0.0 {
        (segmented_model.pre_knee_slope - segmented_model.post_knee_slope)
            / segmented_model.pre_knee_slope
    } else {
        f64::NEG_INFINITY
    };
    let bootstrap = bootstrap(reports, config);
    let validation = validate_candidate(reports, segmented_model.breakpoint);
    let analysis = KneeAnalysis {
        method: "continuous_hinge_ols_with_time_bucket_bootstrap".into(),
        observations: points.len(),
        unique_rates,
        sparse_phases,
        failed_attempts: reports
            .iter()
            .map(|report| report.stats.overall.failed)
            .sum(),
        timed_out_attempts: reports
            .iter()
            .map(|report| report.stats.overall.timed_out)
            .sum(),
        null_model,
        segmented_model: segmented_model.clone(),
        model_improvement,
        required_model_improvement: MINIMUM_MODEL_IMPROVEMENT,
        slope_reduction,
        required_slope_reduction: MINIMUM_SLOPE_REDUCTION,
        bootstrap: bootstrap.clone(),
        validation: validation.clone(),
    };

    if model_improvement < MINIMUM_MODEL_IMPROVEMENT {
        warnings.push(format!(
            "segmented model improvement {:.1}% was below the required {:.1}%",
            model_improvement * 100.0,
            MINIMUM_MODEL_IMPROVEMENT * 100.0
        ));
    }
    if slope_reduction < MINIMUM_SLOPE_REDUCTION
        || segmented_model.post_knee_slope >= segmented_model.pre_knee_slope
    {
        warnings.push(format!(
            "post-knee slope reduction {:.1}% was below the required {:.1}%",
            slope_reduction.max(0.0) * 100.0,
            MINIMUM_SLOPE_REDUCTION * 100.0
        ));
    }
    if !validation.target_signal_present {
        warnings.push(
            "throughput breakpoint was not corroborated by latency, reliability, or in-flight growth"
                .into(),
        );
    }
    if bootstrap.valid_samples < config.bootstrap_samples / 2 {
        warnings.push("fewer than half of bootstrap samples produced a valid breakpoint".into());
    }

    let statistically_supported = model_improvement >= MINIMUM_MODEL_IMPROVEMENT
        && slope_reduction >= MINIMUM_SLOPE_REDUCTION
        && segmented_model.post_knee_slope < segmented_model.pre_knee_slope
        && validation.target_signal_present;
    if !statistically_supported {
        return outcome(
            no_knee_classification(termination),
            None,
            slo_maximum_rate,
            Some(Box::new(analysis)),
            warnings,
        );
    }

    let lower_bound = bootstrap
        .lower_bound
        .unwrap_or(segmented_model.breakpoint)
        .min(segmented_model.breakpoint);
    let upper_bound = bootstrap
        .upper_bound
        .unwrap_or(segmented_model.breakpoint)
        .max(segmented_model.breakpoint);
    let recommended_operating_rate = slo_maximum_rate
        .map(|maximum| maximum.min(lower_bound * config.safety_factor))
        .unwrap_or(lower_bound * config.safety_factor);
    let knee = KneeEstimate {
        offered_rate: segmented_model.breakpoint,
        lower_bound,
        upper_bound,
        recommended_operating_rate,
    };
    let classification = if slo_maximum_rate.is_some_and(|maximum| maximum < knee.offered_rate) {
        warnings.push("the configured SLO is exceeded before the estimated knee".into());
        RunClassification::SloExceeded
    } else {
        RunClassification::TargetSaturated
    };
    outcome(
        classification,
        Some(knee),
        slo_maximum_rate,
        Some(Box::new(analysis)),
        warnings,
    )
}

#[derive(Debug, Clone, Copy)]
struct Point {
    x: f64,
    y: f64,
}

fn fit_line(points: &[Point]) -> Option<LinearFit> {
    let count = points.len() as f64;
    let mean_x = points.iter().map(|point| point.x).sum::<f64>() / count;
    let mean_y = points.iter().map(|point| point.y).sum::<f64>() / count;
    let xx = points
        .iter()
        .map(|point| (point.x - mean_x).powi(2))
        .sum::<f64>();
    if xx <= f64::EPSILON {
        return None;
    }
    let slope = points
        .iter()
        .map(|point| (point.x - mean_x) * (point.y - mean_y))
        .sum::<f64>()
        / xx;
    let intercept = mean_y - slope * mean_x;
    Some(LinearFit {
        intercept,
        slope,
        sum_squared_error: points
            .iter()
            .map(|point| (point.y - (intercept + slope * point.x)).powi(2))
            .sum(),
    })
}

fn best_segmented_fit(points: &[Point]) -> Option<SegmentedFit> {
    let mut rates = points.iter().map(|point| point.x).collect::<Vec<_>>();
    rates.sort_by(f64::total_cmp);
    rates.dedup_by(|left, right| left.total_cmp(right).is_eq());
    rates
        .iter()
        .copied()
        .skip(1)
        .take(rates.len().saturating_sub(2))
        .filter_map(|breakpoint| fit_segmented_at(points, breakpoint))
        .min_by(|left, right| left.sum_squared_error.total_cmp(&right.sum_squared_error))
}

fn fit_segmented_at(points: &[Point], breakpoint: f64) -> Option<SegmentedFit> {
    let mut normal = [[0.0; 4]; 3];
    for point in points {
        let features = [1.0, point.x, (point.x - breakpoint).max(0.0)];
        for row in 0..3 {
            for column in 0..3 {
                normal[row][column] += features[row] * features[column];
            }
            normal[row][3] += features[row] * point.y;
        }
    }
    let coefficients = solve_three(normal)?;
    let intercept = coefficients[0];
    let pre_knee_slope = coefficients[1];
    let post_knee_slope = coefficients[1] + coefficients[2];
    let sum_squared_error = points
        .iter()
        .map(|point| {
            let prediction = intercept
                + pre_knee_slope * point.x
                + coefficients[2] * (point.x - breakpoint).max(0.0);
            (point.y - prediction).powi(2)
        })
        .sum();
    Some(SegmentedFit {
        intercept,
        breakpoint,
        pre_knee_slope,
        post_knee_slope,
        sum_squared_error,
    })
}

fn solve_three(mut matrix: [[f64; 4]; 3]) -> Option<[f64; 3]> {
    for pivot in 0..3 {
        let selected = (pivot..3).max_by(|left, right| {
            matrix[*left][pivot]
                .abs()
                .total_cmp(&matrix[*right][pivot].abs())
        })?;
        if matrix[selected][pivot].abs() <= 1e-12 {
            return None;
        }
        matrix.swap(pivot, selected);
        let divisor = matrix[pivot][pivot];
        for value in matrix[pivot].iter_mut().skip(pivot) {
            *value /= divisor;
        }
        for row in 0..3 {
            if row == pivot {
                continue;
            }
            let factor = matrix[row][pivot];
            let pivot_row = matrix[pivot];
            for (column, value) in matrix[row].iter_mut().enumerate().skip(pivot) {
                *value -= factor * pivot_row[column];
            }
        }
    }
    Some([matrix[0][3], matrix[1][3], matrix[2][3]])
}

fn improvement(null_sse: f64, segmented_sse: f64) -> f64 {
    if null_sse <= 1e-12 {
        0.0
    } else {
        (1.0 - segmented_sse / null_sse).clamp(0.0, 1.0)
    }
}

fn bootstrap(reports: &[PhaseReport], config: &AnalysisConfig) -> BootstrapSummary {
    let mut generator = DeterministicRng::new(config.bootstrap_seed);
    let mut breakpoints = Vec::new();
    for _ in 0..config.bootstrap_samples {
        let points = reports
            .iter()
            .filter(|report| report.stats.overall.attempts > 0)
            .map(|report| {
                let y = if report.quality.buckets.is_empty() {
                    report.goodput_rate
                } else {
                    (0..report.quality.buckets.len())
                        .map(|_| {
                            let index = generator.index(report.quality.buckets.len());
                            report.quality.buckets[index].goodput_rate
                        })
                        .sum::<f64>()
                        / report.quality.buckets.len() as f64
                };
                Point {
                    x: report.offered_rate,
                    y,
                }
            })
            .collect::<Vec<_>>();
        let Some(null) = fit_line(&points) else {
            continue;
        };
        let Some(segmented) = best_segmented_fit(&points) else {
            continue;
        };
        let reduction = if segmented.pre_knee_slope > 0.0 {
            (segmented.pre_knee_slope - segmented.post_knee_slope) / segmented.pre_knee_slope
        } else {
            f64::NEG_INFINITY
        };
        if improvement(null.sum_squared_error, segmented.sum_squared_error)
            >= MINIMUM_MODEL_IMPROVEMENT
            && reduction >= MINIMUM_SLOPE_REDUCTION
        {
            breakpoints.push(segmented.breakpoint);
        }
    }
    breakpoints.sort_by(f64::total_cmp);
    BootstrapSummary {
        seed: config.bootstrap_seed,
        requested_samples: config.bootstrap_samples,
        valid_samples: breakpoints.len() as u32,
        confidence_level: 0.95,
        lower_bound: percentile(&breakpoints, 0.025),
        upper_bound: percentile(&breakpoints, 0.975),
    }
}

fn validate_candidate(reports: &[PhaseReport], breakpoint: f64) -> CandidateValidation {
    let baseline = reports
        .iter()
        .min_by(|left, right| left.offered_rate.total_cmp(&right.offered_rate));
    let post = reports
        .iter()
        .filter(|report| report.offered_rate >= breakpoint)
        .collect::<Vec<_>>();
    let client_latency_ratio = baseline
        .and_then(|report| report.stats.overall.client_latency_ns.p95)
        .filter(|value| *value > 0)
        .and_then(|baseline| {
            post.iter()
                .filter_map(|report| report.stats.overall.client_latency_ns.p95)
                .max()
                .map(|maximum| maximum as f64 / baseline as f64)
        });
    let total_latency_ratio = baseline
        .and_then(|report| report.stats.overall.total_latency_ns.p95)
        .filter(|value| *value > 0)
        .and_then(|baseline| {
            post.iter()
                .filter_map(|report| report.stats.overall.total_latency_ns.p95)
                .max()
                .map(|maximum| maximum as f64 / baseline as f64)
        });
    let baseline_unsuccessful = baseline
        .map(|report| report.stats.overall.unsuccessful_rate())
        .unwrap_or(0.0);
    let baseline_error = baseline
        .map(|report| report.stats.overall.error_rate())
        .unwrap_or(0.0);
    let baseline_timeout = baseline
        .map(|report| report.stats.overall.timeout_rate())
        .unwrap_or(0.0);
    let post_unsuccessful = post
        .iter()
        .map(|report| report.stats.overall.unsuccessful_rate())
        .fold(0.0_f64, f64::max);
    let unsuccessful_rate_increase = (post_unsuccessful - baseline_unsuccessful).max(0.0);
    let post_error = post
        .iter()
        .map(|report| report.stats.overall.error_rate())
        .fold(0.0_f64, f64::max);
    let post_timeout = post
        .iter()
        .map(|report| report.stats.overall.timeout_rate())
        .fold(0.0_f64, f64::max);
    let error_rate_increase = (post_error - baseline_error).max(0.0);
    let timeout_rate_increase = (post_timeout - baseline_timeout).max(0.0);
    let in_flight_ratio = baseline
        .map(|report| report.in_flight_high_water)
        .filter(|value| *value > 0)
        .and_then(|baseline| {
            post.iter()
                .map(|report| report.in_flight_high_water)
                .max()
                .map(|maximum| maximum as f64 / baseline as f64)
        });
    let dispatch_lag_valid = !reports.iter().any(dispatch_lag_invalid);
    let target_signal_present = client_latency_ratio
        .is_some_and(|ratio| ratio >= LATENCY_VALIDATION_MULTIPLIER)
        || total_latency_ratio.is_some_and(|ratio| ratio >= LATENCY_VALIDATION_MULTIPLIER)
        || unsuccessful_rate_increase >= RELIABILITY_VALIDATION_INCREASE
        || in_flight_ratio.is_some_and(|ratio| ratio >= IN_FLIGHT_VALIDATION_MULTIPLIER);
    CandidateValidation {
        client_latency_ratio,
        total_latency_ratio,
        error_rate_increase,
        timeout_rate_increase,
        unsuccessful_rate_increase,
        in_flight_ratio,
        dispatch_lag_valid,
        target_signal_present,
    }
}

fn dispatch_lag_invalid(report: &PhaseReport) -> bool {
    let threshold = MINIMUM_DISPATCH_LAG_NS.max(
        (report.elapsed_ns as f64 * DISPATCH_LAG_FRACTION)
            .round()
            .min(u64::MAX as f64) as u64,
    );
    report
        .stats
        .overall
        .dispatch_lag_ns
        .p99
        .is_some_and(|lag| lag > threshold)
}

fn slo_maximum(reports: &[PhaseReport], config: &AnalysisConfig) -> Option<f64> {
    if config.latency_slo_ms.is_none() && config.maximum_unsuccessful_rate.is_none() {
        return None;
    }
    let mut ordered = reports.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| left.offered_rate.total_cmp(&right.offered_rate));
    let mut maximum = None;
    for report in ordered {
        let latency_valid = config.latency_slo_ms.is_none_or(|maximum_ms| {
            report
                .stats
                .overall
                .client_latency_ns
                .p95
                .is_some_and(|latency| latency as f64 / 1_000_000.0 <= maximum_ms)
        });
        let reliability_valid = config
            .maximum_unsuccessful_rate
            .is_none_or(|maximum| report.stats.overall.unsuccessful_rate() <= maximum);
        if !latency_valid || !reliability_valid {
            break;
        }
        maximum = Some(report.offered_rate);
    }
    Some(maximum.unwrap_or(0.0))
}

fn unique_rate_count(points: &[Point]) -> usize {
    let mut rates = points.iter().map(|point| point.x).collect::<Vec<_>>();
    rates.sort_by(f64::total_cmp);
    rates.dedup_by(|left, right| left.total_cmp(right).is_eq());
    rates.len()
}

fn percentile(values: &[f64], quantile: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let index = ((values.len() - 1) as f64 * quantile).round() as usize;
    values.get(index).copied()
}

fn sparse_classification(termination: AnalysisTermination) -> RunClassification {
    match termination {
        AnalysisTermination::MaximumLoadReached => RunClassification::MaximumLoadReached,
        AnalysisTermination::CompletedPlan | AnalysisTermination::BracketRefined => {
            RunClassification::NoKneeObserved
        }
    }
}

fn no_knee_classification(termination: AnalysisTermination) -> RunClassification {
    match termination {
        AnalysisTermination::MaximumLoadReached => RunClassification::MaximumLoadReached,
        AnalysisTermination::CompletedPlan | AnalysisTermination::BracketRefined => {
            RunClassification::NoKneeObserved
        }
    }
}

fn outcome(
    classification: RunClassification,
    knee: Option<KneeEstimate>,
    slo_maximum_rate: Option<f64>,
    analysis: Option<Box<KneeAnalysis>>,
    warnings: Vec<String>,
) -> RunOutcome {
    RunOutcome {
        classification,
        knee,
        slo_maximum_rate,
        analysis,
        warnings,
    }
}

struct DeterministicRng(u64);

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    fn next(&mut self) -> u64 {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        value
    }

    fn index(&mut self, length: usize) -> usize {
        (self.next() % length as u64) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::{
        DistributionStats, MeasurementBucket, PhaseQuality, SampleStats, StatsReport,
    };

    fn distribution(value: u64) -> DistributionStats {
        DistributionStats {
            samples: 100,
            min: Some(value),
            p50: Some(value),
            p95: Some(value),
            p99: Some(value),
            max: Some(value),
        }
    }

    fn report(
        rate: f64,
        goodput: f64,
        latency_ms: f64,
        in_flight: u64,
        unsuccessful: u64,
        dispatch_p99_ns: u64,
    ) -> PhaseReport {
        let attempts = rate.round() as u64;
        let failed = unsuccessful / 2;
        let timed_out = unsuccessful - failed;
        let latency_ns = (latency_ms * 1_000_000.0).round() as u64;
        let bucket_rate = goodput;
        PhaseReport {
            offered_rate: rate,
            goodput_rate: goodput,
            elapsed_ns: 1_000_000_000,
            in_flight_high_water: in_flight,
            stats: StatsReport {
                overall: SampleStats {
                    attempts,
                    successful: attempts.saturating_sub(unsuccessful),
                    failed,
                    timed_out,
                    errors_by_code: Vec::new(),
                    client_latency_ns: distribution(latency_ns),
                    total_latency_ns: distribution(latency_ns.saturating_add(dispatch_p99_ns)),
                    dispatch_lag_ns: distribution(dispatch_p99_ns),
                },
                variants: Vec::new(),
            },
            quality: PhaseQuality {
                stationary: true,
                reason: None,
                buckets: (0..5)
                    .map(|index| MeasurementBucket {
                        start_offset_ns: index * 200_000_000,
                        duration_ns: 200_000_000,
                        attempts: attempts / 5,
                        successful: (goodput / 5.0).round() as u64,
                        failed: 0,
                        timed_out: 0,
                        goodput_rate: bucket_rate,
                    })
                    .collect(),
            },
        }
    }

    fn clear_knee() -> Vec<PhaseReport> {
        [100.0, 150.0, 200.0, 250.0, 300.0, 350.0, 400.0]
            .into_iter()
            .map(|rate| {
                let above = (rate - 250.0_f64).max(0.0);
                report(
                    rate,
                    rate - above * 0.95,
                    if rate <= 250.0 { 10.0 } else { 50.0 },
                    if rate <= 250.0 { 2 } else { 12 },
                    0,
                    100_000,
                )
            })
            .collect()
    }

    #[test]
    fn clear_knee_returns_fit_bounds_and_recommendation() {
        let config = AnalysisConfig {
            bootstrap_samples: 100,
            bootstrap_seed: 7,
            ..AnalysisConfig::default()
        };
        let outcome = analyze(&clear_knee(), &config, AnalysisTermination::CompletedPlan);
        let repeated = analyze(&clear_knee(), &config, AnalysisTermination::CompletedPlan);
        assert_eq!(
            outcome
                .analysis
                .as_ref()
                .map(|analysis| &analysis.bootstrap),
            repeated
                .analysis
                .as_ref()
                .map(|analysis| &analysis.bootstrap)
        );

        assert_eq!(outcome.classification, RunClassification::TargetSaturated);
        let knee = outcome.knee.unwrap();
        assert_eq!(knee.offered_rate, 250.0);
        assert!(knee.lower_bound <= knee.offered_rate);
        assert!(knee.upper_bound >= knee.offered_rate);
        assert_eq!(
            knee.recommended_operating_rate,
            knee.lower_bound * config.safety_factor
        );
        let analysis = outcome.analysis.unwrap();
        assert!(analysis.model_improvement >= MINIMUM_MODEL_IMPROVEMENT);
        assert!(analysis.slope_reduction >= MINIMUM_SLOPE_REDUCTION);
        assert_eq!(analysis.bootstrap.seed, 7);
        assert_eq!(analysis.bootstrap.requested_samples, 100);
    }

    #[test]
    fn soft_slope_change_is_not_reported_as_a_knee() {
        let reports = [100.0, 150.0, 200.0, 250.0, 300.0, 350.0, 400.0]
            .into_iter()
            .map(|rate| {
                let above = (rate - 250.0_f64).max(0.0);
                report(
                    rate,
                    rate - above * 0.35,
                    if rate <= 250.0 { 10.0 } else { 30.0 },
                    if rate <= 250.0 { 2 } else { 6 },
                    0,
                    100_000,
                )
            })
            .collect::<Vec<_>>();
        let outcome = analyze(
            &reports,
            &AnalysisConfig::default(),
            AnalysisTermination::CompletedPlan,
        );

        assert_eq!(outcome.classification, RunClassification::NoKneeObserved);
        assert!(outcome.knee.is_none());
        assert!(outcome.analysis.is_some());
    }

    #[test]
    fn noisy_curve_without_a_validation_signal_does_not_fabricate_a_knee() {
        let reports = [
            (100.0, 98.0),
            (150.0, 157.0),
            (200.0, 190.0),
            (250.0, 262.0),
            (300.0, 286.0),
            (350.0, 357.0),
            (400.0, 389.0),
        ]
        .into_iter()
        .map(|(rate, goodput)| report(rate, goodput, 10.0, 2, 0, 100_000))
        .collect::<Vec<_>>();
        let outcome = analyze(
            &reports,
            &AnalysisConfig::default(),
            AnalysisTermination::CompletedPlan,
        );

        assert_eq!(outcome.classification, RunClassification::NoKneeObserved);
        assert!(outcome.knee.is_none());
        assert!(outcome.analysis.is_some());
    }

    #[test]
    fn linear_system_uses_the_null_model() {
        let reports = [100.0, 150.0, 200.0, 250.0, 300.0, 350.0]
            .into_iter()
            .map(|rate| report(rate, rate, 10.0, 2, 0, 100_000))
            .collect::<Vec<_>>();
        let outcome = analyze(
            &reports,
            &AnalysisConfig::default(),
            AnalysisTermination::MaximumLoadReached,
        );

        assert_eq!(
            outcome.classification,
            RunClassification::MaximumLoadReached
        );
        assert!(outcome.knee.is_none());
    }

    #[test]
    fn nonstationary_measurement_is_rejected_before_fitting() {
        let mut reports = clear_knee();
        reports[2].quality.stationary = false;
        let outcome = analyze(
            &reports,
            &AnalysisConfig::default(),
            AnalysisTermination::CompletedPlan,
        );

        assert_eq!(
            outcome.classification,
            RunClassification::UnstableMeasurement
        );
        assert!(outcome.analysis.is_none());
    }

    #[test]
    fn false_target_knee_from_generator_lag_is_invalidated() {
        let mut reports = clear_knee();
        reports[5].stats.overall.dispatch_lag_ns.p99 = Some(200_000_000);
        let outcome = analyze(
            &reports,
            &AnalysisConfig::default(),
            AnalysisTermination::CompletedPlan,
        );

        assert_eq!(
            outcome.classification,
            RunClassification::GeneratorSaturated
        );
        assert!(outcome.knee.is_none());
    }

    #[test]
    fn latency_slo_can_limit_capacity_before_the_knee() {
        let config = AnalysisConfig {
            latency_slo_ms: Some(20.0),
            maximum_unsuccessful_rate: Some(0.01),
            ..AnalysisConfig::default()
        };
        let mut reports = clear_knee();
        reports[3].stats.overall.client_latency_ns = distribution(30_000_000);
        let outcome = analyze(&reports, &config, AnalysisTermination::CompletedPlan);

        assert_eq!(outcome.classification, RunClassification::SloExceeded);
        assert_eq!(outcome.slo_maximum_rate, Some(200.0));
        assert!(outcome.knee.is_some());
    }

    #[test]
    fn failures_and_timeouts_constrain_the_reliability_slo() {
        let mut reports = clear_knee();
        reports[4] = report(300.0, 250.0, 50.0, 12, 12, 100_000);
        reports[5] = report(350.0, 252.5, 50.0, 12, 20, 100_000);
        let config = AnalysisConfig {
            maximum_unsuccessful_rate: Some(0.02),
            ..AnalysisConfig::default()
        };
        let outcome = analyze(&reports, &config, AnalysisTermination::CompletedPlan);

        assert_eq!(outcome.slo_maximum_rate, Some(250.0));
        let analysis = outcome.analysis.unwrap();
        assert_eq!(analysis.failed_attempts, 16);
        assert_eq!(analysis.timed_out_attempts, 16);
        assert!(analysis.validation.error_rate_increase > 0.0);
        assert!(analysis.validation.timeout_rate_increase > 0.0);
    }
}

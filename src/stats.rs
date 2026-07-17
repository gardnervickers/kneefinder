//! Statistics grouped by fully bound operation variants.

use std::{collections::BTreeMap, fmt};

use serde::{Deserialize, Serialize};

use crate::protocol::{ArgumentValue, OperationResult, OperationStatus};

pub const DEFAULT_MAX_VARIANTS: usize = 1_024;

/// One graphable point produced by a completed measurement phase.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PhaseReport {
    pub offered_rate: f64,
    pub goodput_rate: f64,
    pub elapsed_ns: u64,
    pub stats: StatsReport,
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
}

impl fmt::Display for StatsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyVariants { maximum } => write!(
                formatter,
                "operation result cardinality exceeds the configured limit of {maximum} variants"
            ),
        }
    }
}

impl std::error::Error for StatsError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::OperationId;

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
}

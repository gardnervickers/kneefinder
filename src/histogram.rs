//! Bounded HdrHistogram encoding and aggregation for latency measurements.

use std::{fmt, io::Cursor};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use hdrhistogram::{
    Histogram,
    serialization::{Deserializer, Serializer, V2Serializer},
};
use serde::{Deserialize, Serialize};

const V2_COOKIE: u32 = 0x1c84_9313;
const V2_HEADER_BYTES: usize = 40;
const IDENTITY_CONVERSION_RATIO_BITS: u64 = 1.0_f64.to_bits();

/// Histogram encoding accepted on the adapter protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistogramEncoding {
    /// Uncompressed HdrHistogram V2 binary data encoded with standard base64.
    HdrV2Base64,
}

/// Exact range and precision required for every mergeable latency histogram.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistogramSpec {
    pub lowest_discernible_ns: u64,
    pub highest_trackable_ns: u64,
    pub significant_figures: u8,
}

/// A histogram carried in a text-framed protocol message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncodedHistogram {
    pub encoding: HistogramEncoding,
    pub data: String,
}

/// Resource limits applied before decoding an untrusted histogram.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistogramDecodeLimits {
    pub maximum_encoded_bytes: usize,
    pub maximum_decoded_bytes: usize,
    pub maximum_distinct_values: usize,
}

impl Default for HistogramDecodeLimits {
    fn default() -> Self {
        Self {
            maximum_encoded_bytes: 4 * 1024 * 1024,
            maximum_decoded_bytes: 3 * 1024 * 1024,
            maximum_distinct_values: 1_000_000,
        }
    }
}

/// One bounded, non-resizing latency histogram.
#[derive(Debug, Clone)]
pub struct LatencyHistogram {
    histogram: Histogram<u64>,
}

impl LatencyHistogram {
    /// Construct an empty histogram with the exact supplied range and precision.
    pub fn new(spec: HistogramSpec) -> Result<Self, HistogramError> {
        let histogram = Histogram::new_with_bounds(
            spec.lowest_discernible_ns,
            spec.highest_trackable_ns,
            spec.significant_figures,
        )
        .map_err(|error| HistogramError::InvalidSpec(error.to_string()))?;
        let result = Self { histogram };
        if result.spec() != spec {
            return Err(HistogramError::IncompatibleSpec {
                expected: spec,
                actual: result.spec(),
            });
        }
        Ok(result)
    }

    /// Return the effective merge contract for this histogram.
    pub fn spec(&self) -> HistogramSpec {
        HistogramSpec {
            lowest_discernible_ns: self.histogram.low(),
            highest_trackable_ns: self.histogram.high(),
            significant_figures: self.histogram.sigfig(),
        }
    }

    /// Return the number of recorded observations.
    pub fn len(&self) -> u64 {
        self.histogram.len()
    }

    /// Return whether no observations have been recorded.
    pub fn is_empty(&self) -> bool {
        self.histogram.is_empty()
    }

    /// Return the number of counter cells allocated by this histogram.
    pub fn distinct_values(&self) -> usize {
        self.histogram.distinct_values()
    }

    /// Record one latency observation without clamping or resizing.
    pub fn record(&mut self, value_ns: u64) -> Result<(), HistogramError> {
        if value_ns > self.spec().highest_trackable_ns {
            return Err(HistogramError::Record {
                value_ns,
                message: format!(
                    "value exceeds the negotiated maximum of {} ns",
                    self.spec().highest_trackable_ns
                ),
            });
        }
        self.len()
            .checked_add(1)
            .ok_or(HistogramError::CountOverflow)?;
        self.histogram
            .record(value_ns)
            .map_err(|error| HistogramError::Record {
                value_ns,
                message: error.to_string(),
            })
    }

    /// Merge another histogram with an identical range and precision.
    pub fn merge(&mut self, other: &Self) -> Result<(), HistogramError> {
        if self.spec() != other.spec() {
            return Err(HistogramError::IncompatibleSpec {
                expected: self.spec(),
                actual: other.spec(),
            });
        }
        self.len()
            .checked_add(other.len())
            .ok_or(HistogramError::CountOverflow)?;
        self.histogram
            .add(&other.histogram)
            .map_err(|error| HistogramError::Merge(error.to_string()))
    }

    /// Alias for [`Self::merge`].
    pub fn add(&mut self, other: &Self) -> Result<(), HistogramError> {
        self.merge(other)
    }

    /// Return whether two histograms contain the same counts under the same
    /// range and precision contract.
    pub fn equivalent(&self, other: &Self) -> bool {
        self.spec() == other.spec() && self.histogram == other.histogram
    }

    /// Return the lowest recorded latency, or `None` when empty.
    pub fn min(&self) -> Option<u64> {
        (!self.is_empty()).then(|| self.histogram.min())
    }

    /// Return the highest recorded latency, or `None` when empty.
    pub fn max(&self) -> Option<u64> {
        (!self.is_empty()).then(|| self.histogram.max())
    }

    /// Return the value at a quantile in the inclusive range `[0, 1]`.
    pub fn value_at_quantile(&self, quantile: f64) -> Result<Option<u64>, HistogramError> {
        if !quantile.is_finite() || !(0.0..=1.0).contains(&quantile) {
            return Err(HistogramError::InvalidQuantile(quantile));
        }
        Ok((!self.is_empty()).then(|| self.histogram.value_at_quantile(quantile)))
    }

    /// Return values for quantiles in the order supplied by the caller.
    pub fn values_at_quantiles(
        &self,
        quantiles: &[f64],
    ) -> Result<Vec<Option<u64>>, HistogramError> {
        quantiles
            .iter()
            .copied()
            .map(|quantile| self.value_at_quantile(quantile))
            .collect()
    }

    /// Encode this histogram as uncompressed HdrHistogram V2 binary data.
    pub fn encode(&self) -> Result<EncodedHistogram, HistogramError> {
        let mut bytes = Vec::new();
        V2Serializer::new()
            .serialize(&self.histogram, &mut bytes)
            .map_err(|error| HistogramError::Encode(format!("{error:?}")))?;
        Ok(EncodedHistogram {
            encoding: HistogramEncoding::HdrV2Base64,
            data: STANDARD.encode(bytes),
        })
    }

    /// Decode an untrusted histogram under explicit byte and allocation limits.
    pub fn decode(
        encoded: &EncodedHistogram,
        expected_spec: HistogramSpec,
        limits: HistogramDecodeLimits,
    ) -> Result<Self, HistogramError> {
        if encoded.data.len() > limits.maximum_encoded_bytes {
            return Err(HistogramError::EncodedTooLarge {
                actual: encoded.data.len(),
                maximum: limits.maximum_encoded_bytes,
            });
        }

        let maximum_decoded_length = decoded_length_upper_bound(encoded.data.as_bytes())?;
        if maximum_decoded_length > limits.maximum_decoded_bytes {
            return Err(HistogramError::DecodedTooLarge {
                actual: maximum_decoded_length,
                maximum: limits.maximum_decoded_bytes,
            });
        }

        let bytes = STANDARD
            .decode(encoded.data.as_bytes())
            .map_err(|error| HistogramError::MalformedBase64(error.to_string()))?;
        if bytes.len() > limits.maximum_decoded_bytes {
            return Err(HistogramError::DecodedTooLarge {
                actual: bytes.len(),
                maximum: limits.maximum_decoded_bytes,
            });
        }
        preflight_v2(&bytes, expected_spec)?;

        let expected_shape = Self::new(expected_spec)?;
        if expected_shape.distinct_values() > limits.maximum_distinct_values {
            return Err(HistogramError::TooManyDistinctValues {
                actual: expected_shape.distinct_values(),
                maximum: limits.maximum_distinct_values,
            });
        }

        let mut cursor = Cursor::new(bytes.as_slice());
        let histogram: Histogram<u64> = Deserializer::new()
            .deserialize(&mut cursor)
            .map_err(|error| HistogramError::Decode(error.to_string()))?;
        if cursor.position() != bytes.len() as u64 {
            return Err(HistogramError::TrailingBytes {
                consumed: cursor.position() as usize,
                actual: bytes.len(),
            });
        }

        let decoded = Self { histogram };
        if decoded.spec() != expected_spec {
            return Err(HistogramError::IncompatibleSpec {
                expected: expected_spec,
                actual: decoded.spec(),
            });
        }
        if decoded.distinct_values() > limits.maximum_distinct_values {
            return Err(HistogramError::TooManyDistinctValues {
                actual: decoded.distinct_values(),
                maximum: limits.maximum_distinct_values,
            });
        }
        let exact_count = decoded
            .histogram
            .iter_recorded()
            .try_fold(0_u64, |total, value| {
                total.checked_add(value.count_since_last_iteration())
            });
        if exact_count != Some(decoded.len()) {
            return Err(HistogramError::CountOverflow);
        }
        Ok(decoded)
    }
}

fn decoded_length_upper_bound(encoded: &[u8]) -> Result<usize, HistogramError> {
    let padding = encoded
        .iter()
        .rev()
        .take_while(|byte| **byte == b'=')
        .count();
    encoded
        .len()
        .checked_add(3)
        .and_then(|length| length.checked_div(4))
        .and_then(|groups| groups.checked_mul(3))
        .and_then(|length| length.checked_sub(padding.min(2)))
        .ok_or(HistogramError::SizeOverflow)
}

fn preflight_v2(bytes: &[u8], expected_spec: HistogramSpec) -> Result<(), HistogramError> {
    if bytes.len() < V2_HEADER_BYTES {
        return Err(HistogramError::TruncatedHeader {
            actual: bytes.len(),
            required: V2_HEADER_BYTES,
        });
    }

    let cookie = read_u32(bytes, 0);
    if cookie != V2_COOKIE {
        return Err(HistogramError::UnsupportedCookie(cookie));
    }

    let payload_length = read_u32(bytes, 4) as usize;
    let expected_length = V2_HEADER_BYTES
        .checked_add(payload_length)
        .ok_or(HistogramError::SizeOverflow)?;
    if bytes.len() != expected_length {
        return if bytes.len() > expected_length {
            Err(HistogramError::TrailingBytes {
                consumed: expected_length,
                actual: bytes.len(),
            })
        } else {
            Err(HistogramError::TruncatedPayload {
                actual: bytes.len(),
                required: expected_length,
            })
        };
    }

    let significant_figures = read_u32(bytes, 12);
    let lowest_discernible_ns = read_u64(bytes, 16);
    let highest_trackable_ns = read_u64(bytes, 24);
    let conversion_ratio_bits = read_u64(bytes, 32);
    if conversion_ratio_bits != IDENTITY_CONVERSION_RATIO_BITS {
        return Err(HistogramError::UnsupportedConversionRatio(f64::from_bits(
            conversion_ratio_bits,
        )));
    }
    let significant_figures = u8::try_from(significant_figures).map_err(|_| {
        HistogramError::InvalidHeader("significant figures do not fit in u8".into())
    })?;
    let actual_spec = HistogramSpec {
        lowest_discernible_ns,
        highest_trackable_ns,
        significant_figures,
    };
    if actual_spec != expected_spec {
        return Err(HistogramError::IncompatibleSpec {
            expected: expected_spec,
            actual: actual_spec,
        });
    }
    Ok(())
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("fixed header field"),
    )
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("fixed header field"),
    )
}

/// Failure to create, record, merge, encode, or decode a latency histogram.
#[derive(Debug, Clone, PartialEq)]
pub enum HistogramError {
    InvalidSpec(String),
    Record {
        value_ns: u64,
        message: String,
    },
    CountOverflow,
    IncompatibleSpec {
        expected: HistogramSpec,
        actual: HistogramSpec,
    },
    Merge(String),
    InvalidQuantile(f64),
    Encode(String),
    EncodedTooLarge {
        actual: usize,
        maximum: usize,
    },
    DecodedTooLarge {
        actual: usize,
        maximum: usize,
    },
    TooManyDistinctValues {
        actual: usize,
        maximum: usize,
    },
    MalformedBase64(String),
    TruncatedHeader {
        actual: usize,
        required: usize,
    },
    TruncatedPayload {
        actual: usize,
        required: usize,
    },
    UnsupportedCookie(u32),
    UnsupportedConversionRatio(f64),
    InvalidHeader(String),
    Decode(String),
    TrailingBytes {
        consumed: usize,
        actual: usize,
    },
    SizeOverflow,
}

impl fmt::Display for HistogramError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSpec(message) => write!(formatter, "invalid histogram spec: {message}"),
            Self::Record { value_ns, message } => {
                write!(
                    formatter,
                    "cannot record histogram value {value_ns} ns: {message}"
                )
            }
            Self::CountOverflow => formatter.write_str("histogram sample count overflow"),
            Self::IncompatibleSpec { expected, actual } => write!(
                formatter,
                "histogram spec {actual:?} does not match expected spec {expected:?}"
            ),
            Self::Merge(message) => write!(formatter, "cannot merge histograms: {message}"),
            Self::InvalidQuantile(quantile) => {
                write!(formatter, "histogram quantile {quantile} is outside [0, 1]")
            }
            Self::Encode(message) => write!(formatter, "cannot encode histogram: {message}"),
            Self::EncodedTooLarge { actual, maximum } => write!(
                formatter,
                "encoded histogram is {actual} bytes, exceeding the {maximum}-byte limit"
            ),
            Self::DecodedTooLarge { actual, maximum } => write!(
                formatter,
                "decoded histogram is at most {actual} bytes, exceeding the {maximum}-byte limit"
            ),
            Self::TooManyDistinctValues { actual, maximum } => write!(
                formatter,
                "histogram uses {actual} counter cells, exceeding the {maximum}-cell limit"
            ),
            Self::MalformedBase64(message) => {
                write!(formatter, "histogram contains malformed base64: {message}")
            }
            Self::TruncatedHeader { actual, required } => write!(
                formatter,
                "histogram header is {actual} bytes; at least {required} bytes are required"
            ),
            Self::TruncatedPayload { actual, required } => write!(
                formatter,
                "histogram payload is {actual} bytes; its header requires {required} bytes"
            ),
            Self::UnsupportedCookie(cookie) => {
                write!(
                    formatter,
                    "histogram cookie {cookie:#010x} is not uncompressed V2"
                )
            }
            Self::UnsupportedConversionRatio(ratio) => write!(
                formatter,
                "histogram conversion ratio {ratio} is unsupported; expected 1.0"
            ),
            Self::InvalidHeader(message) => {
                write!(formatter, "invalid histogram header: {message}")
            }
            Self::Decode(message) => write!(formatter, "cannot decode histogram: {message}"),
            Self::TrailingBytes { consumed, actual } => write!(
                formatter,
                "histogram decoder consumed {consumed} of {actual} bytes"
            ),
            Self::SizeOverflow => formatter.write_str("histogram size calculation overflow"),
        }
    }
}

impl std::error::Error for HistogramError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> HistogramSpec {
        HistogramSpec {
            lowest_discernible_ns: 1,
            highest_trackable_ns: 10_000_000,
            significant_figures: 3,
        }
    }

    fn generous_limits() -> HistogramDecodeLimits {
        HistogramDecodeLimits {
            maximum_encoded_bytes: 1_000_000,
            maximum_decoded_bytes: 1_000_000,
            maximum_distinct_values: 1_000_000,
        }
    }

    #[test]
    fn v2_base64_round_trip_preserves_distribution() {
        let mut histogram = LatencyHistogram::new(spec()).unwrap();
        for value in [0, 10, 10, 1_000, 9_000_000] {
            histogram.record(value).unwrap();
        }

        let encoded = histogram.encode().unwrap();
        let decoded = LatencyHistogram::decode(&encoded, spec(), generous_limits()).unwrap();

        assert_eq!(decoded.spec(), spec());
        assert_eq!(decoded.len(), 5);
        assert_eq!(decoded.min(), Some(0));
        assert!(decoded.max().unwrap() >= 9_000_000);
        assert_eq!(
            decoded.value_at_quantile(0.5).unwrap(),
            histogram.value_at_quantile(0.5).unwrap()
        );
    }

    #[test]
    fn merging_preserves_all_samples() {
        let mut left = LatencyHistogram::new(spec()).unwrap();
        let mut right = LatencyHistogram::new(spec()).unwrap();
        left.record(10).unwrap();
        right.record(20).unwrap();
        right.record(30).unwrap();

        left.merge(&right).unwrap();

        assert_eq!(left.len(), 3);
        assert_eq!(left.min(), Some(10));
        assert!(left.max().unwrap() >= 30);
    }

    #[test]
    fn incompatible_histograms_do_not_merge() {
        let mut left = LatencyHistogram::new(spec()).unwrap();
        let right = LatencyHistogram::new(HistogramSpec {
            significant_figures: 2,
            ..spec()
        })
        .unwrap();

        assert!(matches!(
            left.merge(&right),
            Err(HistogramError::IncompatibleSpec { .. })
        ));
    }

    #[test]
    fn malformed_base64_is_rejected() {
        let encoded = EncodedHistogram {
            encoding: HistogramEncoding::HdrV2Base64,
            data: "%%%".into(),
        };
        assert!(matches!(
            LatencyHistogram::decode(&encoded, spec(), generous_limits()),
            Err(HistogramError::MalformedBase64(_))
        ));
    }

    #[test]
    fn compressed_or_unknown_cookie_is_rejected_before_deserialization() {
        let histogram = LatencyHistogram::new(spec()).unwrap();
        let mut bytes = STANDARD.decode(histogram.encode().unwrap().data).unwrap();
        bytes[..4].copy_from_slice(&0x1c84_9314_u32.to_be_bytes());
        let encoded = EncodedHistogram {
            encoding: HistogramEncoding::HdrV2Base64,
            data: STANDARD.encode(bytes),
        };

        assert!(matches!(
            LatencyHistogram::decode(&encoded, spec(), generous_limits()),
            Err(HistogramError::UnsupportedCookie(_))
        ));
    }

    #[test]
    fn mismatched_wire_spec_is_rejected() {
        let encoded = LatencyHistogram::new(spec()).unwrap().encode().unwrap();
        let expected = HistogramSpec {
            highest_trackable_ns: spec().highest_trackable_ns * 2,
            ..spec()
        };

        assert!(matches!(
            LatencyHistogram::decode(&encoded, expected, generous_limits()),
            Err(HistogramError::IncompatibleSpec { .. })
        ));
    }

    #[test]
    fn encoded_and_decoded_size_limits_are_enforced() {
        let encoded = LatencyHistogram::new(spec()).unwrap().encode().unwrap();
        let encoded_limit = HistogramDecodeLimits {
            maximum_encoded_bytes: encoded.data.len() - 1,
            ..generous_limits()
        };
        assert!(matches!(
            LatencyHistogram::decode(&encoded, spec(), encoded_limit),
            Err(HistogramError::EncodedTooLarge { .. })
        ));

        let decoded_limit = HistogramDecodeLimits {
            maximum_decoded_bytes: 1,
            ..generous_limits()
        };
        assert!(matches!(
            LatencyHistogram::decode(&encoded, spec(), decoded_limit),
            Err(HistogramError::DecodedTooLarge { .. })
        ));
    }

    #[test]
    fn counter_cell_limit_is_enforced_before_deserialization() {
        let encoded = LatencyHistogram::new(spec()).unwrap().encode().unwrap();
        let limits = HistogramDecodeLimits {
            maximum_distinct_values: 1,
            ..generous_limits()
        };
        assert!(matches!(
            LatencyHistogram::decode(&encoded, spec(), limits),
            Err(HistogramError::TooManyDistinctValues { .. })
        ));
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let histogram = LatencyHistogram::new(spec()).unwrap();
        let mut bytes = STANDARD.decode(histogram.encode().unwrap().data).unwrap();
        bytes.push(0);
        let encoded = EncodedHistogram {
            encoding: HistogramEncoding::HdrV2Base64,
            data: STANDARD.encode(bytes),
        };

        assert!(matches!(
            LatencyHistogram::decode(&encoded, spec(), generous_limits()),
            Err(HistogramError::TrailingBytes { .. })
        ));
    }

    #[test]
    fn values_above_the_negotiated_maximum_are_rejected() {
        let mut histogram = LatencyHistogram::new(spec()).unwrap();
        assert!(matches!(
            histogram.record(spec().highest_trackable_ns + 1),
            Err(HistogramError::Record { .. })
        ));
    }
}

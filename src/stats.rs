use std::collections::hash_map::RandomState;
use std::collections::HashSet;

use hyperloglogplus::{HyperLogLog, HyperLogLogPlus};

use crate::traversal::ScalarValue;

/// Tracks distinct values for a field, switching from exact to approximate
/// counting once the cap is exceeded.
pub enum DistinctCounter {
    /// Exact set of distinct values, up to the cap.
    Exact(HashSet<ScalarValue>),
    /// Approximate HyperLogLog++ counter (used after the cap is exceeded).
    Approx {
        hll: HyperLogLogPlus<ScalarValue, RandomState>,
        /// How many distinct values were seen before switching (retained for reporting).
        #[allow(dead_code)]
        cap_exceeded_at: usize,
        /// Cached count (updated on every `insert`; HLL `count()` needs `&mut self`).
        cached_count: u64,
    },
}

impl DistinctCounter {
    pub fn new() -> Self {
        DistinctCounter::Exact(HashSet::new())
    }

    /// Record a new value. `cap` is the threshold at which we switch to HLL.
    pub fn insert(&mut self, value: ScalarValue, cap: usize) {
        match self {
            DistinctCounter::Exact(set) => {
                set.insert(value.clone());
                if set.len() > cap {
                    // Transition to HyperLogLog++.
                    // Precision 14 gives ~0.8% error rate with ~16 KB memory.
                    let mut hll: HyperLogLogPlus<ScalarValue, RandomState> =
                        HyperLogLogPlus::new(14, RandomState::new())
                            .expect("valid HLL precision");
                    let cap_exceeded_at = set.len();
                    for v in set.drain() {
                        hll.add(&v);
                    }
                    hll.add(&value);
                    let cached_count = hll.count().round() as u64;
                    *self = DistinctCounter::Approx { hll, cap_exceeded_at, cached_count };
                }
            }
            DistinctCounter::Approx { hll, cached_count, .. } => {
                hll.add(&value);
                *cached_count = hll.count().round() as u64;
            }
        }
    }

    /// Returns the estimated distinct count.
    pub fn count(&self) -> u64 {
        match self {
            DistinctCounter::Exact(set) => set.len() as u64,
            DistinctCounter::Approx { cached_count, .. } => *cached_count,
        }
    }

    /// Returns true if we switched to approximate counting.
    pub fn is_approximate(&self) -> bool {
        matches!(self, DistinctCounter::Approx { .. })
    }

    /// If exact, returns the set of known values (sorted for determinism).
    pub fn exact_values(&self) -> Option<Vec<&ScalarValue>> {
        match self {
            DistinctCounter::Exact(set) => {
                let mut v: Vec<&ScalarValue> = set.iter().collect();
                v.sort();
                Some(v)
            }
            DistinctCounter::Approx { .. } => None,
        }
    }
}

/// Per-path statistics accumulated incrementally.
pub struct FieldStats {
    /// Total number of times this path was observed (any type).
    pub count: u64,
    /// Number of times the value was null.
    pub null_count: u64,
    /// Minimum scalar value observed (across bool/int/float/string).
    pub min_value: Option<ScalarValue>,
    /// Maximum scalar value observed.
    pub max_value: Option<ScalarValue>,
    /// Minimum array length observed (only set for array-type paths).
    pub array_len_min: Option<u64>,
    /// Maximum array length observed.
    pub array_len_max: Option<u64>,
    /// Sum of all array lengths (for computing average).
    pub array_len_sum: u64,
    /// Number of array observations (denominator for average).
    pub array_len_count: u64,
    /// Distinct value counter.
    pub distinct: DistinctCounter,
}

impl FieldStats {
    pub fn new() -> Self {
        FieldStats {
            count: 0,
            null_count: 0,
            min_value: None,
            max_value: None,
            array_len_min: None,
            array_len_max: None,
            array_len_sum: 0,
            array_len_count: 0,
            distinct: DistinctCounter::new(),
        }
    }

    /// Record a scalar observation.
    pub fn record_scalar(&mut self, value: &ScalarValue, distinct_cap: usize) {
        self.count += 1;
        if matches!(value, ScalarValue::Null) {
            self.null_count += 1;
        } else {
            // Update min/max.
            match &self.min_value {
                None => self.min_value = Some(value.clone()),
                Some(current) if value < current => self.min_value = Some(value.clone()),
                _ => {}
            }
            match &self.max_value {
                None => self.max_value = Some(value.clone()),
                Some(current) if value > current => self.max_value = Some(value.clone()),
                _ => {}
            }
            self.distinct.insert(value.clone(), distinct_cap);
        }
    }

    /// Record an array length observation.
    pub fn record_array_len(&mut self, len: usize) {
        self.count += 1;
        let len = len as u64;
        self.array_len_sum += len;
        self.array_len_count += 1;
        self.array_len_min = Some(match self.array_len_min {
            None => len,
            Some(current) => current.min(len),
        });
        self.array_len_max = Some(match self.array_len_max {
            None => len,
            Some(current) => current.max(len),
        });
    }

    /// Record an object or other non-scalar, non-array observation (just increments count).
    pub fn record_presence(&mut self) {
        self.count += 1;
    }

    /// Average array length, if any arrays were observed.
    pub fn array_len_avg(&self) -> Option<f64> {
        if self.array_len_count == 0 {
            None
        } else {
            Some(self.array_len_sum as f64 / self.array_len_count as f64)
        }
    }
}

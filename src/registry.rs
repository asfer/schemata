use std::collections::{BTreeMap, HashSet};

use crate::stats::FieldStats;
use crate::traversal::{JsonType, Observation};

/// Tracks the distinct keys seen at an object-typed path.
/// Once `max_keys` is exceeded the field is flagged as unbounded.
pub struct KeyTracker {
    pub keys: HashSet<String>,
    pub is_unbounded: bool,
    /// Whether we have already emitted the stderr warning for this path.
    pub warned: bool,
}

impl KeyTracker {
    pub fn new() -> Self {
        KeyTracker {
            keys: HashSet::new(),
            is_unbounded: false,
            warned: false,
        }
    }

    /// Insert keys. Returns `true` if the unbounded threshold was just crossed
    /// (i.e. the caller should emit a warning).
    pub fn insert_keys(&mut self, keys: &[String], max_keys: usize) -> bool {
        if self.is_unbounded {
            return false;
        }
        for k in keys {
            self.keys.insert(k.clone());
        }
        if self.keys.len() > max_keys {
            self.is_unbounded = true;
            // Free the memory — we no longer need the exact set.
            self.keys = HashSet::new();
            return true;
        }
        false
    }
}

/// A single entry in the registry for one path.
pub struct PathEntry {
    /// How many times each JSON type was observed at this path.
    pub type_counts: BTreeMap<JsonType, u64>,
    /// Accumulated statistics for this path.
    pub stats: FieldStats,
    /// Key tracker — only populated for object-typed paths.
    pub key_tracker: Option<KeyTracker>,
}

impl PathEntry {
    pub fn new() -> Self {
        PathEntry {
            type_counts: BTreeMap::new(),
            stats: FieldStats::new(),
            key_tracker: None,
        }
    }

    /// Returns true if this path has been observed with more than one distinct
    /// non-null type (a true type collision, not just nullability).
    pub fn has_type_collision(&self) -> bool {
        let non_null_types = self
            .type_counts
            .keys()
            .filter(|t| **t != JsonType::Null)
            .count();
        non_null_types > 1
    }

    /// Returns the types sorted by observation count descending, null first if present.
    pub fn types_by_frequency(&self) -> Vec<(&JsonType, u64)> {
        let mut pairs: Vec<(&JsonType, u64)> =
            self.type_counts.iter().map(|(t, c)| (t, *c)).collect();
        pairs.sort_by(|a, b| {
            // null always first
            match (a.0 == &JsonType::Null, b.0 == &JsonType::Null) {
                (true, false) => std::cmp::Ordering::Less,
                (false, true) => std::cmp::Ordering::Greater,
                _ => b.1.cmp(&a.1), // descending by count
            }
        });
        pairs
    }
}

/// Central registry: maps dot-notation paths to their accumulated data.
pub struct PathRegistry {
    /// Ordered map for deterministic output.
    pub entries: BTreeMap<String, PathEntry>,
    pub max_keys: usize,
    pub distinct_cap: usize,
    /// Total number of records processed.
    pub record_count: u64,
}

impl PathRegistry {
    pub fn new(max_keys: usize, distinct_cap: usize) -> Self {
        PathRegistry {
            entries: BTreeMap::new(),
            max_keys,
            distinct_cap,
            record_count: 0,
        }
    }

    /// Process all observations from a single document.
    pub fn process_observations(&mut self, observations: Vec<Observation>) {
        for obs in observations {
            self.record(obs);
        }
    }

    /// Increment the record counter.
    pub fn increment_records(&mut self) {
        self.record_count += 1;
    }

    fn record(&mut self, obs: Observation) {
        let entry = self
            .entries
            .entry(obs.path.clone())
            .or_insert_with(PathEntry::new);

        // Increment type count.
        *entry.type_counts.entry(obs.json_type.clone()).or_insert(0) += 1;

        match obs.json_type {
            JsonType::Null => {
                if let Some(scalar) = obs.scalar {
                    entry.stats.record_scalar(&scalar, self.distinct_cap);
                }
            }
            JsonType::Bool | JsonType::Integer | JsonType::Float | JsonType::String => {
                if let Some(scalar) = obs.scalar {
                    entry.stats.record_scalar(&scalar, self.distinct_cap);
                }
            }
            JsonType::Array => {
                if let Some(len) = obs.array_len {
                    entry.stats.record_array_len(len);
                }
            }
            JsonType::Object => {
                entry.stats.record_presence();

                // Initialise key tracker on first object observation.
                if entry.key_tracker.is_none() {
                    entry.key_tracker = Some(KeyTracker::new());
                }

                if let Some(keys) = obs.object_keys {
                    let tracker = entry.key_tracker.as_mut().unwrap();
                    let just_crossed = tracker.insert_keys(&keys, self.max_keys);
                    if just_crossed && !tracker.warned {
                        tracker.warned = true;
                        eprintln!(
                            "[WARN] Path {:?} has exceeded {} distinct keys — \
                             treating as an unbounded map (e.g. keyed by IDs). \
                             Use --max-keys to adjust this threshold.",
                            obs.path, self.max_keys
                        );
                    }
                }
            }
        }
    }

    /// Collect all paths that have type collisions (more than one non-null type).
    pub fn type_collisions(&self) -> Vec<(&str, &PathEntry)> {
        self.entries
            .iter()
            .filter(|(_, e)| e.has_type_collision())
            .map(|(p, e)| (p.as_str(), e))
            .collect()
    }

    /// Emit type-collision warnings to stderr.
    pub fn warn_type_collisions(&self) {
        for (path, entry) in self.type_collisions() {
            let type_summary: Vec<String> = entry
                .types_by_frequency()
                .iter()
                .filter(|(t, _)| **t != JsonType::Null)
                .map(|(t, c)| format!("{} ({}x)", t, c))
                .collect();
            eprintln!(
                "[WARN] Type collision at path {:?}: observed types [{}]. \
                 This will require data transformation.",
                path,
                type_summary.join(", ")
            );
        }
    }
}

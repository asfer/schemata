//! Emits a detailed JSON report of the path registry: type counts, statistics,
//! collision flags, and unbounded-key annotations.

use serde_json::{json, Value};

use crate::registry::PathRegistry;
use crate::traversal::ScalarValue;

/// Build the full JSON report from the registry.
pub fn build_report(registry: &PathRegistry) -> Value {
    let mut paths = serde_json::Map::new();

    for (path, entry) in &registry.entries {
        let mut path_obj = serde_json::Map::new();

        // --- type_counts ---
        let mut type_counts = serde_json::Map::new();
        for (t, count) in &entry.type_counts {
            type_counts.insert(t.as_str().to_string(), json!(count));
        }
        path_obj.insert("type_counts".to_string(), Value::Object(type_counts));

        // --- type_collision flag ---
        if entry.has_type_collision() {
            path_obj.insert("type_collision".to_string(), json!(true));
            let collision_detail: Vec<Value> = entry
                .types_by_frequency()
                .iter()
                .filter(|(t, _)| **t != crate::traversal::JsonType::Null)
                .map(|(t, c)| json!({"type": t.as_str(), "count": c}))
                .collect();
            path_obj.insert(
                "collision_types".to_string(),
                Value::Array(collision_detail),
            );
        }

        // --- unbounded_keys flag ---
        if let Some(tracker) = &entry.key_tracker {
            if tracker.is_unbounded {
                path_obj.insert("unbounded_keys".to_string(), json!(true));
                path_obj.insert(
                    "unbounded_keys_note".to_string(),
                    json!(format!(
                        "Exceeded {} distinct keys — field treated as an unbounded map.",
                        registry.max_keys
                    )),
                );
            } else {
                let mut sorted_keys: Vec<&String> = tracker.keys.iter().collect();
                sorted_keys.sort();
                path_obj.insert(
                    "observed_keys".to_string(),
                    Value::Array(sorted_keys.into_iter().map(|k| json!(k)).collect()),
                );
            }
        }

        // --- statistics ---
        let stats = &entry.stats;
        let mut stats_obj = serde_json::Map::new();

        stats_obj.insert("count".to_string(), json!(stats.count));
        stats_obj.insert("null_count".to_string(), json!(stats.null_count));

        if let Some(min) = &stats.min_value {
            stats_obj.insert("min".to_string(), scalar_to_json(min));
        }
        if let Some(max) = &stats.max_value {
            stats_obj.insert("max".to_string(), scalar_to_json(max));
        }

        // Distinct value stats
        let distinct_count = stats.distinct.count();
        stats_obj.insert("distinct_count".to_string(), json!(distinct_count));
        stats_obj.insert(
            "distinct_count_approximate".to_string(),
            json!(stats.distinct.is_approximate()),
        );
        if let Some(exact_vals) = stats.distinct.exact_values() {
            if exact_vals.len() <= 50 {
                // Only inline values if there are 50 or fewer — avoids huge output.
                let vals: Vec<Value> = exact_vals.iter().map(|v| scalar_to_json(v)).collect();
                stats_obj.insert("distinct_values".to_string(), Value::Array(vals));
            }
        }

        // Array length stats
        if let Some(min_len) = stats.array_len_min {
            stats_obj.insert("array_len_min".to_string(), json!(min_len));
        }
        if let Some(max_len) = stats.array_len_max {
            stats_obj.insert("array_len_max".to_string(), json!(max_len));
        }
        if let Some(avg) = stats.array_len_avg() {
            stats_obj.insert(
                "array_len_avg".to_string(),
                json!((avg * 100.0).round() / 100.0),
            );
        }

        path_obj.insert("stats".to_string(), Value::Object(stats_obj));

        paths.insert(path.clone(), Value::Object(path_obj));
    }

    json!({
        "record_count": registry.record_count,
        "path_count": registry.entries.len(),
        "paths": Value::Object(paths),
    })
}

fn scalar_to_json(v: &ScalarValue) -> Value {
    match v {
        ScalarValue::Null => Value::Null,
        ScalarValue::Bool(b) => json!(b),
        ScalarValue::Integer(i) => json!(i),
        ScalarValue::Float(f) => json!(f.into_inner()),
        ScalarValue::String(s) => json!(s),
    }
}

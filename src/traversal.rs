use serde_json::Value;

/// The JSON type of a value observed at a path.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum JsonType {
    Null,
    Bool,
    Integer,
    Float,
    String,
    Array,
    Object,
}

impl JsonType {
    pub fn as_str(&self) -> &'static str {
        match self {
            JsonType::Null => "null",
            JsonType::Bool => "boolean",
            JsonType::Integer => "integer",
            JsonType::Float => "float",
            JsonType::String => "string",
            JsonType::Array => "array",
            JsonType::Object => "object",
        }
    }
}

impl std::fmt::Display for JsonType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A single observation emitted during traversal.
#[derive(Debug)]
pub struct Observation {
    /// Dot-notation path, using `$` as the array element placeholder.
    /// e.g. `"a.b"`, `"a.$.b"`, `"a.$"`.
    pub path: String,
    /// The JSON type observed at this path.
    pub json_type: JsonType,
    /// The scalar value, if this is a leaf (null/bool/int/float/string).
    pub scalar: Option<ScalarValue>,
    /// If this is an array, its length.
    pub array_len: Option<usize>,
    /// If this is an object, the list of keys present.
    pub object_keys: Option<Vec<String>>,
}

/// A comparable scalar value for min/max and distinct tracking.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ScalarValue {
    Null,
    Bool(bool),
    Integer(i64),
    Float(ordered_float::OrderedFloat<f64>),
    String(String),
}

impl PartialOrd for ScalarValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScalarValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use ScalarValue::*;
        match (self, other) {
            (Null, Null) => std::cmp::Ordering::Equal,
            (Bool(a), Bool(b)) => a.cmp(b),
            (Integer(a), Integer(b)) => a.cmp(b),
            (Float(a), Float(b)) => a.cmp(b),
            (String(a), String(b)) => a.cmp(b),
            // Cross-type ordering: define a canonical order for mixed comparisons
            _ => self.type_rank().cmp(&other.type_rank()),
        }
    }
}

impl ScalarValue {
    fn type_rank(&self) -> u8 {
        match self {
            ScalarValue::Null => 0,
            ScalarValue::Bool(_) => 1,
            ScalarValue::Integer(_) => 2,
            ScalarValue::Float(_) => 3,
            ScalarValue::String(_) => 4,
        }
    }

    #[allow(dead_code)]
    pub fn as_display_string(&self) -> String {
        match self {
            ScalarValue::Null => "null".to_string(),
            ScalarValue::Bool(b) => b.to_string(),
            ScalarValue::Integer(i) => i.to_string(),
            ScalarValue::Float(f) => f.to_string(),
            ScalarValue::String(s) => s.clone(),
        }
    }
}

/// Traverse a JSON value and emit observations for every node.
///
/// `path` is the current dot-notation path prefix (empty string for the root).
/// Observations are pushed into `out`.
pub fn traverse(value: &Value, path: &str, out: &mut Vec<Observation>) {
    match value {
        Value::Null => {
            out.push(Observation {
                path: path.to_string(),
                json_type: JsonType::Null,
                scalar: Some(ScalarValue::Null),
                array_len: None,
                object_keys: None,
            });
        }
        Value::Bool(b) => {
            out.push(Observation {
                path: path.to_string(),
                json_type: JsonType::Bool,
                scalar: Some(ScalarValue::Bool(*b)),
                array_len: None,
                object_keys: None,
            });
        }
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                out.push(Observation {
                    path: path.to_string(),
                    json_type: JsonType::Integer,
                    scalar: Some(ScalarValue::Integer(i)),
                    array_len: None,
                    object_keys: None,
                });
            } else if let Some(f) = n.as_f64() {
                out.push(Observation {
                    path: path.to_string(),
                    json_type: JsonType::Float,
                    scalar: Some(ScalarValue::Float(ordered_float::OrderedFloat(f))),
                    array_len: None,
                    object_keys: None,
                });
            }
        }
        Value::String(s) => {
            out.push(Observation {
                path: path.to_string(),
                json_type: JsonType::String,
                scalar: Some(ScalarValue::String(s.clone())),
                array_len: None,
                object_keys: None,
            });
        }
        Value::Array(arr) => {
            // Emit an observation for the array itself (with its length).
            out.push(Observation {
                path: path.to_string(),
                json_type: JsonType::Array,
                scalar: None,
                array_len: Some(arr.len()),
                object_keys: None,
            });
            // Recurse into each element using the `$` placeholder.
            let child_path = if path.is_empty() {
                "$".to_string()
            } else {
                format!("{path}.$")
            };
            for item in arr {
                traverse(item, &child_path, out);
            }
        }
        Value::Object(map) => {
            let keys: Vec<String> = map.keys().cloned().collect();
            // Emit an observation for the object itself.
            out.push(Observation {
                path: path.to_string(),
                json_type: JsonType::Object,
                scalar: None,
                array_len: None,
                object_keys: Some(keys),
            });
            // Recurse into each field.
            for (key, val) in map {
                let child_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                traverse(val, &child_path, out);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn paths(value: &Value) -> Vec<(String, JsonType)> {
        let mut obs = Vec::new();
        traverse(value, "", &mut obs);
        obs.into_iter().map(|o| (o.path, o.json_type)).collect()
    }

    #[test]
    fn test_flat_object() {
        let v = json!({"a": 1, "b": "hello"});
        let p = paths(&v);
        assert!(p.contains(&("".to_string(), JsonType::Object)));
        assert!(p.contains(&("a".to_string(), JsonType::Integer)));
        assert!(p.contains(&("b".to_string(), JsonType::String)));
    }

    #[test]
    fn test_nested_object() {
        let v = json!({"a": {"b": 1}});
        let p = paths(&v);
        assert!(p.contains(&("a".to_string(), JsonType::Object)));
        assert!(p.contains(&("a.b".to_string(), JsonType::Integer)));
    }

    #[test]
    fn test_array_of_objects() {
        let v = json!({"a": [{"b": 1}, {"b": 2}]});
        let p = paths(&v);
        assert!(p.contains(&("a".to_string(), JsonType::Array)));
        assert!(p.contains(&("a.$".to_string(), JsonType::Object)));
        assert!(p.contains(&("a.$.b".to_string(), JsonType::Integer)));
    }

    #[test]
    fn test_null_value() {
        let v = json!({"x": null});
        let p = paths(&v);
        assert!(p.contains(&("x".to_string(), JsonType::Null)));
    }

    #[test]
    fn test_float_vs_integer() {
        let v = json!({"x": 1, "y": 1.5});
        let p = paths(&v);
        assert!(p.contains(&("x".to_string(), JsonType::Integer)));
        assert!(p.contains(&("y".to_string(), JsonType::Float)));
    }
}

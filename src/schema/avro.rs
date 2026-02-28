//! Converts the PathRegistry into an Avro schema (JSON encoding).
//!
//! Strategy:
//!   1. Build an in-memory tree from the flat dot-notation paths.
//!   2. Walk the tree recursively to emit Avro field definitions.
//!
//! Path segment rules:
//!   - A plain segment (e.g. `foo`) is an object field.
//!   - A `$` segment means "array element" — the parent path is an array.
//!
//! Type mapping:
//!   - null        → "null"
//!   - boolean     → "boolean"
//!   - integer     → "long"
//!   - float       → "double"
//!   - string      → "string"
//!   - array       → {"type":"array","items":<element_type>}
//!   - object      → {"type":"record","name":<name>,"fields":[...]}
//!   - unbounded   → {"type":"map","values":<value_type>}
//!   - multi-type  → union [...] with null first, then by frequency desc

use std::collections::BTreeMap;

use serde_json::{json, Value};

use crate::registry::{PathEntry, PathRegistry};
use crate::traversal::JsonType;

// ---------------------------------------------------------------------------
// Tree node
// ---------------------------------------------------------------------------

/// An intermediate tree built from the flat path registry before Avro emission.
#[derive(Default)]
struct Node {
    /// Child nodes keyed by path segment.
    children: BTreeMap<String, Node>,
    /// The registry entry for this exact path (if any).
    entry: Option<*const PathEntry>,
}

// SAFETY: PathRegistry outlives the tree; we only read through these pointers.
unsafe impl Send for Node {}
unsafe impl Sync for Node {}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Build an Avro schema Value from the registry.
/// The top-level schema is always a `record` named `"Root"`.
pub fn build_schema(registry: &PathRegistry) -> Value {
    let tree = build_tree(registry);
    emit_record(&tree, "Root", registry)
}

// ---------------------------------------------------------------------------
// Tree construction
// ---------------------------------------------------------------------------

fn build_tree(registry: &PathRegistry) -> Node {
    let mut root = Node::default();
    for (path, entry) in &registry.entries {
        let segments: Vec<&str> = if path.is_empty() {
            vec![]
        } else {
            path.split('.').collect()
        };
        insert_node(&mut root, &segments, entry as *const PathEntry);
    }
    root
}

fn insert_node(node: &mut Node, segments: &[&str], entry: *const PathEntry) {
    if segments.is_empty() {
        node.entry = Some(entry);
        return;
    }
    let head = segments[0];
    let child = node.children.entry(head.to_string()).or_default();
    insert_node(child, &segments[1..], entry);
}

// ---------------------------------------------------------------------------
// Avro emission
// ---------------------------------------------------------------------------

fn emit_record(node: &Node, name: &str, registry: &PathRegistry) -> Value {
    let fields: Vec<Value> = node
        .children
        .iter()
        .filter(|(seg, _)| *seg != "$") // array element nodes are handled by their parent
        .map(|(seg, child)| emit_field(seg, child, registry))
        .collect();

    json!({
        "type": "record",
        "name": name,
        "fields": fields,
    })
}

fn emit_field(name: &str, node: &Node, registry: &PathRegistry) -> Value {
    let avro_type = emit_type(name, node, registry);
    json!({
        "name": name,
        "type": avro_type,
    })
}

/// Determine the Avro type for a node.
fn emit_type(name: &str, node: &Node, registry: &PathRegistry) -> Value {
    // Does this node have an array-element child (`$`)?
    let is_array = node.children.contains_key("$");

    if is_array {
        return emit_array_type(name, node, registry);
    }

    // Does this node have non-`$` children? → it's a record/object.
    let has_object_children = node.children.keys().any(|k| k != "$");

    if has_object_children {
        // Check if it's an unbounded map.
        if let Some(entry_ptr) = node.entry {
            let entry = unsafe { &*entry_ptr };
            if let Some(tracker) = &entry.key_tracker {
                if tracker.is_unbounded {
                    // Determine the value type from the children.
                    let value_type = infer_map_value_type(node, registry);
                    return json!({
                        "type": "map",
                        "values": value_type,
                    });
                }
            }
        }
        // Regular record.
        let record_name = pascal_case(name);
        return emit_record(node, &record_name, registry);
    }

    // Leaf node — derive type from the registry entry.
    if let Some(entry_ptr) = node.entry {
        let entry = unsafe { &*entry_ptr };
        return leaf_avro_type(name, entry, registry);
    }

    // Fallback: no entry, no children — treat as null.
    json!("null")
}

fn emit_array_type(name: &str, node: &Node, registry: &PathRegistry) -> Value {
    let element_node = &node.children["$"];
    let items = emit_type(name, element_node, registry);
    // Wrap in union with null to make arrays optional by default.
    json!({
        "type": "array",
        "items": items,
    })
}

/// For an unbounded map, infer the value type from the children of the node.
/// We look at the `$` child if present (array-of-values map), otherwise
/// we look at the direct children's types.
fn infer_map_value_type(node: &Node, registry: &PathRegistry) -> Value {
    // If there's a `$` child, the values are arrays.
    if let Some(arr_child) = node.children.get("$") {
        let items = emit_type("value", arr_child, registry);
        return json!({"type": "array", "items": items});
    }
    // Otherwise collect all child types and union them.
    let child_types: Vec<Value> = node
        .children
        .iter()
        .filter(|(k, _)| *k != "$")
        .map(|(k, child)| emit_type(k, child, registry))
        .collect();

    if child_types.is_empty() {
        json!("string") // best-effort default for unknown map values
    } else if child_types.len() == 1 {
        child_types.into_iter().next().unwrap()
    } else {
        Value::Array(child_types)
    }
}

/// Build the Avro type for a leaf node from its PathEntry.
fn leaf_avro_type(_name: &str, entry: &PathEntry, _registry: &PathRegistry) -> Value {
    let types_by_freq = entry.types_by_frequency();

    // Collect the distinct Avro type strings.
    let mut avro_types: Vec<Value> = Vec::new();
    let mut has_null = false;
    let mut collision = false;
    let mut non_null_count = 0usize;

    for (json_type, _count) in &types_by_freq {
        match json_type {
            JsonType::Null => {
                has_null = true;
            }
            _ => {
                non_null_count += 1;
                let avro = json_type_to_avro(json_type);
                if !avro_types.contains(&avro) {
                    avro_types.push(avro);
                }
            }
        }
    }

    if non_null_count > 1 {
        collision = true;
    }

    // Build the union.
    let mut union: Vec<Value> = Vec::new();
    if has_null {
        union.push(json!("null"));
    }
    union.extend(avro_types);

    // If there's only one type and no null, return it directly (not a union).
    if union.len() == 1 {
        let t = union.remove(0);
        return t;
    }

    // Multi-type: emit a union.
    let union_val = Value::Array(union);

    if collision {
        // Wrap in a record field with a doc annotation warning about the collision.
        let type_summary: Vec<String> = types_by_freq
            .iter()
            .filter(|(t, _)| **t != JsonType::Null)
            .map(|(t, c)| format!("{}={}", t.as_str(), c))
            .collect();
        // We can't embed doc here (that's on the field, not the type), so we
        // return a special sentinel object that emit_field will detect.
        // Instead, we return the union directly — the collision warning is
        // already printed to stderr by the registry.
        let _ = type_summary; // used in registry warnings
        union_val
    } else {
        union_val
    }
}

fn json_type_to_avro(t: &JsonType) -> Value {
    match t {
        JsonType::Null => json!("null"),
        JsonType::Bool => json!("boolean"),
        JsonType::Integer => json!("long"),
        JsonType::Float => json!("double"),
        JsonType::String => json!("string"),
        JsonType::Array => json!({"type": "array", "items": "null"}), // placeholder
        JsonType::Object => json!({"type": "record", "name": "Object", "fields": []}),
    }
}

/// Convert a snake_case or camelCase name to PascalCase for Avro record names.
fn pascal_case(s: &str) -> String {
    let mut result = String::new();
    let mut capitalise_next = true;
    for ch in s.chars() {
        if ch == '_' || ch == '-' {
            capitalise_next = true;
        } else if capitalise_next {
            result.extend(ch.to_uppercase());
            capitalise_next = false;
        } else {
            result.push(ch);
        }
    }
    if result.is_empty() {
        "Record".to_string()
    } else {
        result
    }
}

// ---------------------------------------------------------------------------
// Collision doc annotation pass
// ---------------------------------------------------------------------------

/// Walk the emitted Avro schema and add `doc` annotations to fields that have
/// type collisions, using the registry for lookup.
pub fn annotate_collisions(schema: &mut Value, registry: &PathRegistry) {
    annotate_record(schema, "", registry);
}

fn annotate_record(schema: &mut Value, prefix: &str, registry: &PathRegistry) {
    if let Some(fields) = schema
        .get_mut("fields")
        .and_then(|f| f.as_array_mut())
    {
        for field in fields.iter_mut() {
            let field_name = field
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("")
                .to_string();
            let path = if prefix.is_empty() {
                field_name.clone()
            } else {
                format!("{}.{}", prefix, field_name)
            };

            // Check for collision at this path.
            if let Some(entry) = registry.entries.get(&path) {
                if entry.has_type_collision() {
                    let type_summary: Vec<String> = entry
                        .types_by_frequency()
                        .iter()
                        .filter(|(t, _)| **t != JsonType::Null)
                        .map(|(t, c)| format!("{}={}", t.as_str(), c))
                        .collect();
                    let doc = format!(
                        "TYPE COLLISION: {} — manual transformation required",
                        type_summary.join(", ")
                    );
                    if let Some(obj) = field.as_object_mut() {
                        obj.insert("doc".to_string(), json!(doc));
                    }
                }
                // Annotate unbounded maps.
                if let Some(tracker) = &entry.key_tracker {
                    if tracker.is_unbounded {
                        if let Some(obj) = field.as_object_mut() {
                            obj.insert(
                                "doc".to_string(),
                                json!(format!(
                                    "UNBOUNDED MAP: exceeded {} distinct keys",
                                    registry.max_keys
                                )),
                            );
                        }
                    }
                }
            }

            // Recurse into nested records.
            if let Some(field_type) = field.get_mut("type") {
                annotate_value(field_type, &path, registry);
            }
        }
    }
}

fn annotate_value(val: &mut Value, prefix: &str, registry: &PathRegistry) {
    match val {
        Value::Object(obj) => {
            if obj.get("type").and_then(|t| t.as_str()) == Some("record") {
                annotate_record(val, prefix, registry);
            } else if obj.get("type").and_then(|t| t.as_str()) == Some("array") {
                if let Some(items) = obj.get_mut("items") {
                    let array_prefix = format!("{}.{}", prefix, "$");
                    annotate_value(items, &array_prefix, registry);
                }
            }
        }
        Value::Array(union) => {
            for item in union.iter_mut() {
                annotate_value(item, prefix, registry);
            }
        }
        _ => {}
    }
}

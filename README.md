## **Schemata**

### **Core Functionality**

* **Structure Inference:** Analyzes newline-delimited JSON (JSONL) to map dot-notation paths and observed JSON types.
* **Deep Metadata:** Records field statistics, including min/max values, null rates, cardinality (distinct values), and array lengths.
* **Flexible Output:** Generates formal **Avro schemas** or comprehensive **JSON reports**.

### **Key Benefits**

* **Prevent Pipeline Breaks:** Spot type collisions between documents before they reach production.
* **Smart Modeling:** Identify unbounded keys (e.g., ID-based keys) to correctly model them as **Maps** instead of **Records**.
* **Schema Automation:** Instantly bootstrap schemas for new data sources.
w

## Building

Requires [Rust](https://rustup.rs) 1.70 or later.

```bash
# development build
cargo build

# install the binary to ~/.cargo/bin
cargo install --path .
```

## Quick Start

```bash
# Infer an Avro schema from a local file
schemata --pretty events.jsonl

# Get a full JSON report with statistics
schemata --pretty --output json events.jsonl

# Pipe from stdin
cat events.jsonl | schemata --pretty
```

## Usage

```
schemata [OPTIONS] [FILES]...
```

If no files are provided, `schemata` reads from stdin. Running it without
arguments or a pipe prints this help.

### Options

| Flag | Default | Description |
|---|---|---|
| `-n, --limit <N>` | — | Stop after N records |
| `--max-keys <N>` | `1000` | Distinct key threshold before a field is flagged as an unbounded map |
| `--distinct-cap <N>` | `1000` | Exact distinct value cap before switching to HyperLogLog++ |
| `-o, --output <FORMAT>` | `avro` | Output format: `avro` or `json` |
| `-p, --pretty` | — | Pretty-print the output |
| `-h, --help` | — | Print help |
| `-V, --version` | — | Print version |

### Path Notation

Fields are identified by dot-notation paths. Array elements use `$` as a
placeholder so the element schema is captured independently of array length.

| JSON | Path | Type |
|---|---|---|
| `{"a": 1}` | `a` | `integer` |
| `{"a": {"b": 1}}` | `a.b` | `integer` |
| `{"a": [1, 2]}` | `a` | `array` · `a.$` → `integer` |
| `{"a": [{"b": 1}]}` | `a.$.b` | `integer` |

## Output Formats

### Avro (default)

Emits a valid [Avro schema](https://avro.apache.org/docs/current/specification/)
in JSON encoding. The top-level schema is always a `record` named `Root`.

- Fields seen as both `null` and a concrete type become nullable unions: `["null", "long"]`
- Fields with a **type collision** (multiple non-null types) emit a union and a
  `doc` annotation so the problem is visible in the schema itself:
  ```json
  {
    "name": "score",
    "type": ["double", "string"],
    "doc": "TYPE COLLISION: float=4, string=1 — manual transformation required"
  }
  ```
- Fields whose distinct key count exceeds `--max-keys` become Avro `map` types
  with a `doc` annotation.
- Arrays become `{"type": "array", "items": <element_type>}`.
- Nested objects become named `record` types (PascalCase of the field name).

### JSON Report

Emits a flat JSON document keyed by path. Each entry contains the type counts,
field statistics, and any collision or unbounded-key annotations:

```json
{
  "record_count": 1000,
  "path_count": 42,
  "paths": {
    "user.age": {
      "type_counts": { "integer": 980, "null": 20 },
      "stats": {
        "count": 1000,
        "null_count": 20,
        "min": 18,
        "max": 95,
        "distinct_count": 78,
        "distinct_count_approximate": false
      }
    }
  }
}
```

Fields with type collisions include `"type_collision": true` and a
`"collision_types"` array. Unbounded-key objects include `"unbounded_keys": true`.

## Cloud Data Lakes

`schemata` reads plain JSONL from stdin. Decompression and cloud storage access
are handled by standard shell tools — pipe the data in and `schemata` does the
rest.

### Google Cloud Storage (GCS)

```bash
# Uncompressed file
gsutil cat gs://my-bucket/data/events.jsonl | schemata --pretty

# Gzip-compressed file
gsutil cat gs://my-bucket/data/events.jsonl.gz | gunzip | schemata --pretty

# Multiple objects via wildcard
gsutil cat gs://my-bucket/data/2024-*.jsonl | schemata --pretty

# Compressed wildcard
gsutil cat gs://my-bucket/data/2024-*.jsonl.gz | gunzip | schemata --pretty

# Save schema to a file (diagnostics go to stderr, schema to stdout)
gsutil cat gs://my-bucket/data/events.jsonl | schemata > schema.avsc
```

### Amazon S3

```bash
# Uncompressed file
aws s3 cp s3://my-bucket/data/events.jsonl - | schemata --pretty

# Gzip-compressed file
aws s3 cp s3://my-bucket/data/events.jsonl.gz - | gunzip | schemata --pretty

# Multiple objects (list then stream each)
aws s3 ls s3://my-bucket/data/ --recursive | awk '{print $4}' \
  | xargs -I{} aws s3 cp s3://my-bucket/{} - \
  | schemata --pretty

# Save schema to a file
aws s3 cp s3://my-bucket/data/events.jsonl - | schemata > schema.avsc
```

## Diagnostics

All diagnostic output goes to **stderr** so stdout always contains only the
schema. Redirect stderr separately if you need to capture both:

```bash
schemata events.jsonl > schema.avsc 2> warnings.log
```

| Message | Meaning |
|---|---|
| `[WARN] Type collision at path "x.y"` | Field observed with multiple non-null types — manual transformation likely needed |
| `[WARN] Path "x.y" has exceeded N distinct keys` | Field has too many distinct keys to be a fixed record — treated as a map |
| `[WARN] Skipping invalid JSON on record N` | A line could not be parsed as JSON and was skipped |
| `[INFO] Processed N records ...` | Summary printed after analysis completes |

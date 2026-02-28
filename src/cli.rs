use clap::{Parser, ValueEnum};
use std::path::PathBuf;

/// Analyse JSONL documents to infer schemas, detect type collisions, and emit Avro/JSON schemas.
///
/// Reads newline-delimited JSON (JSONL) from stdin or from one or more files.
/// For GCS data, pipe through gsutil:
///
///   gsutil cat gs://bucket/data.jsonl.gz | gunzip | schemata
///
///   gsutil cat gs://bucket/data.jsonl | schemata
#[derive(Parser, Debug)]
#[command(name = "schemata", version)]
pub struct Cli {
    /// Input JSONL files. If none are provided, reads from stdin.
    pub files: Vec<PathBuf>,

    /// Maximum number of records to analyse. Reads all records if not set.
    #[arg(short = 'n', long, value_name = "N")]
    pub limit: Option<usize>,

    /// Number of distinct keys an object field may have before it is flagged as an
    /// unbounded map (e.g. a field keyed by IDs). Once exceeded, the field is treated
    /// as an Avro `map` type and a warning is printed to stderr.
    #[arg(long, default_value = "1000", value_name = "N")]
    pub max_keys: usize,

    /// Number of distinct values to track exactly per field before switching to an
    /// approximate HyperLogLog counter. Keeping this low reduces memory usage for
    /// high-cardinality fields.
    #[arg(long, default_value = "1000", value_name = "N")]
    pub distinct_cap: usize,

    /// Output format.
    #[arg(short, long, default_value = "avro", value_name = "FORMAT")]
    pub output: OutputFormat,

    /// Pretty-print the output (applies to both avro and json formats).
    #[arg(short, long)]
    pub pretty: bool,
}

#[derive(ValueEnum, Clone, Debug, PartialEq, Eq)]
pub enum OutputFormat {
    /// Emit an Avro schema (JSON encoding of the Avro schema specification).
    Avro,
    /// Emit a detailed JSON report with path statistics, type counts, and collision warnings.
    Json,
}

mod cli;
mod input;
mod registry;
mod schema;
mod stats;
mod traversal;

use anyhow::{Context, Result};
use clap::{CommandFactory, Parser};
use std::io::IsTerminal;

use cli::{Cli, OutputFormat};
use input::open_inputs;
use registry::PathRegistry;
use traversal::traverse;

fn main() -> Result<()> {
    let cli = Cli::parse();

    // If no files were given and stdin is a TTY (not a pipe), show help and exit.
    if cli.files.is_empty() && std::io::stdin().is_terminal() {
        Cli::command().print_help()?;
        println!();
        std::process::exit(0);
    }

    let mut registry = PathRegistry::new(cli.max_keys, cli.distinct_cap);

    let lines = open_inputs(&cli.files)?;

    let mut processed = 0usize;
    let mut skipped = 0usize;

    for line_result in lines {
        // Honour the record limit.
        if let Some(limit) = cli.limit {
            if processed >= limit {
                break;
            }
        }

        let line = line_result.context("failed to read input line")?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        match serde_json::from_str::<serde_json::Value>(line) {
            Ok(value) => {
                let mut observations = Vec::new();
                traverse(&value, "", &mut observations);
                registry.process_observations(observations);
                registry.increment_records();
                processed += 1;
            }
            Err(e) => {
                skipped += 1;
                eprintln!(
                    "[WARN] Skipping invalid JSON on record {}: {}",
                    processed + skipped,
                    e
                );
            }
        }
    }

    // Emit type-collision warnings to stderr after all records are processed.
    registry.warn_type_collisions();

    // Print summary to stderr.
    eprintln!(
        "[INFO] Processed {} records ({} skipped), {} unique paths discovered.",
        processed,
        skipped,
        registry.entries.len()
    );

    // Emit output to stdout.
    let output = match cli.output {
        OutputFormat::Avro => {
            let mut schema = schema::avro::build_schema(&registry);
            schema::avro::annotate_collisions(&mut schema, &registry);
            if cli.pretty {
                serde_json::to_string_pretty(&schema)?
            } else {
                serde_json::to_string(&schema)?
            }
        }
        OutputFormat::Json => {
            let report = schema::json_report::build_report(&registry);
            if cli.pretty {
                serde_json::to_string_pretty(&report)?
            } else {
                serde_json::to_string(&report)?
            }
        }
    };

    println!("{}", output);

    Ok(())
}

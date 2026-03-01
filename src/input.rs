use anyhow::{Context, Result};
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::PathBuf;

/// A boxed iterator over lines from one or more input sources.
pub type LineIter = Box<dyn Iterator<Item = Result<String>>>;

/// Build a line iterator from the given file paths.
/// If `files` is empty, reads from stdin.
/// Files are read sequentially in the order provided.
pub fn open_inputs(files: &[PathBuf]) -> Result<LineIter> {
    if files.is_empty() {
        let reader = BufReader::new(io::stdin());
        let iter = reader
            .lines()
            .map(|r| r.context("failed to read line from stdin"));
        Ok(Box::new(iter))
    } else {
        let iters: Vec<LineIter> = files
            .iter()
            .map(|path| -> Result<LineIter> {
                let file = File::open(path)
                    .with_context(|| format!("failed to open file: {}", path.display()))?;
                let reader = BufReader::new(file);
                let path_str = path.display().to_string();
                let iter = reader.lines().map(move |r| {
                    r.with_context(|| format!("failed to read line from {path_str}"))
                });
                Ok(Box::new(iter))
            })
            .collect::<Result<_>>()?;

        Ok(Box::new(iters.into_iter().flatten()))
    }
}

//! Copy report models and mutable report builder.

use std::collections::BTreeMap;
use std::fmt;

use crate::spec::CopyErrorRecord;

/// Aggregate counters and diagnostics for one `copy_tree` run.
#[derive(Debug, Default, Clone)]
pub struct CopyReport {
    /// Number of scanned entries that matched filters.
    pub cnt_matched: u64,
    /// Total scanned directory/file entries.
    pub cnt_scanned: u64,
    /// Number of copied entries successfully committed.
    pub cnt_copied: u64,
    /// Number of entries skipped by strategy or dry-run.
    pub cnt_skipped: u64,
    /// Non-fatal warnings collected during traversal/copy.
    pub warnings: Vec<String>,
    /// Per-entry failures.
    pub errors: Vec<CopyErrorRecord>,
}

impl CopyReport {
    /// Number of collected hard errors.
    pub fn error_count(&self) -> usize {
        self.errors.len()
    }

    /// Number of collected warnings.
    pub fn warning_count(&self) -> usize {
        self.warnings.len()
    }

    /// Machine-readable counters.
    pub fn to_dict(&self) -> BTreeMap<String, u64> {
        let mut counts = BTreeMap::new();
        counts.insert("cnt_matched".to_string(), self.cnt_matched);
        counts.insert("cnt_scanned".to_string(), self.cnt_scanned);
        counts.insert("cnt_copied".to_string(), self.cnt_copied);
        counts.insert("cnt_skipped".to_string(), self.cnt_skipped);
        counts.insert("cnt_errors".to_string(), self.error_count() as u64);
        counts.insert("cnt_warnings".to_string(), self.warning_count() as u64);
        counts
    }

    /// Human-readable one-line summary.
    pub fn format(&self, prefix: &str) -> String {
        let counts = self.to_dict();
        format!(
            "{prefix} matched={} scanned={} copied={} skipped={} errors={} warnings={}",
            counts["cnt_matched"],
            counts["cnt_scanned"],
            counts["cnt_copied"],
            counts["cnt_skipped"],
            counts["cnt_errors"],
            counts["cnt_warnings"]
        )
    }
}

impl fmt::Display for CopyReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.format("[COPY]"))
    }
}

/// Mutable accumulator for copy statistics.
#[derive(Debug, Default, Clone)]
pub struct CopyReportBuilder {
    /// See [`CopyReport::cnt_matched`].
    pub cnt_matched: u64,
    /// See [`CopyReport::cnt_scanned`].
    pub cnt_scanned: u64,
    /// See [`CopyReport::cnt_copied`].
    pub cnt_copied: u64,
    /// See [`CopyReport::cnt_skipped`].
    pub cnt_skipped: u64,
    /// See [`CopyReport::errors`].
    pub errors: Vec<CopyErrorRecord>,
    /// See [`CopyReport::warnings`].
    pub warnings: Vec<String>,
}

impl CopyReportBuilder {
    /// Increment one or more named counters by `value`.
    ///
    /// Unknown names are ignored intentionally to keep call-sites concise.
    pub fn add_counts(&mut self, fields: &[&str], value: u64) {
        for _field in fields {
            match *_field {
                "cnt_matched" => self.cnt_matched += value,
                "cnt_scanned" => self.cnt_scanned += value,
                "cnt_copied" => self.cnt_copied += value,
                "cnt_skipped" => self.cnt_skipped += value,
                _ => {}
            }
        }
    }

    /// Increment matched count by one.
    pub fn add_matched(&mut self) {
        self.cnt_matched += 1;
    }

    /// Increment scanned count by one.
    pub fn add_scanned(&mut self) {
        self.cnt_scanned += 1;
    }

    /// Increment copied count by one.
    pub fn add_copied(&mut self) {
        self.cnt_copied += 1;
    }

    /// Increment skipped count by one.
    pub fn add_skipped(&mut self) {
        self.cnt_skipped += 1;
    }

    /// Add warning message.
    pub fn add_warning(&mut self, warning: String) {
        self.warnings.push(warning);
    }

    /// Add one path-scoped error.
    pub fn add_error(&mut self, path: std::path::PathBuf, exception: String) {
        self.errors.push(CopyErrorRecord { path, exception });
    }

    /// Finalize builder into immutable report.
    pub fn build(self) -> CopyReport {
        CopyReport {
            cnt_matched: self.cnt_matched,
            cnt_scanned: self.cnt_scanned,
            cnt_copied: self.cnt_copied,
            cnt_skipped: self.cnt_skipped,
            errors: self.errors,
            warnings: self.warnings,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CopyReport;

    #[test]
    fn report_copy_to_dict_and_format_match_python_style() {
        let report = CopyReport {
            cnt_matched: 5,
            cnt_scanned: 8,
            cnt_copied: 3,
            cnt_skipped: 2,
            warnings: vec!["w".to_string()],
            errors: vec![],
        };

        let counts = report.to_dict();
        assert_eq!(counts["cnt_matched"], 5);
        assert_eq!(counts["cnt_scanned"], 8);
        assert_eq!(counts["cnt_copied"], 3);
        assert_eq!(counts["cnt_skipped"], 2);
        assert_eq!(counts["cnt_errors"], 0);
        assert_eq!(counts["cnt_warnings"], 1);

        let txt = report.format("[COPY]");
        assert_eq!(
            txt,
            "[COPY] matched=5 scanned=8 copied=3 skipped=2 errors=0 warnings=1"
        );
        assert_eq!(report.to_string(), txt);
    }
}

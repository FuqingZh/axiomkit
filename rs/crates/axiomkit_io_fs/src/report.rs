//! Copy report models and mutable report builder.

use std::collections::BTreeMap;
use std::fmt;

use crate::spec::SpecCopyError;

/// Aggregate counters and diagnostics for one `copy_tree` run.
#[derive(Debug, Default, Clone)]
pub struct ReportCopy {
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
    pub errors: Vec<SpecCopyError>,
}

impl ReportCopy {
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
        let mut dict_counts = BTreeMap::new();
        dict_counts.insert("cnt_matched".to_string(), self.cnt_matched);
        dict_counts.insert("cnt_scanned".to_string(), self.cnt_scanned);
        dict_counts.insert("cnt_copied".to_string(), self.cnt_copied);
        dict_counts.insert("cnt_skipped".to_string(), self.cnt_skipped);
        dict_counts.insert("cnt_errors".to_string(), self.error_count() as u64);
        dict_counts.insert("cnt_warnings".to_string(), self.warning_count() as u64);
        dict_counts
    }

    /// Human-readable one-line summary.
    pub fn format(&self, prefix: &str) -> String {
        let dict_counts = self.to_dict();
        format!(
            "{prefix} matched={} scanned={} copied={} skipped={} errors={} warnings={}",
            dict_counts["cnt_matched"],
            dict_counts["cnt_scanned"],
            dict_counts["cnt_copied"],
            dict_counts["cnt_skipped"],
            dict_counts["cnt_errors"],
            dict_counts["cnt_warnings"]
        )
    }
}

impl fmt::Display for ReportCopy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.format("[COPY]"))
    }
}

/// Mutable accumulator for copy statistics.
#[derive(Debug, Default, Clone)]
pub struct ReportCopyBuilder {
    /// See [`ReportCopy::cnt_matched`].
    pub cnt_matched: u64,
    /// See [`ReportCopy::cnt_scanned`].
    pub cnt_scanned: u64,
    /// See [`ReportCopy::cnt_copied`].
    pub cnt_copied: u64,
    /// See [`ReportCopy::cnt_skipped`].
    pub cnt_skipped: u64,
    /// See [`ReportCopy::errors`].
    pub errors: Vec<SpecCopyError>,
    /// See [`ReportCopy::warnings`].
    pub warnings: Vec<String>,
}

impl ReportCopyBuilder {
    /// Increment one or more named counters by `value`.
    ///
    /// Unknown names are ignored intentionally to keep call-sites concise.
    pub fn add_counts(&mut self, field_names: &[&str], value: u64) {
        for field_name in field_names {
            match *field_name {
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
        self.errors.push(SpecCopyError { path, exception });
    }

    /// Finalize builder into immutable report.
    pub fn build(self) -> ReportCopy {
        ReportCopy {
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
    use super::ReportCopy;

    #[test]
    fn report_copy_to_dict_and_format_match_python_style() {
        let report = ReportCopy {
            cnt_matched: 5,
            cnt_scanned: 8,
            cnt_copied: 3,
            cnt_skipped: 2,
            warnings: vec!["w".to_string()],
            errors: vec![],
        };

        let dict_counts = report.to_dict();
        assert_eq!(dict_counts["cnt_matched"], 5);
        assert_eq!(dict_counts["cnt_scanned"], 8);
        assert_eq!(dict_counts["cnt_copied"], 3);
        assert_eq!(dict_counts["cnt_skipped"], 2);
        assert_eq!(dict_counts["cnt_errors"], 0);
        assert_eq!(dict_counts["cnt_warnings"], 1);

        let txt = report.format("[COPY]");
        assert_eq!(
            txt,
            "[COPY] matched=5 scanned=8 copied=3 skipped=2 errors=0 warnings=1"
        );
        assert_eq!(report.to_string(), txt);
    }
}

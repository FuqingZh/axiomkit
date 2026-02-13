use std::collections::BTreeMap;

use axiomkit_io_fs::{
    CopyTreeError, EnumCopyDepthLimitMode, EnumCopyDirectoryConflictStrategy,
    EnumCopyFileConflictStrategy, EnumCopyPatternMode, EnumCopySymlinkStrategy, ReportCopy,
    SpecCopyError, SpecCopyOptions, copy_tree,
};
use pyo3::exceptions::{PyNotADirectoryError, PyOSError, PyValueError};
use pyo3::prelude::*;

const N_BRIDGE_ABI_VERSION: u64 = 1;
const C_BRIDGE_CONTRACT_VERSION: &str = "axiomkit.fs.copy_tree.v1";
const C_BRIDGE_TRANSPORT: &str = "rust_native";

#[pyclass(name = "SpecCopyError")]
#[derive(Debug, Clone)]
struct PySpecCopyError {
    #[pyo3(get)]
    path: String,
    #[pyo3(get)]
    exception: String,
}

impl From<SpecCopyError> for PySpecCopyError {
    fn from(spec_error: SpecCopyError) -> Self {
        Self {
            path: spec_error.path.to_string_lossy().to_string(),
            exception: spec_error.exception,
        }
    }
}

#[pyclass(name = "ReportCopy")]
#[derive(Debug, Clone)]
struct PyReportCopy {
    #[pyo3(get)]
    cnt_matched: u64,
    #[pyo3(get)]
    cnt_scanned: u64,
    #[pyo3(get)]
    cnt_copied: u64,
    #[pyo3(get)]
    cnt_skipped: u64,
    #[pyo3(get)]
    warnings: Vec<String>,
    #[pyo3(get)]
    errors: Vec<PySpecCopyError>,
}

impl From<ReportCopy> for PyReportCopy {
    fn from(report_copy: ReportCopy) -> Self {
        Self {
            cnt_matched: report_copy.cnt_matched,
            cnt_scanned: report_copy.cnt_scanned,
            cnt_copied: report_copy.cnt_copied,
            cnt_skipped: report_copy.cnt_skipped,
            warnings: report_copy.warnings,
            errors: report_copy
                .errors
                .into_iter()
                .map(PySpecCopyError::from)
                .collect(),
        }
    }
}

#[pymethods]
impl PyReportCopy {
    #[getter]
    fn error_count(&self) -> usize {
        self.errors.len()
    }

    #[getter]
    fn warning_count(&self) -> usize {
        self.warnings.len()
    }

    fn to_dict(&self) -> BTreeMap<String, u64> {
        let mut dict_counts = BTreeMap::new();
        dict_counts.insert("cnt_matched".to_string(), self.cnt_matched);
        dict_counts.insert("cnt_scanned".to_string(), self.cnt_scanned);
        dict_counts.insert("cnt_copied".to_string(), self.cnt_copied);
        dict_counts.insert("cnt_skipped".to_string(), self.cnt_skipped);
        dict_counts.insert("cnt_errors".to_string(), self.error_count() as u64);
        dict_counts.insert("cnt_warnings".to_string(), self.warning_count() as u64);
        dict_counts
    }

    #[pyo3(signature = (prefix = "[COPY]"))]
    fn format(&self, prefix: &str) -> String {
        format!(
            "{prefix} matched={} scanned={} copied={} skipped={} errors={} warnings={}",
            self.cnt_matched,
            self.cnt_scanned,
            self.cnt_copied,
            self.cnt_skipped,
            self.error_count(),
            self.warning_count()
        )
    }

    fn __str__(&self) -> String {
        self.format("[COPY]")
    }
}

fn parse_rule_pattern(value: &str) -> PyResult<EnumCopyPatternMode> {
    match value {
        "glob" => Ok(EnumCopyPatternMode::Glob),
        "regex" => Ok(EnumCopyPatternMode::Regex),
        "literal" => Ok(EnumCopyPatternMode::Literal),
        _ => Err(PyValueError::new_err(format!(
            "Invalid pattern strategy: `{value}`. Expected one of: ['glob', 'regex', 'literal']"
        ))),
    }
}

fn parse_rule_conflict_file(value: &str) -> PyResult<EnumCopyFileConflictStrategy> {
    match value {
        "skip" => Ok(EnumCopyFileConflictStrategy::Skip),
        "overwrite" => Ok(EnumCopyFileConflictStrategy::Overwrite),
        "error" => Ok(EnumCopyFileConflictStrategy::Error),
        _ => Err(PyValueError::new_err(format!(
            "Invalid file conflict strategy: `{value}`. Expected one of: ['skip', 'overwrite', 'error']"
        ))),
    }
}

fn parse_rule_conflict_dir(value: &str) -> PyResult<EnumCopyDirectoryConflictStrategy> {
    match value {
        "skip" => Ok(EnumCopyDirectoryConflictStrategy::Skip),
        "merge" => Ok(EnumCopyDirectoryConflictStrategy::Merge),
        "error" => Ok(EnumCopyDirectoryConflictStrategy::Error),
        _ => Err(PyValueError::new_err(format!(
            "Invalid directory conflict strategy: `{value}`. Expected one of: ['skip', 'merge', 'error']"
        ))),
    }
}

fn parse_rule_symlink(value: &str) -> PyResult<EnumCopySymlinkStrategy> {
    match value {
        "dereference" => Ok(EnumCopySymlinkStrategy::Dereference),
        "copy_symlinks" => Ok(EnumCopySymlinkStrategy::CopySymlinks),
        "skip_symlinks" => Ok(EnumCopySymlinkStrategy::SkipSymlinks),
        _ => Err(PyValueError::new_err(format!(
            "Invalid symlink strategy: `{value}`. Expected one of: ['dereference', 'copy_symlinks', 'skip_symlinks']"
        ))),
    }
}

fn parse_rule_depth_limit(value: &str) -> PyResult<EnumCopyDepthLimitMode> {
    match value {
        "at_most" => Ok(EnumCopyDepthLimitMode::AtMost),
        "exact" => Ok(EnumCopyDepthLimitMode::Exact),
        _ => Err(PyValueError::new_err(format!(
            "Invalid depth mode: `{value}`. Expected one of: ['at_most', 'exact']"
        ))),
    }
}

fn map_copy_tree_error(exception: CopyTreeError) -> PyErr {
    match exception {
        CopyTreeError::SourceNotDirectory(path_src) => PyNotADirectoryError::new_err(format!(
            "Source is not a directory: {}",
            path_src.display()
        )),
        CopyTreeError::DestinationInitFailed { path, message } => PyOSError::new_err(format!(
            "Failed to initialize destination {}: {message}",
            path.display()
        )),
        CopyTreeError::InvalidDepthLimit(message) | CopyTreeError::InvalidPattern(message) => {
            PyValueError::new_err(message)
        }
        CopyTreeError::SourceDestinationOverlap {
            source,
            destination,
        } => PyValueError::new_err(format!(
            "Source and destination directories overlap: {} <-> {}",
            source.display(),
            destination.display()
        )),
    }
}

#[pyfunction(name = "copy_tree")]
#[pyo3(signature = (
    dir_source,
    dir_destination,
    patterns_include_files = None,
    patterns_exclude_files = None,
    patterns_include_dirs = None,
    patterns_exclude_dirs = None,
    rule_pattern = "glob",
    rule_conflict_file = "skip",
    rule_conflict_dir = "skip",
    rule_symlink = "copy_symlinks",
    depth_limit = None,
    rule_depth_limit = "at_most",
    num_workers_max = None,
    if_keep_tree = true,
    if_dry_run = false
))]
#[allow(clippy::too_many_arguments)]
fn copy_tree_py(
    py: Python<'_>,
    dir_source: String,
    dir_destination: String,
    patterns_include_files: Option<Vec<String>>,
    patterns_exclude_files: Option<Vec<String>>,
    patterns_include_dirs: Option<Vec<String>>,
    patterns_exclude_dirs: Option<Vec<String>>,
    rule_pattern: &str,
    rule_conflict_file: &str,
    rule_conflict_dir: &str,
    rule_symlink: &str,
    depth_limit: Option<usize>,
    rule_depth_limit: &str,
    num_workers_max: Option<usize>,
    if_keep_tree: bool,
    if_dry_run: bool,
) -> PyResult<PyReportCopy> {
    let spec_cp_options = SpecCopyOptions {
        patterns_include_files,
        patterns_exclude_files,
        patterns_include_dirs,
        patterns_exclude_dirs,
        rule_pattern: parse_rule_pattern(rule_pattern)?,
        rule_conflict_file: parse_rule_conflict_file(rule_conflict_file)?,
        rule_conflict_dir: parse_rule_conflict_dir(rule_conflict_dir)?,
        rule_symlink: parse_rule_symlink(rule_symlink)?,
        depth_limit,
        rule_depth_limit: parse_rule_depth_limit(rule_depth_limit)?,
        num_workers_max,
        if_keep_tree,
        if_dry_run,
    };

    let report_copy = py.allow_threads(|| copy_tree(dir_source, dir_destination, spec_cp_options));
    let report_copy = report_copy.map_err(map_copy_tree_error)?;
    Ok(PyReportCopy::from(report_copy))
}

#[pymodule]
fn _axiomkit_io_fs_rs(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PySpecCopyError>()?;
    module.add_class::<PyReportCopy>()?;
    module.add_function(wrap_pyfunction!(copy_tree_py, module)?)?;
    module.add("__bridge_abi__", N_BRIDGE_ABI_VERSION)?;
    module.add("__bridge_contract__", C_BRIDGE_CONTRACT_VERSION)?;
    module.add("__bridge_transport__", C_BRIDGE_TRANSPORT)?;
    Ok(())
}

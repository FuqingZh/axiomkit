use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{StructArray, TryExtend};
use arrow::datatypes::{ArrowDataType, ArrowSchema, Field as ArrowField};
use arrow::record_batch::RecordBatchT;
use axiomkit_io_fs::{
    CopyDepthLimitMode, CopyDirectoryConflictStrategy, CopyErrorRecord, CopyFileConflictStrategy,
    CopyOptionsSpec, CopyPatternMode, CopyReport, CopySymlinkStrategy, CopyTreeError, copy_tree,
};
use axiomkit_io_xlsx::constant::{
    ColumnIdentifier, derive_default_xlsx_formats, derive_default_xlsx_write_options,
};
use axiomkit_io_xlsx::spec::{
    AutofitCellsPolicySpec, AutofitColumnsRule, CellFormatSpec, IntegerCoerceMode,
    ScientificPolicySpec, ScientificScope, SheetSliceSpec, XlsxValuePolicySpec,
    XlsxWriteOptionsSpec,
};
use axiomkit_io_xlsx::{XlsxSheetWriteOptionsSpec, XlsxWriter as RsXlsxWriter};
use polars::prelude::DataFrame;
use pyo3::exceptions::{PyNotADirectoryError, PyOSError, PyRuntimeError, PyValueError};
use pyo3::ffi as pyffi;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyList, PyModule, PyTuple};

const FS_BRIDGE_ABI_VERSION: u64 = 1;
const FS_BRIDGE_CONTRACT_VERSION: &str = "axiomkit.fs.copy_tree.v1";
const FS_BRIDGE_TRANSPORT: &str = "rust_native";

const XLSX_BRIDGE_ABI_VERSION: u64 = 1;
const XLSX_BRIDGE_CONTRACT_VERSION: &str = "axiomkit.xlsx.writer.v1";
const XLSX_BRIDGE_TRANSPORT: &str = "arrow_c_data";
const C_ARROW_ARRAY_STREAM_CAPSULE_NAME: &[u8] = b"arrow_array_stream\0";

#[pyclass(name = "CopyErrorRecord")]
#[derive(Debug, Clone)]
struct PySpecCopyError {
    #[pyo3(get)]
    path: String,
    #[pyo3(get)]
    exception: String,
}

impl From<CopyErrorRecord> for PySpecCopyError {
    fn from(error_record: CopyErrorRecord) -> Self {
        Self {
            path: error_record.path.to_string_lossy().to_string(),
            exception: error_record.exception,
        }
    }
}

#[pyclass(name = "CopyReport")]
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

impl From<CopyReport> for PyReportCopy {
    fn from(report_copy: CopyReport) -> Self {
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
        let mut counts = BTreeMap::new();
        counts.insert("cnt_matched".to_string(), self.cnt_matched);
        counts.insert("cnt_scanned".to_string(), self.cnt_scanned);
        counts.insert("cnt_copied".to_string(), self.cnt_copied);
        counts.insert("cnt_skipped".to_string(), self.cnt_skipped);
        counts.insert("cnt_errors".to_string(), self.error_count() as u64);
        counts.insert("cnt_warnings".to_string(), self.warning_count() as u64);
        counts
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

fn parse_rule_pattern(value: &str) -> PyResult<CopyPatternMode> {
    match value {
        "glob" => Ok(CopyPatternMode::Glob),
        "regex" => Ok(CopyPatternMode::Regex),
        "literal" => Ok(CopyPatternMode::Literal),
        _ => Err(PyValueError::new_err(format!(
            "Invalid pattern strategy: `{value}`. Expected one of: ['glob', 'regex', 'literal']"
        ))),
    }
}

fn parse_rule_conflict_file(value: &str) -> PyResult<CopyFileConflictStrategy> {
    match value {
        "skip" => Ok(CopyFileConflictStrategy::Skip),
        "overwrite" => Ok(CopyFileConflictStrategy::Overwrite),
        "error" => Ok(CopyFileConflictStrategy::Error),
        _ => Err(PyValueError::new_err(format!(
            "Invalid file conflict strategy: `{value}`. Expected one of: ['skip', 'overwrite', 'error']"
        ))),
    }
}

fn parse_rule_conflict_dir(value: &str) -> PyResult<CopyDirectoryConflictStrategy> {
    match value {
        "skip" => Ok(CopyDirectoryConflictStrategy::Skip),
        "merge" => Ok(CopyDirectoryConflictStrategy::Merge),
        "error" => Ok(CopyDirectoryConflictStrategy::Error),
        _ => Err(PyValueError::new_err(format!(
            "Invalid directory conflict strategy: `{value}`. Expected one of: ['skip', 'merge', 'error']"
        ))),
    }
}

fn parse_rule_symlink(value: &str) -> PyResult<CopySymlinkStrategy> {
    match value {
        "dereference" => Ok(CopySymlinkStrategy::Dereference),
        "copy_symlinks" => Ok(CopySymlinkStrategy::CopySymlinks),
        "skip_symlinks" => Ok(CopySymlinkStrategy::SkipSymlinks),
        _ => Err(PyValueError::new_err(format!(
            "Invalid symlink strategy: `{value}`. Expected one of: ['dereference', 'copy_symlinks', 'skip_symlinks']"
        ))),
    }
}

fn parse_rule_depth_limit(value: &str) -> PyResult<CopyDepthLimitMode> {
    match value {
        "at_most" => Ok(CopyDepthLimitMode::AtMost),
        "exact" => Ok(CopyDepthLimitMode::Exact),
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
    workers_max = None,
    should_keep_tree = true,
    should_dry_run = false
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
    workers_max: Option<usize>,
    should_keep_tree: bool,
    should_dry_run: bool,
) -> PyResult<PyReportCopy> {
    let copy_options = CopyOptionsSpec {
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
        workers_max,
        should_keep_tree,
        should_dry_run,
    };

    let report = py.allow_threads(|| copy_tree(dir_source, dir_destination, copy_options));
    let report = report.map_err(map_copy_tree_error)?;
    Ok(PyReportCopy::from(report))
}

#[pyclass(name = "XlsxWriter")]
struct PyXlsxWriter {
    #[pyo3(get)]
    file_out: String,
    inner: RsXlsxWriter,
}

#[pymethods]
impl PyXlsxWriter {
    #[new]
    #[pyo3(signature = (
        file_out,
        fmt_text = None,
        fmt_integer = None,
        fmt_decimal = None,
        fmt_scientific = None,
        fmt_header = None,
        write_options = None
    ))]
    fn new(
        file_out: String,
        fmt_text: Option<&Bound<'_, PyAny>>,
        fmt_integer: Option<&Bound<'_, PyAny>>,
        fmt_decimal: Option<&Bound<'_, PyAny>>,
        fmt_scientific: Option<&Bound<'_, PyAny>>,
        fmt_header: Option<&Bound<'_, PyAny>>,
        write_options: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let path_file_out = PathBuf::from(&file_out);

        let dict_default_fmts = derive_default_xlsx_formats();
        let cfg_fmt_text_default = dict_default_fmts
            .get("text")
            .cloned()
            .ok_or_else(|| PyValueError::new_err("Missing default format: text"))?;
        let cfg_fmt_int_default = dict_default_fmts
            .get("integer")
            .cloned()
            .ok_or_else(|| PyValueError::new_err("Missing default format: integer"))?;
        let cfg_fmt_dec_default = dict_default_fmts
            .get("decimal")
            .cloned()
            .ok_or_else(|| PyValueError::new_err("Missing default format: decimal"))?;
        let cfg_fmt_sci_default = dict_default_fmts
            .get("scientific")
            .cloned()
            .ok_or_else(|| PyValueError::new_err("Missing default format: scientific"))?;
        let cfg_fmt_header_default = dict_default_fmts
            .get("header")
            .cloned()
            .ok_or_else(|| PyValueError::new_err("Missing default format: header"))?;

        let c_fmt_text = parse_spec_cell_format(fmt_text)?.unwrap_or(cfg_fmt_text_default);
        let c_fmt_integer = parse_spec_cell_format(fmt_integer)?.unwrap_or(cfg_fmt_int_default);
        let c_fmt_decimal = parse_spec_cell_format(fmt_decimal)?.unwrap_or(cfg_fmt_dec_default);
        let c_fmt_scientific =
            parse_spec_cell_format(fmt_scientific)?.unwrap_or(cfg_fmt_sci_default);
        let c_fmt_header = parse_spec_cell_format(fmt_header)?.unwrap_or(cfg_fmt_header_default);

        let cfg_write_options = parse_spec_xlsx_write_options(write_options)?
            .unwrap_or_else(derive_default_xlsx_write_options);

        let inner = RsXlsxWriter::new(
            path_file_out,
            c_fmt_text,
            c_fmt_integer,
            c_fmt_decimal,
            c_fmt_scientific,
            c_fmt_header,
            cfg_write_options,
        );

        Ok(Self { file_out, inner })
    }

    fn __enter__(slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc=None, _tb=None))]
    fn __exit__(
        &mut self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc: Option<&Bound<'_, PyAny>>,
        _tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        self.close()
    }

    fn close(&mut self) -> PyResult<()> {
        self.inner.close().map_err(PyRuntimeError::new_err)
    }

    fn report(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let l_reports = self.inner.report();

        let module_spec = py.import("axiomkit.io.xlsx.spec")?;
        let cls_spec_sheet_slice = module_spec.getattr("SheetSliceSpec")?;
        let cls_spec_xlsx_report = module_spec.getattr("XlsxReport")?;

        let mut l_report_obj = Vec::with_capacity(l_reports.len());
        for report in l_reports {
            let mut l_sheet_obj = Vec::with_capacity(report.sheets.len());
            for sheet in report.sheets {
                l_sheet_obj.push(create_sheet_slice_object(&cls_spec_sheet_slice, &sheet)?);
            }

            let inst_report =
                cls_spec_xlsx_report.call1((PyList::new(py, l_sheet_obj)?, report.warnings))?;
            l_report_obj.push(inst_report.unbind());
        }

        let tup_report = PyTuple::new(py, l_report_obj)?;
        Ok(tup_report.into_any().unbind())
    }

    #[pyo3(signature = (
        df,
        sheet_name,
        df_header = None,
        cols_integer = None,
        cols_decimal = None,
        num_frozen_cols = 0,
        num_frozen_rows = None,
        should_merge_header = false,
        should_keep_missing_values = None,
        policy_autofit = None,
        policy_scientific = None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn write_sheet<'py>(
        mut slf: PyRefMut<'py, Self>,
        py: Python<'py>,
        df: &Bound<'py, PyAny>,
        sheet_name: &str,
        df_header: Option<&Bound<'py, PyAny>>,
        cols_integer: Option<&Bound<'py, PyAny>>,
        cols_decimal: Option<&Bound<'py, PyAny>>,
        num_frozen_cols: usize,
        num_frozen_rows: Option<usize>,
        should_merge_header: bool,
        should_keep_missing_values: Option<bool>,
        policy_autofit: Option<&Bound<'py, PyAny>>,
        policy_scientific: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let df_data = derive_dataframe_from_any_dataframe(py, df)?;
        let df_header_data = match df_header {
            Some(df_header_raw) if !df_header_raw.is_none() => {
                Some(derive_dataframe_from_any_dataframe(py, df_header_raw)?)
            }
            Some(_) => None,
            None => None,
        };

        let cfg_sheet_write_options = XlsxSheetWriteOptionsSpec {
            cols_integer: parse_column_refs(cols_integer)?,
            cols_decimal: parse_column_refs(cols_decimal)?,
            num_frozen_cols,
            num_frozen_rows,
            should_merge_header,
            should_keep_missing_values,
            policy_autofit: parse_spec_autofit_cells_policy(policy_autofit)?
                .unwrap_or_else(AutofitCellsPolicySpec::default),
            policy_scientific: parse_spec_scientific_policy(policy_scientific)?
                .unwrap_or_else(ScientificPolicySpec::default),
        };

        slf.inner
            .write_sheet_from_dataframes(
                &df_data,
                sheet_name,
                df_header_data.as_ref(),
                &cfg_sheet_write_options,
            )
            .map_err(PyValueError::new_err)?;

        Ok(slf)
    }
}

fn create_sheet_slice_object(
    cls_spec_sheet_slice: &Bound<'_, PyAny>,
    sheet: &SheetSliceSpec,
) -> PyResult<Py<PyAny>> {
    let inst_sheet = cls_spec_sheet_slice.call1((
        sheet.sheet_name.clone(),
        sheet.row_start_inclusive,
        sheet.row_end_exclusive,
        sheet.col_start_inclusive,
        sheet.col_end_exclusive,
    ))?;
    Ok(inst_sheet.into_any().unbind())
}

fn derive_dataframe_from_any_dataframe(
    py: Python<'_>,
    df: &Bound<'_, PyAny>,
) -> PyResult<DataFrame> {
    let df_polars = convert_to_polars_dataframe(py, df)?;
    let obj_capsule = df_polars.call_method0("__arrow_c_stream__")?;
    derive_dataframe_from_arrow_c_stream_capsule(&obj_capsule)
}

fn derive_dataframe_from_arrow_c_stream_capsule(
    obj_capsule: &Bound<'_, PyAny>,
) -> PyResult<DataFrame> {
    let ptr_capsule = obj_capsule.as_ptr();
    let ptr_stream_name = C_ARROW_ARRAY_STREAM_CAPSULE_NAME
        .as_ptr()
        .cast::<std::os::raw::c_char>();

    let is_capsule_valid = unsafe { pyffi::PyCapsule_IsValid(ptr_capsule, ptr_stream_name) };
    if is_capsule_valid == 0 {
        return Err(PyValueError::new_err(
            "Expected a valid `arrow_array_stream` PyCapsule.",
        ));
    }

    let ptr_stream = unsafe { pyffi::PyCapsule_GetPointer(ptr_capsule, ptr_stream_name) };
    if ptr_stream.is_null() {
        return Err(PyValueError::new_err(
            "Arrow C stream capsule pointer is null.",
        ));
    }

    let stream = ptr_stream.cast::<arrow::ffi::ArrowArrayStream>();
    let mut reader = unsafe { arrow::ffi::ArrowArrayStreamReader::try_new(&mut *stream) }
        .map_err(|err| PyValueError::new_err(format!("Failed to open Arrow C stream: {err}")))?;

    let schema_arrow = derive_arrow_schema_from_stream_field(reader.field())?;
    let schema_ref = Arc::new(schema_arrow.clone());
    let mut df = DataFrame::empty_with_arrow_schema(&schema_arrow);

    while let Some(res_array) = unsafe { reader.next() } {
        let array_row_batch = res_array.map_err(|err| {
            PyValueError::new_err(format!("Failed to read Arrow stream batch: {err}"))
        })?;

        let array_struct = array_row_batch
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| {
                PyValueError::new_err(
                    "Arrow C stream must yield StructArray batches for DataFrame import.",
                )
            })?;

        let l_arrays = array_struct.values().to_vec();
        let record_batch = RecordBatchT::try_new(array_struct.len(), schema_ref.clone(), l_arrays)
            .map_err(|err| {
                PyValueError::new_err(format!(
                    "Failed to construct Arrow record batch from stream: {err}"
                ))
            })?;

        df.try_extend(std::iter::once(record_batch))
            .map_err(|err| {
                PyValueError::new_err(format!(
                    "Failed to append Arrow record batch to DataFrame: {err}"
                ))
            })?;
    }

    Ok(df)
}

fn derive_arrow_schema_from_stream_field(field: &ArrowField) -> PyResult<ArrowSchema> {
    match field.dtype() {
        ArrowDataType::Struct(fields) => Ok(fields
            .iter()
            .cloned()
            .map(|field_inner| (field_inner.name.clone(), field_inner))
            .collect::<ArrowSchema>()),
        dtype => Err(PyValueError::new_err(format!(
            "Arrow stream schema must be Struct, got: {dtype:?}"
        ))),
    }
}

fn parse_spec_cell_format(obj: Option<&Bound<'_, PyAny>>) -> PyResult<Option<CellFormatSpec>> {
    let Some(obj) = obj else {
        return Ok(None);
    };
    if obj.is_none() {
        return Ok(None);
    }

    Ok(Some(CellFormatSpec {
        font_name: extract_optional_attr::<String>(obj, "font_name")?,
        font_size: extract_optional_attr::<i64>(obj, "font_size")?,
        bold: extract_optional_attr::<bool>(obj, "bold")?,
        italic: extract_optional_attr::<bool>(obj, "italic")?,
        align: extract_optional_attr::<String>(obj, "align")?,
        valign: extract_optional_attr::<String>(obj, "valign")?,
        border: extract_optional_attr::<i64>(obj, "border")?,
        text_wrap: extract_optional_attr::<bool>(obj, "text_wrap")?,
        top: extract_optional_attr::<i64>(obj, "top")?,
        bottom: extract_optional_attr::<i64>(obj, "bottom")?,
        left: extract_optional_attr::<i64>(obj, "left")?,
        right: extract_optional_attr::<i64>(obj, "right")?,
        num_format: extract_optional_attr::<String>(obj, "num_format")?,
        bg_color: extract_optional_attr::<String>(obj, "bg_color")?,
        font_color: extract_optional_attr::<String>(obj, "font_color")?,
    }))
}

fn parse_spec_xlsx_write_options(
    obj: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<XlsxWriteOptionsSpec>> {
    let Some(obj) = obj else {
        return Ok(None);
    };
    if obj.is_none() {
        return Ok(None);
    }

    let mut cfg_write_options = derive_default_xlsx_write_options();

    if let Some(value_policy_obj) = extract_optional_attr_bound(obj, "value_policy")? {
        let mut value_policy = XlsxValuePolicySpec::default();
        if let Some(v) = extract_optional_attr::<String>(&value_policy_obj, "missing_value_str")? {
            value_policy.missing_value_str = v;
        }
        if let Some(v) = extract_optional_attr::<String>(&value_policy_obj, "nan_str")? {
            value_policy.nan_str = v;
        }
        if let Some(v) = extract_optional_attr::<String>(&value_policy_obj, "posinf_str")? {
            value_policy.posinf_str = v;
        }
        if let Some(v) = extract_optional_attr::<String>(&value_policy_obj, "neginf_str")? {
            value_policy.neginf_str = v;
        }
        if let Some(v) = extract_optional_attr::<String>(&value_policy_obj, "integer_coerce")? {
            value_policy.integer_coerce = if v == "coerce" {
                IntegerCoerceMode::Coerce
            } else {
                IntegerCoerceMode::Strict
            };
        }
        cfg_write_options.value_policy = value_policy;
    }

    if let Some(v) = extract_optional_attr::<bool>(obj, "should_keep_missing_values")? {
        cfg_write_options.should_keep_missing_values = v;
    }
    if let Some(v) = extract_optional_attr::<bool>(obj, "should_infer_numeric_cols")? {
        cfg_write_options.should_infer_numeric_cols = v;
    }
    if let Some(v) = extract_optional_attr::<bool>(obj, "should_infer_integer_cols")? {
        cfg_write_options.should_infer_integer_cols = v;
    }

    if let Some(row_chunk_policy_obj) = extract_optional_attr_bound(obj, "row_chunk_policy")? {
        if let Some(v) = extract_optional_attr::<usize>(&row_chunk_policy_obj, "width_large")? {
            cfg_write_options.row_chunk_policy.width_large = v;
        }
        if let Some(v) = extract_optional_attr::<usize>(&row_chunk_policy_obj, "width_medium")? {
            cfg_write_options.row_chunk_policy.width_medium = v;
        }
        if let Some(v) = extract_optional_attr::<usize>(&row_chunk_policy_obj, "size_large")? {
            cfg_write_options.row_chunk_policy.size_large = v;
        }
        if let Some(v) = extract_optional_attr::<usize>(&row_chunk_policy_obj, "size_medium")? {
            cfg_write_options.row_chunk_policy.size_medium = v;
        }
        if let Some(v) = extract_optional_attr::<usize>(&row_chunk_policy_obj, "size_default")? {
            cfg_write_options.row_chunk_policy.size_default = v;
        }
        if let Some(v) = extract_optional_attr::<usize>(&row_chunk_policy_obj, "fixed_size")? {
            cfg_write_options.row_chunk_policy.fixed_size = Some(v);
        }
    }

    if let Some(base_format_patch_obj) = extract_optional_attr_bound(obj, "base_format_patch")?
        && let Some(fmt_patch) = parse_spec_cell_format(Some(&base_format_patch_obj))?
    {
        cfg_write_options.base_format_patch = fmt_patch;
    }

    Ok(Some(cfg_write_options))
}

fn parse_rule_autofit_columns(value: &str) -> PyResult<AutofitColumnsRule> {
    match value {
        "none" => Ok(AutofitColumnsRule::None),
        "header" => Ok(AutofitColumnsRule::Header),
        "body" => Ok(AutofitColumnsRule::Body),
        "all" => Ok(AutofitColumnsRule::All),
        _ => Err(PyValueError::new_err(
            "policy_autofit.rule_columns must be one of: 'none', 'header', 'body', 'all'.",
        )),
    }
}

fn parse_rule_scientific_scope(value: &str) -> PyResult<ScientificScope> {
    match value {
        "none" => Ok(ScientificScope::None),
        "decimal" => Ok(ScientificScope::Decimal),
        "integer" => Ok(ScientificScope::Integer),
        "all" => Ok(ScientificScope::All),
        _ => Err(PyValueError::new_err(
            "policy_scientific.rule_scope must be one of: 'none', 'decimal', 'integer', 'all'.",
        )),
    }
}

fn parse_spec_autofit_cells_policy(
    obj: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<AutofitCellsPolicySpec>> {
    let Some(obj) = obj else {
        return Ok(None);
    };
    if obj.is_none() {
        return Ok(None);
    }

    let mut policy = AutofitCellsPolicySpec::default();

    if let Some(v) = extract_optional_attr::<String>(obj, "rule_columns")? {
        policy.rule_columns = parse_rule_autofit_columns(&v)?;
    }
    if obj.hasattr("height_body_inferred_max")? {
        let val = obj.getattr("height_body_inferred_max")?;
        if val.is_none() {
            policy.height_body_inferred_max = None;
        } else {
            policy.height_body_inferred_max = Some(val.extract::<usize>()?);
        }
    }
    if let Some(v) = extract_optional_attr::<usize>(obj, "width_cell_min")? {
        policy.width_cell_min = v;
    }
    if let Some(v) = extract_optional_attr::<usize>(obj, "width_cell_max")? {
        policy.width_cell_max = v;
    }
    if let Some(v) = extract_optional_attr::<usize>(obj, "width_cell_padding")? {
        policy.width_cell_padding = v;
    }

    Ok(Some(policy))
}

fn parse_spec_scientific_policy(
    obj: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<ScientificPolicySpec>> {
    let Some(obj) = obj else {
        return Ok(None);
    };
    if obj.is_none() {
        return Ok(None);
    }

    let mut policy = ScientificPolicySpec::default();

    if let Some(v) = extract_optional_attr::<String>(obj, "rule_scope")? {
        policy.rule_scope = parse_rule_scientific_scope(&v)?;
    }
    if let Some(v) = extract_optional_attr::<f64>(obj, "thr_min")? {
        policy.thr_min = v;
    }
    if let Some(v) = extract_optional_attr::<f64>(obj, "thr_max")? {
        policy.thr_max = v;
    }
    if obj.hasattr("height_body_inferred_max")? {
        let val = obj.getattr("height_body_inferred_max")?;
        if val.is_none() {
            policy.height_body_inferred_max = None;
        } else {
            policy.height_body_inferred_max = Some(val.extract::<usize>()?);
        }
    }

    Ok(Some(policy))
}

fn parse_column_refs(value: Option<&Bound<'_, PyAny>>) -> PyResult<Option<Vec<ColumnIdentifier>>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_none() {
        return Ok(None);
    }

    if let Ok(b) = value.extract::<bool>() {
        if !b {
            return Ok(None);
        }
        return Err(PyValueError::new_err(
            "Column refs must be str, int, sequence[str | int], False, or None.",
        ));
    }

    if let Ok(c_value) = value.extract::<String>() {
        return Ok(Some(vec![ColumnIdentifier::Name(c_value)]));
    }
    if let Ok(idx) = value.extract::<usize>() {
        return Ok(Some(vec![ColumnIdentifier::Index(idx)]));
    }

    if let Ok(iter) = value.try_iter() {
        let mut refs = Vec::new();
        for item in iter {
            let item = item?;
            refs.push(parse_single_column_ref(&item)?);
        }
        return Ok(Some(refs));
    }

    Err(PyValueError::new_err(
        "Column refs must be str, int, sequence[str | int], False, or None.",
    ))
}

fn parse_single_column_ref(value: &Bound<'_, PyAny>) -> PyResult<ColumnIdentifier> {
    if let Ok(b) = value.extract::<bool>() {
        return Err(PyValueError::new_err(format!(
            "Column ref items must be str or int, got bool {b}."
        )));
    }
    if let Ok(name) = value.extract::<String>() {
        return Ok(ColumnIdentifier::Name(name));
    }
    if let Ok(idx) = value.extract::<usize>() {
        return Ok(ColumnIdentifier::Index(idx));
    }
    Err(PyValueError::new_err(
        "Column ref items must be str or int.",
    ))
}

fn convert_to_polars_dataframe<'py>(
    py: Python<'py>,
    df: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let module_polars = py.import("polars")?;
    let cls_dataframe = module_polars.getattr("DataFrame")?;

    if df.is_instance(&cls_dataframe)? {
        return Ok(df.clone());
    }

    cls_dataframe.call1((df,))
}

fn extract_optional_attr<T>(obj: &Bound<'_, PyAny>, attr: &str) -> PyResult<Option<T>>
where
    for<'a> T: FromPyObject<'a>,
{
    if !obj.hasattr(attr)? {
        return Ok(None);
    }
    let val = obj.getattr(attr)?;
    if val.is_none() {
        return Ok(None);
    }
    Ok(Some(val.extract::<T>()?))
}

fn extract_optional_attr_bound<'py>(
    obj: &Bound<'py, PyAny>,
    attr: &str,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    if !obj.hasattr(attr)? {
        return Ok(None);
    }
    let val = obj.getattr(attr)?;
    if val.is_none() {
        return Ok(None);
    }
    Ok(Some(val))
}

#[pymodule]
fn _axiomkit_rs(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PySpecCopyError>()?;
    module.add_class::<PyReportCopy>()?;
    module.add_function(wrap_pyfunction!(copy_tree_py, module)?)?;
    module.add_class::<PyXlsxWriter>()?;

    module.add("__bridge_fs_abi__", FS_BRIDGE_ABI_VERSION)?;
    module.add("__bridge_fs_contract__", FS_BRIDGE_CONTRACT_VERSION)?;
    module.add("__bridge_fs_transport__", FS_BRIDGE_TRANSPORT)?;
    module.add("__bridge_xlsx_abi__", XLSX_BRIDGE_ABI_VERSION)?;
    module.add("__bridge_xlsx_contract__", XLSX_BRIDGE_CONTRACT_VERSION)?;
    module.add("__bridge_xlsx_transport__", XLSX_BRIDGE_TRANSPORT)?;
    Ok(())
}

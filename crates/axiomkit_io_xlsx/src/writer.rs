//! XLSX writer kernel that converts DataFrame IPC into workbook output.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::path::PathBuf;

use polars::prelude::{AnyValue, DataFrame, IpcReader, SerReader};
use rust_xlsxwriter::{Format, FormatAlign, FormatBorder, Workbook, Worksheet, XlsxError};

use crate::constant::{ColumnIdentifier, LEN_SHEET_NAME_MAX};
use crate::spec::{
    AutofitMode, AutofitPolicy, CellFormatPatch, CellValue, ColumnFormatPlan, ScientificPolicy,
    ScientificScope, SheetSlice, XlsxReport, XlsxValuePolicy, XlsxWriteOptions,
};
use crate::util::{
    apply_vertical_run_text_blankout, calculate_row_chunk_size, convert_cell_value,
    create_horizontal_merge_tracker, generate_row_chunks, plan_horizontal_merges,
    plan_sheet_slices, sanitize_sheet_name, select_sorted_indices_from_refs,
    validate_unique_columns,
};

/// Per-sheet call options (aligned with Python `XlsxWriter.write_sheet` kwargs).
#[derive(Default, Debug, Clone)]
pub struct XlsxSheetWriteOptions {
    /// Integer columns by typed name or zero-based index.
    pub cols_integer: Option<Vec<ColumnIdentifier>>,
    /// Decimal columns by typed name or zero-based index.
    pub cols_decimal: Option<Vec<ColumnIdentifier>>,
    /// Number of frozen columns.
    pub num_frozen_cols: usize,
    /// Number of frozen top rows; defaults to header height when `None`.
    pub num_frozen_rows: Option<usize>,
    /// Enable merged multi-row header behavior.
    pub should_merge_header: bool,
    /// Override writer-level keep-missing behavior.
    pub should_keep_missing_values: Option<bool>,
    /// Column autofit policy.
    pub policy_autofit: AutofitPolicy,
    /// Scientific-format trigger policy.
    pub policy_scientific: ScientificPolicy,
}

struct ColumnFormatPlanOptions<'a> {
    /// Number of columns in current sheet slice.
    pub width_data: usize,
    /// Slice-local numeric column indices.
    pub cols_idx_numeric: &'a [usize],
    /// Slice-local integer column indices.
    pub cols_idx_integer: &'a [usize],
    /// Slice-local explicit decimal column indices.
    pub cols_idx_decimal: Option<&'a [usize]>,
    /// Optional per-column format overrides.
    pub cols_fmt_overrides: &'a BTreeMap<usize, CellFormatPatch>,
    /// Base text format.
    pub fmt_text: &'a CellFormatPatch,
    /// Base integer format.
    pub fmt_integer: &'a CellFormatPatch,
    /// Base decimal format.
    pub fmt_decimal: &'a CellFormatPatch,
    /// Global write options.
    pub options_write: &'a XlsxWriteOptions,
}

/// Stateful workbook writer.
pub struct XlsxWriter {
    path_file_out: PathBuf,
    workbook: Workbook,
    fmt_text: CellFormatPatch,
    fmt_integer: CellFormatPatch,
    fmt_decimal: CellFormatPatch,
    fmt_scientific: CellFormatPatch,
    fmt_header: CellFormatPatch,
    options_write: XlsxWriteOptions,
    existing_sheet_names: BTreeSet<String>,
    reports: Vec<XlsxReport>,
    is_closed: bool,
}

impl XlsxWriter {
    /// Create writer bound to output path and format/options presets.
    ///
    /// The workbook is buffered in memory until [`Self::close`] is called.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        path_file_out: PathBuf,
        fmt_text: CellFormatPatch,
        fmt_integer: CellFormatPatch,
        fmt_decimal: CellFormatPatch,
        fmt_scientific: CellFormatPatch,
        fmt_header: CellFormatPatch,
        options_write: XlsxWriteOptions,
    ) -> Self {
        Self {
            path_file_out,
            workbook: Workbook::new(),
            fmt_text,
            fmt_integer,
            fmt_decimal,
            fmt_scientific,
            fmt_header,
            options_write,
            existing_sheet_names: BTreeSet::new(),
            reports: Vec::new(),
            is_closed: false,
        }
    }

    /// Return output file path as string.
    pub fn file_out(&self) -> String {
        self.path_file_out.to_string_lossy().to_string()
    }

    /// Return immutable snapshot of per-sheet write reports.
    pub fn report(&self) -> Vec<XlsxReport> {
        self.reports.clone()
    }

    /// Flush workbook to disk. Idempotent.
    pub fn close(&mut self) -> Result<(), String> {
        if self.is_closed {
            return Ok(());
        }
        self.workbook
            .save(&self.path_file_out)
            .map_err(format_xlsx_error_text)?;
        self.is_closed = true;
        Ok(())
    }

    /// Write one sheet from in-memory dataframes.
    pub fn write_sheet_from_dataframes(
        &mut self,
        df_data: &DataFrame,
        sheet_name: &str,
        df_header: Option<&DataFrame>,
        options: &XlsxSheetWriteOptions,
    ) -> Result<(), String> {
        if self.is_closed {
            return Err("Cannot write after close().".to_string());
        }
        self.write_sheet(df_data, sheet_name, df_header, options)
    }

    /// Write one sheet from IPC-serialized dataframe bytes.
    ///
    /// `ipc_df` and optional `ipc_df_header` must be valid Polars IPC payloads.
    pub fn write_sheet_from_ipc_bytes(
        &mut self,
        ipc_df: &[u8],
        sheet_name: &str,
        ipc_df_header: Option<&[u8]>,
        options: &XlsxSheetWriteOptions,
    ) -> Result<(), String> {
        if self.is_closed {
            return Err("Cannot write after close().".to_string());
        }

        let df_data = read_dataframe_from_ipc_bytes(ipc_df)?;
        let df_header = match ipc_df_header {
            Some(val) => Some(read_dataframe_from_ipc_bytes(val)?),
            None => None,
        };
        self.write_sheet_from_dataframes(&df_data, sheet_name, df_header.as_ref(), options)
    }

    fn write_sheet(
        &mut self,
        df_data: &DataFrame,
        sheet_name: &str,
        df_header: Option<&DataFrame>,
        options: &XlsxSheetWriteOptions,
    ) -> Result<(), String> {
        validate_policy_autofit(&options.policy_autofit)?;
        validate_policy_scientific(&options.policy_scientific)?;

        let should_keep_missing_values = options
            .should_keep_missing_values
            .unwrap_or(self.options_write.should_keep_missing_values);
        let value_policy = self.options_write.value_policy.clone();

        let col_names: Vec<&str> = df_data.get_column_names_str();
        validate_unique_columns(&col_names)?;

        let width_df = col_names.len();
        let height_df = df_data.height();

        let mut header_grid = vec![
            col_names
                .iter()
                .map(|&_val| _val.to_string())
                .collect::<Vec<String>>(),
        ];
        if let Some(df_header_custom) = df_header {
            let header_cols: Vec<&str> = df_header_custom.get_column_names_str();
            validate_unique_columns(&header_cols)?;

            let header_height = df_header_custom.height();
            if header_height == 0 {
                return Err(
                    "df_header must have >= 1 row (0-row header is not allowed).".to_string(),
                );
            }
            let header_width = df_header_custom.width();
            if header_width != width_df {
                return Err("df_header.width must equal df.width.".to_string());
            }

            header_grid = extract_string_grid_from_dataframe(df_header_custom)?;
        }

        let cols_idx_numeric = if self.options_write.should_infer_numeric_cols {
            select_numeric_column_indices(df_data)
        } else {
            vec![]
        };

        let cols_idx_integer_inferred = if self.options_write.should_infer_integer_cols {
            select_integer_column_indices(df_data, &cols_idx_numeric)
        } else {
            vec![]
        };

        let cols_idx_integer_specified =
            select_sorted_indices_from_refs(&col_names, options.cols_integer.as_deref())?;
        let cols_idx_decimal_specified =
            select_sorted_indices_from_refs(&col_names, options.cols_decimal.as_deref())?;

        let cols_idx_integer = if cols_idx_integer_specified.is_empty() {
            cols_idx_integer_inferred
        } else {
            cols_idx_integer_specified
        };
        let header_row_count = header_grid.len();

        let mut report = XlsxReport {
            sheets: vec![],
            warnings: vec![],
        };

        let sheet_slices = plan_sheet_slices(
            height_df,
            width_df,
            header_row_count,
            &sanitize_sheet_name(sheet_name, "_"),
            &mut report,
        )?;

        let num_frozen_rows = options.num_frozen_rows.unwrap_or(header_row_count);

        for _sheet_slice in sheet_slices {
            let sheet_slice = _sheet_slice;
            let sheet_name_unique = self.ensure_unique_sheet_name(&sheet_slice.sheet_name);
            let worksheet = self.workbook.add_worksheet();
            worksheet
                .set_name(&sheet_name_unique)
                .map_err(format_xlsx_error_text)?;

            let cols_idx_numeric_slice = calculate_slice_indices(
                &cols_idx_numeric,
                sheet_slice.col_start_inclusive,
                sheet_slice.col_end_exclusive,
            );
            let cols_idx_integer_slice = calculate_slice_indices(
                &cols_idx_integer,
                sheet_slice.col_start_inclusive,
                sheet_slice.col_end_exclusive,
            );
            let cols_idx_decimal_slice = calculate_slice_indices(
                &cols_idx_decimal_specified,
                sheet_slice.col_start_inclusive,
                sheet_slice.col_end_exclusive,
            );
            let column_format_plan = plan_column_formats(ColumnFormatPlanOptions {
                width_data: sheet_slice.col_end_exclusive - sheet_slice.col_start_inclusive,
                cols_idx_numeric: &cols_idx_numeric_slice,
                cols_idx_integer: &cols_idx_integer_slice,
                cols_idx_decimal: if cols_idx_decimal_slice.is_empty() {
                    None
                } else {
                    Some(&cols_idx_decimal_slice)
                },
                cols_fmt_overrides: &BTreeMap::new(),
                fmt_text: &self.fmt_text,
                fmt_integer: &self.fmt_integer,
                fmt_decimal: &self.fmt_decimal,
                options_write: &self.options_write,
            });

            let data_formats_by_col: Vec<Format> = column_format_plan
                .fmts_by_col
                .iter()
                .map(create_rust_xlsx_format)
                .collect();
            let fmt_scientific_patch = self
                .fmt_scientific
                .merge(&self.options_write.base_format_patch);
            let fmt_scientific = create_rust_xlsx_format(&fmt_scientific_patch);
            let fmt_header = create_rust_xlsx_format(&self.fmt_header);

            let header_grid_slice = header_grid
                .iter()
                .map(|row| {
                    row[sheet_slice.col_start_inclusive..sheet_slice.col_end_exclusive].to_vec()
                })
                .collect::<Vec<_>>();

            let mut header_widths_by_col = vec![0usize; data_formats_by_col.len()];
            let mut body_widths_by_col = vec![0usize; data_formats_by_col.len()];

            let should_autofit_columns = !matches!(options.policy_autofit.mode, AutofitMode::None);

            if should_autofit_columns && !data_formats_by_col.is_empty() {
                for _col_idx in 0..data_formats_by_col.len() {
                    let col_idx = _col_idx;
                    for _row in &header_grid_slice {
                        let row = _row;
                        let value = &row[col_idx];
                        if value.is_empty() {
                            continue;
                        }
                        header_widths_by_col[col_idx] = usize::max(
                            header_widths_by_col[col_idx],
                            estimate_width_len(
                                &CellValue::String(value.clone()),
                                false,
                                false,
                                false,
                                &options.policy_scientific,
                                should_keep_missing_values,
                                &value_policy,
                            ),
                        );
                    }
                }
            }

            write_header(
                worksheet,
                header_grid_slice,
                options.should_merge_header,
                &fmt_header,
            )?;

            worksheet
                .set_freeze_panes(
                    cast_row_num(num_frozen_rows)?,
                    cast_col_num(options.num_frozen_cols)?,
                )
                .map_err(format_xlsx_error_text)?;

            let numeric_cols_idx: BTreeSet<usize> =
                cols_idx_numeric_slice.iter().copied().collect();
            let integer_cols_idx: BTreeSet<usize> =
                cols_idx_integer_slice.iter().copied().collect();
            let decimal_cols_idx: BTreeSet<usize> =
                cols_idx_decimal_slice.iter().copied().collect();
            let is_decimal_explicit = !decimal_cols_idx.is_empty();

            let mut cols_slice = Vec::with_capacity(data_formats_by_col.len());
            let rows_data_in_sheet =
                sheet_slice.row_end_exclusive - sheet_slice.row_start_inclusive;
            for _col_idx_abs in sheet_slice.col_start_inclusive..sheet_slice.col_end_exclusive {
                let col_idx_abs = _col_idx_abs;
                cols_slice.push(
                    df_data.get_columns()[col_idx_abs]
                        .slice(sheet_slice.row_start_inclusive as i64, rows_data_in_sheet),
                );
            }
            let rows_chunk = calculate_row_chunk_size(
                data_formats_by_col.len(),
                &self.options_write.row_chunk_policy,
            );
            if rows_chunk == 0 {
                return Err("row_chunk_policy resolved to 0 rows; expected >= 1.".to_string());
            }
            let row_chunks = generate_row_chunks(rows_data_in_sheet, rows_chunk);

            let mut rows_seen_for_autofit = 0usize;
            for _row_chunk in row_chunks {
                let (row_chunk_start, row_chunk_len) = _row_chunk;
                let row_chunk_end = row_chunk_start + row_chunk_len;
                for _row_local in row_chunk_start..row_chunk_end {
                    let row_local = _row_local;
                    for _col in cols_slice.iter().enumerate() {
                        let (col_idx, col) = _col;
                        let is_numeric_col = numeric_cols_idx.contains(&col_idx);
                        let is_integer_col = integer_cols_idx.contains(&col_idx);
                        let is_decimal_specified = decimal_cols_idx.contains(&col_idx);
                        let is_scientific_candidate = is_scientific_candidate_col(
                            &options.policy_scientific,
                            is_integer_col,
                            is_decimal_explicit,
                            is_decimal_specified,
                        );

                        let value_raw = convert_any_value_to_cell_value(
                            col.get(row_local)
                                .map_err(|err| format!("Failed to access cell value: {err}"))?,
                        );
                        let value = convert_cell_value(
                            &value_raw,
                            is_numeric_col,
                            is_integer_col,
                            should_keep_missing_values,
                            &value_policy,
                        );

                        if should_autofit_columns
                            && (options.policy_autofit.height_body_inferred_max.is_none()
                                || rows_seen_for_autofit
                                    < options.policy_autofit.height_body_inferred_max.unwrap_or(0))
                        {
                            body_widths_by_col[col_idx] = usize::max(
                                body_widths_by_col[col_idx],
                                estimate_width_len(
                                    &value,
                                    is_numeric_col,
                                    is_integer_col,
                                    is_scientific_candidate,
                                    &options.policy_scientific,
                                    should_keep_missing_values,
                                    &value_policy,
                                ),
                            );
                        }

                        let should_use_scientific = should_use_scientific_value(
                            &value,
                            is_numeric_col,
                            is_scientific_candidate,
                            &options.policy_scientific,
                        );
                        let fmt_cell = if should_use_scientific {
                            &fmt_scientific
                        } else {
                            &data_formats_by_col[col_idx]
                        };

                        write_cell_with_format(
                            worksheet,
                            header_row_count + row_local,
                            col_idx,
                            &value,
                            fmt_cell,
                        )?;
                    }

                    if should_autofit_columns
                        && (options.policy_autofit.height_body_inferred_max.is_none()
                            || rows_seen_for_autofit
                                < options.policy_autofit.height_body_inferred_max.unwrap_or(0))
                    {
                        rows_seen_for_autofit += 1;
                    }
                }
            }

            if should_autofit_columns && !data_formats_by_col.is_empty() {
                let width_min = usize::max(1, options.policy_autofit.width_cell_min);
                let width_max = usize::min(
                    255,
                    usize::max(width_min, options.policy_autofit.width_cell_max),
                );
                let width_padding = options.policy_autofit.width_cell_padding;

                for _col_idx in 0..data_formats_by_col.len() {
                    let col_idx = _col_idx;
                    let width_recorded = match options.policy_autofit.mode {
                        AutofitMode::Header => header_widths_by_col[col_idx],
                        AutofitMode::Body => body_widths_by_col[col_idx],
                        AutofitMode::All => {
                            usize::max(header_widths_by_col[col_idx], body_widths_by_col[col_idx])
                        }
                        AutofitMode::None => header_widths_by_col[col_idx],
                    };
                    let width_final = usize::min(
                        width_max,
                        usize::max(width_min, width_recorded + width_padding),
                    );
                    worksheet
                        .set_column_width(cast_col_num(col_idx)?, width_final as f64)
                        .map_err(format_xlsx_error_text)?;
                }
            }

            report.sheets.push(SheetSlice {
                sheet_name: sheet_name_unique,
                row_start_inclusive: sheet_slice.row_start_inclusive,
                row_end_exclusive: sheet_slice.row_end_exclusive,
                col_start_inclusive: sheet_slice.col_start_inclusive,
                col_end_exclusive: sheet_slice.col_end_exclusive,
            });
        }

        self.reports.push(report);
        Ok(())
    }

    fn ensure_unique_sheet_name(&mut self, name: &str) -> String {
        if !self.existing_sheet_names.contains(name) {
            self.existing_sheet_names.insert(name.to_string());
            return name.to_string();
        }

        let base_name: String = name
            .chars()
            .take(usize::max(1, LEN_SHEET_NAME_MAX - 3))
            .collect();

        let mut idx = 2usize;
        loop {
            let candidate: String = format!("{base_name}__{idx}")
                .chars()
                .take(LEN_SHEET_NAME_MAX)
                .collect();
            if !self.existing_sheet_names.contains(&candidate) {
                self.existing_sheet_names.insert(candidate.clone());
                return candidate;
            }
            idx += 1;
        }
    }
}

/// Estimate displayed width units for one normalized cell value.
///
/// Used by autofit inference logic.
fn estimate_width_len(
    value: &CellValue,
    is_numeric_col: bool,
    is_integer_col: bool,
    is_scientific_candidate: bool,
    policy_scientific: &ScientificPolicy,
    should_keep_missing_values: bool,
    value_policy: &XlsxValuePolicy,
) -> usize {
    match value {
        CellValue::None => {
            if should_keep_missing_values {
                value_policy.missing_value_str.len()
            } else {
                0
            }
        }
        CellValue::String(s) => {
            if s.is_empty() {
                return 0;
            }
            if !is_numeric_col {
                return estimate_unicode_string_width(s);
            }
            if is_integer_col && let Ok(val) = s.parse::<i64>() {
                return val.to_string().len();
            }
            estimate_unicode_string_width(s)
        }
        CellValue::Number(n) => {
            if !is_numeric_col {
                return estimate_unicode_string_width(&n.to_string());
            }
            if should_use_scientific_value(
                value,
                is_numeric_col,
                is_scientific_candidate,
                policy_scientific,
            ) {
                return format!("{n:.2E}").len();
            }
            if is_integer_col {
                return (*n as i64).to_string().len();
            }
            format!("{n:.4}").len()
        }
    }
}

fn estimate_unicode_string_width(s: &str) -> usize {
    let ascii_count = s.chars().filter(|chr| chr.is_ascii()).count();
    let non_ascii_count = s.chars().count().saturating_sub(ascii_count);
    ascii_count + (non_ascii_count as f64 * 1.6).round() as usize
}

/// Build per-column base/final format plans for current sheet slice.
fn plan_column_formats(options: ColumnFormatPlanOptions<'_>) -> ColumnFormatPlan {
    let ColumnFormatPlanOptions {
        width_data,
        cols_idx_numeric,
        cols_idx_integer,
        cols_idx_decimal,
        cols_fmt_overrides,
        fmt_text,
        fmt_integer,
        fmt_decimal,
        options_write,
    } = options;

    let numeric_cols_idx: BTreeSet<usize> = cols_idx_numeric.iter().copied().collect();
    let integer_cols_idx: BTreeSet<usize> = cols_idx_integer.iter().copied().collect();
    let decimal_cols_idx: Option<BTreeSet<usize>> =
        cols_idx_decimal.map(|vals| vals.iter().copied().collect());
    let mut fmts_base_by_col = Vec::with_capacity(width_data);
    let mut fmts_by_col = Vec::with_capacity(width_data);

    for _col_idx in 0..width_data {
        let col_idx = _col_idx;
        let mut fmt_base = if integer_cols_idx.contains(&col_idx) {
            fmt_integer.clone()
        } else if decimal_cols_idx
            .as_ref()
            .map_or(numeric_cols_idx.contains(&col_idx), |indices| {
                indices.contains(&col_idx)
            })
        {
            fmt_decimal.clone()
        } else {
            fmt_text.clone()
        };

        fmt_base = fmt_base.merge(&options_write.base_format_patch);

        let fmt_final = if let Some(fmt_override) = cols_fmt_overrides.get(&col_idx) {
            fmt_base.merge(fmt_override)
        } else {
            fmt_base.clone()
        };

        fmts_base_by_col.push(fmt_base);
        fmts_by_col.push(fmt_final);
    }

    ColumnFormatPlan {
        fmts_by_col,
        fmts_base_by_col,
    }
}

fn read_dataframe_from_ipc_bytes(ipc_df: &[u8]) -> Result<DataFrame, String> {
    IpcReader::new(Cursor::new(ipc_df))
        .finish()
        .map_err(|err| format!("Failed to read IPC DataFrame bytes: {err}"))
}

fn validate_policy_autofit(policy_autofit: &AutofitPolicy) -> Result<(), String> {
    if policy_autofit.width_cell_min == 0 {
        return Err("policy_autofit.width_cell_min must be >= 1.".to_string());
    }
    if policy_autofit.width_cell_max < policy_autofit.width_cell_min {
        return Err(
            "policy_autofit.width_cell_max must be >= policy_autofit.width_cell_min.".to_string(),
        );
    }
    Ok(())
}

fn validate_policy_scientific(policy_scientific: &ScientificPolicy) -> Result<(), String> {
    if policy_scientific.thr_min < 0.0 {
        return Err("policy_scientific.thr_min must be >= 0.".to_string());
    }
    if policy_scientific.thr_max <= 0.0 {
        return Err("policy_scientific.thr_max must be > 0.".to_string());
    }
    if policy_scientific.thr_min > policy_scientific.thr_max {
        return Err("policy_scientific.thr_min must be <= policy_scientific.thr_max.".to_string());
    }
    Ok(())
}

fn is_scientific_candidate_col(
    policy_scientific: &ScientificPolicy,
    is_integer_col: bool,
    is_decimal_explicit: bool,
    is_decimal_specified: bool,
) -> bool {
    match policy_scientific.scope {
        ScientificScope::None => false,
        ScientificScope::Decimal => {
            if is_integer_col {
                false
            } else if is_decimal_explicit {
                is_decimal_specified
            } else {
                true
            }
        }
        ScientificScope::Integer => is_integer_col,
        ScientificScope::All => true,
    }
}

fn should_use_scientific_value(
    value: &CellValue,
    is_numeric_col: bool,
    is_scientific_candidate: bool,
    policy_scientific: &ScientificPolicy,
) -> bool {
    if !is_numeric_col || !is_scientific_candidate {
        return false;
    }
    let CellValue::Number(value_num) = value else {
        return false;
    };
    if !value_num.is_finite() {
        return false;
    }
    let value_abs = value_num.abs();
    value_abs >= policy_scientific.thr_max
        || (value_abs > 0.0 && value_abs < policy_scientific.thr_min)
}

fn select_numeric_column_indices(df: &DataFrame) -> Vec<usize> {
    df.get_columns()
        .iter()
        .enumerate()
        .filter_map(|(idx, col)| {
            if col.dtype().is_numeric() {
                Some(idx)
            } else {
                None
            }
        })
        .collect()
}

fn select_integer_column_indices(df: &DataFrame, cols_idx_numeric: &[usize]) -> Vec<usize> {
    cols_idx_numeric
        .iter()
        .copied()
        .filter(|idx| df.get_columns()[*idx].dtype().is_integer())
        .collect()
}

fn extract_string_grid_from_dataframe(df: &DataFrame) -> Result<Vec<Vec<String>>, String> {
    let height = df.height();
    let width = df.width();
    let cols = df.get_columns();

    let mut grid = vec![vec![String::new(); width]; height];
    for (_row_index, _row_values) in grid.iter_mut().enumerate() {
        for (_col_index, _cell_value) in _row_values.iter_mut().enumerate() {
            let value = cols[_col_index]
                .get(_row_index)
                .map_err(|err| format!("Failed to read header cell value: {err}"))?;
            *_cell_value = format_header_text_from_any_value(value);
        }
    }

    Ok(grid)
}

fn format_header_text_from_any_value(value: AnyValue<'_>) -> String {
    match value {
        AnyValue::Null => String::new(),
        // Keep raw string payload for header cells; AnyValue::to_string() wraps
        // Utf8 values in quotes, which leaks into XLSX header text.
        AnyValue::String(val) => val.to_string(),
        AnyValue::StringOwned(val) => val.to_string(),
        _ => value.to_string(),
    }
}

fn convert_any_value_to_cell_value(value: AnyValue<'_>) -> CellValue {
    match value {
        AnyValue::Null => CellValue::None,
        AnyValue::String(val) => CellValue::String(val.to_string()),
        AnyValue::StringOwned(val) => CellValue::String(val.to_string()),
        AnyValue::Boolean(val) => CellValue::String(if val { "True" } else { "False" }.to_string()),
        AnyValue::UInt8(val) => CellValue::Number(val as f64),
        AnyValue::UInt16(val) => CellValue::Number(val as f64),
        AnyValue::UInt32(val) => CellValue::Number(val as f64),
        AnyValue::UInt64(val) => CellValue::Number(val as f64),
        AnyValue::Int8(val) => CellValue::Number(val as f64),
        AnyValue::Int16(val) => CellValue::Number(val as f64),
        AnyValue::Int32(val) => CellValue::Number(val as f64),
        AnyValue::Int64(val) => CellValue::Number(val as f64),
        AnyValue::Int128(val) => CellValue::Number(val as f64),
        AnyValue::Float32(val) => CellValue::Number(val as f64),
        AnyValue::Float64(val) => CellValue::Number(val),
        _ => CellValue::String(value.to_string()),
    }
}

fn calculate_slice_indices(
    indices: &[usize],
    col_start_inclusive: usize,
    col_end_exclusive: usize,
) -> Vec<usize> {
    indices
        .iter()
        .filter_map(|idx| {
            if *idx >= col_start_inclusive && *idx < col_end_exclusive {
                Some(*idx - col_start_inclusive)
            } else {
                None
            }
        })
        .collect()
}

fn write_header_cell(
    worksheet: &mut Worksheet,
    row_idx: usize,
    col_idx: usize,
    text: &str,
    fmt_header: &Format,
) -> Result<(), String> {
    if text.is_empty() {
        worksheet
            .write_blank(cast_row_num(row_idx)?, cast_col_num(col_idx)?, fmt_header)
            .map_err(format_xlsx_error_text)?;
    } else {
        worksheet
            .write_string_with_format(
                cast_row_num(row_idx)?,
                cast_col_num(col_idx)?,
                text,
                fmt_header,
            )
            .map_err(format_xlsx_error_text)?;
    }
    Ok(())
}

fn write_header(
    worksheet: &mut Worksheet,
    mut header_grid: Vec<Vec<String>>,
    should_merge: bool,
    fmt_header: &Format,
) -> Result<(), String> {
    if !should_merge {
        for (_row_idx, _row_values) in header_grid.iter().enumerate() {
            for (_col_idx, _cell_value) in _row_values.iter().enumerate() {
                write_header_cell(worksheet, _row_idx, _col_idx, _cell_value, fmt_header)?;
            }
        }
        return Ok(());
    }

    apply_vertical_run_text_blankout(&mut header_grid);
    let horizontal_merges_by_row = plan_horizontal_merges(&header_grid);
    let horizontal_merge_tracker = create_horizontal_merge_tracker(&horizontal_merges_by_row);

    for (_row_idx, _row_values) in header_grid.iter().enumerate() {
        for (_col_idx, _cell_value) in _row_values.iter().enumerate() {
            if horizontal_merge_tracker
                .get(&(_row_idx, _col_idx))
                .copied()
                .unwrap_or(false)
            {
                continue;
            }

            write_header_cell(worksheet, _row_idx, _col_idx, _cell_value, fmt_header)?;
        }

        if let Some(merges) = horizontal_merges_by_row.get(&_row_idx) {
            for _merge in merges {
                let merge = _merge;
                worksheet
                    .merge_range(
                        cast_row_num(_row_idx)?,
                        cast_col_num(merge.col_idx_start)?,
                        cast_row_num(_row_idx)?,
                        cast_col_num(merge.col_idx_end)?,
                        &merge.text,
                        fmt_header,
                    )
                    .map_err(format_xlsx_error_text)?;
            }
        }
    }

    Ok(())
}

fn write_cell_with_format(
    worksheet: &mut Worksheet,
    row_idx: usize,
    col_idx: usize,
    value: &CellValue,
    format: &Format,
) -> Result<(), String> {
    match value {
        CellValue::None => {
            worksheet
                .write_blank(cast_row_num(row_idx)?, cast_col_num(col_idx)?, format)
                .map_err(format_xlsx_error_text)?;
        }
        CellValue::String(val) => {
            worksheet
                .write_string_with_format(
                    cast_row_num(row_idx)?,
                    cast_col_num(col_idx)?,
                    val,
                    format,
                )
                .map_err(format_xlsx_error_text)?;
        }
        CellValue::Number(val) => {
            worksheet
                .write_number_with_format(
                    cast_row_num(row_idx)?,
                    cast_col_num(col_idx)?,
                    *val,
                    format,
                )
                .map_err(format_xlsx_error_text)?;
        }
    }
    Ok(())
}

fn create_rust_xlsx_format(spec: &CellFormatPatch) -> Format {
    let mut format = Format::new();

    if let Some(val) = &spec.font_name {
        format = format.set_font_name(val.clone());
    }
    if let Some(val) = spec.font_size {
        format = format.set_font_size(val as f64);
    }
    if spec.bold.unwrap_or(false) {
        format = format.set_bold();
    }
    if spec.italic.unwrap_or(false) {
        format = format.set_italic();
    }

    if let Some(val) = &spec.align
        && let Some(align) = parse_format_align(val)
    {
        format = format.set_align(align);
    }
    if let Some(val) = &spec.valign
        && let Some(align) = parse_format_align(val)
    {
        format = format.set_align(align);
    }

    if let Some(val) = &spec.num_format {
        format = format.set_num_format(val.clone());
    }
    if let Some(val) = &spec.bg_color {
        format = format.set_background_color(val.as_str());
    }
    if let Some(val) = &spec.font_color {
        format = format.set_font_color(val.as_str());
    }

    if let Some(val) = spec.border {
        format = format.set_border(parse_format_border(val));
    }
    if let Some(val) = spec.top {
        format = format.set_border_top(parse_format_border(val));
    }
    if let Some(val) = spec.bottom {
        format = format.set_border_bottom(parse_format_border(val));
    }
    if let Some(val) = spec.left {
        format = format.set_border_left(parse_format_border(val));
    }
    if let Some(val) = spec.right {
        format = format.set_border_right(parse_format_border(val));
    }

    if spec.text_wrap.unwrap_or(false) {
        format = format.set_text_wrap();
    }

    format
}

fn parse_format_border(border: i64) -> FormatBorder {
    match border {
        0 => FormatBorder::None,
        1 => FormatBorder::Thin,
        2 => FormatBorder::Medium,
        3 => FormatBorder::Dashed,
        4 => FormatBorder::Dotted,
        5 => FormatBorder::Thick,
        6 => FormatBorder::Double,
        7 => FormatBorder::Hair,
        8 => FormatBorder::MediumDashed,
        9 => FormatBorder::DashDot,
        10 => FormatBorder::MediumDashDot,
        11 => FormatBorder::DashDotDot,
        12 => FormatBorder::MediumDashDotDot,
        13 => FormatBorder::SlantDashDot,
        _ => FormatBorder::None,
    }
}

fn parse_format_align(align: &str) -> Option<FormatAlign> {
    let value = align.trim().to_ascii_lowercase();
    match value.as_str() {
        "general" => Some(FormatAlign::General),
        "left" => Some(FormatAlign::Left),
        "center" => Some(FormatAlign::Center),
        "right" => Some(FormatAlign::Right),
        "fill" => Some(FormatAlign::Fill),
        "justify" => Some(FormatAlign::Justify),
        "center_across" => Some(FormatAlign::CenterAcross),
        "distributed" => Some(FormatAlign::Distributed),
        "top" => Some(FormatAlign::Top),
        "bottom" => Some(FormatAlign::Bottom),
        "vcenter" | "vertical_center" => Some(FormatAlign::VerticalCenter),
        "vjustify" | "vertical_justify" => Some(FormatAlign::VerticalJustify),
        "vdistributed" | "vertical_distributed" => Some(FormatAlign::VerticalDistributed),
        _ => None,
    }
}

fn cast_row_num(value: usize) -> Result<u32, String> {
    u32::try_from(value).map_err(|_| format!("row index overflow: {value}"))
}

fn cast_col_num(value: usize) -> Result<u16, String> {
    u16::try_from(value).map_err(|_| format!("column index overflow: {value}"))
}

fn format_xlsx_error_text(err: XlsxError) -> String {
    format!("xlsx write error: {err}")
}

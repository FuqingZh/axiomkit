//! XLSX writer kernel that converts DataFrame IPC into workbook output.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::path::PathBuf;

use polars::prelude::{AnyValue, DataFrame, IpcReader, SerReader};
use rust_xlsxwriter::{Format, FormatAlign, FormatBorder, Workbook, Worksheet, XlsxError};

use crate::constant::N_LEN_EXCEL_SHEET_NAME_MAX;
use crate::spec::{
    AutofitCellsPolicySpec, AutofitColumnsRule, CellFormatSpec, CellValue, ColumnFormatPlanSpec,
    ScientificPolicySpec, ScientificScope, SheetSliceSpec, XlsxReport, XlsxValuePolicySpec,
    XlsxWriteOptionsSpec,
};
use crate::util::{
    apply_vertical_run_text_blankout, calculate_row_chunk_size, convert_cell_value,
    derive_horizontal_merge_tracker, generate_row_chunks, plan_horizontal_merges,
    plan_sheet_slices, sanitize_sheet_name, select_sorted_indices_from_refs,
    validate_unique_columns,
};

/// Per-sheet call options (aligned with Python `XlsxWriter.write_sheet` kwargs).
#[derive(Default, Debug, Clone)]
pub struct XlsxSheetWriteOptionsSpec {
    /// Integer columns by name/index-string.
    pub cols_integer: Option<Vec<String>>,
    /// Decimal columns by name/index-string.
    pub cols_decimal: Option<Vec<String>>,
    /// Number of frozen columns.
    pub col_freeze: usize,
    /// Frozen row index; defaults to header height when `None`.
    pub row_freeze: Option<usize>,
    /// Enable merged multi-row header behavior.
    pub should_merge_header: bool,
    /// Override writer-level keep-missing behavior.
    pub should_keep_missing_values: Option<bool>,
    /// Column autofit policy.
    pub policy_autofit: AutofitCellsPolicySpec,
    /// Scientific-format trigger policy.
    pub policy_scientific: ScientificPolicySpec,
}

pub struct ColumnFormatPlanOptionsSpec<'a> {
    /// Number of columns in current sheet slice.
    pub width_data: usize,
    /// Slice-local numeric column indices.
    pub cols_idx_numeric: &'a [usize],
    /// Slice-local integer column indices.
    pub cols_idx_integer: &'a [usize],
    /// Slice-local explicit decimal column indices.
    pub cols_idx_decimal: Option<&'a [usize]>,
    /// Slice-local scientific column indices.
    pub cols_idx_scientific: &'a [usize],
    /// Optional per-column format overrides.
    pub cols_fmt_overrides: &'a BTreeMap<usize, CellFormatSpec>,
    /// Base text format.
    pub fmt_text: &'a CellFormatSpec,
    /// Base integer format.
    pub fmt_integer: &'a CellFormatSpec,
    /// Base decimal format.
    pub fmt_decimal: &'a CellFormatSpec,
    /// Base scientific format.
    pub fmt_scientific: &'a CellFormatSpec,
    /// Global write options.
    pub write_options: &'a XlsxWriteOptionsSpec,
}

/// Stateful workbook writer.
pub struct XlsxWriter {
    path_file_out: PathBuf,
    workbook: Workbook,
    fmt_text: CellFormatSpec,
    fmt_integer: CellFormatSpec,
    fmt_decimal: CellFormatSpec,
    fmt_scientific: CellFormatSpec,
    fmt_header: CellFormatSpec,
    write_options: XlsxWriteOptionsSpec,
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
        fmt_text: CellFormatSpec,
        fmt_integer: CellFormatSpec,
        fmt_decimal: CellFormatSpec,
        fmt_scientific: CellFormatSpec,
        fmt_header: CellFormatSpec,
        write_options: XlsxWriteOptionsSpec,
    ) -> Self {
        Self {
            path_file_out,
            workbook: Workbook::new(),
            fmt_text,
            fmt_integer,
            fmt_decimal,
            fmt_scientific,
            fmt_header,
            write_options,
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
            .map_err(derive_xlsx_error_text)?;
        self.is_closed = true;
        Ok(())
    }

    /// Write one sheet from in-memory dataframes.
    pub fn write_sheet_from_dataframes(
        &mut self,
        df_data: &DataFrame,
        sheet_name: &str,
        df_header: Option<&DataFrame>,
        options: &XlsxSheetWriteOptionsSpec,
    ) -> Result<(), String> {
        if self.is_closed {
            return Err("Cannot write after close().".to_string());
        }
        self.write_sheet(df_data, sheet_name, df_header, options)
    }

    /// Write one sheet from IPC-serialized dataframe bytes.
    ///
    /// `v_ipc_df` and optional `v_ipc_df_header` must be valid Polars IPC payloads.
    pub fn write_sheet_from_ipc_bytes(
        &mut self,
        ipc_df: &[u8],
        sheet_name: &str,
        ipc_df_header: Option<&[u8]>,
        options: &XlsxSheetWriteOptionsSpec,
    ) -> Result<(), String> {
        if self.is_closed {
            return Err("Cannot write after close().".to_string());
        }

        let df_data = derive_dataframe_from_ipc_bytes(ipc_df)?;
        let df_header = match ipc_df_header {
            Some(val) => Some(derive_dataframe_from_ipc_bytes(val)?),
            None => None,
        };
        self.write_sheet_from_dataframes(&df_data, sheet_name, df_header.as_ref(), options)
    }

    fn write_sheet(
        &mut self,
        df_data: &DataFrame,
        sheet_name: &str,
        df_header: Option<&DataFrame>,
        options: &XlsxSheetWriteOptionsSpec,
    ) -> Result<(), String> {
        validate_policy_autofit(&options.policy_autofit)?;
        validate_policy_scientific(&options.policy_scientific)?;

        let should_keep_missing_values = options
            .should_keep_missing_values
            .unwrap_or(self.write_options.keep_missing_values);
        let value_policy = self.write_options.value_policy.clone();

        let col_names: Vec<String> = df_data
            .get_column_names_str()
            .into_iter()
            .map(ToString::to_string)
            .collect();
        validate_unique_columns(&col_names)?;

        let width_df = col_names.len();
        let height_df = df_data.height();

        let mut header_grid = vec![col_names.clone()];
        if let Some(df_header_custom) = df_header {
            let header_cols: Vec<String> = df_header_custom
                .get_column_names_str()
                .into_iter()
                .map(ToString::to_string)
                .collect();
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

            header_grid = derive_string_grid_from_dataframe(df_header_custom)?;
        }

        let cols_idx_numeric = if self.write_options.infer_numeric_cols {
            derive_numeric_column_indices(df_data)
        } else {
            vec![]
        };

        let cols_idx_integer_inferred = if self.write_options.infer_integer_cols {
            derive_integer_column_indices(df_data, &cols_idx_numeric)
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
        let cols_idx_scientific = derive_scientific_column_indices(
            df_data,
            &cols_idx_numeric,
            &cols_idx_integer,
            &cols_idx_decimal_specified,
            &options.policy_scientific,
        )?;

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

        let row_freeze = options.row_freeze.unwrap_or(header_row_count);

        for _sheet_slice in sheet_slices {
            let sheet_slice = _sheet_slice;
            let sheet_name_unique = self.derive_unique_sheet_name(&sheet_slice.sheet_name);
            let worksheet = self.workbook.add_worksheet();
            worksheet
                .set_name(&sheet_name_unique)
                .map_err(derive_xlsx_error_text)?;

            let cols_idx_numeric_slice = derive_slice_indices(
                &cols_idx_numeric,
                sheet_slice.col_start_inclusive,
                sheet_slice.col_end_exclusive,
            );
            let cols_idx_integer_slice = derive_slice_indices(
                &cols_idx_integer,
                sheet_slice.col_start_inclusive,
                sheet_slice.col_end_exclusive,
            );
            let cols_idx_decimal_slice = derive_slice_indices(
                &cols_idx_decimal_specified,
                sheet_slice.col_start_inclusive,
                sheet_slice.col_end_exclusive,
            );
            let cols_idx_scientific_slice = derive_slice_indices(
                &cols_idx_scientific,
                sheet_slice.col_start_inclusive,
                sheet_slice.col_end_exclusive,
            );

            let column_format_plan = plan_column_formats(ColumnFormatPlanOptionsSpec {
                width_data: sheet_slice.col_end_exclusive - sheet_slice.col_start_inclusive,
                cols_idx_numeric: &cols_idx_numeric_slice,
                cols_idx_integer: &cols_idx_integer_slice,
                cols_idx_decimal: if cols_idx_decimal_slice.is_empty() {
                    None
                } else {
                    Some(&cols_idx_decimal_slice)
                },
                cols_idx_scientific: &cols_idx_scientific_slice,
                cols_fmt_overrides: &BTreeMap::new(),
                fmt_text: &self.fmt_text,
                fmt_integer: &self.fmt_integer,
                fmt_decimal: &self.fmt_decimal,
                fmt_scientific: &self.fmt_scientific,
                write_options: &self.write_options,
            });

            let data_formats_by_col: Vec<Format> = column_format_plan
                .fmts_by_col
                .iter()
                .map(derive_rust_xlsx_format)
                .collect();
            let fmt_header = derive_rust_xlsx_format(&self.fmt_header);

            let header_grid_slice = header_grid
                .iter()
                .map(|row| {
                    row[sheet_slice.col_start_inclusive..sheet_slice.col_end_exclusive].to_vec()
                })
                .collect::<Vec<_>>();

            let mut header_widths_by_col = vec![0usize; data_formats_by_col.len()];
            let mut body_widths_by_col = vec![0usize; data_formats_by_col.len()];

            let should_autofit_columns = !matches!(
                options.policy_autofit.rule_columns,
                AutofitColumnsRule::None
            );

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
                .set_freeze_panes(cast_row_num(row_freeze)?, cast_col_num(options.col_freeze)?)
                .map_err(derive_xlsx_error_text)?;

            let numeric_cols_idx: BTreeSet<usize> =
                cols_idx_numeric_slice.iter().copied().collect();
            let integer_cols_idx: BTreeSet<usize> =
                cols_idx_integer_slice.iter().copied().collect();
            let scientific_cols_idx: BTreeSet<usize> =
                cols_idx_scientific_slice.iter().copied().collect();

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
                &self.write_options.row_chunk_policy,
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
                        let is_scientific_col = scientific_cols_idx.contains(&col_idx);

                        let value_raw = derive_cell_value_from_any_value(
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
                                    is_scientific_col,
                                    should_keep_missing_values,
                                    &value_policy,
                                ),
                            );
                        }

                        write_cell_with_format(
                            worksheet,
                            header_row_count + row_local,
                            col_idx,
                            &value,
                            &data_formats_by_col[col_idx],
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
                    let width_recorded = match options.policy_autofit.rule_columns {
                        AutofitColumnsRule::Header => header_widths_by_col[col_idx],
                        AutofitColumnsRule::Body => body_widths_by_col[col_idx],
                        AutofitColumnsRule::All => {
                            usize::max(header_widths_by_col[col_idx], body_widths_by_col[col_idx])
                        }
                        AutofitColumnsRule::None => header_widths_by_col[col_idx],
                    };
                    let width_final = usize::min(
                        width_max,
                        usize::max(width_min, width_recorded + width_padding),
                    );
                    worksheet
                        .set_column_width(cast_col_num(col_idx)?, width_final as f64)
                        .map_err(derive_xlsx_error_text)?;
                }
            }

            report.sheets.push(SheetSliceSpec {
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

    fn derive_unique_sheet_name(&mut self, name: &str) -> String {
        if !self.existing_sheet_names.contains(name) {
            self.existing_sheet_names.insert(name.to_string());
            return name.to_string();
        }

        let base_name: String = name
            .chars()
            .take(usize::max(1, N_LEN_EXCEL_SHEET_NAME_MAX - 3))
            .collect();

        let mut idx = 2usize;
        loop {
            let candidate: String = format!("{base_name}__{idx}")
                .chars()
                .take(N_LEN_EXCEL_SHEET_NAME_MAX)
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
pub fn estimate_width_len(
    value: &CellValue,
    is_numeric_col: bool,
    is_integer_col: bool,
    is_scientific_col: bool,
    should_keep_missing_values: bool,
    value_policy: &XlsxValuePolicySpec,
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
            if is_scientific_col && let Ok(val) = s.parse::<f64>() {
                return format!("{val:.2E}").len();
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
            if is_scientific_col {
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
pub fn plan_column_formats(options: ColumnFormatPlanOptionsSpec<'_>) -> ColumnFormatPlanSpec {
    let ColumnFormatPlanOptionsSpec {
        width_data,
        cols_idx_numeric,
        cols_idx_integer,
        cols_idx_decimal,
        cols_idx_scientific,
        cols_fmt_overrides,
        fmt_text,
        fmt_integer,
        fmt_decimal,
        fmt_scientific,
        write_options,
    } = options;

    let numeric_cols_idx: BTreeSet<usize> = cols_idx_numeric.iter().copied().collect();
    let integer_cols_idx: BTreeSet<usize> = cols_idx_integer.iter().copied().collect();
    let decimal_cols_idx: Option<BTreeSet<usize>> =
        cols_idx_decimal.map(|vals| vals.iter().copied().collect());
    let scientific_cols_idx: BTreeSet<usize> = cols_idx_scientific.iter().copied().collect();

    let mut fmts_base_by_col = Vec::with_capacity(width_data);
    let mut fmts_by_col = Vec::with_capacity(width_data);

    for _col_idx in 0..width_data {
        let col_idx = _col_idx;
        let mut fmt_base = if scientific_cols_idx.contains(&col_idx) {
            fmt_scientific.clone()
        } else if integer_cols_idx.contains(&col_idx) {
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

        fmt_base = fmt_base.merge(&write_options.base_format_patch);

        let fmt_final = if let Some(fmt_override) = cols_fmt_overrides.get(&col_idx) {
            fmt_base.merge(fmt_override)
        } else {
            fmt_base.clone()
        };

        fmts_base_by_col.push(fmt_base);
        fmts_by_col.push(fmt_final);
    }

    ColumnFormatPlanSpec {
        fmts_by_col,
        fmts_base_by_col,
    }
}

fn derive_dataframe_from_ipc_bytes(ipc_df: &[u8]) -> Result<DataFrame, String> {
    IpcReader::new(Cursor::new(ipc_df))
        .finish()
        .map_err(|err| format!("Failed to read IPC DataFrame bytes: {err}"))
}

fn validate_policy_autofit(policy_autofit: &AutofitCellsPolicySpec) -> Result<(), String> {
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

fn validate_policy_scientific(policy_scientific: &ScientificPolicySpec) -> Result<(), String> {
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

fn derive_numeric_column_indices(df: &DataFrame) -> Vec<usize> {
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

fn derive_integer_column_indices(df: &DataFrame, cols_idx_numeric: &[usize]) -> Vec<usize> {
    cols_idx_numeric
        .iter()
        .copied()
        .filter(|idx| df.get_columns()[*idx].dtype().is_integer())
        .collect()
}

fn derive_scientific_column_indices(
    df: &DataFrame,
    cols_idx_numeric: &[usize],
    cols_idx_integer: &[usize],
    cols_idx_decimal_specified: &[usize],
    policy_scientific: &ScientificPolicySpec,
) -> Result<Vec<usize>, String> {
    if cols_idx_numeric.is_empty() || matches!(policy_scientific.rule_scope, ScientificScope::None)
    {
        return Ok(vec![]);
    }

    let integer_cols_idx: BTreeSet<usize> = cols_idx_integer.iter().copied().collect();
    let decimal_cols_idx_specified: BTreeSet<usize> =
        cols_idx_decimal_specified.iter().copied().collect();
    let is_decimal_explicit = !decimal_cols_idx_specified.is_empty();

    let rows_sample_max = match policy_scientific.height_body_inferred_max {
        Some(max_rows) => usize::min(df.height(), max_rows),
        None => df.height(),
    };
    let cols = df.get_columns();

    let mut scientific_cols = Vec::new();
    for _col_idx in cols_idx_numeric {
        let col_idx = *_col_idx;
        let is_integer_col = integer_cols_idx.contains(&col_idx);
        let should_include = match policy_scientific.rule_scope {
            ScientificScope::None => false,
            ScientificScope::Decimal => {
                if is_integer_col {
                    false
                } else if is_decimal_explicit {
                    decimal_cols_idx_specified.contains(&col_idx)
                } else {
                    true
                }
            }
            ScientificScope::Integer => is_integer_col,
            ScientificScope::All => true,
        };
        if !should_include {
            continue;
        }

        let col = &cols[col_idx];
        let mut should_use_scientific = false;
        for _row_idx in 0..rows_sample_max {
            let row_idx = _row_idx;
            let value = col
                .get(row_idx)
                .map_err(|err| format!("Failed to inspect scientific trigger value: {err}"))?;
            let Some(value_num) = derive_f64_from_any_value(value) else {
                continue;
            };
            if !value_num.is_finite() {
                continue;
            }

            let value_abs = value_num.abs();
            if value_abs >= policy_scientific.thr_max
                || (value_abs > 0.0 && value_abs < policy_scientific.thr_min)
            {
                should_use_scientific = true;
                break;
            }
        }

        if should_use_scientific {
            scientific_cols.push(col_idx);
        }
    }

    Ok(scientific_cols)
}

fn derive_f64_from_any_value(value: AnyValue<'_>) -> Option<f64> {
    match value {
        AnyValue::UInt8(val) => Some(val as f64),
        AnyValue::UInt16(val) => Some(val as f64),
        AnyValue::UInt32(val) => Some(val as f64),
        AnyValue::UInt64(val) => Some(val as f64),
        AnyValue::Int8(val) => Some(val as f64),
        AnyValue::Int16(val) => Some(val as f64),
        AnyValue::Int32(val) => Some(val as f64),
        AnyValue::Int64(val) => Some(val as f64),
        AnyValue::Int128(val) => Some(val as f64),
        AnyValue::Float32(val) => Some(val as f64),
        AnyValue::Float64(val) => Some(val),
        AnyValue::String(val) => val.parse::<f64>().ok(),
        AnyValue::StringOwned(val) => val.parse::<f64>().ok(),
        _ => None,
    }
}

fn derive_string_grid_from_dataframe(df: &DataFrame) -> Result<Vec<Vec<String>>, String> {
    let height = df.height();
    let width = df.width();
    let cols = df.get_columns();

    let mut grid = vec![vec![String::new(); width]; height];
    for _row in grid.iter_mut().enumerate() {
        let (row_idx, row_values) = _row;
        for _cell in row_values.iter_mut().enumerate() {
            let (col_idx, cell_value) = _cell;
            let value = cols[col_idx]
                .get(row_idx)
                .map_err(|err| format!("Failed to read header cell value: {err}"))?;
            *cell_value = derive_header_text_from_any_value(value);
        }
    }

    Ok(grid)
}

fn derive_header_text_from_any_value(value: AnyValue<'_>) -> String {
    match value {
        AnyValue::Null => String::new(),
        // Keep raw string payload for header cells; AnyValue::to_string() wraps
        // Utf8 values in quotes, which leaks into XLSX header text.
        AnyValue::String(val) => val.to_string(),
        AnyValue::StringOwned(val) => val.to_string(),
        _ => value.to_string(),
    }
}

fn derive_cell_value_from_any_value(value: AnyValue<'_>) -> CellValue {
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

fn derive_slice_indices(
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

fn write_header(
    worksheet: &mut Worksheet,
    mut header_grid: Vec<Vec<String>>,
    should_merge: bool,
    fmt_header: &Format,
) -> Result<(), String> {
    if !should_merge {
        for (row_idx, row_values) in header_grid.iter().enumerate() {
            for (col_idx, cell_value) in row_values.iter().enumerate() {
                if cell_value.is_empty() {
                    worksheet
                        .write_blank(cast_row_num(row_idx)?, cast_col_num(col_idx)?, fmt_header)
                        .map_err(derive_xlsx_error_text)?;
                } else {
                    worksheet
                        .write_string_with_format(
                            cast_row_num(row_idx)?,
                            cast_col_num(col_idx)?,
                            cell_value,
                            fmt_header,
                        )
                        .map_err(derive_xlsx_error_text)?;
                }
            }
        }
        return Ok(());
    }

    apply_vertical_run_text_blankout(&mut header_grid);
    let horizontal_merges_by_row = plan_horizontal_merges(&header_grid);
    let horizontal_merge_tracker = derive_horizontal_merge_tracker(&horizontal_merges_by_row);

    for (row_idx, row_values) in header_grid.iter().enumerate() {
        for (col_idx, cell_value) in row_values.iter().enumerate() {
            if horizontal_merge_tracker
                .get(&(row_idx, col_idx))
                .copied()
                .unwrap_or(false)
            {
                continue;
            }

            if cell_value.is_empty() {
                worksheet
                    .write_blank(cast_row_num(row_idx)?, cast_col_num(col_idx)?, fmt_header)
                    .map_err(derive_xlsx_error_text)?;
            } else {
                worksheet
                    .write_string_with_format(
                        cast_row_num(row_idx)?,
                        cast_col_num(col_idx)?,
                        cell_value,
                        fmt_header,
                    )
                    .map_err(derive_xlsx_error_text)?;
            }
        }

        if let Some(merges) = horizontal_merges_by_row.get(&row_idx) {
            for _merge in merges {
                let merge = _merge;
                worksheet
                    .merge_range(
                        cast_row_num(row_idx)?,
                        cast_col_num(merge.col_idx_start)?,
                        cast_row_num(row_idx)?,
                        cast_col_num(merge.col_idx_end)?,
                        &merge.text,
                        fmt_header,
                    )
                    .map_err(derive_xlsx_error_text)?;
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
                .map_err(derive_xlsx_error_text)?;
        }
        CellValue::String(val) => {
            worksheet
                .write_string_with_format(
                    cast_row_num(row_idx)?,
                    cast_col_num(col_idx)?,
                    val,
                    format,
                )
                .map_err(derive_xlsx_error_text)?;
        }
        CellValue::Number(val) => {
            worksheet
                .write_number_with_format(
                    cast_row_num(row_idx)?,
                    cast_col_num(col_idx)?,
                    *val,
                    format,
                )
                .map_err(derive_xlsx_error_text)?;
        }
    }
    Ok(())
}

fn derive_rust_xlsx_format(spec: &CellFormatSpec) -> Format {
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
        && let Some(align) = derive_format_align(val)
    {
        format = format.set_align(align);
    }
    if let Some(val) = &spec.valign
        && let Some(align) = derive_format_align(val)
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
        format = format.set_border(derive_format_border(val));
    }
    if let Some(val) = spec.top {
        format = format.set_border_top(derive_format_border(val));
    }
    if let Some(val) = spec.bottom {
        format = format.set_border_bottom(derive_format_border(val));
    }
    if let Some(val) = spec.left {
        format = format.set_border_left(derive_format_border(val));
    }
    if let Some(val) = spec.right {
        format = format.set_border_right(derive_format_border(val));
    }

    if spec.text_wrap.unwrap_or(false) {
        format = format.set_text_wrap();
    }

    format
}

fn derive_format_border(border: i64) -> FormatBorder {
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

fn derive_format_align(align: &str) -> Option<FormatAlign> {
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

fn derive_xlsx_error_text(err: XlsxError) -> String {
    format!("xlsx write error: {err}")
}

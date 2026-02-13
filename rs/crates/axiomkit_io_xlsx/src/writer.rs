//! XLSX writer kernel that converts DataFrame IPC into workbook output.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::path::PathBuf;

use polars::prelude::{AnyValue, DataFrame, IpcReader, SerReader};
use rust_xlsxwriter::{Format, FormatAlign, FormatBorder, Workbook, Worksheet, XlsxError};

use crate::conf::N_LEN_EXCEL_SHEET_NAME_MAX;
use crate::spec::{
    EnumAutofitColumnsRule, EnumCellValue, EnumScientificScope, SpecAutofitCellsPolicy,
    SpecCellFormat, SpecColumnFormatPlan, SpecScientificPolicy, SpecSheetSlice, SpecXlsxReport,
    SpecXlsxValuePolicy, SpecXlsxWriteOptions,
};
use crate::util::{
    apply_vertical_run_text_blankout, calculate_row_chunk_size, convert_cell_value,
    derive_horizontal_merge_tracker, generate_row_chunks, plan_horizontal_merges,
    plan_sheet_slices, sanitize_sheet_name, select_sorted_indices_from_refs,
    validate_unique_columns,
};

/// Per-sheet call options (aligned with Python `XlsxWriter.write_sheet` kwargs).
#[derive(Default, Debug, Clone)]
pub struct SpecXlsxSheetWriteOptions {
    /// Integer columns by name/index-string.
    pub cols_integer: Option<Vec<String>>,
    /// Decimal columns by name/index-string.
    pub cols_decimal: Option<Vec<String>>,
    /// Number of frozen columns.
    pub col_freeze: usize,
    /// Frozen row index; defaults to header height when `None`.
    pub row_freeze: Option<usize>,
    /// Enable merged multi-row header behavior.
    pub if_merge_header: bool,
    /// Override writer-level keep-missing behavior.
    pub if_keep_missing_values: Option<bool>,
    /// Column autofit policy.
    pub policy_autofit: SpecAutofitCellsPolicy,
    /// Scientific-format trigger policy.
    pub policy_scientific: SpecScientificPolicy,
}

pub struct SpecColumnFormatPlanOptions<'a> {
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
    pub cols_fmt_overrides: &'a BTreeMap<usize, SpecCellFormat>,
    /// Base text format.
    pub fmt_text: &'a SpecCellFormat,
    /// Base integer format.
    pub fmt_integer: &'a SpecCellFormat,
    /// Base decimal format.
    pub fmt_decimal: &'a SpecCellFormat,
    /// Base scientific format.
    pub fmt_scientific: &'a SpecCellFormat,
    /// Global write options.
    pub write_options: &'a SpecXlsxWriteOptions,
}

/// Stateful workbook writer.
pub struct XlsxWriter {
    path_file_out: PathBuf,
    workbook: Workbook,
    fmt_text: SpecCellFormat,
    fmt_integer: SpecCellFormat,
    fmt_decimal: SpecCellFormat,
    fmt_scientific: SpecCellFormat,
    fmt_header: SpecCellFormat,
    write_options: SpecXlsxWriteOptions,
    set_sheet_names_existing: BTreeSet<String>,
    l_reports: Vec<SpecXlsxReport>,
    if_closed: bool,
}

impl XlsxWriter {
    /// Create writer bound to output path and format/options presets.
    ///
    /// The workbook is buffered in memory until [`Self::close`] is called.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        path_file_out: PathBuf,
        fmt_text: SpecCellFormat,
        fmt_integer: SpecCellFormat,
        fmt_decimal: SpecCellFormat,
        fmt_scientific: SpecCellFormat,
        fmt_header: SpecCellFormat,
        write_options: SpecXlsxWriteOptions,
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
            set_sheet_names_existing: BTreeSet::new(),
            l_reports: Vec::new(),
            if_closed: false,
        }
    }

    /// Return output file path as string.
    pub fn file_out(&self) -> String {
        self.path_file_out.to_string_lossy().to_string()
    }

    /// Return immutable snapshot of per-sheet write reports.
    pub fn report(&self) -> Vec<SpecXlsxReport> {
        self.l_reports.clone()
    }

    /// Flush workbook to disk. Idempotent.
    pub fn close(&mut self) -> Result<(), String> {
        if self.if_closed {
            return Ok(());
        }
        self.workbook
            .save(&self.path_file_out)
            .map_err(derive_xlsx_error_text)?;
        self.if_closed = true;
        Ok(())
    }

    /// Write one sheet from in-memory dataframes.
    pub fn write_sheet_from_dataframes(
        &mut self,
        df_data: &DataFrame,
        sheet_name: &str,
        df_header: Option<&DataFrame>,
        options: &SpecXlsxSheetWriteOptions,
    ) -> Result<(), String> {
        if self.if_closed {
            return Err("Cannot write after close().".to_string());
        }
        self.write_sheet(df_data, sheet_name, df_header, options)
    }

    /// Write one sheet from IPC-serialized dataframe bytes.
    ///
    /// `v_ipc_df` and optional `v_ipc_df_header` must be valid Polars IPC payloads.
    pub fn write_sheet_from_ipc_bytes(
        &mut self,
        v_ipc_df: &[u8],
        sheet_name: &str,
        v_ipc_df_header: Option<&[u8]>,
        options: &SpecXlsxSheetWriteOptions,
    ) -> Result<(), String> {
        if self.if_closed {
            return Err("Cannot write after close().".to_string());
        }

        let df_data = derive_dataframe_from_ipc_bytes(v_ipc_df)?;
        let df_header = match v_ipc_df_header {
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
        options: &SpecXlsxSheetWriteOptions,
    ) -> Result<(), String> {
        validate_policy_autofit(&options.policy_autofit)?;
        validate_policy_scientific(&options.policy_scientific)?;

        let if_keep_missing_values = options
            .if_keep_missing_values
            .unwrap_or(self.write_options.keep_missing_values);
        let value_policy = self.write_options.value_policy.clone();

        let l_colnames_df: Vec<String> = df_data
            .get_column_names_str()
            .into_iter()
            .map(ToString::to_string)
            .collect();
        validate_unique_columns(&l_colnames_df)?;

        let n_width_df = l_colnames_df.len();
        let n_height_df = df_data.height();

        let mut l_header_grid = vec![l_colnames_df.clone()];
        if let Some(df_header_custom) = df_header {
            let l_header_cols: Vec<String> = df_header_custom
                .get_column_names_str()
                .into_iter()
                .map(ToString::to_string)
                .collect();
            validate_unique_columns(&l_header_cols)?;

            let n_header_height = df_header_custom.height();
            if n_header_height == 0 {
                return Err(
                    "df_header must have >= 1 row (0-row header is not allowed).".to_string(),
                );
            }
            let n_header_width = df_header_custom.width();
            if n_header_width != n_width_df {
                return Err("df_header.width must equal df.width.".to_string());
            }

            l_header_grid = derive_string_grid_from_dataframe(df_header_custom)?;
        }

        let l_cols_idx_numeric = if self.write_options.infer_numeric_cols {
            derive_numeric_column_indices(df_data)
        } else {
            vec![]
        };

        let l_cols_idx_integer_inferred = if self.write_options.infer_integer_cols {
            derive_integer_column_indices(df_data, &l_cols_idx_numeric)
        } else {
            vec![]
        };

        let l_cols_idx_integer_specified =
            select_sorted_indices_from_refs(&l_colnames_df, options.cols_integer.as_deref())?;
        let l_cols_idx_decimal_specified =
            select_sorted_indices_from_refs(&l_colnames_df, options.cols_decimal.as_deref())?;

        let l_cols_idx_integer = if l_cols_idx_integer_specified.is_empty() {
            l_cols_idx_integer_inferred
        } else {
            l_cols_idx_integer_specified
        };
        let l_cols_idx_scientific = derive_scientific_column_indices(
            df_data,
            &l_cols_idx_numeric,
            &l_cols_idx_integer,
            &l_cols_idx_decimal_specified,
            &options.policy_scientific,
        )?;

        let n_rows_header = l_header_grid.len();

        let mut report = SpecXlsxReport {
            sheets: vec![],
            warnings: vec![],
        };

        let l_sheet_parts = plan_sheet_slices(
            n_height_df,
            n_width_df,
            n_rows_header,
            &sanitize_sheet_name(sheet_name, "_"),
            &mut report,
        )?;

        let n_row_freeze = options.row_freeze.unwrap_or(n_rows_header);

        for sheet_slice in l_sheet_parts {
            let sheet_name_unique = self.derive_unique_sheet_name(&sheet_slice.sheet_name);
            let worksheet = self.workbook.add_worksheet();
            worksheet
                .set_name(&sheet_name_unique)
                .map_err(derive_xlsx_error_text)?;

            let l_cols_idx_numeric_slice = derive_slice_indices(
                &l_cols_idx_numeric,
                sheet_slice.col_start_inclusive,
                sheet_slice.col_end_exclusive,
            );
            let l_cols_idx_integer_slice = derive_slice_indices(
                &l_cols_idx_integer,
                sheet_slice.col_start_inclusive,
                sheet_slice.col_end_exclusive,
            );
            let l_cols_idx_decimal_slice = derive_slice_indices(
                &l_cols_idx_decimal_specified,
                sheet_slice.col_start_inclusive,
                sheet_slice.col_end_exclusive,
            );
            let l_cols_idx_scientific_slice = derive_slice_indices(
                &l_cols_idx_scientific,
                sheet_slice.col_start_inclusive,
                sheet_slice.col_end_exclusive,
            );

            let plan_col_formats = plan_column_formats(SpecColumnFormatPlanOptions {
                width_data: sheet_slice.col_end_exclusive - sheet_slice.col_start_inclusive,
                cols_idx_numeric: &l_cols_idx_numeric_slice,
                cols_idx_integer: &l_cols_idx_integer_slice,
                cols_idx_decimal: if l_cols_idx_decimal_slice.is_empty() {
                    None
                } else {
                    Some(&l_cols_idx_decimal_slice)
                },
                cols_idx_scientific: &l_cols_idx_scientific_slice,
                cols_fmt_overrides: &BTreeMap::new(),
                fmt_text: &self.fmt_text,
                fmt_integer: &self.fmt_integer,
                fmt_decimal: &self.fmt_decimal,
                fmt_scientific: &self.fmt_scientific,
                write_options: &self.write_options,
            });

            let l_fmt_data_by_col: Vec<Format> = plan_col_formats
                .fmts_by_col
                .iter()
                .map(derive_rust_xlsx_format)
                .collect();
            let fmt_header = derive_rust_xlsx_format(&self.fmt_header);

            let l_header_grid_slice = l_header_grid
                .iter()
                .map(|row| {
                    row[sheet_slice.col_start_inclusive..sheet_slice.col_end_exclusive].to_vec()
                })
                .collect::<Vec<_>>();

            let mut l_width_by_col_header = vec![0usize; l_fmt_data_by_col.len()];
            let mut l_width_by_col_body = vec![0usize; l_fmt_data_by_col.len()];

            let if_autofit_columns = !matches!(
                options.policy_autofit.rule_columns,
                EnumAutofitColumnsRule::None
            );

            if if_autofit_columns && !l_fmt_data_by_col.is_empty() {
                for n_idx_col in 0..l_fmt_data_by_col.len() {
                    for row in &l_header_grid_slice {
                        let value = &row[n_idx_col];
                        if value.is_empty() {
                            continue;
                        }
                        l_width_by_col_header[n_idx_col] = usize::max(
                            l_width_by_col_header[n_idx_col],
                            estimate_width_len(
                                &EnumCellValue::String(value.clone()),
                                false,
                                false,
                                false,
                                if_keep_missing_values,
                                &value_policy,
                            ),
                        );
                    }
                }
            }

            write_header(
                worksheet,
                l_header_grid_slice,
                options.if_merge_header,
                &fmt_header,
            )?;

            worksheet
                .set_freeze_panes(
                    cast_row_num(n_row_freeze)?,
                    cast_col_num(options.col_freeze)?,
                )
                .map_err(derive_xlsx_error_text)?;

            let set_cols_idx_numeric: BTreeSet<usize> =
                l_cols_idx_numeric_slice.iter().copied().collect();
            let set_cols_idx_integer: BTreeSet<usize> =
                l_cols_idx_integer_slice.iter().copied().collect();
            let set_cols_idx_scientific: BTreeSet<usize> =
                l_cols_idx_scientific_slice.iter().copied().collect();

            let mut l_cols_slice = Vec::with_capacity(l_fmt_data_by_col.len());
            let n_rows_data_this_sheet =
                sheet_slice.row_end_exclusive - sheet_slice.row_start_inclusive;
            for n_idx_col_abs in sheet_slice.col_start_inclusive..sheet_slice.col_end_exclusive {
                l_cols_slice.push(df_data.get_columns()[n_idx_col_abs].slice(
                    sheet_slice.row_start_inclusive as i64,
                    n_rows_data_this_sheet,
                ));
            }
            let n_rows_chunk = calculate_row_chunk_size(
                l_fmt_data_by_col.len(),
                &self.write_options.row_chunk_policy,
            );
            if n_rows_chunk == 0 {
                return Err("row_chunk_policy resolved to 0 rows; expected >= 1.".to_string());
            }
            let l_row_chunks = generate_row_chunks(n_rows_data_this_sheet, n_rows_chunk);

            let mut n_rows_seen_for_autofit = 0usize;
            for (n_row_chunk_start, n_rows_chunk_len) in l_row_chunks {
                let n_row_chunk_end = n_row_chunk_start + n_rows_chunk_len;
                for n_row_local in n_row_chunk_start..n_row_chunk_end {
                    for (n_idx_col, col) in l_cols_slice.iter().enumerate() {
                        let if_is_numeric_col = set_cols_idx_numeric.contains(&n_idx_col);
                        let if_is_integer_col = set_cols_idx_integer.contains(&n_idx_col);
                        let if_is_scientific_col = set_cols_idx_scientific.contains(&n_idx_col);

                        let value_raw = derive_cell_value_from_any_value(
                            col.get(n_row_local)
                                .map_err(|err| format!("Failed to access cell value: {err}"))?,
                        );
                        let value = convert_cell_value(
                            &value_raw,
                            if_is_numeric_col,
                            if_is_integer_col,
                            if_keep_missing_values,
                            &value_policy,
                        );

                        if if_autofit_columns
                            && (options.policy_autofit.height_body_inferred_max.is_none()
                                || n_rows_seen_for_autofit
                                    < options.policy_autofit.height_body_inferred_max.unwrap_or(0))
                        {
                            l_width_by_col_body[n_idx_col] = usize::max(
                                l_width_by_col_body[n_idx_col],
                                estimate_width_len(
                                    &value,
                                    if_is_numeric_col,
                                    if_is_integer_col,
                                    if_is_scientific_col,
                                    if_keep_missing_values,
                                    &value_policy,
                                ),
                            );
                        }

                        write_cell_with_format(
                            worksheet,
                            n_rows_header + n_row_local,
                            n_idx_col,
                            &value,
                            &l_fmt_data_by_col[n_idx_col],
                        )?;
                    }

                    if if_autofit_columns
                        && (options.policy_autofit.height_body_inferred_max.is_none()
                            || n_rows_seen_for_autofit
                                < options.policy_autofit.height_body_inferred_max.unwrap_or(0))
                    {
                        n_rows_seen_for_autofit += 1;
                    }
                }
            }

            if if_autofit_columns && !l_fmt_data_by_col.is_empty() {
                let n_min = usize::max(1, options.policy_autofit.width_cell_min);
                let n_max = usize::min(
                    255,
                    usize::max(n_min, options.policy_autofit.width_cell_max),
                );
                let n_pad = options.policy_autofit.width_cell_padding;

                for n_idx_col in 0..l_fmt_data_by_col.len() {
                    let n_width_recorded = match options.policy_autofit.rule_columns {
                        EnumAutofitColumnsRule::Header => l_width_by_col_header[n_idx_col],
                        EnumAutofitColumnsRule::Body => l_width_by_col_body[n_idx_col],
                        EnumAutofitColumnsRule::All => usize::max(
                            l_width_by_col_header[n_idx_col],
                            l_width_by_col_body[n_idx_col],
                        ),
                        EnumAutofitColumnsRule::None => l_width_by_col_header[n_idx_col],
                    };
                    let n_width_final =
                        usize::min(n_max, usize::max(n_min, n_width_recorded + n_pad));
                    worksheet
                        .set_column_width(cast_col_num(n_idx_col)?, n_width_final as f64)
                        .map_err(derive_xlsx_error_text)?;
                }
            }

            report.sheets.push(SpecSheetSlice {
                sheet_name: sheet_name_unique,
                row_start_inclusive: sheet_slice.row_start_inclusive,
                row_end_exclusive: sheet_slice.row_end_exclusive,
                col_start_inclusive: sheet_slice.col_start_inclusive,
                col_end_exclusive: sheet_slice.col_end_exclusive,
            });
        }

        self.l_reports.push(report);
        Ok(())
    }

    fn derive_unique_sheet_name(&mut self, name: &str) -> String {
        if !self.set_sheet_names_existing.contains(name) {
            self.set_sheet_names_existing.insert(name.to_string());
            return name.to_string();
        }

        let base_name: String = name
            .chars()
            .take(usize::max(1, N_LEN_EXCEL_SHEET_NAME_MAX - 3))
            .collect();

        let mut n_idx = 2usize;
        loop {
            let candidate: String = format!("{base_name}__{n_idx}")
                .chars()
                .take(N_LEN_EXCEL_SHEET_NAME_MAX)
                .collect();
            if !self.set_sheet_names_existing.contains(&candidate) {
                self.set_sheet_names_existing.insert(candidate.clone());
                return candidate;
            }
            n_idx += 1;
        }
    }
}

/// Estimate displayed width units for one normalized cell value.
///
/// Used by autofit inference logic.
pub fn estimate_width_len(
    value: &EnumCellValue,
    if_is_numeric_col: bool,
    if_is_integer_col: bool,
    if_is_scientific_col: bool,
    if_keep_missing_values: bool,
    value_policy: &SpecXlsxValuePolicy,
) -> usize {
    match value {
        EnumCellValue::None => {
            if if_keep_missing_values {
                value_policy.missing_value_str.len()
            } else {
                0
            }
        }
        EnumCellValue::String(s) => {
            if s.is_empty() {
                return 0;
            }
            if !if_is_numeric_col {
                return estimate_unicode_string_width(s);
            }
            if if_is_scientific_col && let Ok(val) = s.parse::<f64>() {
                return format!("{val:.2E}").len();
            }
            if if_is_integer_col && let Ok(val) = s.parse::<i64>() {
                return val.to_string().len();
            }
            estimate_unicode_string_width(s)
        }
        EnumCellValue::Number(n) => {
            if !if_is_numeric_col {
                return estimate_unicode_string_width(&n.to_string());
            }
            if if_is_scientific_col {
                return format!("{n:.2E}").len();
            }
            if if_is_integer_col {
                return (*n as i64).to_string().len();
            }
            format!("{n:.4}").len()
        }
    }
}

fn estimate_unicode_string_width(s: &str) -> usize {
    let n_ascii = s.chars().filter(|chr| chr.is_ascii()).count();
    let n_non_ascii = s.chars().count().saturating_sub(n_ascii);
    n_ascii + (n_non_ascii as f64 * 1.6).round() as usize
}

/// Build per-column base/final format plans for current sheet slice.
pub fn plan_column_formats(options: SpecColumnFormatPlanOptions<'_>) -> SpecColumnFormatPlan {
    let SpecColumnFormatPlanOptions {
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

    let set_cols_idx_numeric: BTreeSet<usize> = cols_idx_numeric.iter().copied().collect();
    let set_cols_idx_integer: BTreeSet<usize> = cols_idx_integer.iter().copied().collect();
    let set_cols_idx_decimal: Option<BTreeSet<usize>> =
        cols_idx_decimal.map(|vals| vals.iter().copied().collect());
    let set_cols_idx_scientific: BTreeSet<usize> = cols_idx_scientific.iter().copied().collect();

    let mut fmts_base_by_col = Vec::with_capacity(width_data);
    let mut fmts_by_col = Vec::with_capacity(width_data);

    for col_idx in 0..width_data {
        let mut fmt_base = if set_cols_idx_scientific.contains(&col_idx) {
            fmt_scientific.clone()
        } else if set_cols_idx_integer.contains(&col_idx) {
            fmt_integer.clone()
        } else if set_cols_idx_decimal
            .as_ref()
            .map_or(set_cols_idx_numeric.contains(&col_idx), |set_idx| {
                set_idx.contains(&col_idx)
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

    SpecColumnFormatPlan {
        fmts_by_col,
        fmts_base_by_col,
    }
}

fn derive_dataframe_from_ipc_bytes(v_ipc_df: &[u8]) -> Result<DataFrame, String> {
    IpcReader::new(Cursor::new(v_ipc_df))
        .finish()
        .map_err(|err| format!("Failed to read IPC DataFrame bytes: {err}"))
}

fn validate_policy_autofit(policy_autofit: &SpecAutofitCellsPolicy) -> Result<(), String> {
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

fn validate_policy_scientific(policy_scientific: &SpecScientificPolicy) -> Result<(), String> {
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
        .filter_map(|(n_idx, c_col)| {
            if c_col.dtype().is_numeric() {
                Some(n_idx)
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
        .filter(|n_idx| df.get_columns()[*n_idx].dtype().is_integer())
        .collect()
}

fn derive_scientific_column_indices(
    df: &DataFrame,
    cols_idx_numeric: &[usize],
    cols_idx_integer: &[usize],
    cols_idx_decimal_specified: &[usize],
    policy_scientific: &SpecScientificPolicy,
) -> Result<Vec<usize>, String> {
    if cols_idx_numeric.is_empty()
        || matches!(policy_scientific.rule_scope, EnumScientificScope::None)
    {
        return Ok(vec![]);
    }

    let set_cols_idx_integer: BTreeSet<usize> = cols_idx_integer.iter().copied().collect();
    let set_cols_idx_decimal_specified: BTreeSet<usize> =
        cols_idx_decimal_specified.iter().copied().collect();
    let if_decimal_is_explicit = !set_cols_idx_decimal_specified.is_empty();

    let n_rows_sample_max = match policy_scientific.height_body_inferred_max {
        Some(n_max) => usize::min(df.height(), n_max),
        None => df.height(),
    };
    let l_cols = df.get_columns();

    let mut l_cols_idx_scientific = Vec::new();
    for n_idx_col in cols_idx_numeric {
        let if_is_integer_col = set_cols_idx_integer.contains(n_idx_col);
        let if_include = match policy_scientific.rule_scope {
            EnumScientificScope::None => false,
            EnumScientificScope::Decimal => {
                if if_is_integer_col {
                    false
                } else if if_decimal_is_explicit {
                    set_cols_idx_decimal_specified.contains(n_idx_col)
                } else {
                    true
                }
            }
            EnumScientificScope::Integer => if_is_integer_col,
            EnumScientificScope::All => true,
        };
        if !if_include {
            continue;
        }

        let col = &l_cols[*n_idx_col];
        let mut if_use_scientific = false;
        for n_idx_row in 0..n_rows_sample_max {
            let value = col
                .get(n_idx_row)
                .map_err(|err| format!("Failed to inspect scientific trigger value: {err}"))?;
            let Some(n_value) = derive_f64_from_any_value(value) else {
                continue;
            };
            if !n_value.is_finite() {
                continue;
            }

            let n_abs = n_value.abs();
            if n_abs >= policy_scientific.thr_max
                || (n_abs > 0.0 && n_abs < policy_scientific.thr_min)
            {
                if_use_scientific = true;
                break;
            }
        }

        if if_use_scientific {
            l_cols_idx_scientific.push(*n_idx_col);
        }
    }

    Ok(l_cols_idx_scientific)
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
    let n_height = df.height();
    let n_width = df.width();
    let l_cols = df.get_columns();

    let mut l_grid = vec![vec![String::new(); n_width]; n_height];
    for (_idx_row, _val_row) in l_grid.iter_mut().enumerate() {
        for (_idx_col, _val_cell) in _val_row.iter_mut().enumerate() {
            let value = l_cols[_idx_col]
                .get(_idx_row)
                .map_err(|err| format!("Failed to read header cell value: {err}"))?;
            *_val_cell = derive_header_text_from_any_value(value);
        }
    }

    Ok(l_grid)
}

fn derive_header_text_from_any_value(value: AnyValue<'_>) -> String {
    match value {
        AnyValue::Null => String::new(),
        _ => value.to_string(),
    }
}

fn derive_cell_value_from_any_value(value: AnyValue<'_>) -> EnumCellValue {
    match value {
        AnyValue::Null => EnumCellValue::None,
        AnyValue::String(val) => EnumCellValue::String(val.to_string()),
        AnyValue::StringOwned(val) => EnumCellValue::String(val.to_string()),
        AnyValue::Boolean(val) => {
            EnumCellValue::String(if val { "True" } else { "False" }.to_string())
        }
        AnyValue::UInt8(val) => EnumCellValue::Number(val as f64),
        AnyValue::UInt16(val) => EnumCellValue::Number(val as f64),
        AnyValue::UInt32(val) => EnumCellValue::Number(val as f64),
        AnyValue::UInt64(val) => EnumCellValue::Number(val as f64),
        AnyValue::Int8(val) => EnumCellValue::Number(val as f64),
        AnyValue::Int16(val) => EnumCellValue::Number(val as f64),
        AnyValue::Int32(val) => EnumCellValue::Number(val as f64),
        AnyValue::Int64(val) => EnumCellValue::Number(val as f64),
        AnyValue::Int128(val) => EnumCellValue::Number(val as f64),
        AnyValue::Float32(val) => EnumCellValue::Number(val as f64),
        AnyValue::Float64(val) => EnumCellValue::Number(val),
        _ => EnumCellValue::String(value.to_string()),
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
    if_merge: bool,
    fmt_header: &Format,
) -> Result<(), String> {
    if !if_merge {
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
    let dict_horizontal_merges_by_row = plan_horizontal_merges(&header_grid);
    let dict_horizontal_merge_tracker =
        derive_horizontal_merge_tracker(&dict_horizontal_merges_by_row);

    for (row_idx, row_values) in header_grid.iter().enumerate() {
        for (col_idx, cell_value) in row_values.iter().enumerate() {
            if dict_horizontal_merge_tracker
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

        if let Some(l_merges) = dict_horizontal_merges_by_row.get(&row_idx) {
            for merge in l_merges {
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
    value: &EnumCellValue,
    format: &Format,
) -> Result<(), String> {
    match value {
        EnumCellValue::None => {
            worksheet
                .write_blank(cast_row_num(row_idx)?, cast_col_num(col_idx)?, format)
                .map_err(derive_xlsx_error_text)?;
        }
        EnumCellValue::String(val) => {
            worksheet
                .write_string_with_format(
                    cast_row_num(row_idx)?,
                    cast_col_num(col_idx)?,
                    val,
                    format,
                )
                .map_err(derive_xlsx_error_text)?;
        }
        EnumCellValue::Number(val) => {
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

fn derive_rust_xlsx_format(spec: &SpecCellFormat) -> Format {
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

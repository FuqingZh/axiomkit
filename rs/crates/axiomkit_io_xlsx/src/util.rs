//! Stateless helper utilities used by the XLSX writer kernel.

use std::collections::{BTreeMap, BTreeSet};

use crate::conf::{
    N_LEN_EXCEL_SHEET_NAME_MAX, N_NCOLS_EXCEL_MAX, N_NROWS_EXCEL_MAX, TUP_EXCEL_ILLEGAL,
};
use crate::spec::{
    EnumCellValue, EnumIntegerCoerceMode, SpecCellBorder, SpecSheetHorizontalMerge, SpecSheetSlice,
    SpecXlsxReport, SpecXlsxRowChunkPolicy, SpecXlsxValuePolicy,
};

////////////////////////////////////////////////////////////////////////////////
// #region CellValueConversion

/// Convert `NaN`/`Inf` to policy string; return error for finite values.
pub fn convert_nan_inf_to_str(
    x: f64,
    value_policy: &SpecXlsxValuePolicy,
) -> Result<String, String> {
    if x.is_nan() {
        return Ok(value_policy.nan_str.clone());
    }
    if x.is_infinite() {
        return Ok(if x.is_sign_positive() {
            value_policy.posinf_str.clone()
        } else {
            value_policy.neginf_str.clone()
        });
    }
    Err("Input is neither NaN nor Inf.".to_string())
}

/// Normalize cell value according to numeric/integer flags and value policy.
pub fn convert_cell_value(
    value: &EnumCellValue,
    if_is_numeric_col: bool,
    if_is_integer_col: bool,
    if_keep_missing_values: bool,
    value_policy: &SpecXlsxValuePolicy,
) -> EnumCellValue {
    if matches!(value, EnumCellValue::None) {
        return if if_keep_missing_values {
            EnumCellValue::String(value_policy.missing_value_str.clone())
        } else {
            EnumCellValue::None
        };
    }

    if !if_is_numeric_col {
        return match value {
            EnumCellValue::String(s) => EnumCellValue::String(s.clone()),
            EnumCellValue::Number(n) => EnumCellValue::String(n.to_string()),
            EnumCellValue::None => EnumCellValue::None,
        };
    }

    if if_is_integer_col {
        return match value {
            EnumCellValue::Number(n) => {
                if !n.is_finite() {
                    if if_keep_missing_values {
                        EnumCellValue::String(
                            convert_nan_inf_to_str(*n, value_policy)
                                .unwrap_or_else(|_| value_policy.nan_str.clone()),
                        )
                    } else {
                        EnumCellValue::None
                    }
                } else if value_policy.integer_coerce == EnumIntegerCoerceMode::Coerce {
                    EnumCellValue::Number(*n as i64 as f64)
                } else if n.fract() == 0.0 {
                    EnumCellValue::Number(*n)
                } else {
                    EnumCellValue::String(n.to_string())
                }
            }
            EnumCellValue::String(s) => {
                if value_policy.integer_coerce == EnumIntegerCoerceMode::Coerce {
                    if let Ok(v) = s.parse::<i64>() {
                        EnumCellValue::Number(v as f64)
                    } else if let Ok(v) = s.parse::<f64>() {
                        if v.is_finite() {
                            EnumCellValue::Number(v as i64 as f64)
                        } else if if_keep_missing_values {
                            EnumCellValue::String(
                                convert_nan_inf_to_str(v, value_policy)
                                    .unwrap_or_else(|_| value_policy.nan_str.clone()),
                            )
                        } else {
                            EnumCellValue::None
                        }
                    } else {
                        EnumCellValue::String(s.clone())
                    }
                } else if let Ok(v) = s.parse::<i64>() {
                    EnumCellValue::Number(v as f64)
                } else {
                    EnumCellValue::String(s.clone())
                }
            }
            EnumCellValue::None => EnumCellValue::None,
        };
    }

    match value {
        EnumCellValue::Number(n) => {
            if n.is_finite() {
                EnumCellValue::Number(*n)
            } else if if_keep_missing_values {
                EnumCellValue::String(
                    convert_nan_inf_to_str(*n, value_policy)
                        .unwrap_or_else(|_| value_policy.nan_str.clone()),
                )
            } else {
                EnumCellValue::None
            }
        }
        EnumCellValue::String(s) => {
            if let Ok(v) = s.parse::<f64>() {
                if v.is_finite() {
                    EnumCellValue::Number(v)
                } else if if_keep_missing_values {
                    EnumCellValue::String(
                        convert_nan_inf_to_str(v, value_policy)
                            .unwrap_or_else(|_| value_policy.nan_str.clone()),
                    )
                } else {
                    EnumCellValue::None
                }
            } else {
                EnumCellValue::String(s.clone())
            }
        }
        EnumCellValue::None => EnumCellValue::None,
    }
}

// #endregion
////////////////////////////////////////////////////////////////////////////////
// #region DataFrameLikeUtils

/// Validate that `columns` has no duplicated names.
pub fn validate_unique_columns(columns: &[String]) -> Result<(), String> {
    if columns.len() == columns.iter().collect::<BTreeSet<_>>().len() {
        return Ok(());
    }

    let mut dict_pos: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (n_idx, c_name) in columns.iter().enumerate() {
        dict_pos.entry(c_name).or_default().push(n_idx);
    }

    let c_msg = dict_pos
        .iter()
        .filter_map(|(c_name, l_pos)| {
            if l_pos.len() > 1 {
                Some(format!(
                    "{c_name:?} x{} at indices {:?}",
                    l_pos.len(),
                    l_pos
                ))
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("; ");

    Err(format!("Duplicate column names detected: {c_msg}"))
}

/// Resolve mixed refs (`name` or numeric string index) to sorted unique indices.
pub fn select_sorted_indices_from_refs(
    columns: &[String],
    refs: Option<&[String]>,
) -> Result<Vec<usize>, String> {
    let Some(refs) = refs else {
        return Ok(vec![]);
    };

    let mut set_idx = BTreeSet::new();
    for ref_col in refs {
        if let Ok(n_idx) = ref_col.parse::<usize>() {
            set_idx.insert(n_idx);
            continue;
        }

        let Some(n_idx) = columns.iter().position(|c_name| c_name == ref_col) else {
            return Err(format!("Column not found: {ref_col:?}"));
        };
        set_idx.insert(n_idx);
    }

    Ok(set_idx.into_iter().collect())
}

// #endregion
////////////////////////////////////////////////////////////////////////////////
// #region RowChunking

/// Derive row chunk size from dataframe width and chunk policy.
pub fn calculate_row_chunk_size(width_df: usize, policy: &SpecXlsxRowChunkPolicy) -> usize {
    if let Some(n_fixed_size) = policy.fixed_size {
        return n_fixed_size;
    }
    if width_df >= policy.width_large {
        return policy.size_large;
    }
    if width_df >= policy.width_medium {
        return policy.size_medium;
    }
    policy.size_default
}

/// Generate `(row_start, row_len)` chunks for `n_rows_total`.
pub fn generate_row_chunks(n_rows_total: usize, size_rows_chunk: usize) -> Vec<(usize, usize)> {
    let mut l_chunks = Vec::new();
    let mut n_row_cursor = 0;
    while n_row_cursor < n_rows_total {
        let n_rows_per_chunk = usize::min(size_rows_chunk, n_rows_total - n_row_cursor);
        l_chunks.push((n_row_cursor, n_rows_per_chunk));
        n_row_cursor += n_rows_per_chunk;
    }
    l_chunks
}

// #endregion
////////////////////////////////////////////////////////////////////////////////
// #region SheetNormalization

/// Replace invalid chars and trim to valid Excel sheet name.
pub fn sanitize_sheet_name(name: &str, replace_to: &str) -> String {
    let mut c_name = name.to_string();
    for c_illegal in TUP_EXCEL_ILLEGAL {
        c_name = c_name.replace(c_illegal, replace_to);
    }
    c_name = c_name.trim().to_string();
    if c_name.is_empty() {
        c_name = "Sheet".to_string();
    }

    c_name.chars().take(N_LEN_EXCEL_SHEET_NAME_MAX).collect()
}

/// Split logical dataframe range into Excel-compliant sheet slices.
pub fn plan_sheet_slices(
    height_df: usize,
    width_df: usize,
    height_header: usize,
    sheet_name: &str,
    report: &mut SpecXlsxReport,
) -> Result<Vec<SpecSheetSlice>, String> {
    if height_header == 0 {
        return Err("height_header must be >= 1.".to_string());
    }

    let n_rows_data_max = N_NROWS_EXCEL_MAX
        .checked_sub(height_header)
        .ok_or_else(|| {
            format!("Header too tall: height_header={height_header} exceeds Excel limit.")
        })?;

    if n_rows_data_max == 0 {
        return Err(format!(
            "Header too tall: height_header={height_header} exceeds Excel limit."
        ));
    }

    let mut l_col_slices = Vec::new();
    let mut n_col_start = 0;
    while n_col_start < width_df {
        let n_col_end = usize::min(width_df, n_col_start + N_NCOLS_EXCEL_MAX);
        l_col_slices.push((n_col_start, n_col_end));
        n_col_start = n_col_end;
    }

    let mut l_row_slices = Vec::new();
    let mut n_row_start = 0;
    while n_row_start < height_df {
        let n_row_end = usize::min(height_df, n_row_start + n_rows_data_max);
        l_row_slices.push((n_row_start, n_row_end));
        n_row_start = n_row_end;
    }

    if l_row_slices.is_empty() {
        l_row_slices.push((0, 0));
    }

    let n_parts_total = l_col_slices.len() * l_row_slices.len();

    let mut l_sheet_parts = Vec::new();
    let mut n_idx_part = 1;
    for (col_start, col_end) in &l_col_slices {
        for (row_start, row_end) in &l_row_slices {
            let c_part_sheet_name = if n_parts_total == 1 {
                sheet_name.to_string()
            } else {
                create_sheet_identifier(sheet_name, n_idx_part)
            };

            l_sheet_parts.push(SpecSheetSlice {
                sheet_name: c_part_sheet_name,
                row_start_inclusive: *row_start,
                row_end_exclusive: *row_end,
                col_start_inclusive: *col_start,
                col_end_exclusive: *col_end,
            });
            n_idx_part += 1;
        }
    }

    if n_parts_total > 1 {
        report.warn(format!(
            "Excel limit overflow: split into {} sheets (columns-first, then rows).",
            l_sheet_parts.len()
        ));
    }

    Ok(l_sheet_parts)
}

/// Create suffixed sheet name (`base_1`, `base_2`, ...), respecting length cap.
pub fn create_sheet_identifier(base_name: &str, part_idx_1based: usize) -> String {
    let c_sheet_name_suffix = format!("_{part_idx_1based}");
    let n_len_base_name_max = N_LEN_EXCEL_SHEET_NAME_MAX.saturating_sub(c_sheet_name_suffix.len());

    let c_sheet_name_base: String = base_name
        .chars()
        .take(usize::max(1, n_len_base_name_max))
        .collect();

    format!("{c_sheet_name_base}{c_sheet_name_suffix}")
}

// #endregion
////////////////////////////////////////////////////////////////////////////////
// #region HeaderMergeUtils

/// Convert sorted indices to contiguous inclusive ranges.
pub fn derive_contiguous_ranges(sorted_indices: &[usize]) -> Vec<(usize, usize)> {
    if sorted_indices.is_empty() {
        return vec![];
    }

    let mut l_contiguous_ranges = Vec::new();
    let mut n_idx_start = sorted_indices[0];
    let mut n_idx_end = sorted_indices[0];

    for idx in &sorted_indices[1..] {
        if *idx == n_idx_end + 1 {
            n_idx_end = *idx;
        } else {
            l_contiguous_ranges.push((n_idx_start, n_idx_end));
            n_idx_start = *idx;
            n_idx_end = *idx;
        }
    }

    l_contiguous_ranges.push((n_idx_start, n_idx_end));
    l_contiguous_ranges
}

/// Plan horizontal merges for repeated non-empty header text per row.
pub fn plan_horizontal_merges(
    header_grid: &[Vec<String>],
) -> BTreeMap<usize, Vec<SpecSheetHorizontalMerge>> {
    let mut dict_horizontal_merges_map = BTreeMap::new();
    if header_grid.is_empty() {
        return dict_horizontal_merges_map;
    }

    let n_rows = header_grid.len();
    let n_cols = header_grid[0].len();

    for (_idx_row, _) in header_grid.iter().enumerate().take(n_rows) {
        let v_str_current_row = &header_grid[_idx_row];
        let mut n_col_idx = 0;

        while n_col_idx < n_cols {
            let c_cell_val = &v_str_current_row[n_col_idx];
            if c_cell_val.is_empty() {
                n_col_idx += 1;
                continue;
            }

            let mut n_col_idx_end = n_col_idx + 1;
            while n_col_idx_end < n_cols && v_str_current_row[n_col_idx_end] == *c_cell_val {
                n_col_idx_end += 1;
            }

            if n_col_idx_end - n_col_idx > 1 {
                dict_horizontal_merges_map
                    .entry(_idx_row)
                    .or_insert_with(Vec::new)
                    .push(SpecSheetHorizontalMerge {
                        row_idx_start: _idx_row,
                        col_idx_start: n_col_idx,
                        col_idx_end: n_col_idx_end - 1,
                        text: c_cell_val.clone(),
                    });
            }
            n_col_idx = n_col_idx_end;
        }
    }

    dict_horizontal_merges_map
}

/// Generate contiguous vertical runs `(col, row_start, row_end, text)`.
pub fn _generate_vertical_runs(header_grid: &[Vec<String>]) -> Vec<(usize, usize, usize, String)> {
    let mut v_run_collection = Vec::new();
    if header_grid.is_empty() {
        return v_run_collection;
    }
    let Some(v_header_row_0) = header_grid.first() else {
        return v_run_collection;
    };

    let n_rows = header_grid.len();
    let n_cols = v_header_row_0.len();

    debug_assert!(
        header_grid.iter().all(|_row| _row.len() == n_cols),
        "All rows must have the same number of columns."
    );

    for (_idx_col, _) in v_header_row_0.iter().enumerate() {
        let mut n_row_idx_start = 0;
        while n_row_idx_start < n_rows {
            let c_val_cell_current = &header_grid[n_row_idx_start][_idx_col];
            if c_val_cell_current.is_empty() {
                n_row_idx_start += 1;
                continue;
            }

            let mut n_row_idx_next = n_row_idx_start + 1;
            while n_row_idx_next < n_rows
                && header_grid[n_row_idx_next][_idx_col] == *c_val_cell_current
            {
                n_row_idx_next += 1;
            }

            let n_len_vertical_run = n_row_idx_next - n_row_idx_start;
            if n_len_vertical_run > 1 {
                v_run_collection.push((
                    _idx_col,
                    n_row_idx_start,
                    n_row_idx_next - 1,
                    c_val_cell_current.clone(),
                ));
            }

            n_row_idx_start = n_row_idx_next;
        }
    }

    v_run_collection
}

/// Build border plan to simulate vertical merge visuals without merge cells.
pub fn plan_vertical_visual_merge_borders(
    header_grid: &[Vec<String>],
) -> BTreeMap<(usize, usize), SpecCellBorder> {
    let mut dict_plan_vertical_merge_border = BTreeMap::new();

    for (col_idx, row_start, row_end, _) in _generate_vertical_runs(header_grid) {
        for row_idx_within_merge in row_start..=row_end {
            dict_plan_vertical_merge_border.insert(
                (row_idx_within_merge, col_idx),
                SpecCellBorder {
                    top: if row_idx_within_merge == row_start {
                        1
                    } else {
                        0
                    },
                    bottom: if row_idx_within_merge == row_end {
                        1
                    } else {
                        0
                    },
                    left: 1,
                    right: 1,
                },
            );
        }
    }

    dict_plan_vertical_merge_border
}

/// Clear repeated text in vertical runs, keeping only first row text.
pub fn apply_vertical_run_text_blankout(header_grid: &mut [Vec<String>]) {
    for (col_idx, row_start, row_end, _) in _generate_vertical_runs(header_grid) {
        for _row in header_grid.iter_mut().take(row_end + 1).skip(row_start + 1) {
            _row[col_idx].clear();
        }
    }
}

/// Build lookup map for cells covered by a horizontal merge (excluding anchor).
pub fn derive_horizontal_merge_tracker(
    row_horizontal_merge_mapping: &BTreeMap<usize, Vec<SpecSheetHorizontalMerge>>,
) -> BTreeMap<(usize, usize), bool> {
    let mut dict_merged_cells_tracker = BTreeMap::new();

    for (row_idx, horizontal_merges) in row_horizontal_merge_mapping {
        for merge in horizontal_merges {
            for col_idx in (merge.col_idx_start + 1)..=merge.col_idx_end {
                dict_merged_cells_tracker.insert((*row_idx, col_idx), true);
            }
        }
    }

    dict_merged_cells_tracker
}

// #endregion
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_vertical_runs_detects_only_contiguous_non_empty_runs() {
        let grid = vec![
            vec!["A".to_string(), "X".to_string()],
            vec!["A".to_string(), "".to_string()],
            vec!["A".to_string(), "X".to_string()],
            vec!["".to_string(), "X".to_string()],
            vec!["B".to_string(), "X".to_string()],
            vec!["B".to_string(), "Y".to_string()],
        ];

        assert_eq!(
            _generate_vertical_runs(&grid),
            vec![
                (0, 0, 2, "A".to_string()),
                (0, 4, 5, "B".to_string()),
                (1, 2, 4, "X".to_string())
            ]
        );
    }

    #[test]
    fn test_apply_vertical_run_text_blankout() {
        let mut grid = vec![
            vec!["A".to_string(), "B".to_string()],
            vec!["A".to_string(), "B".to_string()],
            vec!["".to_string(), "B".to_string()],
            vec!["C".to_string(), "B".to_string()],
            vec!["C".to_string(), "".to_string()],
        ];

        apply_vertical_run_text_blankout(&mut grid);

        assert_eq!(grid[0][0], "A");
        assert_eq!(grid[1][0], "");
        assert_eq!(grid[3][0], "C");
        assert_eq!(grid[4][0], "");

        assert_eq!(grid[0][1], "B");
        assert_eq!(grid[1][1], "");
        assert_eq!(grid[2][1], "");
        assert_eq!(grid[3][1], "");
    }
}

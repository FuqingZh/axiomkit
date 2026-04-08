//! `axiomkit_io_xlsx` v1:
//! Rust-side XLSX helper kernel.
//!
//! Architecture mirrors Python `io/xlsx` modules:
//! - `conf`   : constants and default presets
//! - `spec`   : specs/models/options
//! - `util`   : pure helper functions
//! - `writer` : pure-Rust writer kernel
pub mod constant;
pub mod spec;
pub mod util;
pub mod writer;

pub use constant::{LEN_EXCEL_SHEET_NAME_MAX, NCOLS_EXCEL_MAX, NROWS_EXCEL_MAX, TUP_EXCEL_ILLEGAL};
pub use spec::{
    AutofitMode, AutofitPolicy, CellBorder, CellFormatPatch, IntegerCoerceMode, ScientificPolicy,
    ScientificScope, SheetHorizontalMerge, SheetSlice, XlsxReport, XlsxRowChunkPolicy,
    XlsxValuePolicy, XlsxWriteOptions,
};
pub use util::{
    apply_vertical_run_text_blankout, calculate_row_chunk_size, convert_nan_inf_to_str,
    derive_contiguous_ranges, derive_horizontal_merge_tracker, plan_horizontal_merges,
    plan_sheet_slices, plan_vertical_visual_merge_borders, sanitize_sheet_name,
};
pub use writer::{XlsxSheetWriteOptions, XlsxWriter};

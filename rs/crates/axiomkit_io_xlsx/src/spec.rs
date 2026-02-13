//! Shared XLSX specification models.

use std::collections::BTreeMap;

////////////////////////////////////////////////////////////////////////////////
// #region CellFormatSpecification

/// Cell format specification aligned with Python `SpecCellFormat`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct SpecCellFormat {
    /// Font family name.
    pub font_name: Option<String>,
    /// Font size in points.
    pub font_size: Option<i64>,
    /// Bold style.
    pub bold: Option<bool>,
    /// Italic style.
    pub italic: Option<bool>,

    /// Horizontal alignment.
    pub align: Option<String>,
    /// Vertical alignment.
    pub valign: Option<String>,
    /// Border style for all sides.
    pub border: Option<i64>,
    /// Text wrap.
    pub text_wrap: Option<bool>,

    /// Top border override.
    pub top: Option<i64>,
    /// Bottom border override.
    pub bottom: Option<i64>,
    /// Left border override.
    pub left: Option<i64>,
    /// Right border override.
    pub right: Option<i64>,

    /// Number format code.
    pub num_format: Option<String>,
    /// Background fill color.
    pub bg_color: Option<String>,
    /// Font color.
    pub font_color: Option<String>,
}

/// Scalar value for generic format-map representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnumCellFormatValue {
    /// String format property value.
    String(String),
    /// Integer format property value.
    Integer(i64),
    /// Boolean format property value.
    Boolean(bool),
}

/// Normalized cell value during conversion/write pipeline.
#[derive(Debug, Clone, PartialEq)]
pub enum EnumCellValue {
    /// Missing/blank value.
    None,
    /// Text value.
    String(String),
    /// Numeric value.
    Number(f64),
}

impl SpecCellFormat {
    /// Return a new format by overlaying `patch` onto `self`.
    pub fn with_(&self, patch: SpecCellFormat) -> SpecCellFormat {
        self.merge(&patch)
    }

    /// Merge two formats with right-side non-`None` overwrite semantics.
    pub fn merge(&self, other: &SpecCellFormat) -> SpecCellFormat {
        SpecCellFormat {
            font_name: other.font_name.clone().or_else(|| self.font_name.clone()),
            font_size: other.font_size.or(self.font_size),
            bold: other.bold.or(self.bold),
            italic: other.italic.or(self.italic),
            align: other.align.clone().or_else(|| self.align.clone()),
            valign: other.valign.clone().or_else(|| self.valign.clone()),
            border: other.border.or(self.border),
            text_wrap: other.text_wrap.or(self.text_wrap),
            top: other.top.or(self.top),
            bottom: other.bottom.or(self.bottom),
            left: other.left.or(self.left),
            right: other.right.or(self.right),
            num_format: other.num_format.clone().or_else(|| self.num_format.clone()),
            bg_color: other.bg_color.clone().or_else(|| self.bg_color.clone()),
            font_color: other.font_color.clone().or_else(|| self.font_color.clone()),
        }
    }

    /// Convert format into key-value map compatible with xlsxwriter properties.
    pub fn to_xlsxwriter(&self) -> BTreeMap<String, EnumCellFormatValue> {
        let mut dict_fmt = BTreeMap::new();

        if let Some(value) = &self.font_name {
            dict_fmt.insert(
                "font_name".to_string(),
                EnumCellFormatValue::String(value.clone()),
            );
        }
        if let Some(value) = self.font_size {
            dict_fmt.insert("font_size".to_string(), EnumCellFormatValue::Integer(value));
        }
        if let Some(value) = self.bold {
            dict_fmt.insert("bold".to_string(), EnumCellFormatValue::Boolean(value));
        }
        if let Some(value) = self.italic {
            dict_fmt.insert("italic".to_string(), EnumCellFormatValue::Boolean(value));
        }

        if let Some(value) = &self.align {
            dict_fmt.insert(
                "align".to_string(),
                EnumCellFormatValue::String(value.clone()),
            );
        }
        if let Some(value) = &self.valign {
            dict_fmt.insert(
                "valign".to_string(),
                EnumCellFormatValue::String(value.clone()),
            );
        }
        if let Some(value) = self.border {
            dict_fmt.insert("border".to_string(), EnumCellFormatValue::Integer(value));
        }
        if let Some(value) = self.text_wrap {
            dict_fmt.insert("text_wrap".to_string(), EnumCellFormatValue::Boolean(value));
        }

        if let Some(value) = self.top {
            dict_fmt.insert("top".to_string(), EnumCellFormatValue::Integer(value));
        }
        if let Some(value) = self.bottom {
            dict_fmt.insert("bottom".to_string(), EnumCellFormatValue::Integer(value));
        }
        if let Some(value) = self.left {
            dict_fmt.insert("left".to_string(), EnumCellFormatValue::Integer(value));
        }
        if let Some(value) = self.right {
            dict_fmt.insert("right".to_string(), EnumCellFormatValue::Integer(value));
        }

        if let Some(value) = &self.num_format {
            dict_fmt.insert(
                "num_format".to_string(),
                EnumCellFormatValue::String(value.clone()),
            );
        }
        if let Some(value) = &self.bg_color {
            dict_fmt.insert(
                "bg_color".to_string(),
                EnumCellFormatValue::String(value.clone()),
            );
        }
        if let Some(value) = &self.font_color {
            dict_fmt.insert(
                "font_color".to_string(),
                EnumCellFormatValue::String(value.clone()),
            );
        }

        dict_fmt
    }
}

/// Border tuple for top/bottom/left/right.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecCellBorder {
    /// Top border style.
    pub top: i64,
    /// Bottom border style.
    pub bottom: i64,
    /// Left border style.
    pub left: i64,
    /// Right border style.
    pub right: i64,
}

// #endregion
////////////////////////////////////////////////////////////////////////////////
// #region ColumnFormatSpecification

/// Planned final/base formats by column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecColumnFormatPlan {
    /// Final format applied at write time.
    pub fmts_by_col: Vec<SpecCellFormat>,
    /// Base format before per-column override.
    pub fmts_base_by_col: Vec<SpecCellFormat>,
}

// #endregion
////////////////////////////////////////////////////////////////////////////////
// #region WriteOptions

/// Integer conversion policy for numeric-looking values in integer columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EnumIntegerCoerceMode {
    /// Coerce numeric values to integer representation when possible.
    Coerce,
    /// Keep non-integer numeric values as text in integer columns.
    #[default]
    Strict,
}

/// Value conversion policy for missing/NaN/Inf and integer coercion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecXlsxValuePolicy {
    /// Replacement text for missing value when keep-missing is enabled.
    pub missing_value_str: String,
    /// Replacement text for NaN.
    pub nan_str: String,
    /// Replacement text for positive infinity.
    pub posinf_str: String,
    /// Replacement text for negative infinity.
    pub neginf_str: String,
    /// Integer conversion mode.
    pub integer_coerce: EnumIntegerCoerceMode,
}

impl Default for SpecXlsxValuePolicy {
    fn default() -> Self {
        Self {
            missing_value_str: "NA".to_string(),
            nan_str: "NaN".to_string(),
            posinf_str: "Inf".to_string(),
            neginf_str: "-Inf".to_string(),
            integer_coerce: EnumIntegerCoerceMode::Strict,
        }
    }
}

/// Policy for selecting row chunk size in write pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecXlsxRowChunkPolicy {
    /// Width threshold for large table.
    pub width_large: usize,
    /// Width threshold for medium table.
    pub width_medium: usize,
    /// Chunk size used when width >= `width_large`.
    pub size_large: usize,
    /// Chunk size used when width >= `width_medium`.
    pub size_medium: usize,
    /// Default chunk size.
    pub size_default: usize,
    /// Force exact chunk size when set.
    pub fixed_size: Option<usize>,
}

impl Default for SpecXlsxRowChunkPolicy {
    fn default() -> Self {
        Self {
            width_large: 8_000,
            width_medium: 2_000,
            size_large: 1_000,
            size_medium: 2_000,
            size_default: 10_000,
            fixed_size: None,
        }
    }
}

/// Scientific formatting candidate scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EnumScientificScope {
    /// Disable scientific auto-selection entirely.
    None,
    /// Apply to decimal-like numeric columns (default).
    #[default]
    Decimal,
    /// Apply to integer columns.
    Integer,
    /// Apply to all numeric columns.
    All,
}

/// Scientific formatting policy used in per-sheet write.
#[derive(Debug, Clone, PartialEq)]
pub struct SpecScientificPolicy {
    /// Scientific trigger scope.
    pub rule_scope: EnumScientificScope,
    /// Lower absolute bound trigger (exclusive, except zero).
    pub thr_min: f64,
    /// Upper absolute bound trigger (inclusive).
    pub thr_max: f64,
    /// Max body rows to inspect for scientific inference.
    pub height_body_inferred_max: Option<usize>,
}

impl Default for SpecScientificPolicy {
    fn default() -> Self {
        Self {
            rule_scope: EnumScientificScope::Decimal,
            thr_min: 0.0001,
            thr_max: 1_000_000_000_000.0,
            height_body_inferred_max: Some(20_000),
        }
    }
}

/// Autofit rule for column width inference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EnumAutofitColumnsRule {
    /// Disable autofit.
    None,
    /// Infer width from header cells only (default).
    #[default]
    Header,
    /// Infer width from body cells only.
    Body,
    /// Infer width from both header and body cells.
    All,
}

/// Autofit policy for per-sheet write call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecAutofitCellsPolicy {
    /// Autofit width inference rule.
    pub rule_columns: EnumAutofitColumnsRule,
    /// Max body rows inspected when body-based inference is active.
    pub height_body_inferred_max: Option<usize>,
    /// Minimum final width.
    pub width_cell_min: usize,
    /// Maximum final width.
    pub width_cell_max: usize,
    /// Width padding added after inference.
    pub width_cell_padding: usize,
}

impl Default for SpecAutofitCellsPolicy {
    fn default() -> Self {
        Self {
            rule_columns: EnumAutofitColumnsRule::Header,
            height_body_inferred_max: Some(20_000),
            width_cell_min: 8,
            width_cell_max: 60,
            width_cell_padding: 2,
        }
    }
}

/// Writer-wide options controlling value conversion and formatting defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecXlsxWriteOptions {
    /// Value conversion policy.
    pub value_policy: SpecXlsxValuePolicy,
    /// Keep missing/NaN/Inf as text instead of blank.
    pub keep_missing_values: bool,
    /// Infer numeric columns from dtypes.
    pub infer_numeric_cols: bool,
    /// Infer integer subset from numeric columns.
    pub infer_integer_cols: bool,
    /// Row chunking policy.
    pub row_chunk_policy: SpecXlsxRowChunkPolicy,
    /// Base patch merged into all per-column formats.
    pub base_format_patch: SpecCellFormat,
}

impl Default for SpecXlsxWriteOptions {
    fn default() -> Self {
        Self {
            value_policy: SpecXlsxValuePolicy::default(),
            keep_missing_values: false,
            infer_numeric_cols: true,
            infer_integer_cols: true,
            row_chunk_policy: SpecXlsxRowChunkPolicy::default(),
            base_format_patch: SpecCellFormat {
                border: Some(0),
                top: Some(0),
                bottom: Some(0),
                left: Some(0),
                right: Some(0),
                ..Default::default()
            },
        }
    }
}

// #endregion
////////////////////////////////////////////////////////////////////////////////
// #region SheetFormatSpecification

/// Concrete sheet part emitted to workbook (after Excel-limit slicing).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecSheetSlice {
    /// Actual unique sheet name in workbook.
    pub sheet_name: String,
    /// Inclusive source row start.
    pub row_start_inclusive: usize,
    /// Exclusive source row end.
    pub row_end_exclusive: usize,
    /// Inclusive source column start.
    pub col_start_inclusive: usize,
    /// Exclusive source column end.
    pub col_end_exclusive: usize,
}

/// Horizontal merge plan item.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecSheetHorizontalMerge {
    /// Row index where merge is applied.
    pub row_idx_start: usize,
    /// Start column index (inclusive).
    pub col_idx_start: usize,
    /// End column index (inclusive).
    pub col_idx_end: usize,
    /// Merge display text.
    pub text: String,
}

// #endregion
////////////////////////////////////////////////////////////////////////////////
// #region ReportSpecification

/// Per-write call report.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SpecXlsxReport {
    /// Sheet slices produced by the write call.
    pub sheets: Vec<SpecSheetSlice>,
    /// Non-fatal warnings.
    pub warnings: Vec<String>,
}

impl SpecXlsxReport {
    /// Add a warning message.
    pub fn warn(&mut self, msg: impl AsRef<str>) {
        self.warnings.push(msg.as_ref().to_string());
    }
}

// #endregion
////////////////////////////////////////////////////////////////////////////////

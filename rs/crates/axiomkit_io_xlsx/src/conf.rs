//! XLSX constants and default preset factories.

use std::collections::BTreeMap;

use crate::spec::{SpecCellFormat, SpecXlsxWriteOptions};

/// Excel worksheet maximum row count.
pub const N_NROWS_EXCEL_MAX: usize = 1_048_576;
/// Excel worksheet maximum column count.
pub const N_NCOLS_EXCEL_MAX: usize = 16_384;
/// Excel sheet name maximum length.
pub const N_LEN_EXCEL_SHEET_NAME_MAX: usize = 31;
/// Characters not allowed in sheet names.
pub const TUP_EXCEL_ILLEGAL: [&str; 7] = ["*", ":", "?", "/", "\\", "[", "]"];

/// Canonical format preset keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnumFmtKey {
    /// Generic text cell format.
    Text,
    /// Integer number format.
    Integer,
    /// Decimal number format.
    Decimal,
    /// Scientific number format.
    Scientific,
    /// Header cell format.
    Header,
}

/// Column selector reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnumColumnIdentifier {
    /// Select by column name.
    Name(String),
    /// Select by zero-based column index.
    Index(usize),
}

/// Build default named format presets used by [`crate::writer::XlsxWriter`].
pub fn derive_default_xlsx_formats() -> BTreeMap<String, SpecCellFormat> {
    let cfg_base_fmt_spec = SpecCellFormat {
        font_name: Some("Times New Roman".to_string()),
        font_size: Some(11),
        border: Some(1),
        align: Some("left".to_string()),
        valign: Some("vcenter".to_string()),
        ..Default::default()
    };

    let mut dict_fmt = BTreeMap::new();
    dict_fmt.insert("text".to_string(), cfg_base_fmt_spec.clone());
    dict_fmt.insert(
        "header".to_string(),
        cfg_base_fmt_spec.with_(SpecCellFormat {
            bold: Some(true),
            align: Some("center".to_string()),
            ..Default::default()
        }),
    );
    dict_fmt.insert(
        "integer".to_string(),
        cfg_base_fmt_spec.with_(SpecCellFormat {
            num_format: Some("0".to_string()),
            ..Default::default()
        }),
    );
    dict_fmt.insert(
        "decimal".to_string(),
        cfg_base_fmt_spec.with_(SpecCellFormat {
            num_format: Some("0.0000".to_string()),
            ..Default::default()
        }),
    );
    dict_fmt.insert(
        "scientific".to_string(),
        cfg_base_fmt_spec.with_(SpecCellFormat {
            num_format: Some("0.00E+0".to_string()),
            ..Default::default()
        }),
    );

    dict_fmt
}

/// Build default write options.
pub fn derive_default_xlsx_write_options() -> SpecXlsxWriteOptions {
    SpecXlsxWriteOptions::default()
}

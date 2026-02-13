//! `axiomkit_io_fs` v1:
//! Rust-side filesystem copy engine.
//!
//! Architecture mirrors Python `io/fs` modules:
//! - `copy`   : traversal and copy orchestration
//! - `spec`   : enums/options/errors
//! - `report` : run-time report model
//! - `util`   : shared helper functions

pub mod copy;
pub mod report;
pub mod spec;
mod util;

pub use copy::copy_tree;
pub use report::{ReportCopy, ReportCopyBuilder};
pub use spec::{
    CopyTreeError, EnumCopyDepthLimitMode, EnumCopyDirectoryConflictStrategy,
    EnumCopyFileConflictStrategy, EnumCopyPatternMode, EnumCopySymlinkStrategy, SpecCopyError,
    SpecCopyOptions,
};

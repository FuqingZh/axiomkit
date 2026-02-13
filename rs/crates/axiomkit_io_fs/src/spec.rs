//! Copy specification models and top-level error types.

use std::fmt;
use std::path::PathBuf;

////////////////////////////////////////////////////////////////////////////////
// #region EnumsInit

/// Symlink handling policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnumCopySymlinkStrategy {
    /// Follow the link and copy the target bytes/entries.
    Dereference,
    /// Create a symbolic link at destination (do not copy target bytes).
    CopySymlinks,
    /// Ignore symlink entries.
    SkipSymlinks,
}

/// Existing destination file conflict policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnumCopyFileConflictStrategy {
    /// Keep destination file and skip current source file.
    Skip,
    /// Replace destination file with source file.
    Overwrite,
    /// Record an error and skip this file.
    Error,
}

/// Existing destination directory conflict policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnumCopyDirectoryConflictStrategy {
    /// Do not descend/copy into an already existing destination directory.
    Skip,
    /// Reuse destination directory and continue copying children into it.
    Merge,
    /// Record an error when destination directory already exists.
    Error,
}

/// Pattern matching mode for include/exclude lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnumCopyPatternMode {
    /// Shell-like wildcards (`*`, `?`, character classes).
    Glob,
    /// Regular expression pattern.
    Regex,
    /// Exact string match.
    Literal,
}

/// Depth filter mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnumCopyDepthLimitMode {
    /// Include entries with depth `<= depth_limit`.
    AtMost,
    /// Include entries with depth exactly equal to `depth_limit`.
    Exact,
}

// #endregion
////////////////////////////////////////////////////////////////////////////////
// #region StructsAndErrors

/// Input options for `copy_tree`.
#[derive(Debug, Clone)]
pub struct SpecCopyOptions {
    /// Include patterns applied to file basename.
    pub patterns_include_files: Option<Vec<String>>,
    /// Exclude patterns applied to file basename.
    pub patterns_exclude_files: Option<Vec<String>>,
    /// Include patterns applied to directory basename.
    pub patterns_include_dirs: Option<Vec<String>>,
    /// Exclude patterns applied to directory basename.
    pub patterns_exclude_dirs: Option<Vec<String>>,
    /// Pattern interpretation mode.
    pub rule_pattern: EnumCopyPatternMode,
    /// Conflict behavior for destination files.
    pub rule_conflict_file: EnumCopyFileConflictStrategy,
    /// Conflict behavior for destination directories.
    pub rule_conflict_dir: EnumCopyDirectoryConflictStrategy,
    /// Symlink handling behavior.
    pub rule_symlink: EnumCopySymlinkStrategy,
    /// Optional maximum/target depth (depends on `rule_depth_limit`).
    pub depth_limit: Option<usize>,
    /// Depth evaluation mode.
    pub rule_depth_limit: EnumCopyDepthLimitMode,
    /// Maximum worker threads for file-copy stage.
    pub num_workers_max: Option<usize>,
    /// Keep relative source tree structure in destination.
    pub if_keep_tree: bool,
    /// Do not mutate filesystem; record what would happen.
    pub if_dry_run: bool,
}

impl Default for SpecCopyOptions {
    fn default() -> Self {
        Self {
            patterns_include_files: None,
            patterns_exclude_files: None,
            patterns_include_dirs: None,
            patterns_exclude_dirs: None,
            rule_pattern: EnumCopyPatternMode::Glob,
            rule_conflict_file: EnumCopyFileConflictStrategy::Skip,
            rule_conflict_dir: EnumCopyDirectoryConflictStrategy::Skip,
            rule_symlink: EnumCopySymlinkStrategy::CopySymlinks,
            depth_limit: None,
            rule_depth_limit: EnumCopyDepthLimitMode::AtMost,
            num_workers_max: None,
            if_keep_tree: true,
            if_dry_run: false,
        }
    }
}

/// One copy failure item with path + error text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecCopyError {
    /// Failed source or destination path.
    pub path: PathBuf,
    /// User-facing error text.
    pub exception: String,
}

/// "Top-level call failed" errors (input validation / setup stage).
#[derive(Debug)]
pub enum CopyTreeError {
    /// Invalid depth combination or value.
    InvalidDepthLimit(String),
    /// Invalid include/exclude pattern.
    InvalidPattern(String),
    /// Source path is not a directory.
    SourceNotDirectory(PathBuf),
    /// Source and destination overlap (`src` contains `dst` or vice versa).
    SourceDestinationOverlap {
        /// Normalized source directory.
        source: PathBuf,
        /// Normalized destination directory.
        destination: PathBuf,
    },
    /// Destination directory initialization failed.
    DestinationInitFailed {
        /// Destination path that failed initialization.
        path: PathBuf,
        /// Underlying IO error text.
        message: String,
    },
}

impl fmt::Display for CopyTreeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidDepthLimit(msg) => write!(f, "{msg}"),
            Self::InvalidPattern(msg) => write!(f, "{msg}"),
            Self::SourceNotDirectory(path) => {
                write!(f, "Source is not a directory: {}", path.display())
            }
            Self::SourceDestinationOverlap {
                source,
                destination,
            } => write!(
                f,
                "Source and destination directories overlap: {} <-> {}",
                source.display(),
                destination.display()
            ),
            Self::DestinationInitFailed { path, message } => {
                write!(
                    f,
                    "Failed to initialize destination {}: {message}",
                    path.display()
                )
            }
        }
    }
}

impl std::error::Error for CopyTreeError {}

// #endregion
////////////////////////////////////////////////////////////////////////////////

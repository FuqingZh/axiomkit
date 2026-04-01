use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use globset::{Glob, GlobMatcher};
use regex::Regex;

use crate::report::CopyReportBuilder;
use crate::spec::{
    CopyDepthLimitMode, CopyDirectoryConflictMode, CopyFileConflictMode, CopyPatternMode,
    CopySymlinkMode, CopyTreeError,
};

////////////////////////////////////////////////////////////////////////////////
// #region PatternMatching

#[derive(Debug, Clone)]
pub(crate) enum TypeCopyPatternSeq {
    Literal(Vec<String>),
    Glob(Vec<GlobMatcher>),
    Regex(Vec<Regex>),
}

#[derive(Debug, Clone, Default)]
pub(crate) struct CopyPatternsSpec {
    pub(crate) patterns_include_files: Option<TypeCopyPatternSeq>,
    pub(crate) patterns_exclude_files: Option<TypeCopyPatternSeq>,
    pub(crate) patterns_include_dirs: Option<TypeCopyPatternSeq>,
    pub(crate) patterns_exclude_dirs: Option<TypeCopyPatternSeq>,
}

impl CopyPatternsSpec {
    pub(crate) fn from_raw(
        patterns_include_files: Option<&[String]>,
        patterns_exclude_files: Option<&[String]>,
        patterns_include_dirs: Option<&[String]>,
        patterns_exclude_dirs: Option<&[String]>,
        rule_pattern: CopyPatternMode,
    ) -> Result<Self, CopyTreeError> {
        Ok(Self {
            patterns_include_files: _compile(patterns_include_files, rule_pattern)?,
            patterns_exclude_files: _compile(patterns_exclude_files, rule_pattern)?,
            patterns_include_dirs: _compile(patterns_include_dirs, rule_pattern)?,
            patterns_exclude_dirs: _compile(patterns_exclude_dirs, rule_pattern)?,
        })
    }
}

fn _compile(
    patterns: Option<&[String]>,
    rule_pattern: CopyPatternMode,
) -> Result<Option<TypeCopyPatternSeq>, CopyTreeError> {
    let Some(patterns) = patterns else {
        return Ok(None);
    };
    if patterns.is_empty() {
        return Ok(None);
    }

    match rule_pattern {
        CopyPatternMode::Literal => Ok(Some(TypeCopyPatternSeq::Literal(patterns.to_vec()))),
        CopyPatternMode::Glob => {
            let mut glob_matchers = Vec::with_capacity(patterns.len());
            for _pattern in patterns {
                let matcher = Glob::new(_pattern)
                    .map_err(|e| {
                        CopyTreeError::InvalidPattern(format!(
                            "Invalid pattern in include/exclude: {e}"
                        ))
                    })?
                    .compile_matcher();
                glob_matchers.push(matcher);
            }
            Ok(Some(TypeCopyPatternSeq::Glob(glob_matchers)))
        }
        CopyPatternMode::Regex => {
            let mut regexes = Vec::with_capacity(patterns.len());
            for _pattern in patterns {
                let regex = Regex::new(_pattern).map_err(|e| {
                    CopyTreeError::InvalidPattern(format!(
                        "Invalid pattern in include/exclude: {e}"
                    ))
                })?;
                regexes.push(regex);
            }
            Ok(Some(TypeCopyPatternSeq::Regex(regexes)))
        }
    }
}

fn _is_pattern_matching(
    value: &str,
    patterns: Option<&TypeCopyPatternSeq>,
    rule_pattern: CopyPatternMode,
) -> bool {
    let Some(patterns) = patterns else {
        return false;
    };

    match rule_pattern {
        CopyPatternMode::Literal => match patterns {
            TypeCopyPatternSeq::Literal(v) => v.iter().any(|p| value.contains(p)),
            TypeCopyPatternSeq::Glob(_) => false,
            TypeCopyPatternSeq::Regex(_) => false,
        },
        CopyPatternMode::Glob => match patterns {
            TypeCopyPatternSeq::Glob(v) => v.iter().any(|p| p.is_match(value)),
            TypeCopyPatternSeq::Literal(_) => false,
            TypeCopyPatternSeq::Regex(_) => false,
        },
        CopyPatternMode::Regex => match patterns {
            TypeCopyPatternSeq::Regex(v) => v.iter().any(|p| p.is_match(value)),
            TypeCopyPatternSeq::Literal(_) => false,
            TypeCopyPatternSeq::Glob(_) => false,
        },
    }
}

fn _should_include(
    value: &str,
    patterns: Option<&TypeCopyPatternSeq>,
    rule_pattern: CopyPatternMode,
) -> bool {
    match patterns {
        None => true,
        Some(_) => _is_pattern_matching(value, patterns, rule_pattern),
    }
}

fn _should_exclude(
    value: &str,
    patterns: Option<&TypeCopyPatternSeq>,
    rule_pattern: CopyPatternMode,
) -> bool {
    match patterns {
        None => false,
        Some(_) => _is_pattern_matching(value, patterns, rule_pattern),
    }
}

pub(crate) fn should_exclude_by_patterns(
    value: &str,
    patterns_include: Option<&TypeCopyPatternSeq>,
    patterns_exclude: Option<&TypeCopyPatternSeq>,
    rule_pattern: CopyPatternMode,
) -> bool {
    !_should_include(value, patterns_include, rule_pattern)
        || _should_exclude(value, patterns_exclude, rule_pattern)
}

// #endregion
////////////////////////////////////////////////////////////////////////////////
// #region PathUtilities

fn _is_relative_to_base(path: &Path, base: &Path) -> bool {
    path.starts_with(base)
}

fn _normalize_path(path: &Path) -> PathBuf {
    if let Ok(resolved) = fs::canonicalize(path) {
        return resolved;
    }
    if path.is_absolute() {
        return path.to_path_buf();
    }
    std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join(path)
}

fn _absolutize_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }
    std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join(path)
}

pub(crate) fn is_overlap(src: &Path, dst: &Path) -> bool {
    let src_resolved = _normalize_path(src);
    let dst_resolved = _normalize_path(dst);
    _is_relative_to_base(&dst_resolved, &src_resolved)
        || _is_relative_to_base(&src_resolved, &dst_resolved)
}

pub(crate) fn validate_destination_path_safety(
    path_dst_item: &Path,
    path_dir_dst_root: &Path,
) -> Result<(), String> {
    let path_dir_dst_root_abs = _absolutize_path(path_dir_dst_root);
    let path_dst_item_abs = _absolutize_path(path_dst_item);

    if !path_dst_item_abs.starts_with(&path_dir_dst_root_abs) {
        return Err(format!(
            "Unsafe destination path escapes destination root: {} (root={})",
            path_dst_item.display(),
            path_dir_dst_root.display()
        ));
    }

    let path_parent_dst = path_dst_item_abs.parent().ok_or_else(|| {
        format!(
            "Failed to derive parent directory: {}",
            path_dst_item.display()
        )
    })?;
    if !path_parent_dst.starts_with(&path_dir_dst_root_abs) {
        return Err(format!(
            "Unsafe destination parent escapes destination root: {} (root={})",
            path_dst_item.display(),
            path_dir_dst_root.display()
        ));
    }

    let path_parent_rel = path_parent_dst
        .strip_prefix(&path_dir_dst_root_abs)
        .map_err(|_| {
            format!(
                "Unsafe destination parent escapes destination root: {} (root={})",
                path_dst_item.display(),
                path_dir_dst_root.display()
            )
        })?;
    let mut path_cursor = path_dir_dst_root_abs.clone();
    for _part_rel in path_parent_rel.components() {
        path_cursor.push(_part_rel.as_os_str());
        match fs::symlink_metadata(&path_cursor) {
            Ok(meta_cursor) => {
                if meta_cursor.file_type().is_symlink() {
                    return Err(format!(
                        "Unsafe destination path traverses symlink component: {}",
                        path_cursor.display()
                    ));
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(format!(
                    "Failed to inspect destination path component {} ({e})",
                    path_cursor.display()
                ));
            }
        }
    }

    match fs::symlink_metadata(&path_dst_item_abs) {
        Ok(meta_dst_item) => {
            if meta_dst_item.file_type().is_symlink() {
                return Err(format!(
                    "Unsafe destination path is an existing symlink: {}",
                    path_dst_item.display()
                ));
            }
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(format!(
                "Failed to inspect destination path {} ({e})",
                path_dst_item.display()
            ));
        }
    }

    Ok(())
}

pub(crate) fn should_error_broken_symlink(
    path_symlink: &Path,
    rule_symlink: CopySymlinkMode,
) -> bool {
    rule_symlink == CopySymlinkMode::Dereference && !path_symlink.exists()
}

pub(crate) fn should_skip_dir_conflict(
    path_dst: &Path,
    rule_conflict: CopyDirectoryConflictMode,
    report_builder: &mut CopyReportBuilder,
) -> bool {
    if !path_dst.exists() {
        return false;
    }
    if path_dst.is_file() {
        report_builder.add_error(
            path_dst.to_path_buf(),
            format!(
                "Destination is a file, expected directory: {}",
                path_dst.display()
            ),
        );
        return true;
    }

    match rule_conflict {
        CopyDirectoryConflictMode::Skip => {
            report_builder.add_skipped();
            true
        }
        CopyDirectoryConflictMode::Error => {
            report_builder.add_error(
                path_dst.to_path_buf(),
                format!("Destination exists: {}", path_dst.display()),
            );
            true
        }
        CopyDirectoryConflictMode::Merge => false,
    }
}

pub(crate) fn should_skip_file_conflict(
    path_dst: &Path,
    rule_conflict: CopyFileConflictMode,
    report_builder: &mut CopyReportBuilder,
) -> bool {
    if !path_dst.exists() {
        return false;
    }
    if path_dst.is_dir() {
        report_builder.add_error(
            path_dst.to_path_buf(),
            format!("Destination is a directory: {}", path_dst.display()),
        );
        return true;
    }

    match rule_conflict {
        CopyFileConflictMode::Skip => {
            report_builder.add_skipped();
            true
        }
        CopyFileConflictMode::Error => {
            report_builder.add_error(
                path_dst.to_path_buf(),
                format!("Destination exists: {}", path_dst.display()),
            );
            true
        }
        CopyFileConflictMode::Overwrite => false,
    }
}

pub(crate) fn create_symbolic_link(
    path_src: &Path,
    path_dst: &Path,
    report_builder: &mut CopyReportBuilder,
) {
    let target = match fs::read_link(path_src) {
        Ok(v) => v,
        Err(e) => {
            report_builder.add_error(path_dst.to_path_buf(), e.to_string());
            return;
        }
    };

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        match symlink(&target, path_dst) {
            Ok(_) => report_builder.add_copied(),
            Err(e) => report_builder.add_error(path_dst.to_path_buf(), e.to_string()),
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::{symlink_dir, symlink_file};
        let res = if path_src.is_dir() {
            symlink_dir(&target, path_dst)
        } else {
            symlink_file(&target, path_dst)
        };
        match res {
            Ok(_) => report_builder.add_copied(),
            Err(e) => report_builder.add_error(path_dst.to_path_buf(), e.to_string()),
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = target;
        report_builder.add_error(
            path_dst.to_path_buf(),
            "Symbolic links are unsupported on this platform".to_string(),
        );
    }
}

pub(crate) fn copy_file_with_metadata(
    file_src_path: &Path,
    file_dst_path: &Path,
) -> Result<(), io::Error> {
    fs::copy(file_src_path, file_dst_path)?;
    #[cfg(target_os = "linux")]
    {
        apply_metadata_linux(file_src_path, file_dst_path)?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn apply_metadata_linux(file_src_path: &Path, file_dst_path: &Path) -> Result<(), io::Error> {
    use filetime::{FileTime, set_file_times};

    let src_metadata = fs::metadata(file_src_path)?;
    fs::set_permissions(file_dst_path, src_metadata.permissions())?;

    let file_time_access = FileTime::from_last_access_time(&src_metadata);
    let file_time_modify = FileTime::from_last_modification_time(&src_metadata);
    set_file_times(file_dst_path, file_time_access, file_time_modify)?;

    copy_xattrs_linux(file_src_path, file_dst_path);
    Ok(())
}

#[cfg(target_os = "linux")]
fn copy_xattrs_linux(file_src_path: &Path, file_dst_path: &Path) {
    let iter_xattr_names = match xattr::list(file_src_path) {
        Ok(v) => v,
        Err(_) => return,
    };

    for _name in iter_xattr_names {
        let Some(raw_value) = xattr::get(file_src_path, &_name).ok().flatten() else {
            continue;
        };
        let _ = xattr::set(file_dst_path, &_name, &raw_value);
    }
}

pub(crate) fn is_depth_within_limit(
    depth_value: usize,
    depth_limit: Option<usize>,
    rule_depth_limit: CopyDepthLimitMode,
) -> bool {
    match depth_limit {
        None => true,
        Some(limit) => match rule_depth_limit {
            CopyDepthLimitMode::AtMost => depth_value <= limit,
            CopyDepthLimitMode::Exact => depth_value == limit,
        },
    }
}

pub(crate) fn calculate_worker_limit(workers_max: Option<usize>) -> usize {
    let cpu_count = std::thread::available_parallelism()
        .map(|v| v.get())
        .unwrap_or(1);

    match workers_max {
        Some(count) => count.clamp(1, cpu_count),
        None => cpu_count.clamp(1, 8),
    }
}

/// Derive destination path based on `should_keep_tree`.
///
/// # Arguments
/// - `path_src`: Source path of the item being copied.
/// - `path_item_name`: Name of the item being copied.
/// - `path_dir_src`: Source directory path.
/// - `path_dir_dst`: Destination directory path.
/// - `should_keep_tree`:
///   - `true`: Preserve the directory structure relative to `path_dir_src`.
///   - `false`: Copy item directly into `path_dir_dst`.
///
/// # Returns
/// - `PathBuf`: The derived destination path.
///
/// # Examples
/// ```ignore
/// use std::path::Path;
/// let path_src = Path::new("/source/dir/file.txt");
/// let path_item_name = "file.txt";
/// let path_dir_src = Path::new("/source/dir");
/// let path_dir_dst = Path::new("/destination/dir");
///
/// // If keeping tree structure
/// let dest_path = derive_destination_path(path_src, path_item_name, path_dir_src, path_dir_dst, true);
/// assert_eq!(dest_path, Path::new("/destination/dir/file.txt"));
///
/// // If not keeping tree structure
/// let dest_path = derive_destination_path(path_src, path_item_name, path_dir_src, path_dir_dst, false);
/// assert_eq!(dest_path, Path::new("/destination/dir/file.txt"));
/// ```
pub(crate) fn derive_destination_path(
    path_src: &Path,
    path_item_name: &str,
    path_dir_src: &Path,
    path_dir_dst: &Path,
    should_keep_tree: bool,
) -> PathBuf {
    if should_keep_tree {
        return path_dir_dst.join(
            path_src
                .strip_prefix(path_dir_src)
                .unwrap_or(Path::new(path_item_name)),
        );
    }
    path_dir_dst.join(path_item_name)
}

// #endregion
////////////////////////////////////////////////////////////////////////////////

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use globset::{Glob, GlobMatcher};
use regex::Regex;

use crate::report::ReportCopyBuilder;
use crate::spec::{
    CopyTreeError, EnumCopyDepthLimitMode, EnumCopyDirectoryConflictStrategy,
    EnumCopyFileConflictStrategy, EnumCopyPatternMode, EnumCopySymlinkStrategy,
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
pub(crate) struct SpecCopyPatterns {
    pub(crate) patterns_include_files: Option<TypeCopyPatternSeq>,
    pub(crate) patterns_exclude_files: Option<TypeCopyPatternSeq>,
    pub(crate) patterns_include_dirs: Option<TypeCopyPatternSeq>,
    pub(crate) patterns_exclude_dirs: Option<TypeCopyPatternSeq>,
}

impl SpecCopyPatterns {
    pub(crate) fn from_raw(
        patterns_include_files: Option<&[String]>,
        patterns_exclude_files: Option<&[String]>,
        patterns_include_dirs: Option<&[String]>,
        patterns_exclude_dirs: Option<&[String]>,
        rule_pattern: EnumCopyPatternMode,
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
    rule_pattern: EnumCopyPatternMode,
) -> Result<Option<TypeCopyPatternSeq>, CopyTreeError> {
    let Some(patterns) = patterns else {
        return Ok(None);
    };
    if patterns.is_empty() {
        return Ok(None);
    }

    match rule_pattern {
        EnumCopyPatternMode::Literal => Ok(Some(TypeCopyPatternSeq::Literal(patterns.to_vec()))),
        EnumCopyPatternMode::Glob => {
            let mut l_glob = Vec::with_capacity(patterns.len());
            for pattern in patterns {
                let matcher = Glob::new(pattern)
                    .map_err(|e| {
                        CopyTreeError::InvalidPattern(format!(
                            "Invalid pattern in include/exclude: {e}"
                        ))
                    })?
                    .compile_matcher();
                l_glob.push(matcher);
            }
            Ok(Some(TypeCopyPatternSeq::Glob(l_glob)))
        }
        EnumCopyPatternMode::Regex => {
            let mut l_regex = Vec::with_capacity(patterns.len());
            for pattern in patterns {
                let regex = Regex::new(pattern).map_err(|e| {
                    CopyTreeError::InvalidPattern(format!(
                        "Invalid pattern in include/exclude: {e}"
                    ))
                })?;
                l_regex.push(regex);
            }
            Ok(Some(TypeCopyPatternSeq::Regex(l_regex)))
        }
    }
}

fn _is_pattern_matching(
    value: &str,
    patterns: Option<&TypeCopyPatternSeq>,
    rule_pattern: EnumCopyPatternMode,
) -> bool {
    let Some(patterns) = patterns else {
        return false;
    };

    match rule_pattern {
        EnumCopyPatternMode::Literal => match patterns {
            TypeCopyPatternSeq::Literal(v) => v.iter().any(|p| value.contains(p)),
            TypeCopyPatternSeq::Glob(_) => false,
            TypeCopyPatternSeq::Regex(_) => false,
        },
        EnumCopyPatternMode::Glob => match patterns {
            TypeCopyPatternSeq::Glob(v) => v.iter().any(|p| p.is_match(value)),
            TypeCopyPatternSeq::Literal(_) => false,
            TypeCopyPatternSeq::Regex(_) => false,
        },
        EnumCopyPatternMode::Regex => match patterns {
            TypeCopyPatternSeq::Regex(v) => v.iter().any(|p| p.is_match(value)),
            TypeCopyPatternSeq::Literal(_) => false,
            TypeCopyPatternSeq::Glob(_) => false,
        },
    }
}

fn _should_include(
    value: &str,
    patterns: Option<&TypeCopyPatternSeq>,
    rule_pattern: EnumCopyPatternMode,
) -> bool {
    match patterns {
        None => true,
        Some(_) => _is_pattern_matching(value, patterns, rule_pattern),
    }
}

fn _should_exclude(
    value: &str,
    patterns: Option<&TypeCopyPatternSeq>,
    rule_pattern: EnumCopyPatternMode,
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
    rule_pattern: EnumCopyPatternMode,
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
    for part_rel in path_parent_rel.components() {
        path_cursor.push(part_rel.as_os_str());
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
    rule_symlink: EnumCopySymlinkStrategy,
) -> bool {
    rule_symlink == EnumCopySymlinkStrategy::Dereference && !path_symlink.exists()
}

pub(crate) fn should_skip_dir_conflict(
    path_dst: &Path,
    rule_conflict: EnumCopyDirectoryConflictStrategy,
    builder_cp_report: &mut ReportCopyBuilder,
) -> bool {
    if !path_dst.exists() {
        return false;
    }
    if path_dst.is_file() {
        builder_cp_report.add_error(
            path_dst.to_path_buf(),
            format!(
                "Destination is a file, expected directory: {}",
                path_dst.display()
            ),
        );
        return true;
    }

    match rule_conflict {
        EnumCopyDirectoryConflictStrategy::Skip => {
            builder_cp_report.add_skipped();
            true
        }
        EnumCopyDirectoryConflictStrategy::Error => {
            builder_cp_report.add_error(
                path_dst.to_path_buf(),
                format!("Destination exists: {}", path_dst.display()),
            );
            true
        }
        EnumCopyDirectoryConflictStrategy::Merge => false,
    }
}

pub(crate) fn should_skip_file_conflict(
    path_dst: &Path,
    rule_conflict: EnumCopyFileConflictStrategy,
    builder_cp_report: &mut ReportCopyBuilder,
) -> bool {
    if !path_dst.exists() {
        return false;
    }
    if path_dst.is_dir() {
        builder_cp_report.add_error(
            path_dst.to_path_buf(),
            format!("Destination is a directory: {}", path_dst.display()),
        );
        return true;
    }

    match rule_conflict {
        EnumCopyFileConflictStrategy::Skip => {
            builder_cp_report.add_skipped();
            true
        }
        EnumCopyFileConflictStrategy::Error => {
            builder_cp_report.add_error(
                path_dst.to_path_buf(),
                format!("Destination exists: {}", path_dst.display()),
            );
            true
        }
        EnumCopyFileConflictStrategy::Overwrite => false,
    }
}

pub(crate) fn create_symbolic_link(
    path_src: &Path,
    path_dst: &Path,
    builder_cp_report: &mut ReportCopyBuilder,
) {
    let target = match fs::read_link(path_src) {
        Ok(v) => v,
        Err(e) => {
            builder_cp_report.add_error(path_dst.to_path_buf(), e.to_string());
            return;
        }
    };

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        match symlink(&target, path_dst) {
            Ok(_) => builder_cp_report.add_copied(),
            Err(e) => builder_cp_report.add_error(path_dst.to_path_buf(), e.to_string()),
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
            Ok(_) => builder_cp_report.add_copied(),
            Err(e) => builder_cp_report.add_error(path_dst.to_path_buf(), e.to_string()),
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = target;
        builder_cp_report.add_error(
            path_dst.to_path_buf(),
            "Symbolic links are unsupported on this platform".to_string(),
        );
    }
}

pub(crate) fn copy_file_with_metadata(
    path_file_src: &Path,
    path_file_dst: &Path,
) -> Result<(), io::Error> {
    fs::copy(path_file_src, path_file_dst)?;
    #[cfg(target_os = "linux")]
    {
        apply_metadata_linux(path_file_src, path_file_dst)?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn apply_metadata_linux(path_file_src: &Path, path_file_dst: &Path) -> Result<(), io::Error> {
    use filetime::{FileTime, set_file_times};

    let stat_src = fs::metadata(path_file_src)?;
    fs::set_permissions(path_file_dst, stat_src.permissions())?;

    let file_time_access = FileTime::from_last_access_time(&stat_src);
    let file_time_modify = FileTime::from_last_modification_time(&stat_src);
    set_file_times(path_file_dst, file_time_access, file_time_modify)?;

    copy_xattrs_linux(path_file_src, path_file_dst);
    Ok(())
}

#[cfg(target_os = "linux")]
fn copy_xattrs_linux(path_file_src: &Path, path_file_dst: &Path) {
    let iter_xattr_names = match xattr::list(path_file_src) {
        Ok(v) => v,
        Err(_) => return,
    };

    for name in iter_xattr_names {
        let Some(raw_value) = xattr::get(path_file_src, &name).ok().flatten() else {
            continue;
        };
        let _ = xattr::set(path_file_dst, &name, &raw_value);
    }
}

pub(crate) fn is_depth_within_limit(
    depth_value: usize,
    depth_limit: Option<usize>,
    rule_depth_limit: EnumCopyDepthLimitMode,
) -> bool {
    match depth_limit {
        None => true,
        Some(limit) => match rule_depth_limit {
            EnumCopyDepthLimitMode::AtMost => depth_value <= limit,
            EnumCopyDepthLimitMode::Exact => depth_value == limit,
        },
    }
}

pub(crate) fn calculate_worker_limit(num_workers_max: Option<usize>) -> usize {
    let n_cpu = std::thread::available_parallelism()
        .map(|v| v.get())
        .unwrap_or(1);

    match num_workers_max {
        Some(n) => n.clamp(1, n_cpu),
        None => n_cpu.clamp(1, 8),
    }
}

/// Derive destination path based on if_keep_tree option.
///
/// # Arguments
/// - `path_src`: Source path of the item being copied.
/// - `path_item_name`: Name of the item being copied.
/// - `path_dir_src`: Source directory path.
/// - `path_dir_dst`: Destination directory path.
/// - `if_keep_tree`:
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
    if_keep_tree: bool,
) -> PathBuf {
    if if_keep_tree {
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

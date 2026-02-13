//! Filesystem tree traversal and copy orchestration.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use rayon::ThreadPoolBuilder;
use rayon::prelude::*;

use crate::report::{ReportCopy, ReportCopyBuilder};
use crate::spec::{
    CopyTreeError, EnumCopyDepthLimitMode, EnumCopyDirectoryConflictStrategy,
    EnumCopySymlinkStrategy, SpecCopyOptions,
};
use crate::util::{
    SpecCopyPatterns, calculate_worker_limit, copy_file_with_metadata, create_symbolic_link,
    derive_destination_path, is_depth_within_limit, is_overlap, should_error_broken_symlink,
    should_exclude_by_patterns, should_skip_dir_conflict, should_skip_file_conflict,
    validate_destination_path_safety,
};

#[derive(Debug, Clone)]
struct SpecDirEntry {
    path_dir_src_sub: PathBuf,
    name_dir: String,
    if_is_symlink: bool,
}

#[derive(Debug, Clone)]
struct SpecFileEntry {
    path_file_src: PathBuf,
    name_file: String,
    if_is_symlink: bool,
}

#[derive(Debug, Clone)]
struct SpecCopyTaskFile {
    path_file_src: PathBuf,
    path_file_dst: PathBuf,
}

#[derive(Debug)]
struct SpecCopyContext {
    path_dir_src: PathBuf,
    path_dir_dst: PathBuf,
    spec_cp_options: SpecCopyOptions,
    spec_cp_pats: SpecCopyPatterns,
    n_workers_max: usize,
    builder_cp_report: ReportCopyBuilder,
    set_visited_dirs: HashSet<(u64, u64)>,
    l_tasks_file_copy: Vec<SpecCopyTaskFile>,
}

/// Copy a directory tree from `dir_source` to `dir_destination`.
///
/// Behavior is controlled by [`SpecCopyOptions`], including:
/// - include/exclude pattern rules for files and directories,
/// - conflict policies for destination files/directories,
/// - symlink handling strategy,
/// - optional depth limiting,
/// - flatten (`if_keep_tree=false`) vs keep-tree copy mode,
/// - dry-run and worker count.
///
/// This function performs:
/// 1. Input validation and destination safety checks.
/// 2. Directory traversal and file-copy task planning.
/// 3. Batched file-copy execution (serial or rayon thread pool).
/// 4. Report aggregation.
///
/// Returns [`ReportCopy`] when the run completes (with possible per-entry errors
/// stored in the report). Returns [`CopyTreeError`] only for top-level setup and
/// validation failures.
pub fn copy_tree<P, Q>(
    dir_source: P,
    dir_destination: Q,
    spec_cp_options: SpecCopyOptions,
) -> Result<ReportCopy, CopyTreeError>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    let enum_rule_depth_limit = spec_cp_options.rule_depth_limit;
    if spec_cp_options.depth_limit == Some(0) {
        return Err(CopyTreeError::InvalidDepthLimit(
            "Arg `depth_limit` must be >= 1 or None.".to_string(),
        ));
    }
    if spec_cp_options.depth_limit.is_none()
        && enum_rule_depth_limit == EnumCopyDepthLimitMode::Exact
    {
        return Err(CopyTreeError::InvalidDepthLimit(
            "`depth_limit` is required when depth_mode='exact'.".to_string(),
        ));
    }

    let path_dir_src = dir_source.as_ref().to_path_buf();
    let path_dir_dst = dir_destination.as_ref().to_path_buf();

    if !path_dir_src.is_dir() {
        return Err(CopyTreeError::SourceNotDirectory(path_dir_src));
    }
    if is_overlap(&path_dir_src, &path_dir_dst) {
        return Err(CopyTreeError::SourceDestinationOverlap {
            source: path_dir_src,
            destination: path_dir_dst,
        });
    }
    fs::create_dir_all(&path_dir_dst).map_err(|e| CopyTreeError::DestinationInitFailed {
        path: path_dir_dst.clone(),
        message: e.to_string(),
    })?;
    let meta_dir_dst =
        fs::symlink_metadata(&path_dir_dst).map_err(|e| CopyTreeError::DestinationInitFailed {
            path: path_dir_dst.clone(),
            message: e.to_string(),
        })?;
    if meta_dir_dst.file_type().is_symlink() {
        return Err(CopyTreeError::DestinationInitFailed {
            path: path_dir_dst,
            message: "Destination root path must not be a symbolic link.".to_string(),
        });
    }

    let spec_cp_pats = SpecCopyPatterns::from_raw(
        spec_cp_options.patterns_include_files.as_deref(),
        spec_cp_options.patterns_exclude_files.as_deref(),
        spec_cp_options.patterns_include_dirs.as_deref(),
        spec_cp_options.patterns_exclude_dirs.as_deref(),
        spec_cp_options.rule_pattern,
    )?;
    let n_workers_max = calculate_worker_limit(spec_cp_options.num_workers_max);

    let mut spec_cp_ctx = SpecCopyContext {
        path_dir_src: path_dir_src.clone(),
        path_dir_dst,
        spec_cp_options,
        spec_cp_pats,
        n_workers_max,
        builder_cp_report: ReportCopyBuilder::default(),
        set_visited_dirs: HashSet::new(),
        l_tasks_file_copy: Vec::new(),
    };

    walk_directory(&path_dir_src, 0, &mut spec_cp_ctx);
    flush_file_copy_tasks(&mut spec_cp_ctx);
    Ok(spec_cp_ctx.builder_cp_report.build())
}

fn should_error_unsafe_destination_path(
    path_dst: &Path,
    spec_cp_ctx: &mut SpecCopyContext,
) -> bool {
    if let Err(message) = validate_destination_path_safety(path_dst, &spec_cp_ctx.path_dir_dst) {
        spec_cp_ctx
            .builder_cp_report
            .add_error(path_dst.to_path_buf(), message);
        return true;
    }
    false
}

fn flush_file_copy_tasks(spec_cp_ctx: &mut SpecCopyContext) {
    let l_tasks_file_copy = std::mem::take(&mut spec_cp_ctx.l_tasks_file_copy);
    if l_tasks_file_copy.is_empty() {
        return;
    }

    let apply_results = |l_results: Vec<(PathBuf, Result<(), String>)>,
                         builder_cp_report: &mut ReportCopyBuilder| {
        for (path_file_dst, res_copy) in l_results {
            match res_copy {
                Ok(_) => builder_cp_report.add_copied(),
                Err(msg) => builder_cp_report.add_error(path_file_dst, msg),
            }
        }
    };

    if spec_cp_ctx.n_workers_max <= 1 {
        let l_results = l_tasks_file_copy
            .into_iter()
            .map(|spec_task| {
                let res_copy = validate_destination_path_safety(
                    &spec_task.path_file_dst,
                    &spec_cp_ctx.path_dir_dst,
                )
                .and_then(|_| {
                    copy_file_with_metadata(&spec_task.path_file_src, &spec_task.path_file_dst)
                        .map_err(|e| e.to_string())
                });
                (spec_task.path_file_dst, res_copy)
            })
            .collect::<Vec<_>>();
        apply_results(l_results, &mut spec_cp_ctx.builder_cp_report);
        return;
    }

    let thread_pool = ThreadPoolBuilder::new()
        .num_threads(spec_cp_ctx.n_workers_max)
        .build();
    let Ok(thread_pool) = thread_pool else {
        spec_cp_ctx.builder_cp_report.add_warning(format!(
            "Failed to initialize thread pool (workers={}); fallback to serial copy.",
            spec_cp_ctx.n_workers_max
        ));
        let l_results = l_tasks_file_copy
            .into_iter()
            .map(|spec_task| {
                let res_copy = validate_destination_path_safety(
                    &spec_task.path_file_dst,
                    &spec_cp_ctx.path_dir_dst,
                )
                .and_then(|_| {
                    copy_file_with_metadata(&spec_task.path_file_src, &spec_task.path_file_dst)
                        .map_err(|e| e.to_string())
                });
                (spec_task.path_file_dst, res_copy)
            })
            .collect::<Vec<_>>();
        apply_results(l_results, &mut spec_cp_ctx.builder_cp_report);
        return;
    };

    let l_results = thread_pool.install(|| {
        let path_dir_dst_root = spec_cp_ctx.path_dir_dst.clone();
        l_tasks_file_copy
            .into_par_iter()
            .map(|spec_task| {
                let res_copy =
                    validate_destination_path_safety(&spec_task.path_file_dst, &path_dir_dst_root)
                        .and_then(|_| {
                            copy_file_with_metadata(
                                &spec_task.path_file_src,
                                &spec_task.path_file_dst,
                            )
                            .map_err(|e| e.to_string())
                        });
                (spec_task.path_file_dst, res_copy)
            })
            .collect::<Vec<_>>()
    });
    apply_results(l_results, &mut spec_cp_ctx.builder_cp_report);
}

fn walk_directory(path_root: &Path, n_depth_relative: usize, spec_cp_ctx: &mut SpecCopyContext) {
    let enum_rule_symlink = spec_cp_ctx.spec_cp_options.rule_symlink;
    if enum_rule_symlink == EnumCopySymlinkStrategy::Dereference {
        if let Ok(stat_root) = fs::metadata(path_root) {
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                let tuple_dirs_identifier = (stat_root.dev(), stat_root.ino());
                if !spec_cp_ctx.set_visited_dirs.insert(tuple_dirs_identifier) {
                    spec_cp_ctx
                        .builder_cp_report
                        .add_warning(format!("Symlink loop detected: {}", path_root.display()));
                    return;
                }
            }
        } else {
            spec_cp_ctx
                .builder_cp_report
                .add_warning(format!("Failed to stat directory: {}", path_root.display()));
            return;
        }
    }

    let mut l_dirs: Vec<SpecDirEntry> = Vec::new();
    let mut l_files: Vec<SpecFileEntry> = Vec::new();

    let iter_entries = match fs::read_dir(path_root) {
        Ok(iter) => iter,
        Err(e) => {
            spec_cp_ctx.builder_cp_report.add_warning(format!(
                "Failed to read directory {} ({e})",
                path_root.display()
            ));
            return;
        }
    };

    for _entry_res in iter_entries {
        let entry = match _entry_res {
            Ok(v) => v,
            Err(e) => {
                spec_cp_ctx.builder_cp_report.add_warning(format!(
                    "Failed to read directory entry under {} ({e})",
                    path_root.display()
                ));
                continue;
            }
        };

        let path_entry = entry.path();
        let c_name = entry.file_name().to_string_lossy().to_string();
        let cfg_file_type = match entry.file_type() {
            Ok(v) => v,
            Err(e) => {
                spec_cp_ctx
                    .builder_cp_report
                    .add_warning(format!("Failed to inspect {} ({e})", path_entry.display()));
                continue;
            }
        };

        let b_is_symlink = cfg_file_type.is_symlink();
        let b_is_dir = cfg_file_type.is_dir() || (b_is_symlink && path_entry.is_dir());
        if b_is_dir {
            l_dirs.push(SpecDirEntry {
                path_dir_src_sub: path_entry,
                name_dir: c_name,
                if_is_symlink: b_is_symlink,
            });
        } else if cfg_file_type.is_file() || b_is_symlink {
            l_files.push(SpecFileEntry {
                path_file_src: path_entry,
                name_file: c_name,
                if_is_symlink: b_is_symlink,
            });
        } else {
            spec_cp_ctx
                .builder_cp_report
                .add_warning(format!("Special file skipped: {}", path_entry.display()));
        }
    }

    l_dirs.sort_by(|a, b| a.name_dir.cmp(&b.name_dir));
    l_files.sort_by(|a, b| a.name_file.cmp(&b.name_file));

    if spec_cp_ctx.spec_cp_pats.patterns_include_dirs.is_some()
        || spec_cp_ctx.spec_cp_pats.patterns_exclude_dirs.is_some()
    {
        let enum_rule_pattern = spec_cp_ctx.spec_cp_options.rule_pattern;
        l_dirs.retain(|d| {
            !should_exclude_by_patterns(
                &d.name_dir,
                spec_cp_ctx.spec_cp_pats.patterns_include_dirs.as_ref(),
                spec_cp_ctx.spec_cp_pats.patterns_exclude_dirs.as_ref(),
                enum_rule_pattern,
            )
        });
    }

    let depth_limit = spec_cp_ctx.spec_cp_options.depth_limit;
    if depth_limit.is_some_and(|n| n_depth_relative >= n) {
        l_dirs.clear();
    }

    for _dir_entry in l_dirs {
        let path_next = _dir_entry.path_dir_src_sub.clone();
        let b_should_descend = handle_dir_entry(_dir_entry, n_depth_relative + 1, spec_cp_ctx);
        if b_should_descend {
            walk_directory(&path_next, n_depth_relative + 1, spec_cp_ctx);
        }
    }

    for _file_entry in l_files {
        handle_file_entry(_file_entry, n_depth_relative + 1, spec_cp_ctx);
    }
}

fn handle_dir_entry(
    spec_dir_entry: SpecDirEntry,
    depth_value: usize,
    spec_cp_ctx: &mut SpecCopyContext,
) -> bool {
    let depth_limit = spec_cp_ctx.spec_cp_options.depth_limit;
    let enum_rule_depth_limit = spec_cp_ctx.spec_cp_options.rule_depth_limit;
    let b_depth_within = is_depth_within_limit(depth_value, depth_limit, enum_rule_depth_limit);

    let enum_rule_symlink = spec_cp_ctx.spec_cp_options.rule_symlink;
    let enum_rule_conflict_dir = spec_cp_ctx.spec_cp_options.rule_conflict_dir;
    let enum_rule_conflict_file = spec_cp_ctx.spec_cp_options.rule_conflict_file;
    let if_keep_tree = spec_cp_ctx.spec_cp_options.if_keep_tree;
    let if_dry_run = spec_cp_ctx.spec_cp_options.if_dry_run;

    if spec_dir_entry.if_is_symlink {
        if enum_rule_symlink == EnumCopySymlinkStrategy::SkipSymlinks {
            if if_keep_tree && b_depth_within {
                spec_cp_ctx
                    .builder_cp_report
                    .add_counts(&["cnt_scanned", "cnt_matched", "cnt_skipped"], 1);
            }
            return false;
        }

        if should_error_broken_symlink(&spec_dir_entry.path_dir_src_sub, enum_rule_symlink) {
            spec_cp_ctx.builder_cp_report.add_error(
                spec_dir_entry.path_dir_src_sub.clone(),
                format!(
                    "Broken symlink: {}",
                    spec_dir_entry.path_dir_src_sub.display()
                ),
            );
            if if_keep_tree && b_depth_within {
                spec_cp_ctx
                    .builder_cp_report
                    .add_counts(&["cnt_scanned", "cnt_matched"], 1);
            }
            return false;
        }

        if enum_rule_symlink == EnumCopySymlinkStrategy::CopySymlinks {
            if !b_depth_within {
                return false;
            }
            spec_cp_ctx
                .builder_cp_report
                .add_counts(&["cnt_scanned", "cnt_matched"], 1);

            if if_keep_tree {
                let path_dir_dst_sub = derive_destination_path(
                    &spec_dir_entry.path_dir_src_sub,
                    &spec_dir_entry.name_dir,
                    &spec_cp_ctx.path_dir_src,
                    &spec_cp_ctx.path_dir_dst,
                    if_keep_tree,
                );
                if should_error_unsafe_destination_path(&path_dir_dst_sub, spec_cp_ctx) {
                    return false;
                }

                if should_skip_dir_conflict(
                    &path_dir_dst_sub,
                    enum_rule_conflict_dir,
                    &mut spec_cp_ctx.builder_cp_report,
                ) {
                    return false;
                }

                if enum_rule_conflict_dir == EnumCopyDirectoryConflictStrategy::Merge {
                    spec_cp_ctx.builder_cp_report.add_warning(format!(
                        "Merge not applicable to symlink: {}",
                        path_dir_dst_sub.display()
                    ));
                    spec_cp_ctx.builder_cp_report.add_skipped();
                    return false;
                }

                if if_dry_run {
                    spec_cp_ctx.builder_cp_report.add_skipped();
                    return false;
                }

                create_symbolic_link(
                    &spec_dir_entry.path_dir_src_sub,
                    &path_dir_dst_sub,
                    &mut spec_cp_ctx.builder_cp_report,
                );
                return false;
            }

            let path_file_dst = spec_cp_ctx.path_dir_dst.join(&spec_dir_entry.name_dir);
            if should_error_unsafe_destination_path(&path_file_dst, spec_cp_ctx) {
                return false;
            }
            if should_skip_file_conflict(
                &path_file_dst,
                enum_rule_conflict_file,
                &mut spec_cp_ctx.builder_cp_report,
            ) {
                return false;
            }

            if if_dry_run {
                spec_cp_ctx.builder_cp_report.add_skipped();
                return false;
            }

            create_symbolic_link(
                &spec_dir_entry.path_dir_src_sub,
                &path_file_dst,
                &mut spec_cp_ctx.builder_cp_report,
            );
            return false;
        }
    }

    if if_keep_tree && b_depth_within {
        spec_cp_ctx
            .builder_cp_report
            .add_counts(&["cnt_scanned", "cnt_matched"], 1);
        let path_dir_dst_sub = derive_destination_path(
            &spec_dir_entry.path_dir_src_sub,
            &spec_dir_entry.name_dir,
            &spec_cp_ctx.path_dir_src,
            &spec_cp_ctx.path_dir_dst,
            if_keep_tree,
        );
        if should_error_unsafe_destination_path(&path_dir_dst_sub, spec_cp_ctx) {
            return false;
        }

        if should_skip_dir_conflict(
            &path_dir_dst_sub,
            enum_rule_conflict_dir,
            &mut spec_cp_ctx.builder_cp_report,
        ) {
            return false;
        }

        if if_dry_run {
            spec_cp_ctx.builder_cp_report.add_skipped();
        } else if let Err(e) = fs::create_dir_all(&path_dir_dst_sub) {
            spec_cp_ctx
                .builder_cp_report
                .add_error(path_dir_dst_sub, e.to_string());
            return false;
        } else {
            spec_cp_ctx.builder_cp_report.add_copied();
        }
    }

    true
}

fn handle_file_entry(
    spec_file_entry: SpecFileEntry,
    depth_value: usize,
    spec_cp_ctx: &mut SpecCopyContext,
) {
    let depth_limit = spec_cp_ctx.spec_cp_options.depth_limit;
    let enum_rule_depth_limit = spec_cp_ctx.spec_cp_options.rule_depth_limit;
    if !is_depth_within_limit(depth_value, depth_limit, enum_rule_depth_limit) {
        return;
    }

    spec_cp_ctx.builder_cp_report.add_scanned();

    let enum_rule_pattern = spec_cp_ctx.spec_cp_options.rule_pattern;
    if should_exclude_by_patterns(
        &spec_file_entry.name_file,
        spec_cp_ctx.spec_cp_pats.patterns_include_files.as_ref(),
        spec_cp_ctx.spec_cp_pats.patterns_exclude_files.as_ref(),
        enum_rule_pattern,
    ) {
        return;
    }
    spec_cp_ctx.builder_cp_report.add_matched();

    let enum_rule_symlink = spec_cp_ctx.spec_cp_options.rule_symlink;
    if spec_file_entry.if_is_symlink {
        if enum_rule_symlink == EnumCopySymlinkStrategy::SkipSymlinks {
            spec_cp_ctx.builder_cp_report.add_skipped();
            return;
        }

        if should_error_broken_symlink(&spec_file_entry.path_file_src, enum_rule_symlink) {
            spec_cp_ctx.builder_cp_report.add_error(
                spec_file_entry.path_file_src.clone(),
                format!(
                    "Broken symlink: {}",
                    spec_file_entry.path_file_src.display()
                ),
            );
            return;
        }
    }
    if !spec_file_entry.if_is_symlink {
        let meta_file_src = match fs::symlink_metadata(&spec_file_entry.path_file_src) {
            Ok(v) => v,
            Err(e) => {
                spec_cp_ctx
                    .builder_cp_report
                    .add_error(spec_file_entry.path_file_src.clone(), e.to_string());
                return;
            }
        };
        if !meta_file_src.file_type().is_file() {
            spec_cp_ctx.builder_cp_report.add_warning(format!(
                "Special file skipped: {}",
                spec_file_entry.path_file_src.display()
            ));
            spec_cp_ctx.builder_cp_report.add_skipped();
            return;
        }
    } else if enum_rule_symlink == EnumCopySymlinkStrategy::Dereference {
        let meta_file_src_target = match fs::metadata(&spec_file_entry.path_file_src) {
            Ok(v) => v,
            Err(e) => {
                spec_cp_ctx
                    .builder_cp_report
                    .add_error(spec_file_entry.path_file_src.clone(), e.to_string());
                return;
            }
        };
        if !meta_file_src_target.file_type().is_file() {
            spec_cp_ctx.builder_cp_report.add_warning(format!(
                "Special file target skipped: {}",
                spec_file_entry.path_file_src.display()
            ));
            spec_cp_ctx.builder_cp_report.add_skipped();
            return;
        }
    }

    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::MetadataExt;

        if !spec_file_entry.if_is_symlink
            && let Ok(stat_src) = fs::metadata(&spec_file_entry.path_file_src)
            && stat_src.nlink() > 1
        {
            spec_cp_ctx.builder_cp_report.add_warning(format!(
                "Hard link detected: {}",
                spec_file_entry.path_file_src.display()
            ));
        }
    }

    let if_keep_tree = spec_cp_ctx.spec_cp_options.if_keep_tree;
    let path_file_dst = derive_destination_path(
        &spec_file_entry.path_file_src,
        &spec_file_entry.name_file,
        &spec_cp_ctx.path_dir_src,
        &spec_cp_ctx.path_dir_dst,
        if_keep_tree,
    );
    if should_error_unsafe_destination_path(&path_file_dst, spec_cp_ctx) {
        return;
    }

    if if_keep_tree
        && let Some(path_parent_dst) = path_file_dst.parent()
        && let Err(e) = fs::create_dir_all(path_parent_dst)
    {
        spec_cp_ctx
            .builder_cp_report
            .add_error(path_file_dst, e.to_string());
        return;
    }

    let enum_rule_conflict_file = spec_cp_ctx.spec_cp_options.rule_conflict_file;
    if should_skip_file_conflict(
        &path_file_dst,
        enum_rule_conflict_file,
        &mut spec_cp_ctx.builder_cp_report,
    ) {
        return;
    }

    if spec_cp_ctx.spec_cp_options.if_dry_run {
        spec_cp_ctx.builder_cp_report.add_skipped();
        return;
    }

    if spec_file_entry.if_is_symlink && enum_rule_symlink == EnumCopySymlinkStrategy::CopySymlinks {
        create_symbolic_link(
            &spec_file_entry.path_file_src,
            &path_file_dst,
            &mut spec_cp_ctx.builder_cp_report,
        );
        return;
    }

    spec_cp_ctx.l_tasks_file_copy.push(SpecCopyTaskFile {
        path_file_src: spec_file_entry.path_file_src,
        path_file_dst,
    });
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::copy_tree;
    use crate::spec::{
        CopyTreeError, EnumCopyDepthLimitMode, EnumCopyDirectoryConflictStrategy,
        EnumCopyFileConflictStrategy, EnumCopyPatternMode, EnumCopySymlinkStrategy,
        SpecCopyOptions,
    };

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new() -> Self {
            let n = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos();
            let path = std::env::temp_dir().join(format!("axiomkit_fs_test_{n}"));
            std::fs::create_dir_all(&path).expect("create test dir");
            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn write_text(path: &Path, txt: &str) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }
        std::fs::write(path, txt).expect("write text");
    }

    #[test]
    fn copy_tree_smoke_basic() {
        let tmp = TestDir::new();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");

        write_text(&src.join("root.txt"), "root");
        write_text(&src.join("a/file1.txt"), "a");
        write_text(&src.join("b/sub/file2.txt"), "b");

        let report = copy_tree(&src, &dst, SpecCopyOptions::default()).expect("copy tree");
        assert_eq!(report.error_count(), 0);
        assert!(dst.join("root.txt").exists());
        assert!(dst.join("a/file1.txt").exists());
        assert!(dst.join("b/sub/file2.txt").exists());
    }

    #[test]
    fn copy_tree_flatten_with_include_glob() {
        let tmp = TestDir::new();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");

        write_text(&src.join("root.txt"), "root");
        write_text(&src.join("a/file1.txt"), "a");
        write_text(&src.join("a/file1.md"), "a");

        let spec_cp_options = SpecCopyOptions {
            if_keep_tree: false,
            patterns_include_files: Some(vec!["*.txt".to_string()]),
            ..SpecCopyOptions::default()
        };

        let report = copy_tree(&src, &dst, spec_cp_options).expect("copy tree");
        assert_eq!(report.error_count(), 0);
        assert!(dst.join("root.txt").exists());
        assert!(dst.join("file1.txt").exists());
        assert!(!dst.join("file1.md").exists());
    }

    #[test]
    fn copy_tree_depth_exact_works() {
        let tmp = TestDir::new();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");

        write_text(&src.join("root.txt"), "root");
        write_text(&src.join("a/file1.txt"), "a");

        let spec_cp_options = SpecCopyOptions {
            depth_limit: Some(1),
            rule_depth_limit: EnumCopyDepthLimitMode::Exact,
            ..SpecCopyOptions::default()
        };

        let report = copy_tree(&src, &dst, spec_cp_options).expect("copy tree");
        assert_eq!(report.error_count(), 0);
        assert!(dst.join("root.txt").exists());
        assert!(!dst.join("a/file1.txt").exists());
    }

    #[test]
    fn copy_tree_overlap_rejected() {
        let tmp = TestDir::new();
        let src = tmp.path().join("src");
        std::fs::create_dir_all(&src).expect("mkdir src");

        let nested = src.join("nested");
        let err = copy_tree(&src, &nested, SpecCopyOptions::default()).expect_err("must fail");
        assert!(matches!(
            err,
            CopyTreeError::SourceDestinationOverlap { .. }
        ));
    }

    #[cfg(unix)]
    #[test]
    fn copy_tree_symlink_copy_mode() {
        use std::os::unix::fs::symlink;

        let tmp = TestDir::new();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        write_text(&src.join("root.txt"), "root");
        symlink(src.join("root.txt"), src.join("link_root.txt")).expect("create symlink");

        let spec_cp_options = SpecCopyOptions {
            rule_symlink: EnumCopySymlinkStrategy::CopySymlinks,
            ..SpecCopyOptions::default()
        };

        let report = copy_tree(&src, &dst, spec_cp_options).expect("copy tree");
        assert_eq!(report.error_count(), 0);
        assert!(dst.join("link_root.txt").is_symlink());
    }

    #[test]
    fn copy_tree_include_regex_works() {
        let tmp = TestDir::new();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");

        write_text(&src.join("report_01.csv"), "ok");
        write_text(&src.join("report_02.csv"), "ok");
        write_text(&src.join("note.txt"), "txt");

        let spec_cp_options = SpecCopyOptions {
            patterns_include_files: Some(vec![r"^report_\d+\.csv$".to_string()]),
            rule_pattern: EnumCopyPatternMode::Regex,
            ..SpecCopyOptions::default()
        };

        let report = copy_tree(&src, &dst, spec_cp_options).expect("copy tree");
        assert_eq!(report.error_count(), 0);
        assert!(dst.join("report_01.csv").exists());
        assert!(dst.join("report_02.csv").exists());
        assert!(!dst.join("note.txt").exists());
    }

    #[test]
    fn copy_tree_include_exclude_regex_works() {
        let tmp = TestDir::new();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");

        write_text(&src.join("report_keep.csv"), "ok");
        write_text(&src.join("report_skip.csv"), "skip");
        write_text(&src.join("other.csv"), "other");

        let spec_cp_options = SpecCopyOptions {
            patterns_include_files: Some(vec![r"^report_.*\.csv$".to_string()]),
            patterns_exclude_files: Some(vec![r"^report_skip\.csv$".to_string()]),
            rule_pattern: EnumCopyPatternMode::Regex,
            ..SpecCopyOptions::default()
        };

        let report = copy_tree(&src, &dst, spec_cp_options).expect("copy tree");
        assert_eq!(report.error_count(), 0);
        assert!(dst.join("report_keep.csv").exists());
        assert!(!dst.join("report_skip.csv").exists());
        assert!(!dst.join("other.csv").exists());
    }

    #[test]
    fn copy_tree_invalid_regex_rejected() {
        let tmp = TestDir::new();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        write_text(&src.join("a.txt"), "a");

        let spec_cp_options = SpecCopyOptions {
            patterns_include_files: Some(vec!["(".to_string()]),
            rule_pattern: EnumCopyPatternMode::Regex,
            ..SpecCopyOptions::default()
        };

        let err = copy_tree(&src, &dst, spec_cp_options).expect_err("invalid regex must fail");
        assert!(matches!(err, CopyTreeError::InvalidPattern(_)));
    }

    #[test]
    fn copy_tree_glob_char_class_works() {
        let tmp = TestDir::new();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");

        write_text(&src.join("file1.txt"), "1");
        write_text(&src.join("filea.txt"), "a");

        let spec_cp_options = SpecCopyOptions {
            patterns_include_files: Some(vec!["file[0-9].txt".to_string()]),
            rule_pattern: EnumCopyPatternMode::Glob,
            ..SpecCopyOptions::default()
        };

        let report = copy_tree(&src, &dst, spec_cp_options).expect("copy tree");
        assert_eq!(report.error_count(), 0);
        assert!(dst.join("file1.txt").exists());
        assert!(!dst.join("filea.txt").exists());
    }

    #[test]
    fn copy_tree_invalid_glob_rejected() {
        let tmp = TestDir::new();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        write_text(&src.join("a.txt"), "a");

        let spec_cp_options = SpecCopyOptions {
            patterns_include_files: Some(vec!["[".to_string()]),
            rule_pattern: EnumCopyPatternMode::Glob,
            ..SpecCopyOptions::default()
        };

        let err = copy_tree(&src, &dst, spec_cp_options).expect_err("invalid glob must fail");
        assert!(matches!(err, CopyTreeError::InvalidPattern(_)));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn copy_tree_warns_hard_link() {
        let tmp = TestDir::new();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        write_text(&src.join("base.txt"), "base");
        std::fs::hard_link(src.join("base.txt"), src.join("alias.txt")).expect("hard link");

        let report = copy_tree(&src, &dst, SpecCopyOptions::default()).expect("copy tree");
        assert_eq!(report.error_count(), 0);
        assert!(
            report
                .warnings
                .iter()
                .any(|w| w.contains("Hard link detected"))
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn copy_tree_preserves_linux_metadata() {
        use filetime::{FileTime, set_file_times};
        use std::os::unix::fs::PermissionsExt;

        let tmp = TestDir::new();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        let path_file_src = src.join("meta.txt");
        write_text(&path_file_src, "meta");

        std::fs::set_permissions(&path_file_src, std::fs::Permissions::from_mode(0o640))
            .expect("set permissions");
        set_file_times(
            &path_file_src,
            FileTime::from_unix_time(1_700_000_010, 0),
            FileTime::from_unix_time(1_700_000_020, 0),
        )
        .expect("set times");

        let c_xattr_name = "user.axiomkit_fs_test";
        let b_if_has_xattr = xattr::set(&path_file_src, c_xattr_name, b"meta_value").is_ok();

        let report = copy_tree(&src, &dst, SpecCopyOptions::default()).expect("copy tree");
        assert_eq!(report.error_count(), 0);

        let path_file_dst = dst.join("meta.txt");
        let stat_src = std::fs::metadata(&path_file_src).expect("src metadata");
        let stat_dst = std::fs::metadata(&path_file_dst).expect("dst metadata");
        assert_eq!(
            stat_src.permissions().mode() & 0o777,
            stat_dst.permissions().mode() & 0o777
        );
        assert_eq!(
            FileTime::from_last_modification_time(&stat_src),
            FileTime::from_last_modification_time(&stat_dst)
        );

        if b_if_has_xattr {
            let raw_value_dst = xattr::get(&path_file_dst, c_xattr_name)
                .expect("get dst xattr")
                .expect("xattr exists");
            assert_eq!(raw_value_dst, b"meta_value");
        }
    }

    #[test]
    fn copy_tree_with_single_worker_works() {
        let tmp = TestDir::new();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");

        write_text(&src.join("a.txt"), "a");
        write_text(&src.join("b.txt"), "b");
        write_text(&src.join("c.txt"), "c");

        let spec_cp_options = SpecCopyOptions {
            num_workers_max: Some(1),
            ..SpecCopyOptions::default()
        };
        let report = copy_tree(&src, &dst, spec_cp_options).expect("copy tree");

        assert_eq!(report.error_count(), 0);
        assert_eq!(report.cnt_copied, 3);
        assert!(dst.join("a.txt").exists());
        assert!(dst.join("b.txt").exists());
        assert!(dst.join("c.txt").exists());
    }

    #[test]
    fn copy_tree_with_zero_worker_value_falls_back_to_one() {
        let tmp = TestDir::new();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        write_text(&src.join("a.txt"), "a");

        let spec_cp_options = SpecCopyOptions {
            num_workers_max: Some(0),
            ..SpecCopyOptions::default()
        };
        let report = copy_tree(&src, &dst, spec_cp_options).expect("copy tree");

        assert_eq!(report.error_count(), 0);
        assert!(dst.join("a.txt").exists());
    }

    #[cfg(unix)]
    #[test]
    fn copy_tree_rejects_symlink_destination_root() {
        use std::os::unix::fs::symlink;

        let tmp = TestDir::new();
        let src = tmp.path().join("src");
        let dst_real = tmp.path().join("dst_real");
        let dst_link = tmp.path().join("dst_link");
        write_text(&src.join("a.txt"), "a");
        std::fs::create_dir_all(&dst_real).expect("create dst real");
        symlink(&dst_real, &dst_link).expect("create dst symlink");

        let err = copy_tree(&src, &dst_link, SpecCopyOptions::default())
            .expect_err("symlink destination root must fail");
        assert!(matches!(err, CopyTreeError::DestinationInitFailed { .. }));
    }

    #[cfg(unix)]
    #[test]
    fn copy_tree_blocks_destination_symlink_escape_in_merge_mode() {
        use std::os::unix::fs::symlink;

        let tmp = TestDir::new();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        let outside = tmp.path().join("outside");

        write_text(&src.join("escape/file.txt"), "x");
        std::fs::create_dir_all(&dst).expect("create dst");
        std::fs::create_dir_all(&outside).expect("create outside");
        symlink(&outside, dst.join("escape")).expect("create escape symlink");

        let spec_cp_options = SpecCopyOptions {
            rule_conflict_dir: EnumCopyDirectoryConflictStrategy::Merge,
            ..SpecCopyOptions::default()
        };
        let report = copy_tree(&src, &dst, spec_cp_options).expect("copy tree returns report");

        assert!(report.error_count() >= 1);
        assert!(!outside.join("file.txt").exists());
    }

    #[cfg(unix)]
    #[test]
    fn copy_tree_blocks_existing_symlink_target_with_overwrite() {
        use std::os::unix::fs::symlink;

        let tmp = TestDir::new();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        let outside = tmp.path().join("outside");

        write_text(&src.join("a.txt"), "safe");
        std::fs::create_dir_all(&dst).expect("create dst");
        std::fs::create_dir_all(&outside).expect("create outside");
        symlink(outside.join("out.txt"), dst.join("a.txt")).expect("create dst symlink");

        let spec_cp_options = SpecCopyOptions {
            rule_conflict_file: EnumCopyFileConflictStrategy::Overwrite,
            ..SpecCopyOptions::default()
        };
        let report = copy_tree(&src, &dst, spec_cp_options).expect("copy tree returns report");

        assert!(report.error_count() >= 1);
        assert!(!outside.join("out.txt").exists());
    }

    #[cfg(unix)]
    #[test]
    fn copy_tree_skips_special_target_when_dereference_symlink() {
        use std::os::unix::fs::symlink;

        let tmp = TestDir::new();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        write_text(&src.join("normal.txt"), "ok");
        std::fs::create_dir_all(&src).expect("create src");
        symlink("/dev/null", src.join("null_dev")).expect("create symlink to /dev/null");

        let spec_cp_options = SpecCopyOptions {
            rule_symlink: EnumCopySymlinkStrategy::Dereference,
            ..SpecCopyOptions::default()
        };
        let report = copy_tree(&src, &dst, spec_cp_options).expect("copy tree");

        assert!(report.warning_count() >= 1);
        assert!(
            report
                .warnings
                .iter()
                .any(|w| w.contains("Special file target skipped"))
        );
        assert!(!dst.join("null_dev").exists());
        assert!(dst.join("normal.txt").exists());
    }

    #[test]
    fn copy_tree_fuzz_like_randomized_inputs_no_panic() {
        fn derive_name(seed: u64, n_idx: usize) -> String {
            let mut value = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            value ^= (n_idx as u64).wrapping_mul(0x9E3779B97F4A7C15);
            format!("f_{:016x}.txt", value)
        }

        for n_seed in 0_u64..40 {
            let tmp = TestDir::new();
            let src = tmp.path().join("src");
            let dst = tmp.path().join("dst");

            for n_idx in 0..12 {
                let name = derive_name(n_seed, n_idx);
                if n_idx % 3 == 0 {
                    write_text(&src.join("a").join(name), "x");
                } else if n_idx % 3 == 1 {
                    write_text(&src.join("b").join("c").join(name), "x");
                } else {
                    write_text(&src.join(name), "x");
                }
            }

            let mut spec_cp_options = SpecCopyOptions::default();
            match n_seed % 3 {
                0 => {
                    spec_cp_options.rule_pattern = EnumCopyPatternMode::Literal;
                    spec_cp_options.patterns_include_files = Some(vec!["f_".to_string()]);
                }
                1 => {
                    spec_cp_options.rule_pattern = EnumCopyPatternMode::Glob;
                    spec_cp_options.patterns_include_files = Some(vec!["*.txt".to_string()]);
                    spec_cp_options.patterns_exclude_dirs = Some(vec!["b".to_string()]);
                }
                _ => {
                    spec_cp_options.rule_pattern = EnumCopyPatternMode::Regex;
                    spec_cp_options.patterns_include_files =
                        Some(vec![r"^f_[0-9a-f]+\.txt$".to_string()]);
                }
            }

            let report = copy_tree(&src, &dst, spec_cp_options).expect("copy tree");
            assert_eq!(report.error_count(), 0);
        }
    }
}

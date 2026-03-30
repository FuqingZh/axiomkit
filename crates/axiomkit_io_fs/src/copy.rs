//! Filesystem tree traversal and copy orchestration.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use rayon::ThreadPoolBuilder;
use rayon::prelude::*;

use crate::report::{CopyReport, CopyReportBuilder};
use crate::spec::{
    CopyDepthLimitMode, CopyDirectoryConflictStrategy, CopyOptionsSpec, CopySymlinkStrategy,
    CopyTreeError,
};
use crate::util::{
    CopyPatternsSpec, calculate_worker_limit, copy_file_with_metadata, create_symbolic_link,
    derive_destination_path, is_depth_within_limit, is_overlap, should_error_broken_symlink,
    should_exclude_by_patterns, should_skip_dir_conflict, should_skip_file_conflict,
    validate_destination_path_safety,
};

#[derive(Debug, Clone)]
struct DirEntryRecord {
    dir_src_path: PathBuf,
    dir_name: String,
    is_symlink: bool,
}

#[derive(Debug, Clone)]
struct FileEntryRecord {
    file_src_path: PathBuf,
    file_name: String,
    is_symlink: bool,
}

#[derive(Debug, Clone)]
struct CopyTaskFileSpec {
    file_src_path: PathBuf,
    file_dst_path: PathBuf,
}

#[derive(Debug)]
struct CopyContext {
    dir_src_path: PathBuf,
    dir_dst_path: PathBuf,
    copy_options: CopyOptionsSpec,
    copy_patterns: CopyPatternsSpec,
    workers_max: usize,
    report_builder: CopyReportBuilder,
    visited_dirs: HashSet<(u64, u64)>,
    file_copy_tasks: Vec<CopyTaskFileSpec>,
}

/// Copy a directory tree from `dir_source` to `dir_destination`.
///
/// Behavior is controlled by [`CopyOptionsSpec`], including:
/// - include/exclude pattern rules for files and directories,
/// - conflict policies for destination files/directories,
/// - symlink handling strategy,
/// - optional depth limiting,
/// - flatten (`should_keep_tree=false`) vs keep-tree copy mode,
/// - dry-run and worker count.
///
/// This function performs:
/// 1. Input validation and destination safety checks.
/// 2. Directory traversal and file-copy task planning.
/// 3. Batched file-copy execution (serial or rayon thread pool).
/// 4. Report aggregation.
///
/// Returns [`CopyReport`] when the run completes (with possible per-entry errors
/// stored in the report). Returns [`CopyTreeError`] only for top-level setup and
/// validation failures.
pub fn copy_tree(
    dir_source: impl AsRef<Path>,
    dir_destination: impl AsRef<Path>,
    copy_options: CopyOptionsSpec,
) -> Result<CopyReport, CopyTreeError> {
    let rule_depth_limit = copy_options.rule_depth_limit;
    if copy_options.depth_limit == Some(0) {
        return Err(CopyTreeError::InvalidDepthLimit(
            "Arg `depth_limit` must be >= 1 or None.".to_string(),
        ));
    }
    if copy_options.depth_limit.is_none() && rule_depth_limit == CopyDepthLimitMode::Exact {
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

    let copy_patterns = CopyPatternsSpec::from_raw(
        copy_options.patterns_include_files.as_deref(),
        copy_options.patterns_exclude_files.as_deref(),
        copy_options.patterns_include_dirs.as_deref(),
        copy_options.patterns_exclude_dirs.as_deref(),
        copy_options.rule_pattern,
    )?;
    let workers_max = calculate_worker_limit(copy_options.workers_max);

    let mut copy_ctx = CopyContext {
        dir_src_path: path_dir_src.clone(),
        dir_dst_path: path_dir_dst,
        copy_options,
        copy_patterns,
        workers_max,
        report_builder: CopyReportBuilder::default(),
        visited_dirs: HashSet::new(),
        file_copy_tasks: Vec::new(),
    };

    walk_directory(&path_dir_src, 0, &mut copy_ctx);
    flush_file_copy_tasks(&mut copy_ctx);
    Ok(copy_ctx.report_builder.build())
}

fn should_error_unsafe_destination_path(path_dst: &Path, copy_ctx: &mut CopyContext) -> bool {
    if let Err(message) = validate_destination_path_safety(path_dst, &copy_ctx.dir_dst_path) {
        copy_ctx
            .report_builder
            .add_error(path_dst.to_path_buf(), message);
        return true;
    }
    false
}

fn execute_copy_task(task: CopyTaskFileSpec, dir_dst_root: &Path) -> (PathBuf, Result<(), String>) {
    let copy_result =
        validate_destination_path_safety(&task.file_dst_path, dir_dst_root).and_then(|_| {
            copy_file_with_metadata(&task.file_src_path, &task.file_dst_path)
                .map_err(|_e| _e.to_string())
        });

    (task.file_dst_path.clone(), copy_result)
}

fn apply_results(
    results: Vec<(PathBuf, Result<(), String>)>,
    report_builder: &mut CopyReportBuilder,
) {
    for _result in results {
        let (path_dst, copy_result) = _result;
        match copy_result {
            Ok(_) => report_builder.add_copied(),
            Err(message) => report_builder.add_error(path_dst, message),
        }
    }
}

fn flush_file_copy_tasks(copy_ctx: &mut CopyContext) {
    let file_copy_tasks = std::mem::take(&mut copy_ctx.file_copy_tasks);
    if file_copy_tasks.is_empty() {
        return;
    }

    if copy_ctx.workers_max <= 1 {
        let results = file_copy_tasks
            .into_iter()
            .map(|_task| execute_copy_task(_task, &copy_ctx.dir_dst_path))
            .collect::<Vec<_>>();
        apply_results(results, &mut copy_ctx.report_builder);
        return;
    }

    let thread_pool = ThreadPoolBuilder::new()
        .num_threads(copy_ctx.workers_max)
        .build();
    let Ok(thread_pool) = thread_pool else {
        copy_ctx.report_builder.add_warning(format!(
            "Failed to initialize thread pool (workers={}); fallback to serial copy.",
            copy_ctx.workers_max
        ));
        let results = file_copy_tasks
            .into_iter()
            .map(|_task| execute_copy_task(_task, &copy_ctx.dir_dst_path))
            .collect::<Vec<_>>();
        apply_results(results, &mut copy_ctx.report_builder);
        return;
    };

    let results = thread_pool.install(|| {
        let dir_dst_root = copy_ctx.dir_dst_path.clone();
        file_copy_tasks
            .into_par_iter()
            .map(|_task| execute_copy_task(_task, &dir_dst_root))
            .collect::<Vec<_>>()
    });
    apply_results(results, &mut copy_ctx.report_builder);
}

fn walk_directory(path_root: &Path, depth_relative: usize, copy_ctx: &mut CopyContext) {
    let rule_symlink = copy_ctx.copy_options.rule_symlink;
    if rule_symlink == CopySymlinkStrategy::Dereference {
        if let Ok(stat_root) = fs::metadata(path_root) {
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                let dir_identifier = (stat_root.dev(), stat_root.ino());
                if !copy_ctx.visited_dirs.insert(dir_identifier) {
                    copy_ctx
                        .report_builder
                        .add_warning(format!("Symlink loop detected: {}", path_root.display()));
                    return;
                }
            }
        } else {
            copy_ctx
                .report_builder
                .add_warning(format!("Failed to stat directory: {}", path_root.display()));
            return;
        }
    }

    let mut dirs: Vec<DirEntryRecord> = Vec::new();
    let mut files: Vec<FileEntryRecord> = Vec::new();

    let iter_entries = match fs::read_dir(path_root) {
        Ok(iter) => iter,
        Err(e) => {
            copy_ctx.report_builder.add_warning(format!(
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
                copy_ctx.report_builder.add_warning(format!(
                    "Failed to read directory entry under {} ({e})",
                    path_root.display()
                ));
                continue;
            }
        };

        let path_entry = entry.path();
        let entry_name = entry.file_name().to_string_lossy().to_string();
        let file_type = match entry.file_type() {
            Ok(v) => v,
            Err(e) => {
                copy_ctx
                    .report_builder
                    .add_warning(format!("Failed to inspect {} ({e})", path_entry.display()));
                continue;
            }
        };

        let is_symlink = file_type.is_symlink();
        let is_dir = file_type.is_dir() || (is_symlink && path_entry.is_dir());
        if is_dir {
            dirs.push(DirEntryRecord {
                dir_src_path: path_entry,
                dir_name: entry_name,
                is_symlink,
            });
        } else if file_type.is_file() || is_symlink {
            files.push(FileEntryRecord {
                file_src_path: path_entry,
                file_name: entry_name,
                is_symlink,
            });
        } else {
            copy_ctx
                .report_builder
                .add_warning(format!("Special file skipped: {}", path_entry.display()));
        }
    }

    dirs.sort_by(|a, b| a.dir_name.cmp(&b.dir_name));
    files.sort_by(|a, b| a.file_name.cmp(&b.file_name));

    if copy_ctx.copy_patterns.patterns_include_dirs.is_some()
        || copy_ctx.copy_patterns.patterns_exclude_dirs.is_some()
    {
        let rule_pattern = copy_ctx.copy_options.rule_pattern;
        dirs.retain(|_d| {
            !should_exclude_by_patterns(
                &_d.dir_name,
                copy_ctx.copy_patterns.patterns_include_dirs.as_ref(),
                copy_ctx.copy_patterns.patterns_exclude_dirs.as_ref(),
                rule_pattern,
            )
        });
    }

    let depth_limit = copy_ctx.copy_options.depth_limit;
    if depth_limit.is_some_and(|_limit| depth_relative >= _limit) {
        dirs.clear();
    }

    for _dir_entry in dirs {
        let path_next = _dir_entry.dir_src_path.clone();
        let should_descend = handle_dir_entry(_dir_entry, depth_relative + 1, copy_ctx);
        if should_descend {
            walk_directory(&path_next, depth_relative + 1, copy_ctx);
        }
    }

    for _file_entry in files {
        handle_file_entry(_file_entry, depth_relative + 1, copy_ctx);
    }
}

fn handle_dir_entry(
    dir_entry: DirEntryRecord,
    depth_value: usize,
    copy_ctx: &mut CopyContext,
) -> bool {
    let depth_limit = copy_ctx.copy_options.depth_limit;
    let rule_depth_limit = copy_ctx.copy_options.rule_depth_limit;
    let is_depth_within = is_depth_within_limit(depth_value, depth_limit, rule_depth_limit);

    let rule_symlink = copy_ctx.copy_options.rule_symlink;
    let rule_conflict_dir = copy_ctx.copy_options.rule_conflict_dir;
    let rule_conflict_file = copy_ctx.copy_options.rule_conflict_file;
    let should_keep_tree = copy_ctx.copy_options.should_keep_tree;
    let should_dry_run = copy_ctx.copy_options.should_dry_run;

    if dir_entry.is_symlink {
        if rule_symlink == CopySymlinkStrategy::SkipSymlinks {
            if should_keep_tree && is_depth_within {
                copy_ctx
                    .report_builder
                    .add_counts(&["cnt_scanned", "cnt_matched", "cnt_skipped"], 1);
            }
            return false;
        }

        if should_error_broken_symlink(&dir_entry.dir_src_path, rule_symlink) {
            copy_ctx.report_builder.add_error(
                dir_entry.dir_src_path.clone(),
                format!("Broken symlink: {}", dir_entry.dir_src_path.display()),
            );
            if should_keep_tree && is_depth_within {
                copy_ctx
                    .report_builder
                    .add_counts(&["cnt_scanned", "cnt_matched"], 1);
            }
            return false;
        }

        if rule_symlink == CopySymlinkStrategy::CopySymlinks {
            if !is_depth_within {
                return false;
            }
            copy_ctx
                .report_builder
                .add_counts(&["cnt_scanned", "cnt_matched"], 1);

            if should_keep_tree {
                let path_dir_dst_sub = derive_destination_path(
                    &dir_entry.dir_src_path,
                    &dir_entry.dir_name,
                    &copy_ctx.dir_src_path,
                    &copy_ctx.dir_dst_path,
                    should_keep_tree,
                );
                if should_error_unsafe_destination_path(&path_dir_dst_sub, copy_ctx) {
                    return false;
                }

                if should_skip_dir_conflict(
                    &path_dir_dst_sub,
                    rule_conflict_dir,
                    &mut copy_ctx.report_builder,
                ) {
                    return false;
                }

                if rule_conflict_dir == CopyDirectoryConflictStrategy::Merge {
                    copy_ctx.report_builder.add_warning(format!(
                        "Merge not applicable to symlink: {}",
                        path_dir_dst_sub.display()
                    ));
                    copy_ctx.report_builder.add_skipped();
                    return false;
                }

                if should_dry_run {
                    copy_ctx.report_builder.add_skipped();
                    return false;
                }

                create_symbolic_link(
                    &dir_entry.dir_src_path,
                    &path_dir_dst_sub,
                    &mut copy_ctx.report_builder,
                );
                return false;
            }

            let path_file_dst = copy_ctx.dir_dst_path.join(&dir_entry.dir_name);
            if should_error_unsafe_destination_path(&path_file_dst, copy_ctx) {
                return false;
            }
            if should_skip_file_conflict(
                &path_file_dst,
                rule_conflict_file,
                &mut copy_ctx.report_builder,
            ) {
                return false;
            }

            if should_dry_run {
                copy_ctx.report_builder.add_skipped();
                return false;
            }

            create_symbolic_link(
                &dir_entry.dir_src_path,
                &path_file_dst,
                &mut copy_ctx.report_builder,
            );
            return false;
        }
    }

    if should_keep_tree && is_depth_within {
        copy_ctx
            .report_builder
            .add_counts(&["cnt_scanned", "cnt_matched"], 1);
        let path_dir_dst_sub = derive_destination_path(
            &dir_entry.dir_src_path,
            &dir_entry.dir_name,
            &copy_ctx.dir_src_path,
            &copy_ctx.dir_dst_path,
            should_keep_tree,
        );
        if should_error_unsafe_destination_path(&path_dir_dst_sub, copy_ctx) {
            return false;
        }

        if should_skip_dir_conflict(
            &path_dir_dst_sub,
            rule_conflict_dir,
            &mut copy_ctx.report_builder,
        ) {
            return false;
        }

        if should_dry_run {
            copy_ctx.report_builder.add_skipped();
        } else if let Err(e) = fs::create_dir_all(&path_dir_dst_sub) {
            copy_ctx
                .report_builder
                .add_error(path_dir_dst_sub, e.to_string());
            return false;
        } else {
            copy_ctx.report_builder.add_copied();
        }
    }

    true
}

fn handle_file_entry(file_entry: FileEntryRecord, depth_value: usize, copy_ctx: &mut CopyContext) {
    let depth_limit = copy_ctx.copy_options.depth_limit;
    let rule_depth_limit = copy_ctx.copy_options.rule_depth_limit;
    if !is_depth_within_limit(depth_value, depth_limit, rule_depth_limit) {
        return;
    }

    copy_ctx.report_builder.add_scanned();

    let rule_pattern = copy_ctx.copy_options.rule_pattern;
    if should_exclude_by_patterns(
        &file_entry.file_name,
        copy_ctx.copy_patterns.patterns_include_files.as_ref(),
        copy_ctx.copy_patterns.patterns_exclude_files.as_ref(),
        rule_pattern,
    ) {
        return;
    }
    copy_ctx.report_builder.add_matched();

    let rule_symlink = copy_ctx.copy_options.rule_symlink;
    if file_entry.is_symlink {
        if rule_symlink == CopySymlinkStrategy::SkipSymlinks {
            copy_ctx.report_builder.add_skipped();
            return;
        }

        if should_error_broken_symlink(&file_entry.file_src_path, rule_symlink) {
            copy_ctx.report_builder.add_error(
                file_entry.file_src_path.clone(),
                format!("Broken symlink: {}", file_entry.file_src_path.display()),
            );
            return;
        }
    }
    if !file_entry.is_symlink {
        let metadata_src = match fs::symlink_metadata(&file_entry.file_src_path) {
            Ok(v) => v,
            Err(e) => {
                copy_ctx
                    .report_builder
                    .add_error(file_entry.file_src_path.clone(), e.to_string());
                return;
            }
        };
        if !metadata_src.file_type().is_file() {
            copy_ctx.report_builder.add_warning(format!(
                "Special file skipped: {}",
                file_entry.file_src_path.display()
            ));
            copy_ctx.report_builder.add_skipped();
            return;
        }
    } else if rule_symlink == CopySymlinkStrategy::Dereference {
        let metadata_target = match fs::metadata(&file_entry.file_src_path) {
            Ok(v) => v,
            Err(e) => {
                copy_ctx
                    .report_builder
                    .add_error(file_entry.file_src_path.clone(), e.to_string());
                return;
            }
        };
        if !metadata_target.file_type().is_file() {
            copy_ctx.report_builder.add_warning(format!(
                "Special file target skipped: {}",
                file_entry.file_src_path.display()
            ));
            copy_ctx.report_builder.add_skipped();
            return;
        }
    }

    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::MetadataExt;

        if !file_entry.is_symlink
            && let Ok(stat_src) = fs::metadata(&file_entry.file_src_path)
            && stat_src.nlink() > 1
        {
            copy_ctx.report_builder.add_warning(format!(
                "Hard link detected: {}",
                file_entry.file_src_path.display()
            ));
        }
    }

    let should_keep_tree = copy_ctx.copy_options.should_keep_tree;
    let path_file_dst = derive_destination_path(
        &file_entry.file_src_path,
        &file_entry.file_name,
        &copy_ctx.dir_src_path,
        &copy_ctx.dir_dst_path,
        should_keep_tree,
    );
    if should_error_unsafe_destination_path(&path_file_dst, copy_ctx) {
        return;
    }

    if should_keep_tree
        && let Some(path_parent_dst) = path_file_dst.parent()
        && let Err(e) = fs::create_dir_all(path_parent_dst)
    {
        copy_ctx
            .report_builder
            .add_error(path_file_dst, e.to_string());
        return;
    }

    let rule_conflict_file = copy_ctx.copy_options.rule_conflict_file;
    if should_skip_file_conflict(
        &path_file_dst,
        rule_conflict_file,
        &mut copy_ctx.report_builder,
    ) {
        return;
    }

    if copy_ctx.copy_options.should_dry_run {
        copy_ctx.report_builder.add_skipped();
        return;
    }

    if file_entry.is_symlink && rule_symlink == CopySymlinkStrategy::CopySymlinks {
        create_symbolic_link(
            &file_entry.file_src_path,
            &path_file_dst,
            &mut copy_ctx.report_builder,
        );
        return;
    }

    copy_ctx.file_copy_tasks.push(CopyTaskFileSpec {
        file_src_path: file_entry.file_src_path,
        file_dst_path: path_file_dst,
    });
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::copy_tree;
    use crate::spec::{
        CopyDepthLimitMode, CopyDirectoryConflictStrategy, CopyFileConflictStrategy,
        CopyOptionsSpec, CopyPatternMode, CopySymlinkStrategy, CopyTreeError,
    };

    struct TestDir {
        path: PathBuf,
    }

    impl TestDir {
        fn new() -> Self {
            let timestamp_nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos();
            let path = std::env::temp_dir().join(format!("axiomkit_fs_test_{timestamp_nanos}"));
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

        let report = copy_tree(&src, &dst, CopyOptionsSpec::default()).expect("copy tree");
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

        let copy_options = CopyOptionsSpec {
            should_keep_tree: false,
            patterns_include_files: Some(vec!["*.txt".to_string()]),
            ..CopyOptionsSpec::default()
        };

        let report = copy_tree(&src, &dst, copy_options).expect("copy tree");
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

        let copy_options = CopyOptionsSpec {
            depth_limit: Some(1),
            rule_depth_limit: CopyDepthLimitMode::Exact,
            ..CopyOptionsSpec::default()
        };

        let report = copy_tree(&src, &dst, copy_options).expect("copy tree");
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
        let err = copy_tree(&src, &nested, CopyOptionsSpec::default()).expect_err("must fail");
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

        let copy_options = CopyOptionsSpec {
            rule_symlink: CopySymlinkStrategy::CopySymlinks,
            ..CopyOptionsSpec::default()
        };

        let report = copy_tree(&src, &dst, copy_options).expect("copy tree");
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

        let copy_options = CopyOptionsSpec {
            patterns_include_files: Some(vec![r"^report_\d+\.csv$".to_string()]),
            rule_pattern: CopyPatternMode::Regex,
            ..CopyOptionsSpec::default()
        };

        let report = copy_tree(&src, &dst, copy_options).expect("copy tree");
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

        let copy_options = CopyOptionsSpec {
            patterns_include_files: Some(vec![r"^report_.*\.csv$".to_string()]),
            patterns_exclude_files: Some(vec![r"^report_skip\.csv$".to_string()]),
            rule_pattern: CopyPatternMode::Regex,
            ..CopyOptionsSpec::default()
        };

        let report = copy_tree(&src, &dst, copy_options).expect("copy tree");
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

        let copy_options = CopyOptionsSpec {
            patterns_include_files: Some(vec!["(".to_string()]),
            rule_pattern: CopyPatternMode::Regex,
            ..CopyOptionsSpec::default()
        };

        let err = copy_tree(&src, &dst, copy_options).expect_err("invalid regex must fail");
        assert!(matches!(err, CopyTreeError::InvalidPattern(_)));
    }

    #[test]
    fn copy_tree_glob_char_class_works() {
        let tmp = TestDir::new();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");

        write_text(&src.join("file1.txt"), "1");
        write_text(&src.join("filea.txt"), "a");

        let copy_options = CopyOptionsSpec {
            patterns_include_files: Some(vec!["file[0-9].txt".to_string()]),
            rule_pattern: CopyPatternMode::Glob,
            ..CopyOptionsSpec::default()
        };

        let report = copy_tree(&src, &dst, copy_options).expect("copy tree");
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

        let copy_options = CopyOptionsSpec {
            patterns_include_files: Some(vec!["[".to_string()]),
            rule_pattern: CopyPatternMode::Glob,
            ..CopyOptionsSpec::default()
        };

        let err = copy_tree(&src, &dst, copy_options).expect_err("invalid glob must fail");
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

        let report = copy_tree(&src, &dst, CopyOptionsSpec::default()).expect("copy tree");
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
        let path_src_file = src.join("meta.txt");
        write_text(&path_src_file, "meta");

        std::fs::set_permissions(&path_src_file, std::fs::Permissions::from_mode(0o640))
            .expect("set permissions");
        set_file_times(
            &path_src_file,
            FileTime::from_unix_time(1_700_000_010, 0),
            FileTime::from_unix_time(1_700_000_020, 0),
        )
        .expect("set times");

        let xattr_name = "user.axiomkit_fs_test";
        let has_xattr = xattr::set(&path_src_file, xattr_name, b"meta_value").is_ok();

        let report = copy_tree(&src, &dst, CopyOptionsSpec::default()).expect("copy tree");
        assert_eq!(report.error_count(), 0);

        let path_dst_file = dst.join("meta.txt");
        let stat_src = std::fs::metadata(&path_src_file).expect("src metadata");
        let stat_dst = std::fs::metadata(&path_dst_file).expect("dst metadata");
        assert_eq!(
            stat_src.permissions().mode() & 0o777,
            stat_dst.permissions().mode() & 0o777
        );
        assert_eq!(
            FileTime::from_last_modification_time(&stat_src),
            FileTime::from_last_modification_time(&stat_dst)
        );

        if has_xattr {
            let raw_value_dst = xattr::get(&path_dst_file, xattr_name)
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

        let copy_options = CopyOptionsSpec {
            workers_max: Some(1),
            ..CopyOptionsSpec::default()
        };
        let report = copy_tree(&src, &dst, copy_options).expect("copy tree");

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

        let copy_options = CopyOptionsSpec {
            workers_max: Some(0),
            ..CopyOptionsSpec::default()
        };
        let report = copy_tree(&src, &dst, copy_options).expect("copy tree");

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

        let err = copy_tree(&src, &dst_link, CopyOptionsSpec::default())
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

        let copy_options = CopyOptionsSpec {
            rule_conflict_dir: CopyDirectoryConflictStrategy::Merge,
            ..CopyOptionsSpec::default()
        };
        let report = copy_tree(&src, &dst, copy_options).expect("copy tree returns report");

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

        let copy_options = CopyOptionsSpec {
            rule_conflict_file: CopyFileConflictStrategy::Overwrite,
            ..CopyOptionsSpec::default()
        };
        let report = copy_tree(&src, &dst, copy_options).expect("copy tree returns report");

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

        let copy_options = CopyOptionsSpec {
            rule_symlink: CopySymlinkStrategy::Dereference,
            ..CopyOptionsSpec::default()
        };
        let report = copy_tree(&src, &dst, copy_options).expect("copy tree");

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
        fn derive_name(seed: u64, idx: usize) -> String {
            let mut value = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            value ^= (idx as u64).wrapping_mul(0x9E3779B97F4A7C15);
            format!("f_{:016x}.txt", value)
        }

        for _seed in 0_u64..40 {
            let seed = _seed;
            let tmp = TestDir::new();
            let src = tmp.path().join("src");
            let dst = tmp.path().join("dst");

            for _idx in 0..12 {
                let idx = _idx;
                let name = derive_name(seed, idx);
                if idx % 3 == 0 {
                    write_text(&src.join("a").join(name), "x");
                } else if idx % 3 == 1 {
                    write_text(&src.join("b").join("c").join(name), "x");
                } else {
                    write_text(&src.join(name), "x");
                }
            }

            let mut copy_options = CopyOptionsSpec::default();
            match seed % 3 {
                0 => {
                    copy_options.rule_pattern = CopyPatternMode::Literal;
                    copy_options.patterns_include_files = Some(vec!["f_".to_string()]);
                }
                1 => {
                    copy_options.rule_pattern = CopyPatternMode::Glob;
                    copy_options.patterns_include_files = Some(vec!["*.txt".to_string()]);
                    copy_options.patterns_exclude_dirs = Some(vec!["b".to_string()]);
                }
                _ => {
                    copy_options.rule_pattern = CopyPatternMode::Regex;
                    copy_options.patterns_include_files =
                        Some(vec![r"^f_[0-9a-f]+\.txt$".to_string()]);
                }
            }

            let report = copy_tree(&src, &dst, copy_options).expect("copy tree");
            assert_eq!(report.error_count(), 0);
        }
    }
}

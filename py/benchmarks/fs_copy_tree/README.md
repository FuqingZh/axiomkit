# FS copy_tree Benchmarks

This directory stores reproducible benchmark records for `axiomkit.io.copy_tree`
with Rust `debug` vs `release` backend comparison.

## Run

From `py/`:

```bash
PYTHONPATH=src pdm run python scripts/benchmark_fs_copy_tree.py --repeat 3
```

## Output

Each run writes under `benchmarks/fs_copy_tree/results/`:

- `fs_copy_tree_<timestamp>.json`
- `fs_copy_tree_<timestamp>.md`
- `INDEX.md` (append-only archive index)

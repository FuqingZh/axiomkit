# toolbox_py

A small, reusable utility library for the proteomics project.
This package provides stable building blocks for CLI, I/O, and shared infrastructure.

## Directory contract

- core/
  - Minimal, reusable primitives and abstractions.
  - No CLI (argparse), no filesystem I/O, no domain-specific business logic.
  - Keep it thin.

- cli/
  - CLI contract layer: argparse groups, registry, actions, formatters.
  - Responsible for argument validation and help text.
  - Must not perform analysis/IO-heavy work.

- io/
  - Data boundary layer: dataset read/write, partitioning, path-safe sanitization.
  - Focus on file formats (e.g., Parquet datasets), filesystem safety, and reproducibility.
  - Must not import CLI code.

- tools/
  - One-off scripts and maintenance utilities.
  - Not a stable API. Can be deleted/rewritten freely.

- experiential/
  - Experimental prototypes and spikes.
  - Rule: every item must either be promoted to a stable module or removed within 2 weeks.

- runner.py
  - Entry wiring: connects CLI + domain pipelines.
  - Keep thin; delegates logic to dedicated modules.

## Public API (stable)

Preferred imports:

- CLI:
  - `from toolbox_py.cli import ParserRegistry, GroupKey, ParamKey, ParamRegistry`

- I/O:
  - `from toolbox_py.io import write_parquet_dataset`

## Design principles

- One responsibility per module.
- Avoid duplicate implementations across directories.
- Stable modules must have: type hints, clear docstrings, and minimal dependencies.
- Experiments must not leak into stable API by default.

## Promotion rules (experiential -> stable)

Promote only if:

1) Used by at least 2 commands/modules, or
2) Has clear contract + tests, and
3) Name and parameters are stable (no frequent churn).

Otherwise keep it in experiential/ or delete.

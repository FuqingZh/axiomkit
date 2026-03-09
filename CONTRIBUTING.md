# Contributing to axiomkit

This document defines contributor-facing conventions for public API naming,
method boundaries, CLI option naming, export schema/header naming, and
repository workflow.

## Scope

- This file is normative for:
  - public API naming in `py/`, `r/`, and `rs/`
  - module-level function prefixes
  - public method boundaries
  - public CLI option naming
  - public-facing schema/header naming for exports
  - repository workflow expectations
- Existing APIs can migrate incrementally; do not break external contracts
  without deprecation.
- Internal variable naming is guidance, not a merge gate.
- Tooling (`ruff`, `pyright`, tests, CI) is the final gate for merge, but
  naming and architecture review still applies.

## Public API Naming

### Compute and Infer

| Prefix | Use | Notes |
| --- | --- | --- |
| `calculate_` | deterministic numeric calculation | numeric result |
| `derive_` | derive structure, fields, or non-scalar artifacts | structural result |
| `estimate_` | approximate numeric value | approximation allowed |
| `infer_` | infer discrete label, enum, or categorical property | discrete result only |

### Construction

| Prefix | Use | Notes |
| --- | --- | --- |
| `create_` | create or instantiate a single object | single-object construction |
| `generate_` | generate a sequence or batch of outputs | batch or sequence output |
| `sample_` | random sampling | stochastic selection |

### Parse and Encode

| Prefix | Use | Notes |
| --- | --- | --- |
| `parse_` | text, headers, or lightweight metadata to structure | parsing to structure |
| `decode_` | encoded representation to raw value | encoded to raw |
| `encode_` | raw value to encoded representation | raw to encoded |

### Validation

| Prefix | Use | Notes |
| --- | --- | --- |
| `is_` | factual predicate | returns `bool` |
| `should_` | policy predicate | returns `bool` |
| `validate_` | strong validation | invalid input raises |

### Transform

| Prefix | Use | Notes |
| --- | --- | --- |
| `convert_` | equivalent type or format conversion | prefer reversibility |
| `sanitize_` | repair invalid text, field names, or unsupported characters | not business filtering |
| `center_` | location shift only | additive shift only |
| `scale_` | scale change only | multiplicative change only |
| `standardize_` | explicitly defined statistical standardization | for named statistical transforms |
| `normalize_` | broader distribution or scale normalization | use when a more precise transform prefix does not fit |

### Selection and Extraction

| Prefix | Use | Notes |
| --- | --- | --- |
| `filter_` | predicate-based filtering | not projection |
| `select_` | projection or reordering of fields or columns | not predicate filtering |
| `extract_` | extract substructure from nested or composite input | for nested or composite inputs |

### Planning and Application

| Prefix | Use | Notes |
| --- | --- | --- |
| `plan_` | produce a plan or specification | planning step |
| `apply_` | apply a plan to a target | application step |

### I/O

| Prefix | Use | Notes |
| --- | --- | --- |
| `copy_` | copy or migrate objects or filesystem resources | object or filesystem copying |
| `read_` | read and parse into an object | materialized read |
| `scan_` | lazy, lightweight, or metadata-first read | avoid full materialization by default |
| `sink_` | truly streaming write without full materialization | streaming output only |
| `write_` | serialize and write an object | non-streaming persistence |

### Workflow and Presentation

| Prefix | Use |
| --- | --- |
| `prepare_` | preparation step |
| `run_` | main workflow step |
| `finalize_` | finalization step |
| `render_` | presentation rendering |
| `report_` | reporting output |

## Naming Boundaries

| Boundary | Use | Avoid |
| --- | --- | --- |
| `infer_` | discrete results such as `Enum` or `Literal[...]`; if multiple fields are needed, return a clearly named `Spec*` with discrete fields | plain boolean predicates |
| `is_` / `should_` | `is_` for factual `bool` checks; `should_` for policy decisions | using `infer_` for boolean checks |
| `calculate_` / `derive_` | `calculate_` for numeric statistics or measurements; `derive_` for structure, fields, or schemas | mixing numeric outputs into `derive_` without structural intent |
| `create_` / `generate_` | `create_` for a single object; `generate_` for batch or sequence output | using `generate_` for one-off construction |
| `read_` / `scan_` | `read_` for materialized parsing; `scan_` for lazy, lightweight, or metadata-first access | materializing the primary payload by default in `scan_` |
| `write_` / `sink_` | `write_` for ordinary serialization and persistence; `sink_` for truly streaming output | using `sink_` for non-streaming writes |
| `select_` / `filter_` | `select_` for projection or reordering; `filter_` for predicates | mixing projection into `filter_` or predicates into `select_` |
| `sanitize_` | repair invalid input so it can be processed | business filtering or row dropping |
| `center_` / `scale_` / `standardize_` / `normalize_` | prefer the specific transform name when semantics are clear | defaulting to `normalize_` when a more precise prefix fits |
| `validate_` | strong validation; light I/O such as existence checks is acceptable | heavy I/O that belongs in `read_` or `write_` |

## Public Method Boundaries

Public methods should primarily express protocol, lifecycle, or object-local
configuration. Domain behavior should remain module-level functions following
the prefix rules above.

- `close()`: canonical resource termination or commit method
- `run()`: main execution entry for runnable objects
- `render()` / `report()`: produce presentation objects without implicit writes
- `build()`: allowed as the terminal method on `*Builder` types
- `from_*()` / `make()`: allowed as classmethod factories
- `add_*`, `select_*`, `group()`, `command()`, `done()`, `end()`, `with_*`:
  allowed on builders, registries, and fluent configuration helpers

Prefer module-level functions over generic public methods such as:

- `save`, `load`, `export`, `dump`
- `execute`, `start`, `stop`, `finish`, `shutdown`, `dispose`
- `process`, `do`, `get`, `show`

Language-level protocol essentials are allowed where applicable.

## CLI Option Naming

- Boolean options:
  - `is_...`: factual or state toggles
  - `should_...`: policy or strategy toggles
- Non-boolean options:
  - prefer explicit semantic names over abbreviations
  - use `file_...` and `dir_...` for file and directory paths when known
  - use `rule_...` for strategy or mode selectors
  - use `thr_...` for thresholds and cutoffs

## Internal Naming Guidance

Internal names are a readability aid, not a rigid contract.

- Prefer semantic names over type-only names.
- For parameters, use role-oriented names such as `file_...`, `dir_...`,
  `path_...`, `rule_...`, `thr_...`, `df_...`, `lf_...`, `dt_...`, `map_...`,
  `set_...`, and `fn_...` when they improve clarity.
- For local variables, prefer clear domain names; short container hints such as
  `df_...`, `dt_...`, or `map_...` are optional when they materially improve
  scan speed.
- Avoid generic names such as `obj`, `tmp`, `x`, or `value` when a concrete
  role is available.
- Loop variables may use short transient prefixes such as `_sheet`, `_name`,
  `.sheet`, or `.name`, but do not force them when a plain semantic name reads
  better.

## Public-facing Schema and Header Naming

| Context | Rule | Example |
| --- | --- | --- |
| Exported headers | use PascalCase | `SampleId`, `PtmFdr` |
| Standalone abbreviation token | use ALLCAPS | `ID`, `PTM`, `FDR` |
| Abbreviation inside a multi-token header | use PascalCase abbreviation segment | `SampleId`, `PtmFdr` |
| Internal storage | snake_case is acceptable internally; expose PascalCase through views, aliases, or export adapters | `sample_id` -> `SampleId` |

## Workflow

- Keep API changes minimal and explicit.
- Add or update tests with each behavior change.
- Keep docs and examples in sync with public API.
- Prefer additive evolution; deprecate before removal.

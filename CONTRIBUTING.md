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
| `infer_` | infer latent property, classification, schema guess, or structured interpretation from incomplete evidence | not for plain boolean checks or direct numeric computation |

### Construction

| Prefix | Use | Notes |
| --- | --- | --- |
| `create_` | create or instantiate a single object | single-object construction |
| `generate_` | generate a sequence or batch of outputs | batch or sequence output |
| `sample_` | random sampling | stochastic selection |

### Parse and Encode

| Prefix | Use | Notes |
| --- | --- | --- |
| `parse_` | textual or syntactic representation to structured form | parsing to structure |
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
| `scan_` | lazy, deferred, or metadata-first access | use when the API intentionally avoids full materialization by default |
| `sink_` | truly streaming write without full materialization | streaming output only |
| `write_` | serialize and write an object | non-streaming persistence |

### Workflow and Presentation

| Prefix | Use |
| --- | --- |
| `prepare_` | preparation step |
| `run_` | main workflow step |
| `finalize_` | finalization step |
| `summarize_` | reduce data or structure into a concise overview, aggregate description, or summary table |
| `plot_` | construct a plot or plotting object from data or specification |
| `render_` | render an already-defined presentation object or spec for display without implicit persistence |
| `report_` | assemble a human-oriented report artifact such as text, markdown, HTML, or tabular review output without implicit persistence |

## Naming Boundaries

| Boundary | Use | Avoid |
| --- | --- | --- |
| `infer_` | evidence-based interpretation, classification, schema guess, or structured inference result | plain boolean predicates, direct numeric computation, or pure structural reshaping without uncertainty |
| `is_` / `should_` | `is_` for factual `bool` checks; `should_` for policy decisions | using `infer_` for boolean checks |
| `calculate_` / `derive_` | `calculate_` for numeric statistics or measurements; `derive_` for structure, fields, or schemas | mixing numeric outputs into `derive_` without structural intent |
| `create_` / `generate_` | `create_` for a single object; `generate_` for batch or sequence output | using `generate_` for one-off construction |
| `read_` / `scan_` | `read_` for materialized parsing; `scan_` for lazy, deferred, or metadata-first access | materializing the primary payload by default in `scan_` |
| `write_` / `sink_` | `write_` for ordinary serialization and persistence; `sink_` for truly streaming output | using `sink_` for non-streaming writes |
| `select_` / `filter_` | `select_` for projection or reordering; `filter_` for predicates | mixing projection into `filter_` or predicates into `select_` |
| `sanitize_` | minimally repair invalid input so it can be processed while preserving semantics when possible | business filtering, row dropping, or destructive rewriting hidden behind `sanitize_` |
| `center_` / `scale_` / `standardize_` / `normalize_` | prefer the specific transform name when semantics are clear | defaulting to `normalize_` when a more precise prefix fits |
| `validate_` | strong validation; metadata-only or existence-level checks are acceptable | full payload parsing or materialization that belongs in `read_` or `scan_` |
| `summarize_` / `report_` | `summarize_` reduces source data into an overview; `report_` assembles a human-facing review artifact | using `report_` as a catch-all for low-level serialization or raw aggregation |

## Type Naming

Prefer domain-first type names. Let the name answer what the object is, then
use a suffix only when a role word adds real semantic value.

- Prefer `*Spec`, `*Report`, `*Builder`, `*Buffer`, and `*Record` suffixes.
- Prefer domain words such as `Kind`, `Mode`, `State`, `Level`, or `Format`
  over mechanical `*Enum` names when they express the closed set naturally.
- Omit `Enum` entirely when the domain name is already clear enough.
- Avoid prefix forms such as `Spec*`, `Report*`, `Buffer*`, or `Record*`
  unless a generated-code or tooling constraint explicitly requires them.

## Public Method Boundaries

Public methods should primarily express protocol, lifecycle, or object-local
configuration. Domain behavior should remain module-level functions following
the prefix rules above.

- `close()`: canonical resource termination or commit method
- `run()`: main execution entry for runnable objects
- `render()`: produce display or presentation output without implicit writes
- `report()`: produce a human-oriented summary or report artifact without implicit writes
- `build()`: allowed as the terminal method on `*Builder` types
- `from_*()` / `make()`: allowed as classmethod factories
- `add_*`, `select_*`, `group()`, `command()`, `done()`, `end()`, `with_*`:
  allowed on builders, registries, and fluent configuration helpers

Prefer module-level functions over generic public methods such as:

- `save`, `load`, `export`, `dump`
- `execute`, `start`, `stop`, `finish`, `shutdown`, `dispose`
- `process`, `do`, `get`, `show`

At module scope, prefer `create_`, `generate_`, `derive_`, `plan_`, or other
semantic prefixes over `build_...`. Reserve `build()` primarily for the
terminal method on `*Builder` types.

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
  scan speed. Do not cargo-cult container prefixes onto already-clear names.
- Avoid generic names such as `obj`, `tmp`, `x`, or `value` when a concrete
  role is available.
- In Python and Rust, loop binders introduced by `for` should use an
  underscore-prefixed name such as `_sheet` or `_name`.
- In R, loop binders introduced by `for` should use a dot-prefixed name such
  as `.sheet` or `.name`.
- If a loop step needs a semantic working name, derive a new local from the
  loop binder inside the loop body instead of dropping the prefix on the
  binder itself.

## Public-facing Schema and Header Naming

| Context | Rule | Example |
| --- | --- | --- |
| Exported headers | use PascalCase | `SampleId`, `PtmFdr` |
| Standalone abbreviation token | use ALLCAPS | `ID`, `PTM`, `FDR` |
| Abbreviation inside a multi-token header | use PascalCase abbreviation segment | `SampleId`, `PtmFdr` |
| Internal storage | snake_case is acceptable internally; expose PascalCase through views, aliases, or export adapters | `sample_id` -> `SampleId` |

## Workflow

- Keep the business-critical path shallow, direct, and linearly readable.
- Do not introduce wrapper layers whose only effect is renaming or argument
  forwarding without narrowing semantics, stabilizing contracts, or reducing
  duplication.
- Keep API changes minimal and explicit.
- Add or update tests with each behavior change.
- Keep docs and examples in sync with public API.
- Prefer additive evolution; deprecate before removal.

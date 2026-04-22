# Axiomkit Naming, Product, and Architecture Audit

Date: `2026-04-08`

## Summary

This note records the current naming-governance decisions and the product /
architecture audit for `axiomkit` as a Python/R/Rust toolkit.

Two output layers matter:

- short to medium term (`6-12` months): reduce naming drift, improve
  discoverability, and make the highest-frequency APIs feel more coherent
  to repeat users
- long term (`2-3` years): evolve `axiomkit` from a collection of strong
  modules into a more explicit toolkit platform with clear stable layers and
  compatibility policy

The cross-project naming source of truth now lives in
`engineering-canon/global-defaults/references/naming.md` and includes an
explicit taxonomy for `Spec`, `Options`, `Policy`, `Patch`, `Plan`, `Layout`,
`Report`, `Record`, `Buffer`, and `Builder`.

## Naming Audit

### Validation Matrix

| Example | Current role | Verdict | Notes |
| --- | --- | --- | --- |
| `TTestContrast` | caller-authored t-test contrast declaration | keep | Domain-specific name is clearer than generic `ContrastSpec` at the current scope |
| `ContrastPlan` | normalized execution-ready contrast set | keep | `Plan` is accurate and useful |
| `WorkspaceLayoutSpec` | declared layout contract before `WorkspacePlan` | keep for now | Accurate today; reevaluate only if workspace API collapses declaration and materialization |
| `WorkspacePaths` | resolved path facts | keep | Bare domain noun is sufficient |
| `WorkspaceCheckReport` | validation result for humans/orchestration | keep | `Report` is clear |
| `XlsxWriteOptions` | top-level writer config package | keep | Better than catch-all `Spec` |
| `AutofitPolicy` | local write-behavior policy | keep | `Policy` is narrower than `Options` |
| `CellFormatPatch` | mergeable partial overlay | keep | `Patch` communicates overlay semantics directly |
| `CopyReport` / `CopyTreeReport` | copy result artifacts | short-term adjust | Python/Rust naming should converge on one public result name |
| `PathSpec` | declarative CLI validation contract | keep | Strong `Spec` use because parser materializes it later |

### Named Decisions

- `TTestContrast`: keep.
  The public type is high-frequency and user-authored, but `ContrastSpec` was
  too generic for the current scope. `TTestContrast` tells the caller exactly
  which statistical surface it belongs to, while `ContrastPlan` still carries
  the normalized execution role.

- `WorkspaceLayoutSpec`: acceptable, keep in the short term.
  The type really is a declared layout contract that is later validated and
  materialized by `WorkspacePlan`. If the workspace API later becomes simpler
  and loses that two-phase shape, reassess whether `WorkspaceLayout` becomes
  the better public name.

- `CopyReport` vs `CopyTreeReport`: this is the clearest naming drift among
  high-frequency result objects.
  Short-term direction: choose one public result name and make Python/Rust
  report objects align.

### Governance Rules Worth Enforcing

- Do not use `Spec` as a mechanical suffix for all structured types.
- Public dataclasses should express lifecycle:
  declaration, options bag, local policy, patch, plan, report, or record.
- When a public `Spec` exists, ask what materialized artifact it leads to.
  If there is no later artifact and the object is just config, `Spec` is
  usually the wrong name.
- Naming review should happen at API-boundary time, not after multiple modules
  have already diverged.

## High-Frequency User View

### Main Advantages

- The best APIs are task-oriented rather than framework-shaped.
  `copy_tree`, `XlsxWriter`, `read_fasta`, and the stats functions solve real
  workflow problems directly.

- Rust kernel plus thin Python facade is a strong design choice.
  Users can stay in Python while still getting performance-sensitive I/O and a
  clean bridge boundary.

- Optional dependency handling is disciplined.
  Lazy imports and explicit feature error messages keep installation and usage
  boundaries understandable.

- The stats surface is already close to how users think about the job.
  A high-frequency user can express contrasts, run t-tests / ANOVA, and keep
  data in Polars without extra glue abstractions.

### Main Weaknesses

- Naming still feels module-local instead of toolkit-wide.
  The recent `Options` / `Policy` / `Patch` work improved `xlsx`, but similar
  semantics are not yet consistently expressed across `fs`, `workspace`,
  `stats`, and parser utilities.

- Top-level discoverability is weak.
  `axiomkit.__init__` exposes aliases such as `io_xlsx` and `io_fs`, but a new
  or even repeat user still lacks a capability map for “what is stable and what
  should I reach for first”.

- Public facade styles are uneven.
  Some modules export one obvious task function, some expose richer model types,
  and some rely more heavily on submodule knowledge than README-level guidance.

- Documentation concentrates examples in only a few places.
  A high-frequency user can become productive once they know the module, but
  the shortest path to that understanding is still too inconsistent.

### Short-Term Product Optimizations (`6-12` months)

- Publish one capability-oriented docs page:
  “copy files”, “write xlsx”, “read fasta”, “sink parquet”, “run stats”.

- Normalize public export style across high-frequency modules:
  one stable task entrypoint, clearly named supporting types, and consistent
  report/result objects.

- Add “shortest path” examples for each high-frequency API.
  Users should not need to read implementation modules to get started.

- Resolve obvious naming drift where the same role has multiple names across
  languages or modules, especially report/result objects.

## Top-Level Engineering Critique

### Current Structural Weaknesses

- `axiomkit` contains several strong subsystems, but the product identity is
  still “good modules collected together” more than “one toolkit with a unified
  surface”.

- Cross-language bridge contracts exist, but governance around them is not yet
  fully systematized.
  ABI / contract versioning exists; naming review, migration discipline, and
  public-type taxonomy are newer and not yet uniformly embedded.

- Stable-layer boundaries are implied more often than they are documented.
  The repo philosophy prefers Rust core plus thin Python facade, but users still
  need a clearer statement of which surfaces are intended to be stable public
  contracts versus lower-level implementation seams.

- Some modules are mature enough for wider reuse; others still read more like
  personal toolkit slices.
  That is acceptable for a personal toolkit, but it creates ambiguity for
  future maintainers and external adopters.

### Long-Term Evolution Direction (`2-3` years)

- Evolve from “module collection” to “toolkit platform”.
  Make capability layer, contract layer, and workflow layer explicit.

- Define stable public contracts per layer.
  This includes bridge-exposed types, public dataclasses, CLI schemas, and
  migration-note expectations for breaking changes.

- Unify result/report semantics across Python and Rust facades.
  Result objects should feel like one design language, not language-specific
  translations with drift.

- Establish architecture review gates for API work:
  naming review, compatibility review, migration note review, contract tests,
  and benchmark/context review where performance claims matter.

- Keep the core path shallow.
  Avoid wrapper layers that only rename or forward. Every new layer should earn
  its place through semantic narrowing, compatibility stabilization, or real
  reuse value.

## Action Roadmap

### Preserve

- Keep `TTestContrast` / `ContrastPlan`.
- Keep `WorkspaceLayoutSpec` / `WorkspacePlan` for now.
- Keep the Rust-core plus Python-facade direction.
- Keep optional dependency gating and lazy import behavior.

### Short-Term Adjust

- Align report/result naming across `fs` and other high-frequency modules.
- Add a capability-oriented docs entry page and link it from the README.
- Normalize public export shape for high-frequency modules.
- Apply the new naming taxonomy when any public type is touched.

### Long-Term Evolve

- Introduce an explicit stable/experimental/internal surface policy.
- Standardize migration notes for all public breaking changes.
- Standardize bridge-exposed type naming and contract review.
- Revisit module topology so users navigate by capability first, implementation
  second.

## Notes for Future Reviews

- If a future rename is proposed, first classify the type:
  declaration, options, policy, patch, plan, report, record, buffer, builder,
  or bare domain noun.
- If two names are plausible, prefer the one that tells a high-frequency user
  how the object should be used in a workflow.
- If a rename changes public semantics only marginally, do not churn the API
  for cosmetic uniformity alone.

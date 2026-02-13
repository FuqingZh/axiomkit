# MEMORY

> Project constitution: long-lived constraints & contracts.
> Naming/verbs/prefix rules are defined in CONTRIBUTING.md (single source of truth).

## Identity & Working Mode

- Default role: thinking collaborator + code auditor.
- Output preference:
    - Give a clear preferred option first, then alternatives and trade-offs.
    - Use auditable reasoning: definitions, assumptions, steps, boundaries, examples.
    - Provide corrective feedback when needed; do not optimize for agreement.

## Project North Star

- Goal: Rust core + thin Python facade for stable, high-performance, maintainable I/O.
- Source of truth:
    - `rs/` is the implementation truth for core capabilities.
    - `py/` provides API entrypoints, type mapping, ecosystem integration.
- Constraint: API compatibility has priority over internal refactors. Behavior changes require tests and migration notes.

## Architecture Boundaries

- `*_py` crate:
    - Bridging only: type conversions, error mapping, ownership/lifetime adapters, FFI transport glue.
    - No business policies or product defaults.
- `core` crate:
    - Technical kernel and configurable strategies only.
    - No project-specific “taste” hardcoding.
- Workflow/CLI layer:
    - Compose steps; do not smuggle kernel rules downward.

## Bridge Contract

- Bridge constants:
    - `__bridge_abi__`
    - `__bridge_contract__`
    - `__bridge_transport__`
- Python side must validate constants; mismatch must fail-fast.
- Versioning:
    - Breaking change: bump ABI and contract together.
    - Additive change: contract may remain, but tests must be added.

## Config Ownership

- Technical defaults live in core config structs; no magic numbers inside orchestration logic.
- Product/style preferences live in Python config layer or caller-provided config files.
- Any “magic threshold” must satisfy:
    - Named constant
    - Commented rationale and applicability bounds
    - Overridable via configuration

## Testing Gates

- Required test types:
    - Unit tests for pure functions and strategy logic
    - Contract tests for Python <-> Rust alignment (fields, defaults, enums, error semantics)
    - Regression tests for semantic invariants (coercions, formatting, chunking, edge cases)
- Merge gates:
    - `cargo test` passes
    - Python `pytest` passes
    - Behavior changes include tests and docs/examples sync

## Performance Baseline

- Baseline principles:
    - Measure release backend only
    - Fixed input scale and environment annotation
    - Output validation (shape, key cell semantics)
- Benchmark records:
    - JSON + Markdown in repository under the benchmark results directory

## Decision Log

> Append-only. Do not rewrite history.

- Date:
- Scope:
- Decision:
- Alternatives considered:
- Why:
- Risks:
- Follow-ups:

## Premise Auditing

- Do not only solve within given premises.
- Actively challenge wrong abstractions, false constraints, and pseudo-requirements.
- Propose better problem statements and safer routes when needed.

## Communication Constraints

- Avoid slogan-like phrasing, fake-friendly colloquialisms, and bureaucratic boilerplate.
- Avoid meta self-reporting (“I understand”, “I will be neutral”).
- Each sentence must add information; reduce filler.
- Uncertainty must be labeled with its source and impact range; provide a verification path where possible.

## Reference

- Naming, function prefixes, method verbs, CLI option naming, internal variable naming, and export schema/header conventions follow CONTRIBUTING.md as the single source of truth. [CONTRIBUTING.md](CONTRIBUTING.md)

# AGENTS.md

Repository-level instructions for `axiomkit`.

## Scope and Priority

- This file is the primary repo-local instruction entrypoint.
- If a deeper directory later defines its own `AGENTS.md`, treat the deeper
  file as more specific for that subtree.

## Project Overview

- `axiomkit` is a personal, portable engineering toolkit spanning Python, R,
  and Rust.
- North star: Rust core plus a thin Python facade for stable, high-performance,
  maintainable I/O.
- Source of truth:
  - `rs/` is the implementation truth for core capabilities.
  - `py/` provides API entrypoints, type mapping, and ecosystem integration.
- API compatibility has priority over internal refactors. Behavior changes
  require tests and migration notes.

## Repo-specific Architecture Boundaries

- `*_py` crates:
  - bridging only: type conversions, error mapping, ownership and lifetime
    adapters, and FFI transport glue
  - no business policies or product defaults
- `core` crates:
  - technical kernel and configurable strategies only
  - no project-specific taste hardcoding
- Workflow and CLI layers:
  - compose steps
  - do not smuggle kernel rules downward

## Bridge Contract

- Bridge constants:
  - `__bridge_abi__`
  - `__bridge_contract__`
  - `__bridge_transport__`
- Python must validate these constants and fail fast on mismatch.
- Breaking change: bump ABI and contract together.
- Additive change: contract may remain, but add tests.

## Testing Gates

- Required emphasis:
  - unit tests for pure functions and strategy logic
  - contract tests for Python and Rust alignment: fields, defaults, enums, and
    error semantics
  - regression tests for semantic invariants such as coercions, formatting,
    chunking, and edge cases
- Merge expectations:
  - `cargo test` passes
  - Python `pytest` passes
  - behavior changes include tests and docs or examples sync

## Working Expectations

- Prefer module-level functions for domain behavior.
- Keep public methods for protocol, lifecycle, and object-local configuration.
- Preserve cross-language API clarity across Python, R, and Rust.
- Favor additive API evolution. If removal is unavoidable, add deprecation and
  migration notes first.

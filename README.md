# axiomkit

Personal, portable engineering toolkit (Python/R/Rust).

## Install

- `pip install axiomkit`

## Development

- `pdm sync -G dev --no-self`
- GitHub Actions uses a CI-only manylinux image with preinstalled Rust to speed wheel builds; local development does not depend on Docker or GHCR.

## Docs

- Repository instructions: [AGENTS.md](AGENTS.md)
- Python release checklist: [RELEASE.md](RELEASE.md)
- R package: [r/README.md](r/README.md)
- Rust workspace: [crates/](crates/)

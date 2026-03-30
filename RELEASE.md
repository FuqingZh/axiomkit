# Python Release Checklist

This checklist targets publishing the repo-root Python package as `axiomkit` to PyPI/TestPyPI.

## Release Contract

- Official Linux baseline is `glibc >= 2.28`.
- Official binary coverage is Linux only for now.
- Every release must publish:
  - `sdist`
  - `cp310-abi3-manylinux_2_28_x86_64.whl`
  - `cp310-abi3-manylinux_2_28_aarch64.whl`
- Installers should prefer a compatible wheel and fall back to `sdist` when no wheel matches.
- `sdist` fallback requires a local Rust toolchain and native build environment on the user machine.

## 1. Package Metadata

- `pyproject.toml` has valid:
  - `project.name`
  - `tool.maturin`
  - `project.description`
  - `project.requires-python`
  - `project.readme`
  - `project.license`
- Runtime dependencies are declared in `project.dependencies` (full install by default).

## 2. Versioning Notes

- Official tagged releases take the version from the Git tag.
- The publish workflow syncs that tag version into `crates/axiomkit_py/Cargo.toml` and `Cargo.lock` before building artifacts.
- Local non-tag builds still use the checked-in `crates/axiomkit_py/Cargo.toml` package version.
- Publish tags must be canonical PEP 440 and match the built distribution version.
- The release workflow validates tag format before publishing.

## 3. Validation Strategy

From repo root:

```bash
pdm sync -G dev --no-self
pdm run uv --version
pdm run ruff check python tests scripts
pdm run pyright python
```

For local smoke validation from repo root:

```bash
pdm run maturin build --release --interpreter python3.13 --out dist
pdm run maturin sdist --out dist
# Linux only: repair wheel platform tags to manylinux
# Requires patchelf on PATH (e.g. apt install patchelf)
python3 -m pip install -U auditwheel
python3 -m auditwheel repair dist/axiomkit-*.whl -w dist-repaired
rm -f dist/axiomkit-*.whl && mv dist-repaired/*.whl dist/
pdm run python scripts/validate_wheel.py --dist-dir dist --require-sdist --expected-manylinux-tag any
pdm run python scripts/run_package_qa.py --dist-dir dist --tests-dir tests
```

Official release artifacts should come from `.github/workflows/publish.yml`, which builds the Linux wheels inside `manylinux_2_28` containers and validates the aggregated `dist/` bundle before publishing.

## 4. Build Artifacts

Build command:

```bash
pdm run maturin build --release --interpreter python3.13 --out dist
pdm run maturin sdist --out dist
```

Expected output in `dist/`:

- `axiomkit-<version>.tar.gz`
- `axiomkit-<version>-cp310-abi3-manylinux_2_28_x86_64.whl`
- `axiomkit-<version>-cp310-abi3-manylinux_2_28_aarch64.whl`

## 5. Publish Credentials

Set one of:

- `PDM_PUBLISH_USERNAME` + `PDM_PUBLISH_PASSWORD` (token as password), or
- trusted publishing in CI.

For TestPyPI, repository can be:

- `https://test.pypi.org/legacy/`

## 6. Publish

TestPyPI first:

```bash
./scripts/release_pypi.sh --repository testpypi
```

Production PyPI:

```bash
./scripts/release_pypi.sh --repository pypi
```

Notes:

- The local script is a helper for smoke validation and manual publishing assistance.
- Official distributable Linux wheels should be the CI-built manylinux artifacts, not ad hoc local builds.

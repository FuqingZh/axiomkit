# Python Release Checklist

This checklist targets publishing `py/` as `axiomkit` to PyPI/TestPyPI.

## 1. Package Metadata

- `pyproject.toml` has valid:
  - `project.name`
  - `tool.setuptools_scm`
  - `project.description`
  - `project.requires-python`
  - `project.readme`
  - `project.license`
- Optional extras are defined and documented (`cli`, `parquet`, `xlsx`, `fasta`, `stats`, `all`).

## 2. Versioning Notes

- Version is derived from Git tags/SCM metadata.
- Publish tags must be canonical PEP 440 (e.g. `0.0.27`, not `v0.0.27`).
- The release workflow validates tag format before building.

## 3. Validation Strategy

From `py/`:

```bash
pdm sync -G dev --no-self
pdm run uv --version
pdm run ruff check src tests scripts
pdm run pyright src
```

Then run package-level QA against the built wheel (isolated venv):

```bash
pdm run python -m build --wheel --installer uv
pdm run python scripts/validate_wheel.py --dist-dir dist
pdm run python scripts/run_package_qa.py --dist-dir dist --tests-dir tests
```

## 4. Build Artifacts

Build command:

```bash
pdm run python -m build --wheel --installer uv
```

Expected output in `dist/`:

- `axiomkit-<version>-<platform>.whl` (non-`py3-none-any`, includes Rust extensions)

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

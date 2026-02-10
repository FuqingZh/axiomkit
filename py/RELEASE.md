# Python Release Checklist

This checklist targets publishing `py/` as `axiomkit` to PyPI/TestPyPI.

## 1. Package Metadata

- `pyproject.toml` has valid:
  - `project.name`
  - `project.version`
  - `project.description`
  - `project.requires-python`
  - `project.readme`
  - `project.license`
- Optional extras are defined and documented (`cli`, `parquet`, `xlsx`, `fasta`, `stats`, `all`).

## 2. Versioning Notes

- Current version must not already exist on target index.
- If you prefer `0.0.000`, note:
  - PEP 440 canonicalizes it to `0.0.0`.
  - On indexes, `0.0.000` and `0.0.0` are effectively the same release version.
  - Use monotonically increasing versions for each publish.

## 3. Local Validation

From `py/`:

```bash
pdm sync -G dev --no-self
pdm run ruff check src tests
pdm run pytest -q tests
pdm run pyright src
```

## 4. Build Artifacts

```bash
pdm build
```

Expected outputs in `dist/`:

- `axiomkit-<version>-py3-none-any.whl`
- `axiomkit-<version>.tar.gz`

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

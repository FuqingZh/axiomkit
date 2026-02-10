# axiomkit (Python)

## Install

- Core:
  - `pip install axiomkit`
- All features:
  - `pip install "axiomkit[all]"`

## Optional Extras

- `cli`: Rich CLI formatter and headings (`rich`, `rich-argparse`)
- `parquet`: Parquet writer (`polars`)
- `xlsx`: XLSX writer (`polars`, `xlsxwriter`)
- `fasta`: FASTA reader (`biopython`, `pyteomics`, `polars`)
- `stats`: ORA/statistics (`numpy`, `scipy`, `polars`)

When an optional feature is used without required dependencies,
axiomkit raises a `ModuleNotFoundError` with an install hint, e.g.:

- `pip install "axiomkit[xlsx]"`

## Development

- Full dev environment:
  - `pdm sync -G dev --no-self`

## Release

- Release checklist: `RELEASE.md`
- One-shot publish script: `scripts/release_pypi.sh`

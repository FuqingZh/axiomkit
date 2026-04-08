# axiomkit

Personal, portable engineering toolkit (Python/R/Rust).

## Install

- `pip install axiomkit`

## Stats

```python
import polars as pl
from axiomkit.stats import (
    ContrastSpec,
    calculate_anova_one_way,
    calculate_anova_one_way_welch,
    calculate_anova_two_way,
    calculate_t_test_one_sample,
    calculate_t_test_paired,
    calculate_t_test_two_sample,
)

df_values = pl.DataFrame(
    {
        "FeatureId": ["f1", "f1", "f1", "f1", "f2", "f2", "f2", "f2"],
        "Group": ["ctrl", "ctrl", "case", "case", "ctrl", "ctrl", "case", "case"],
        "Value": [1.0, 2.0, 4.0, 5.0, 4.0, 4.0, 6.0, 8.0],
    }
)

df_t = calculate_t_test_two_sample(
    df_values,
    col_feature="FeatureId",
    contrasts=[ContrastSpec(group_test="case", group_ref="ctrl")],
    rule_p_adjust="bh",
)

df_one_sample = calculate_t_test_one_sample(
    df_values.select("FeatureId", "Value"),
    col_feature="FeatureId",
    popmean=3.0,
)

df_paired = calculate_t_test_paired(
    pl.DataFrame(
        {
            "FeatureId": ["f1", "f1", "f1", "f1"],
            "PairId": ["p1", "p1", "p2", "p2"],
            "Group": ["ctrl", "case", "ctrl", "case"],
            "Value": [1.0, 2.0, 4.0, 6.0],
        }
    ),
    col_feature="FeatureId",
    col_pair="PairId",
    contrasts=ContrastSpec(group_test="case", group_ref="ctrl"),
)

df_anova = calculate_anova_one_way(
    df_values,
    col_feature="FeatureId",
    rule_p_adjust="bh",
)

df_anova_welch = calculate_anova_one_way_welch(
    df_values,
    col_feature="FeatureId",
    rule_p_adjust="bh",
)

df_anova_two_way = calculate_anova_two_way(
    pl.DataFrame(
        {
            "FeatureId": ["f1"] * 8,
            "GroupA": ["A1", "A1", "A1", "A1", "A2", "A2", "A2", "A2"],
            "GroupB": ["B1", "B1", "B2", "B2", "B1", "B1", "B2", "B2"],
            "Value": [8.0, 10.0, 6.0, 8.0, 4.0, 5.0, 3.0, 6.0],
        }
    ),
    col_feature="FeatureId",
    col_group_a="GroupA",
    col_group_b="GroupB",
)
```

## Development

- `pdm sync -G dev --no-self`
- GitHub Actions uses a CI-only manylinux image with preinstalled Rust to speed wheel builds; local development does not depend on Docker or GHCR.
- The Linux ARM64 wheel is built as a supplemental artifact on a native GitHub ARM runner; local development paths remain unchanged.

## Docs

- Repository instructions: [AGENTS.md](AGENTS.md)
- Naming / product / architecture audit: [docs/governance/20260408-naming-product-architecture-audit.md](docs/governance/20260408-naming-product-architecture-audit.md)
- Python release checklist: [RELEASE.md](RELEASE.md)
- R package: [r/README.md](r/README.md)
- Rust workspace: [crates/](crates/)

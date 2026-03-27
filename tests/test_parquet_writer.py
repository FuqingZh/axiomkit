from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from axiomkit.io.parquet.writer import sink_parquet_dataset


def test_sink_parquet_dataset_accepts_should_overwrite(tmp_path: Path) -> None:
    dir_out = tmp_path / "dataset"
    dir_out.mkdir()
    (dir_out / "stale.txt").write_text("stale", encoding="utf-8")

    df = pl.DataFrame({"sample": ["s1", "s2"], "value": [1.0, 2.0]})

    sink_parquet_dataset(
        df,
        dir_out,
        should_overwrite=True,
    )

    assert dir_out.exists()
    assert not (dir_out / "stale.txt").exists()
    assert any(path_file.suffix == ".parquet" for path_file in dir_out.rglob("*.parquet"))


def test_sink_parquet_dataset_blocks_nonempty_dir_without_overwrite(
    tmp_path: Path,
) -> None:
    dir_out = tmp_path / "dataset"
    dir_out.mkdir()
    (dir_out / "stale.txt").write_text("stale", encoding="utf-8")

    df = pl.DataFrame({"sample": ["s1"], "value": [1.0]})

    with pytest.raises(FileExistsError, match="should_overwrite"):
        sink_parquet_dataset(df, dir_out)

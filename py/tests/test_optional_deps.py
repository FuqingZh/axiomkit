from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

# Ensure src-layout imports work when running tests from repo checkout.
SRC_DIR = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_DIR))

from axiomkit._optional_deps import import_optional_module  # noqa: E402


def test_optional_import_error_contains_install_hint() -> None:
    with pytest.raises(ModuleNotFoundError) as exc_info:
        import_optional_module(
            module_name=".missing_feature_module",
            package="axiomkit",
            feature="axiomkit.io.xlsx",
            extras=("xlsx",),
            required_modules=("missing_feature_module",),
        )

    message = str(exc_info.value)
    assert "axiomkit.io.xlsx is unavailable" in message
    assert re.search(r'pip install "axiomkit\[xlsx\]"', message)
    assert "pdm sync -G dev -G xlsx" in message

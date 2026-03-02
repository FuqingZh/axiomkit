from __future__ import annotations

import os
import sys
from pathlib import Path


def _configure_import_mode() -> None:
    mode = os.environ.get("AXIOMKIT_TEST_IMPORT_MODE", "src")
    if mode not in {"src", "wheel"}:
        raise RuntimeError(
            "AXIOMKIT_TEST_IMPORT_MODE must be one of {'src', 'wheel'}, "
            f"got {mode!r}."
        )

    if mode == "src":
        src_dir = Path(__file__).resolve().parents[1] / "src"
        c_src_dir = str(src_dir)
        if c_src_dir not in sys.path:
            sys.path.insert(0, c_src_dir)


_configure_import_mode()

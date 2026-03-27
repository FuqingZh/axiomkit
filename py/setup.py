from __future__ import annotations

import shutil
from pathlib import Path

from setuptools import setup
from setuptools.command.sdist import sdist as _sdist
from setuptools_rust import Binding, RustExtension


PATH_PY_ROOT = Path(__file__).resolve().parent
PATH_RS_REPO_ROOT = PATH_PY_ROOT.parent / "rs"
PATH_RS_SDIST_ROOT = PATH_PY_ROOT / "rs"
RS_SDIST_PATHS = (
    Path("rs/Cargo.toml"),
    Path("rs/Cargo.lock"),
    Path("rs/crates/axiomkit_io_fs"),
    Path("rs/crates/axiomkit_io_fs_py"),
    Path("rs/crates/axiomkit_io_xlsx"),
    Path("rs/crates/axiomkit_io_xlsx_py"),
)


def derive_rs_root() -> Path:
    if PATH_RS_SDIST_ROOT.exists():
        return PATH_RS_SDIST_ROOT
    return PATH_RS_REPO_ROOT


def derive_rust_manifest_path(path_relative: str) -> str:
    return str(derive_rs_root() / path_relative)


class SdistWithRustSources(_sdist):
    def make_release_tree(self, base_dir: str, files: list[str]) -> None:
        super().make_release_tree(base_dir, files)

        path_release_root = Path(base_dir)
        path_repo_root = PATH_PY_ROOT.parent

        for path_relative in RS_SDIST_PATHS:
            path_source = path_repo_root / path_relative
            path_target = path_release_root / path_relative
            if path_source.is_dir():
                shutil.copytree(path_source, path_target, dirs_exist_ok=True)
            else:
                path_target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path_source, path_target)


setup(
    rust_extensions=[
        RustExtension(
            "axiomkit.io.fs._axiomkit_io_fs_rs",
            path=derive_rust_manifest_path("crates/axiomkit_io_fs_py/Cargo.toml"),
            binding=Binding.PyO3,
            py_limited_api=True,
        ),
        RustExtension(
            "axiomkit.io.xlsx._axiomkit_io_xlsx_rs",
            path=derive_rust_manifest_path("crates/axiomkit_io_xlsx_py/Cargo.toml"),
            binding=Binding.PyO3,
            py_limited_api=True,
        ),
    ],
    cmdclass={"sdist": SdistWithRustSources},
    options={"bdist_wheel": {"py_limited_api": "cp310"}},
    zip_safe=False,
)

from __future__ import annotations

from setuptools import setup
from setuptools_rust import Binding, RustExtension


setup(
    rust_extensions=[
        RustExtension(
            "axiomkit.io.fs._axiomkit_io_fs_rs",
            path="../rs/crates/axiomkit_io_fs_py/Cargo.toml",
            binding=Binding.PyO3,
            py_limited_api=True,
        ),
        RustExtension(
            "axiomkit.io.xlsx._axiomkit_io_xlsx_rs",
            path="../rs/crates/axiomkit_io_xlsx_py/Cargo.toml",
            binding=Binding.PyO3,
            py_limited_api=True,
        ),
    ],
    options={"bdist_wheel": {"py_limited_api": "cp310"}},
    zip_safe=False,
)

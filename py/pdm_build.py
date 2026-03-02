from __future__ import annotations

from setuptools_rust import Binding, RustExtension


def pdm_build_update_setup_kwargs(context, setup_kwargs) -> None:
    rust_extensions = list(setup_kwargs.get("rust_extensions", []))
    rust_extensions.extend(
        [
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
        ]
    )
    setup_kwargs["rust_extensions"] = rust_extensions
    setup_kwargs["zip_safe"] = False

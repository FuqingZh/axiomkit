use _axiomkit_io_fs_rs::{
    BRIDGE_ABI_VERSION as FS_BRIDGE_ABI_VERSION,
    BRIDGE_CONTRACT_VERSION as FS_BRIDGE_CONTRACT_VERSION,
    BRIDGE_TRANSPORT as FS_BRIDGE_TRANSPORT, register_fs_bindings,
};
use _axiomkit_io_xlsx_rs::{
    BRIDGE_ABI_VERSION as XLSX_BRIDGE_ABI_VERSION,
    BRIDGE_CONTRACT_VERSION as XLSX_BRIDGE_CONTRACT_VERSION,
    BRIDGE_TRANSPORT as XLSX_BRIDGE_TRANSPORT, register_xlsx_bindings,
};
use pyo3::prelude::*;

#[pymodule]
fn _axiomkit_rs(module: &Bound<'_, PyModule>) -> PyResult<()> {
    register_fs_bindings(module)?;
    register_xlsx_bindings(module)?;

    module.add("__bridge_fs_abi__", FS_BRIDGE_ABI_VERSION)?;
    module.add("__bridge_fs_contract__", FS_BRIDGE_CONTRACT_VERSION)?;
    module.add("__bridge_fs_transport__", FS_BRIDGE_TRANSPORT)?;

    module.add("__bridge_xlsx_abi__", XLSX_BRIDGE_ABI_VERSION)?;
    module.add("__bridge_xlsx_contract__", XLSX_BRIDGE_CONTRACT_VERSION)?;
    module.add("__bridge_xlsx_transport__", XLSX_BRIDGE_TRANSPORT)?;

    Ok(())
}

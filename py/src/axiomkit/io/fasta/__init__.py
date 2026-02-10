try:
    from .reader import SpecFastaHeader, calculate_mw_kda, read_fasta
except ModuleNotFoundError as e:
    if e.name == "Bio":
        raise ModuleNotFoundError(
            "Optional dependency missing: 'biopython'. "
            "Install it to use axiomkit.io.fasta (e.g. `pip install biopython`)."
        ) from e
    raise

__all__ = ["SpecFastaHeader", "read_fasta", "calculate_mw_kda"]

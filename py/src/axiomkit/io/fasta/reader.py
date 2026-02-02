import os
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from secrets import token_hex
from tempfile import TemporaryDirectory
from typing import Any, Literal, cast

import polars as pl
from Bio import SeqIO
from Bio.SeqRecord import SeqRecord
from Bio.SeqUtils import molecular_weight
from loguru import logger
from pyteomics import fasta as pt_fasta

################################################################################
# #region Constants
############################################################
# #region HeaderParsing
# 以 " <name> OS=... OX=... GN=... " 这类为典型
_RE_TAIL = r"(?:\s+(?P<name>.*?))?(?:\s+(?:OS=|OX=|GN=|PE=|SV=).*)?\s*$"

# sp|P69556.2|PSBA_TOBAC / tr|A0A...|A0A..._MOUSE
_RE_UNIPROT = re.compile(r"^(?P<db>sp|tr)\|(?P<id>[^|]+)\|(?P<symbol>[^|]+)" + _RE_TAIL)
# gi|<gi>|sp|<acc>|<entry>
_RE_NCBI_GI_SPTR = re.compile(
    r"^gi\|[^|]+\|(?:sp|tr)\|(?P<id>[^|]+)\|(?P<symbol>[^|]+)\|?" + _RE_TAIL
)
# gi|<gi>|ref|<acc>
_RE_NCBI_GI_REF = re.compile(r"^gi\|[^|]+\|ref\|(?P<id>[^|]+)\|?" + _RE_TAIL)
# gi|<gi>
_RE_NCBI_GI_ONLY = re.compile(r"^gi\|(?P<id>[^|]+)$")
# IPI:<id>.<version>
_RE_IPI = re.compile(r"^IPI:(?P<id>\S+?)\.\d+$")
# gnl|<db>|<id>
_RE_GNL = re.compile(r"^gnl\|[^|]+\|(?P<id>[^|]+)" + _RE_TAIL)
# others
_RE_OTHERS = re.compile(
    r"^(?:lcl|bbs|bbm|gim|gb|emb|pir|ref|dbj|prf|pdb|tpg|tpe|tpd|gpp|nat)\|"
    r"(?P<id>[^|]+)"
    r"(?:\|(?P<symbol>[^|]+))?" + _RE_TAIL
)

# #endregion
############################################################
# #region Gene&AminoAcid
_RE_GENE = re.compile(r"(?:GN|Gene_Symbol)=([^\s]+)")
_RE_INVALID_AA = re.compile(r"[^ACDEFGHIKLMNPQRSTVWYUO]")
_AA_TRANSLATION_TABLE = str.maketrans(
    {
        # Common ambiguous/placeholder letters.
        "B": "D",  # Aspartic acid / Asparagine -> choose D
        "Z": "E",  # Glutamic acid / Glutamine -> choose E
        "J": "L",  # Leucine / Isoleucine -> choose L
        # Drop unknowns, stops, and gaps.
        "X": None,
        "*": None,
        "-": None,
    }
)

# #endregion
############################################################
# #region PyteomicsKeyPriorities
_PT_ID_KEYS: tuple[str, ...] = (
    "accession",
    "Accession",
    "id",
    "ID",
    "primary_accession",
    "PrimaryAccession",
    "entry_id",
    "EntryID",
)
_PT_SYMBOL_KEYS: tuple[str, ...] = (
    "entry",
    "Entry",
    "protein",
    "Protein",
)
_PT_NAME_KEYS: tuple[str, ...] = (
    "name",
    "Name",
    "description",
    "Description",
    "full_name",
    "FullName",
    "protein_name",
    "ProteinName",
)
# #endregion
############################################################
# #endregion
################################################################################
# #region Structs


@dataclass(frozen=True, slots=True)
class SpecFastaHeader:
    id: str
    symbol: str
    name: str = ""


@dataclass(frozen=True, slots=True)
class SpecHeaderParsingRule:
    """
    private header rule, e.g.
    pattern = re.compile(r"^CUSTOM\\|(?P<id>[^|]+)\\|(?P<symbol>[^|]+)$")
    """

    pattern: re.Pattern[str]
    id_group: str = "id"
    symbol_group: str = "symbol"
    name_group: str = "name"


@dataclass(frozen=True, slots=True)
class SpecMwResult:
    mw_kda: float | None
    cnt_sanitized_chars: int
    cnt_replaced_chars: int
    is_empty_after_sanitize: bool


L_FALLBACK_HEADER_PARSING_RULES: list[SpecHeaderParsingRule] = [
    SpecHeaderParsingRule(pattern=_RE_UNIPROT),
    SpecHeaderParsingRule(pattern=_RE_NCBI_GI_REF),
    SpecHeaderParsingRule(pattern=_RE_NCBI_GI_SPTR),
    SpecHeaderParsingRule(pattern=_RE_NCBI_GI_ONLY, symbol_group=""),
    SpecHeaderParsingRule(pattern=_RE_IPI),
    SpecHeaderParsingRule(pattern=_RE_GNL),
    SpecHeaderParsingRule(pattern=_RE_OTHERS),
]
# #endregion
################################################################################
# #region FastaParsing


# #tag FastaHeader
def _extract_header_info(header: dict[str, Any]) -> SpecFastaHeader:
    """
    Best-effort mapping from Pyteomics parse() dict to (ID, Name).

    Notes:
    - parse() doc says: returned dict keys depend on 'flavor'.
    - We therefore try a list of common keys in priority order.
    """
    c_id = ""
    for _key in _PT_ID_KEYS:
        obj_value = header.get(_key)
        if isinstance(obj_value, str) and obj_value.strip():
            c_id = obj_value.strip()
            break

    c_symbol = ""
    for _key in _PT_SYMBOL_KEYS:
        obj_value = header.get(_key)
        if isinstance(obj_value, str) and obj_value.strip():
            c_symbol = obj_value.strip()
            break

    c_name = ""
    for _key in _PT_NAME_KEYS:
        obj_value = header.get(_key)
        if isinstance(obj_value, str) and obj_value.strip():
            c_name = obj_value.strip()
            break

    return SpecFastaHeader(id=c_id, symbol=c_symbol, name=c_name)


def _parse_header_with_rules(
    header: str, rules_fallback: Sequence[SpecHeaderParsingRule]
) -> SpecFastaHeader:
    """Parse header using fallback rules."""
    c_id = c_symbol = c_name = ""
    for _rule in rules_fallback:
        if re_match_info := _rule.pattern.search(header):
            dict_groups = re_match_info.groupdict()
            if _rule.id_group and _rule.id_group in dict_groups:
                c_id = re_match_info.group(_rule.id_group) or ""
            else:
                c_id = (
                    re_match_info.group(1)
                    if re_match_info.lastindex and re_match_info.lastindex >= 1
                    else ""
                )

            if _rule.symbol_group and _rule.symbol_group in dict_groups:
                c_symbol = re_match_info.group(_rule.symbol_group) or ""
                # Symbols should be compact tokens; drop trailing description if present.
                if " " in c_symbol:
                    c_symbol = c_symbol.split()[0]

            if _rule.name_group and _rule.name_group in dict_groups:
                c_name = re_match_info.group(_rule.name_group) or ""

            # First match wins: avoid extra regex work and accidental overrides.
            break

    return SpecFastaHeader(id=c_id, symbol=c_symbol, name=c_name)


def parse_fasta_header(
    header: str | None,
    rules_fallback: Sequence[SpecHeaderParsingRule],
) -> SpecFastaHeader:
    """
    Parse a FASTA header with Pyteomics priority, and private-rule backfill.
    """
    if header is None:
        return SpecFastaHeader(id="", symbol="", name="")

    header = header.strip()
    # 1) Pyteomics first.
    spec_fasta_header_parsed = SpecFastaHeader(id="", symbol="", name="")
    try:
        dict_fasta_header = cast(dict[str, Any], pt_fasta.parse(header, flavor="auto"))
        spec_fasta_header_parsed = _extract_header_info(dict_fasta_header)
    except Exception as e:
        logger.warning(
            "pt_fasta.parse failed: header={}, err={}",
            header[:200],
            e,
        )

    # 2) Only if Pyteomics info is incomplete, run private rules to backfill.
    # Treat name as optional to avoid running private rules on most headers.
    b_requires_private_rules = not (
        spec_fasta_header_parsed.id and spec_fasta_header_parsed.symbol
    )
    if b_requires_private_rules and rules_fallback:
        spec_fallback_header_parsed = _parse_header_with_rules(header, rules_fallback)
    else:
        spec_fallback_header_parsed = SpecFastaHeader(id="", symbol="", name="")

    # 3) Merge with Pyteomics priority.
    if (
        spec_fasta_header_parsed.id
        or spec_fasta_header_parsed.symbol
        or spec_fasta_header_parsed.name
        or spec_fallback_header_parsed.id
        or spec_fallback_header_parsed.symbol
        or spec_fallback_header_parsed.name
    ):
        return SpecFastaHeader(
            id=spec_fasta_header_parsed.id or spec_fallback_header_parsed.id or header,
            symbol=spec_fasta_header_parsed.symbol
            or spec_fallback_header_parsed.symbol,
            name=spec_fasta_header_parsed.name or spec_fallback_header_parsed.name,
        )

    # 4) Last resort: keep original header as ID.
    return SpecFastaHeader(id=header, symbol="", name="")


# #tag Gene
def _extract_gene(description: str) -> str:
    re_gene_match = _RE_GENE.search(description or "")
    return re_gene_match.group(1) if re_gene_match else ""


# #tag Sequence
def _sanitize_protein_sequence(seq: str) -> tuple[str, int, int]:
    """
    Clean a protein sequence for molecular weight calculation.

    Strategy:
    - Upper-case.
    - Map common ambiguous letters to a concrete residue.
    - Drop unknowns/stops/gaps and any other invalid characters.
    Returns (sanitized_sequence, cnt_removed, cnt_replaced).
    """
    c_seq_upper = seq.upper()
    n_cnt_replaced_chars = sum(c_seq_upper.count(_c) for _c in ("B", "Z", "J"))
    c_seq_translated = c_seq_upper.translate(_AA_TRANSLATION_TABLE)
    c_seq_sanitized = _RE_INVALID_AA.sub("", c_seq_translated)
    n_cnt_removed_chars = len(c_seq_upper) - len(c_seq_sanitized)
    return c_seq_sanitized, n_cnt_removed_chars, n_cnt_replaced_chars


def calculate_mw_kda(seq: str) -> SpecMwResult:
    c_seq_sanitized, n_cnt_removed_chars, n_cnt_replaced_chars = (
        _sanitize_protein_sequence(seq)
    )
    if not c_seq_sanitized:
        return SpecMwResult(
            mw_kda=None,
            cnt_sanitized_chars=n_cnt_removed_chars,
            cnt_replaced_chars=n_cnt_replaced_chars,
            is_empty_after_sanitize=True,
        )
    mw_da = molecular_weight(c_seq_sanitized, seq_type="protein")
    return SpecMwResult(
        mw_kda=round(mw_da / 1000.0, 3),
        cnt_sanitized_chars=n_cnt_removed_chars,
        cnt_replaced_chars=n_cnt_replaced_chars,
        is_empty_after_sanitize=False,
    )


# #endregion
################################################################################

N_ROWS_PER_PARQUET_CHUNK = 20_000


def _write_rows_to_parquet(
    rows: list[dict[str, Any]],
    dir_tmp: Path,
    *,
    chunk_index: int,
) -> int:
    if not rows:
        return chunk_index
    pl.DataFrame(rows).write_parquet(dir_tmp / f"part-{chunk_index:06d}.parquet")
    rows.clear()
    return chunk_index + 1


def read_fasta(
    files_in: list[os.PathLike[str]] | list[str] | os.PathLike[str] | str,
    *,
    rules_fallback: Sequence[SpecHeaderParsingRule] | None | Literal[False] = None,
    if_include_sequence: bool = False,
    if_deduplicate: bool = True,
    dir_tmp: Path | str | None = None,
) -> pl.DataFrame:
    """
    Read one or more FASTA files into a Polars DataFrame with parsed headers and MW.

    Notes:
        - Header parsing uses Pyteomics (flavor="auto") first,
            then optional fallback regex rules to backfill missing ID/Symbol.
        - ProteinId is derived from parsed ID/Symbol or SeqRecord.id;
            if ``if_deduplicate`` is True, rows are de-duplicated by ProteinId (keep first).
        - Molecular weight is computed after sequence sanitization;
            if a sequence becomes empty, MWKDa is set to null.

    Args:
        files_in (list[os.PathLike[str]] | list[str] | os.PathLike[str] | str):
            One or more FASTA file paths.
        rules_fallback (Sequence[SpecHeaderParsingRule] | None | Literal[False], optional):
            Header parsing fallback rules. Use ``None`` to apply the module defaults,
            or ``False`` to disable fallbacks.
            Defaults to None.
        if_include_sequence (bool, optional):
            If True, keep the raw sequence in the output column ``Sequence``.
            It is recommended to set this to ``False`` for large datasets to save memory.
            Defaults to ``False``.
        if_deduplicate (bool, optional):
            If True, drop duplicate rows by ``ProteinId`` (keep first).
            Defaults to ``True`` to preserve existing behavior.
        dir_tmp (Path | str | None, optional):
            Directory for temporary parquet chunks.
            If None, a temporary directory is created and will be deleted automatically. 
            If specified, it is the caller's responsibility to clean up.
            Defaults to None.

    Returns:
        pl.DataFrame: A DataFrame with columns:
            - ``File``
            - ``ProteinId``
            - ``ProteinSymbol``
            - ``ProteinName``
            - ``GeneSymbol``
            - ``MWKDa``
            - ``Length``
            - ``Sequence`` (optional)

    Examples:
        >>> df = read_fasta("proteins.fasta")
        >>> df = read_fasta(["a.faa", "b.faa"], if_include_sequence=True)
    """
    if rules_fallback is None:
        rules_fallback = L_FALLBACK_HEADER_PARSING_RULES
    elif rules_fallback is False:
        rules_fallback = []

    files_in = (
        [Path(files_in)] if isinstance(files_in, (str, os.PathLike)) else files_in
    )
    l_files_in = [Path(_p) for _p in files_in]

    l_rows: list[dict[str, Any]] = []
    n_cnt_sanitized_seqs = 0
    n_cnt_sanitized_characters = 0
    n_cnt_replaced_characters = 0
    n_cnt_empty_seqs_after_sanitize = 0
    n_chunks = 0

    tmpdir_context = None
    if dir_tmp is None:
        tmpdir_context = TemporaryDirectory()
        path_dir_tmp = Path(tmpdir_context.name)
    else:
        c_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        path_dir_tmp = Path(dir_tmp) / f"fasta-{c_timestamp}-{token_hex(4)}"
        path_dir_tmp.mkdir(parents=True, exist_ok=True)

    try:
        for _file in l_files_in:
            if not _file.is_file():
                logger.warning(f"Skipping non-file: {_file}")
                continue

            for _record in SeqIO.parse(_file, "fasta"):
                _record = cast(SeqRecord, _record)
                c_sequence = str(_record.seq)
                spec_header_parsed = parse_fasta_header(
                    _record.description, rules_fallback
                )

                c_protein_id = (
                    spec_header_parsed.id or spec_header_parsed.symbol or _record.id
                )

                try:
                    spec_mw_result = calculate_mw_kda(c_sequence)
                    if (
                        spec_mw_result.cnt_sanitized_chars > 0
                        or spec_mw_result.cnt_replaced_chars > 0
                    ):
                        n_cnt_sanitized_seqs += 1
                        n_cnt_sanitized_characters += spec_mw_result.cnt_sanitized_chars
                        n_cnt_replaced_characters += spec_mw_result.cnt_replaced_chars
                    if spec_mw_result.is_empty_after_sanitize:
                        n_cnt_empty_seqs_after_sanitize += 1
                except Exception as e:
                    logger.warning(
                        "MW failed: file={}, term={}, err={}",
                        _file.name,
                        c_protein_id,
                        e,
                    )
                    spec_mw_result = SpecMwResult(
                        mw_kda=None,
                        cnt_sanitized_chars=0,
                        cnt_replaced_chars=0,
                        is_empty_after_sanitize=False,
                    )

                dict_row = {
                    "File": str(_file),
                    "ProteinId": c_protein_id,
                    "ProteinSymbol": spec_header_parsed.symbol,
                    "ProteinName": spec_header_parsed.name,
                    "GeneSymbol": _extract_gene(_record.description),
                    "MWKDa": spec_mw_result.mw_kda,
                    "Length": len(c_sequence),
                }
                if if_include_sequence:
                    dict_row["Sequence"] = c_sequence
                l_rows.append(dict_row)

                if len(l_rows) >= N_ROWS_PER_PARQUET_CHUNK:
                    n_chunks = _write_rows_to_parquet(
                        l_rows, path_dir_tmp, chunk_index=n_chunks
                    )

        n_chunks = _write_rows_to_parquet(l_rows, path_dir_tmp, chunk_index=n_chunks)

        if n_chunks == 0:
            dict_schema = {
                "File": pl.Utf8,
                "ProteinId": pl.Utf8,
                "ProteinSymbol": pl.Utf8,
                "ProteinName": pl.Utf8,
                "GeneSymbol": pl.Utf8,
                "MWKDa": pl.Float64,
                "Length": pl.Int64,
            }
            if if_include_sequence:
                dict_schema["Sequence"] = pl.Utf8
            return pl.DataFrame(schema=dict_schema)

        lf_fasta = pl.scan_parquet(path_dir_tmp)

        if if_deduplicate:
            df_dup = (
                lf_fasta.select("ProteinId")
                .group_by("ProteinId")
                .len()
                .filter(pl.col("len") > 1)
                .collect()
            )
            if df_dup.height > 0:
                l_dup = df_dup.get_column("ProteinId").to_list()
                logger.warning(
                    "Detected duplicates in ProteinId ({}): {}",
                    len(l_dup),
                    "; ".join(map(str, l_dup[:200])),
                )

            df_fasta = lf_fasta.unique(subset=["ProteinId"], keep="first").collect()
        else:
            df_fasta = lf_fasta.collect()

        if n_cnt_sanitized_seqs > 0:
            logger.warning(
                "Sanitized sequences for MW calculation: count={}, removed={}, replaced={}",
                n_cnt_sanitized_seqs,
                n_cnt_sanitized_characters,
                n_cnt_replaced_characters,
            )
        if n_cnt_empty_seqs_after_sanitize > 0:
            logger.warning(
                f"Sequences empty after sanitization (MW set to null): count={n_cnt_empty_seqs_after_sanitize}"
            )

        return df_fasta

    finally:
        if tmpdir_context is not None:
            tmpdir_context.cleanup()

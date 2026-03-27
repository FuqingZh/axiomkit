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
class FastaHeaderRecord:
    id: str
    symbol: str
    name: str = ""


@dataclass(frozen=True, slots=True)
class HeaderParsingRuleSpec:
    """
    private header rule, e.g.
    pattern = re.compile(r"^CUSTOM\\|(?P<id>[^|]+)\\|(?P<symbol>[^|]+)$")
    """

    pattern: re.Pattern[str]
    id_group: str = "id"
    symbol_group: str = "symbol"
    name_group: str = "name"


@dataclass(frozen=True, slots=True)
class MwRecord:
    mw_kda: float | None
    cnt_sanitized_chars: int
    cnt_replaced_chars: int
    is_empty_after_sanitize: bool


L_FALLBACK_HEADER_PARSING_RULES: list[HeaderParsingRuleSpec] = [
    HeaderParsingRuleSpec(pattern=_RE_UNIPROT),
    HeaderParsingRuleSpec(pattern=_RE_NCBI_GI_REF),
    HeaderParsingRuleSpec(pattern=_RE_NCBI_GI_SPTR),
    HeaderParsingRuleSpec(pattern=_RE_NCBI_GI_ONLY, symbol_group=""),
    HeaderParsingRuleSpec(pattern=_RE_IPI),
    HeaderParsingRuleSpec(pattern=_RE_GNL),
    HeaderParsingRuleSpec(pattern=_RE_OTHERS),
]
# #endregion
################################################################################
# #region FastaParsing


# #tag FastaHeader
def _extract_header_info(header: dict[str, Any]) -> FastaHeaderRecord:
    """
    Best-effort mapping from Pyteomics parse() dict to (ID, Name).

    Notes:
    - parse() doc says: returned dict keys depend on 'flavor'.
    - We therefore try a list of common keys in priority order.
    """
    id_parsed = ""
    for _key in _PT_ID_KEYS:
        obj_value = header.get(_key)
        if isinstance(obj_value, str) and obj_value.strip():
            id_parsed = obj_value.strip()
            break

    symbol_compound = ""
    for _key in _PT_SYMBOL_KEYS:
        obj_value = header.get(_key)
        if isinstance(obj_value, str) and obj_value.strip():
            symbol_compound = obj_value.strip()
            break

    name_compound = ""
    for _key in _PT_NAME_KEYS:
        obj_value = header.get(_key)
        if isinstance(obj_value, str) and obj_value.strip():
            name_compound = obj_value.strip()
            break

    return FastaHeaderRecord(id=id_parsed, symbol=symbol_compound, name=name_compound)


def _parse_header_with_rules(
    header: str, rules_fallback: Sequence[HeaderParsingRuleSpec]
) -> FastaHeaderRecord:
    """Parse header using fallback rules."""
    id_parsed = symbol_compound = name_compound = ""
    for _rule in rules_fallback:
        if re_match_info := _rule.pattern.search(header):
            groups = re_match_info.groupdict()
            if _rule.id_group and _rule.id_group in groups:
                id_parsed = re_match_info.group(_rule.id_group) or ""
            else:
                id_parsed = (
                    re_match_info.group(1)
                    if re_match_info.lastindex and re_match_info.lastindex >= 1
                    else ""
                )

            if _rule.symbol_group and _rule.symbol_group in groups:
                symbol_compound = re_match_info.group(_rule.symbol_group) or ""
                # Symbols should be compact tokens; drop trailing description if present.
                if " " in symbol_compound:
                    symbol_compound = symbol_compound.split()[0]

            if _rule.name_group and _rule.name_group in groups:
                name_compound = re_match_info.group(_rule.name_group) or ""

            # First match wins: avoid extra regex work and accidental overrides.
            break

    return FastaHeaderRecord(id=id_parsed, symbol=symbol_compound, name=name_compound)


def parse_fasta_header(
    header: str | None,
    rules_fallback: Sequence[HeaderParsingRuleSpec],
) -> FastaHeaderRecord:
    """
    Parse a FASTA header with Pyteomics priority, and private-rule backfill.
    """
    if header is None:
        return FastaHeaderRecord(id="", symbol="", name="")

    header = header.strip()
    # 1) Pyteomics first.
    spec_header_parsed = FastaHeaderRecord(id="", symbol="", name="")
    try:
        header_info = cast(dict[str, Any], pt_fasta.parse(header, flavor="auto"))
        spec_header_parsed = _extract_header_info(header_info)
    except Exception as e:
        logger.warning(
            "pt_fasta.parse failed: header={}, err={}",
            header[:200],
            e,
        )

    # 2) Only if Pyteomics info is incomplete, run private rules to backfill.
    # Treat name as optional to avoid running private rules on most headers.
    is_fallback_rules_needed = not (spec_header_parsed.id and spec_header_parsed.symbol)
    if is_fallback_rules_needed and rules_fallback:
        spec_header_fallback = _parse_header_with_rules(header, rules_fallback)
    else:
        spec_header_fallback = FastaHeaderRecord(id="", symbol="", name="")

    # 3) Merge with Pyteomics priority.
    if (
        spec_header_parsed.id
        or spec_header_parsed.symbol
        or spec_header_parsed.name
        or spec_header_fallback.id
        or spec_header_fallback.symbol
        or spec_header_fallback.name
    ):
        return FastaHeaderRecord(
            id=spec_header_parsed.id or spec_header_fallback.id or header,
            symbol=spec_header_parsed.symbol or spec_header_fallback.symbol,
            name=spec_header_parsed.name or spec_header_fallback.name,
        )

    # 4) Last resort: keep original header as ID.
    return FastaHeaderRecord(id=header, symbol="", name="")


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
    seq_upper = seq.upper()
    cnt_replaced_chars = sum(seq_upper.count(_c) for _c in ("B", "Z", "J"))
    seq_translated = seq_upper.translate(_AA_TRANSLATION_TABLE)
    seq_sanitized = _RE_INVALID_AA.sub("", seq_translated)
    cnt_removed_chars = len(seq_upper) - len(seq_sanitized)
    return seq_sanitized, cnt_removed_chars, cnt_replaced_chars


def calculate_mw_kda(seq: str) -> MwRecord:
    seq_sanitized, cnt_removed_chars, cnt_replaced_chars = _sanitize_protein_sequence(
        seq
    )
    if not seq_sanitized:
        return MwRecord(
            mw_kda=None,
            cnt_sanitized_chars=cnt_removed_chars,
            cnt_replaced_chars=cnt_replaced_chars,
            is_empty_after_sanitize=True,
        )
    mw_da = molecular_weight(seq_sanitized, seq_type="protein")
    return MwRecord(
        mw_kda=round(mw_da / 1000.0, 3),
        cnt_sanitized_chars=cnt_removed_chars,
        cnt_replaced_chars=cnt_replaced_chars,
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
    rules_fallback: Sequence[HeaderParsingRuleSpec] | None | Literal[False] = None,
    should_include_sequence: bool = False,
    should_deduplicate: bool = True,
    dir_tmp: Path | str | None = None,
) -> pl.DataFrame:
    """
    Read one or more FASTA files into a Polars DataFrame with parsed headers and MW.

    Notes:
        - Header parsing uses Pyteomics (flavor="auto") first,
            then optional fallback regex rules to backfill missing ID/Symbol.
        - ProteinId is derived from parsed ID/Symbol or SeqRecord.id;
            if ``should_deduplicate`` is True, rows are de-duplicated by ProteinId (keep first).
        - Molecular weight is computed after sequence sanitization;
            if a sequence becomes empty, MWKDa is set to null.

    Args:
        files_in (list[os.PathLike[str]] | list[str] | os.PathLike[str] | str):
            One or more FASTA file paths.
        rules_fallback (Sequence[HeaderParsingRuleSpec] | None | Literal[False], optional):
            Header parsing fallback rules. Use ``None`` to apply the module defaults,
            or ``False`` to disable fallbacks.
            Defaults to None.
        should_include_sequence (bool, optional):
            If True, keep the raw sequence in the output column ``Sequence``.
            It is recommended to set this to ``False`` for large datasets to save memory.
            Defaults to ``False``.
        should_deduplicate (bool, optional):
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
        >>> df = read_fasta(["a.faa", "b.faa"], should_include_sequence=True)
    """
    if rules_fallback is None:
        rules_fallback = L_FALLBACK_HEADER_PARSING_RULES
    elif rules_fallback is False:
        rules_fallback = []

    files_in = (
        [Path(files_in)] if isinstance(files_in, (str, os.PathLike)) else files_in
    )
    files_in_norm = [Path(_p) for _p in files_in]

    rows_parsed: list[dict[str, Any]] = []
    cnt_sanitized_seqs = 0
    cnt_sanitized_characters = 0
    cnt_replaced_characters = 0
    cnt_empty_seqs_after_sanitize = 0
    cnt_chunks = 0

    tmpdir_context = None
    if dir_tmp is None:
        tmpdir_context = TemporaryDirectory()
        dir_tmp = Path(tmpdir_context.name)
    else:
        timestamp_now = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        dir_tmp = Path(dir_tmp) / f"fasta-{timestamp_now}-{token_hex(4)}"
        dir_tmp.mkdir(parents=True, exist_ok=True)

    try:
        for _file in files_in_norm:
            if not _file.is_file():
                logger.warning(f"Skipping non-file: {_file}")
                continue

            for _record in SeqIO.parse(_file, "fasta"):
                _record = cast(SeqRecord, _record)
                seq_sanitized = str(_record.seq)
                spec_header_parsed = parse_fasta_header(
                    _record.description, rules_fallback
                )

                protein_id_parsed = (
                    spec_header_parsed.id or spec_header_parsed.symbol or _record.id
                )

                try:
                    spec_mw_result = calculate_mw_kda(seq_sanitized)
                    if (
                        spec_mw_result.cnt_sanitized_chars > 0
                        or spec_mw_result.cnt_replaced_chars > 0
                    ):
                        cnt_sanitized_seqs += 1
                        cnt_sanitized_characters += spec_mw_result.cnt_sanitized_chars
                        cnt_replaced_characters += spec_mw_result.cnt_replaced_chars
                    if spec_mw_result.is_empty_after_sanitize:
                        cnt_empty_seqs_after_sanitize += 1
                except Exception as e:
                    logger.warning(
                        "MW failed: file={}, term={}, err={}",
                        _file.name,
                        protein_id_parsed,
                        e,
                    )
                    spec_mw_result = MwRecord(
                        mw_kda=None,
                        cnt_sanitized_chars=0,
                        cnt_replaced_chars=0,
                        is_empty_after_sanitize=False,
                    )

                row = {
                    "File": str(_file),
                    "ProteinId": protein_id_parsed,
                    "ProteinSymbol": spec_header_parsed.symbol,
                    "ProteinName": spec_header_parsed.name,
                    "GeneSymbol": _extract_gene(_record.description),
                    "MWKDa": spec_mw_result.mw_kda,
                    "Length": len(seq_sanitized),
                }
                if should_include_sequence:
                    row["Sequence"] = seq_sanitized
                rows_parsed.append(row)

                if len(rows_parsed) >= N_ROWS_PER_PARQUET_CHUNK:
                    cnt_chunks = _write_rows_to_parquet(
                        rows_parsed, dir_tmp, chunk_index=cnt_chunks
                    )

        cnt_chunks = _write_rows_to_parquet(
            rows_parsed, dir_tmp, chunk_index=cnt_chunks
        )

        if cnt_chunks == 0:
            schema = {
                "File": pl.Utf8,
                "ProteinId": pl.Utf8,
                "ProteinSymbol": pl.Utf8,
                "ProteinName": pl.Utf8,
                "GeneSymbol": pl.Utf8,
                "MWKDa": pl.Float64,
                "Length": pl.Int64,
            }
            if should_include_sequence:
                schema["Sequence"] = pl.Utf8
            return pl.DataFrame(schema=schema)

        lf_fasta = pl.scan_parquet(dir_tmp)

        if should_deduplicate:
            df_dup = (
                lf_fasta.select("ProteinId")
                .group_by("ProteinId")
                .len()
                .filter(pl.col("len") > 1)
                .collect()
            )
            if df_dup.height > 0:
                protein_ids_duplicate = df_dup.get_column("ProteinId").to_list()
                logger.warning(
                    "Detected duplicates in ProteinId ({}): {}",
                    len(protein_ids_duplicate),
                    "; ".join(map(str, protein_ids_duplicate[:200])),
                )

            df_fasta = lf_fasta.unique(subset=["ProteinId"], keep="first").collect()
        else:
            df_fasta = lf_fasta.collect()

        if cnt_sanitized_seqs > 0:
            logger.warning(
                "Sanitized sequences for MW calculation: count={}, removed={}, replaced={}",
                cnt_sanitized_seqs,
                cnt_sanitized_characters,
                cnt_replaced_characters,
            )
        if cnt_empty_seqs_after_sanitize > 0:
            logger.warning(
                f"Sequences empty after sanitization (MW set to null): count={cnt_empty_seqs_after_sanitize}"
            )

        return df_fasta

    finally:
        if tmpdir_context is not None:
            tmpdir_context.cleanup()

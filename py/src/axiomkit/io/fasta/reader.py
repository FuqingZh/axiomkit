import os
import re
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

import polars as pl
from Bio import SeqIO
from Bio.SeqRecord import SeqRecord
from Bio.SeqUtils import molecular_weight
from loguru import logger
from pyteomics import fasta as pt_fasta

# ----------------------------
# Header parsing (private -> pyteomics merge -> fallback)
# ----------------------------
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

# Pyteomics key priorities (module-level to avoid per-call allocation).
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


_DEFAULT_PRIVATE_HEADER_RULES: list[SpecHeaderParsingRule] = [
    SpecHeaderParsingRule(pattern=_RE_UNIPROT),
    SpecHeaderParsingRule(pattern=_RE_NCBI_GI_REF),
    SpecHeaderParsingRule(pattern=_RE_NCBI_GI_SPTR),
    SpecHeaderParsingRule(pattern=_RE_NCBI_GI_ONLY, symbol_group=""),
    SpecHeaderParsingRule(pattern=_RE_IPI),
    SpecHeaderParsingRule(pattern=_RE_GNL),
    SpecHeaderParsingRule(pattern=_RE_OTHERS),
]


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


def _parse_header_with_private_rules(
    header: str, rules_private: Sequence[SpecHeaderParsingRule]
) -> SpecFastaHeader:
    """Parse header using private/custom rules."""
    c_id = c_symbol = c_name = ""
    for rule in rules_private:
        if inst_match_info := rule.pattern.search(header):
            dict_groups = inst_match_info.groupdict()
            if rule.id_group and rule.id_group in dict_groups:
                c_id = inst_match_info.group(rule.id_group) or ""
            else:
                c_id = inst_match_info.group(1) or ""

            if rule.symbol_group and rule.symbol_group in dict_groups:
                c_symbol = inst_match_info.group(rule.symbol_group) or ""
                # Symbols should be compact tokens; drop trailing description if present.
                if " " in c_symbol:
                    c_symbol = c_symbol.split()[0]

            if rule.name_group and rule.name_group in dict_groups:
                c_name = inst_match_info.group(rule.name_group) or ""

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
    inst_header_pt = SpecFastaHeader(id="", symbol="", name="")
    try:
        dict_fasta_header = cast(dict[str, Any], pt_fasta.parse(header, flavor="auto"))
        inst_header_pt = _extract_header_info(dict_fasta_header)
    except Exception:
        pass

    # 2) Only if Pyteomics info is incomplete, run private rules to backfill.
    # Treat name as optional to avoid running private rules on most headers.
    needs_private = not (inst_header_pt.id and inst_header_pt.symbol)
    if needs_private and rules_fallback:
        inst_header_private = _parse_header_with_private_rules(header, rules_fallback)
    else:
        inst_header_private = SpecFastaHeader(id="", symbol="", name="")

    # 3) Merge with Pyteomics priority.
    if (
        inst_header_pt.id
        or inst_header_pt.symbol
        or inst_header_pt.name
        or inst_header_private.id
        or inst_header_private.symbol
        or inst_header_private.name
    ):
        return SpecFastaHeader(
            id=inst_header_pt.id or inst_header_private.id or header,
            symbol=inst_header_pt.symbol or inst_header_private.symbol,
            name=inst_header_pt.name or inst_header_private.name,
        )

    # 4) Last resort: keep original header as ID.
    return SpecFastaHeader(id=header, symbol="", name="")


# ----------------------------
# Sequence features
# ----------------------------

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


def _extract_gene(description: str) -> str:
    m = _RE_GENE.search(description or "")
    return m.group(1) if m else ""


def _sanitize_protein_sequence(seq: str) -> tuple[str, int]:
    """
    Clean a protein sequence for molecular weight calculation.

    Strategy:
    - Upper-case.
    - Map common ambiguous letters to a concrete residue.
    - Drop unknowns/stops/gaps and any other invalid characters.
    Returns (clean_sequence, num_removed_or_changed).
    """
    seq_upper = seq.upper()
    seq_mapped = seq_upper.translate(_AA_TRANSLATION_TABLE)
    seq_clean = _RE_INVALID_AA.sub("", seq_mapped)
    n_delta = len(seq_upper) - len(seq_clean)
    return seq_clean, n_delta


@dataclass(frozen=True, slots=True)
class SpecMwResult:
    mw_kda: float | None
    num_sanitized: int
    is_empty_after_sanitize: bool


def calculate_mw_kda(seq: str) -> SpecMwResult:
    seq_clean, n_delta = _sanitize_protein_sequence(seq)
    if not seq_clean:
        return SpecMwResult(
            mw_kda=None, num_sanitized=n_delta, is_empty_after_sanitize=True
        )
    mw_da = molecular_weight(seq_clean, seq_type="protein")
    return SpecMwResult(
        mw_kda=round(mw_da / 1000.0, 3),
        num_sanitized=n_delta,
        is_empty_after_sanitize=False,
    )


# ----------------------------
# I/O pipeline
# ----------------------------
def read_fasta(
    files_in: list[os.PathLike[str]] | list[str] | os.PathLike[str] | str,
    *,
    rules_fallback: Sequence[SpecHeaderParsingRule] | None | Literal[False] = False,
):
    if rules_fallback is None:
        rules_fallback = _DEFAULT_PRIVATE_HEADER_RULES
    elif rules_fallback is False:
        rules_fallback = []

    files_in = (
        [Path(files_in)] if isinstance(files_in, (str, os.PathLike)) else files_in
    )
    l_files_in = [Path(_p) for _p in files_in]

    l_rows: list[dict[str, Any]] = []
    n_sanitized = 0
    n_sanitized_delta = 0
    n_empty_after_sanitize = 0

    for _file in l_files_in:
        for _record in SeqIO.parse(_file, "fasta"):
            _record = cast(SeqRecord, _record)

            inst_header_parsed = parse_fasta_header(_record.description, rules_fallback)
            c_sequence = str(_record.seq)

            c_protein_id = (
                inst_header_parsed.id or inst_header_parsed.symbol or _record.id
            )

            try:
                inst_mw_result = calculate_mw_kda(c_sequence)
                if inst_mw_result.num_sanitized > 0:
                    n_sanitized += 1
                    n_sanitized_delta += inst_mw_result.num_sanitized
                if inst_mw_result.is_empty_after_sanitize:
                    n_empty_after_sanitize += 1
            except Exception as e:
                logger.warning(
                    "MW failed: file={}, term={}, err={}",
                    _file.name,
                    c_protein_id,
                    e,
                )
                inst_mw_result = SpecMwResult(
                    mw_kda=None, num_sanitized=0, is_empty_after_sanitize=False
                )

            l_rows.append(
                {
                    "File": _file,
                    "ProteinId": c_protein_id,
                    "ProteinSymbol": inst_header_parsed.symbol,
                    "ProteinName": inst_header_parsed.name,
                    "GeneSymbol": _extract_gene(_record.description),
                    "MWKDa": inst_mw_result.mw_kda,
                    "Length": len(c_sequence),
                    "Sequence": c_sequence,
                }
            )

    df_fasta = pl.DataFrame(l_rows)

    df_dup = (
        df_fasta.select("ProteinId")
        .group_by("ProteinId")
        .len()
        .filter(pl.col("len") > 1)
    )
    if df_dup.height > 0:
        l_dup = df_dup.get_column("ProteinId").to_list()
        logger.warning(
            "Detected duplicates in ProteinId ({}): {}",
            len(l_dup),
            "; ".join(map(str, l_dup[:200])),
        )

    df_fasta = df_fasta.unique(subset=["ProteinId"], keep="first")

    if n_sanitized > 0:
        logger.warning(
            "Sanitized sequences for MW calculation: count={}, total_removed_or_changed={}",
            n_sanitized,
            n_sanitized_delta,
        )
    if n_empty_after_sanitize > 0:
        logger.warning(
            "Sequences empty after sanitization (MW set to null): count={}",
            n_empty_after_sanitize,
        )

    return df_fasta

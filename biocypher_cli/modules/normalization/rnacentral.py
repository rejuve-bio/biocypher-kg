"""Normalization helpers for RNAcentral-related inputs.

The RNAcentral GO/Rfam annotation file is expected to be a tab-separated file
with 3 columns:

    <URS..._taxid>\t<GO:...>\t<Rfam:RF.....>

Upstream files can occasionally contain additional columns; this module provides
best-effort normalization to coerce such variants into the 3-column shape.
"""

from __future__ import annotations

import gzip
import os
import re
import tempfile
from pathlib import Path
from typing import Iterable, Optional, Tuple, Union

_GO_RE = re.compile(r"GO:\d{7}")
_RFM_RE = re.compile(r"^(?:Rfam:)?(RF\d{5})$")
_RFM_INNER_RE = re.compile(r"(?:^|\b)(RF\d{5})(?:\b|$)")


def _iter_lines(path: Path) -> Iterable[str]:
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8", errors="replace") as fh:
            yield from fh
    else:
        with open(path, "rt", encoding="utf-8", errors="replace") as fh:
            yield from fh


def _open_writer(path: Path, tmp_path: Path):
    if path.suffix == ".gz":
        return gzip.open(tmp_path, "wt", encoding="utf-8", errors="replace")
    return open(tmp_path, "wt", encoding="utf-8", errors="replace")


def _find_go_and_rfam(fields: Iterable[str]) -> Tuple[Optional[str], Optional[str]]:
    go_term: Optional[str] = None
    rfam_term: Optional[str] = None

    for field in fields:
        field = field.strip()
        if not field:
            continue

        if go_term is None and _GO_RE.fullmatch(field):
            go_term = field
            continue

        if rfam_term is None:
            m = _RFM_RE.fullmatch(field)
            if m:
                rfam_term = f"Rfam:{m.group(1)}"
                continue

            m2 = _RFM_INNER_RE.search(field)
            if m2:
                rfam_term = f"Rfam:{m2.group(1)}"
                continue

    return go_term, rfam_term


def normalize_rnacentral_rfam(path: Union[str, Path]) -> None:
    """Normalize an RNAcentral GO/Rfam annotations file in place.

    - If rows are already 3-column (<rna_id>, <GO>, <Rfam>), this is a no-op.
    - If rows contain extra columns, we locate the first GO-like token and the
      first Rfam-like token anywhere on the row and rewrite it as exactly those
      3 columns.

    Rows where a GO term or Rfam term cannot be found are skipped.

    Works for plain text and .gz files.
    """

    path = Path(path)
    if not path.exists() or path.is_dir():
        return

    with tempfile.NamedTemporaryFile(
        "wb",
        delete=False,
        dir=str(path.parent),
        suffix=path.suffix,
    ) as tmp:
        tmp_path = Path(tmp.name)

    try:
        wrote_any = False
        with _open_writer(path, tmp_path) as out:
            for raw in _iter_lines(path):
                line = raw.rstrip("\n")
                if not line or line.startswith("#"):
                    continue

                fields = line.split("\t")
                rna_id = fields[0].strip() if fields else ""
                if not rna_id:
                    continue

                if len(fields) == 3:
                    go, rfam = fields[1].strip(), fields[2].strip()
                    if _GO_RE.fullmatch(go) and (_RFM_RE.fullmatch(rfam) or rfam.startswith("Rfam:RF")):
                        if rfam.startswith("RF"):
                            rfam = f"Rfam:{rfam}"
                        out.write(f"{rna_id}\t{go}\t{rfam}\n")
                        wrote_any = True
                        continue

                go_term, rfam_term = _find_go_and_rfam(fields[1:])
                if go_term and rfam_term:
                    out.write(f"{rna_id}\t{go_term}\t{rfam_term}\n")
                    wrote_any = True

        if wrote_any:
            os.replace(tmp_path, path)
        else:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
    finally:
        try:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
        except Exception:
            pass

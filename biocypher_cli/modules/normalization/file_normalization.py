"""Shared sample-file normalization.

Used by:
- the CLI sample preparation flow
- scripts/download_and_sample.py

Keep logic here generic and opt-in via filename/content detection.
"""

from __future__ import annotations

import gzip
import tempfile
from pathlib import Path
from typing import Callable, Optional

from .rnacentral import normalize_rnacentral_rfam


def detect_file_format(path: Path) -> str:
    """Detect a coarse file format / normalization rule for a file."""
    try:
        filename = path.name.lower()

        if "rnacentral_rfam_annotations" in filename:
            return "rnacentral_rfam"
        if any(filename.endswith(ext) for ext in (".gff", ".gtf", ".gff3")):
            return "gff"
        if any(ext in filename for ext in (".bed", ".narrowpeak", ".broadpeak")):
            return "bed"

        is_gz = path.suffix == ".gz"
        opener = gzip.open if is_gz else open

        with opener(path, "rt", encoding="utf-8", errors="ignore") as f:
            lines = []
            for i, line in enumerate(f):
                if i >= 10:
                    break
                lines.append(line.strip())

        if not lines:
            return "unknown"

        tab_lines = [line for line in lines if "\t" in line and not line.startswith("#")]
        if tab_lines:
            parts = tab_lines[0].split("\t")
            if len(parts) >= 8 and parts[0] and parts[3].isdigit() and parts[4].isdigit():
                return "gff"

        if tab_lines:
            parts = tab_lines[0].split("\t")
            if len(parts) >= 3 and parts[1].isdigit() and parts[2].isdigit():
                return "bed"

        if any("\t" in line for line in lines):
            return "tabular"

        return "unknown"
    except Exception:
        return "unknown"


def normalize_file_format(path: Path) -> None:
    """Best-effort normalization for common sample file quirks.

    Operates in-place on plain or gzipped files.
    """
    try:
        file_format = detect_file_format(path)

        if file_format == "rnacentral_rfam":
            normalize_rnacentral_rfam(path)
        elif file_format == "gff":
            _normalize_gff_file(path)
        elif file_format in {"bed", "tabular"}:
            _normalize_tabular_file(path)
    except Exception:
        return


def _transform_file_in_place(path: Path, line_processor: Callable[[str], Optional[str]]) -> None:
    is_gz = path.suffix == ".gz"
    opener_in = gzip.open if is_gz else open
    opener_out = gzip.open if is_gz else open

    with tempfile.NamedTemporaryFile(
        "wb",
        delete=False,
        dir=str(path.parent),
        suffix=path.suffix if is_gz else "",
    ) as tmp:
        tmp_path = Path(tmp.name)

    try:
        with opener_in(path, "rt", encoding="utf-8", errors="replace") as src, opener_out(
            tmp_path, "wt", encoding="utf-8", errors="replace"
        ) as out:
            for line in src:
                result = line_processor(line.rstrip("\n"))
                if result is None:
                    continue
                out.write(result)
                out.write("\n")

        tmp_path.replace(path)
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass


def _normalize_tabular_file(path: Path) -> None:
    def processor(line: str) -> Optional[str]:
        if not line.strip():
            return None
        parts = line.split("\t")
        return "\t".join(part.strip() for part in parts)

    _transform_file_in_place(path, processor)


def _normalize_gff_file(path: Path) -> None:
    def processor(line: str) -> Optional[str]:
        if not line or line.startswith("#"):
            return line
        parts = line.split("\t")
        if len(parts) >= 8:
            return line
        return None

    _transform_file_in_place(path, processor)

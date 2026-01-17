"""Download a file (local path or URL) and write a sampled subset.

Example:
  python scripts/download_and_sample.py \
    --input https://example.com/data.tsv.gz \
    --output samples/peregrine/PEREGRINEenhancershg38_sample.gz \
    --limit 100
Supports gzipped or plain text; 
"""
from __future__ import annotations

import argparse
import gzip
import io
import os
import shutil
import sys
import tarfile
import tempfile
import mimetypes
import zipfile
import time
import socket
import re
from dataclasses import dataclass, field
from http.client import RemoteDisconnected
from urllib.error import URLError
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Optional, Tuple
from urllib.parse import urlparse
import urllib.request
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from biocypher_cli.modules.normalization import normalize_file_format
except Exception:
    normalize_file_format = None


def _maybe_normalize_downloaded_file(path: Path) -> None:
    if normalize_file_format is None:
        return
    normalize_file_format(path)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Download (or copy) and sample first N data rows")
    p.add_argument("--input", required=True, help="URL or local path to source file (tsv/csv, plain or .gz)")
    p.add_argument("--output", required=True, help="Destination path; .gz writes gzipped")
    p.add_argument("--limit", type=int, default=100, help="Number of data rows to keep (excluding header unless --no-header)")
    p.add_argument("--timeout", type=float, default=float(os.environ.get("BIOCYPHER_DOWNLOAD_TIMEOUT", "30")), help="Network timeout in seconds for HTTP(S) downloads")
    p.add_argument("--no-header", action="store_true", help="Treat input as headerless; sample first N lines")
    p.add_argument("--delimiter", default="\t", help="Field delimiter (unused for sampling but retained for clarity)")
    p.add_argument("--filter", help="Only include lines containing this substring (case-sensitive)")
    return p.parse_args()


def is_gzip_file(path: Path) -> bool:
    if path.suffix == ".gz":
        return True
    try:
        with path.open("rb") as f:
            magic = f.read(2)
            return magic == b"\x1f\x8b"
    except FileNotFoundError:
        return False


def open_maybe_gzip(path: Path, mode: str):
    if "b" in mode:
        return gzip.open(path, mode) if is_gzip_file(path) else open(path, mode)
    return gzip.open(path, mode) if is_gzip_file(path) else open(path, mode, encoding="utf-8")


def ensure_file_terminator(path: Path) -> None:
    """Ensure sampled file endings are sane.

    - For UniProt `.dat` files (heuristically detected), ensure the file
      terminates with a clean "//" line (no extra blank line before it).
    - For other files, ensure the file ends with exactly one newline
      (remove excess trailing blank lines, append a newline if missing).

    Operates on gzipped or plain text files. Best-effort: non-fatal on error.
    """
    try:
        gz = is_gzip_file(path)
        if gz:
            opener = lambda mode: gzip.open(path, mode)
        else:
            opener = lambda mode: open(path, mode, encoding="utf-8", errors="replace")

        with opener("rt") as fh:
            lines = fh.readlines()

        tail_lines = lines[-16:] if lines else []
        tail = "".join(tail_lines)

        looks_like_uniprot = any(l.startswith("ID   ") or l.startswith("AC   ") for l in tail_lines)

        if looks_like_uniprot:
            if not tail.rstrip().endswith("//"):
                last_line = lines[-1] if lines else ""
                add_prefix = "" if last_line.endswith("\n") else "\n"
                if lines and lines[-1].strip() == "":
                    cleaned = "".join(lines).rstrip("\n")
                    with opener("wt") as fh:
                        fh.write(cleaned)
                        fh.write("\n//\n")
                else:
                    with opener("at") as fh:
                        fh.write(f"{add_prefix}//\n")
        else:
            if not lines:
                return
            cleaned = "\n".join(l.rstrip("\n") for l in lines).rstrip()
            with opener("wt") as fh:
                fh.write(cleaned)
                fh.write("\n")
    except Exception:
        return


def _urlopen_with_retry(src: str, retries: int = 1, delay: float = 1.0, timeout: float = 30.0):
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(
                src,
                headers={
                    "User-Agent": "biocypher-kg downloader (uv)"
                },
            )
            return urllib.request.urlopen(req, timeout=timeout)
        except (RemoteDisconnected, URLError, TimeoutError, socket.timeout):
            if attempt >= retries:
                raise
            time.sleep(delay)


def _resolve_out_path(out_path: Path, suggested_name: str) -> Path:
    """Return a concrete file path for writing.

    If `out_path` looks like a directory (exists as dir, ends with a path
    separator, or has no suffix), create it and append `suggested_name`.
    Otherwise, use `out_path` as-is after creating its parent directory.
    """

    looks_like_dir = str(out_path).endswith(os.sep) or out_path.is_dir() or out_path.suffix == ""
    if looks_like_dir:
        out_path.mkdir(parents=True, exist_ok=True)
        return out_path / suggested_name
    out_path.parent.mkdir(parents=True, exist_ok=True)
    return out_path


def is_vcf_file(path_or_url: str) -> bool:
    """Check if the file is a VCF file."""
    lower = path_or_url.lower()
    return lower.endswith('.vcf') or lower.endswith('.vcf.gz')


def is_uniprot_file(path_or_url: str) -> bool:
    """Check if the file is a UniProt .dat file."""
    lower = path_or_url.lower()
    return 'uniprot' in lower or lower.endswith('.dat') or lower.endswith('.dat.gz')


def sample_uniprot_records(lines_iter: Iterator[str], limit: int) -> Iterator[str]:
    """Sample complete UniProt records (from ID to //)."""
    current_record = []
    records_sampled = 0
    for line in lines_iter:
        current_record.append(line)
        if line.strip() == "//":
            if records_sampled < limit:
                yield from current_record
                records_sampled += 1
                if records_sampled >= limit:
                    break
            current_record = []
    # Do not yield incomplete records at the end


@dataclass(frozen=True)
class FileTypeHandler:
    detector: Callable[[str], bool]
    sampler: Callable[[Iterator[str], int, bool], Iterator[str]]
    post_processors: List[Callable[[Path], None]] = field(default_factory=list)
    download_full: bool = False


def _sample_vcf(lines_iter: Iterator[str], limit: int, _no_header: bool = False, filter_str: str = None) -> Iterator[str]:
    """Sample VCF file: collect headers, then sample data lines."""
    headers: List[str] = []
    data_sampled = 0
    for line in lines_iter:
        if line.startswith('#'):
            headers.append(line)
        else:
            # First data line
            if headers:
                yield from headers
                headers = []
            if filter_str and filter_str not in line:
                continue
            if data_sampled < limit:
                yield line
                data_sampled += 1
            else:
                break

    for line in lines_iter:
        if data_sampled >= limit:
            break
        yield line
        data_sampled += 1


def _sample_default(lines_iter: Iterator[str], limit: int, no_header: bool, filter_str: str = None) -> Iterator[str]:
    """Default sampling: optional header, then limit data lines, optionally filtered."""
    def _looks_like_header(line: str) -> bool:
        s = (line or "").strip()
        if not s:
            return False
        if s.startswith("#"):
            return True
        # Heuristic: headers almost never contain URLs.
        if "http://" in s or "https://" in s:
            return False

        # If most fields are alpha-ish and we don't see long digit runs,
        # this is likely a column header.
        cols = s.split("\t")
        if len(cols) <= 1:
            return True
        alphaish = 0
        has_long_digits = False
        for c in cols:
            c = c.strip()
            if re.search(r"\d{4,}", c):
                has_long_digits = True
            if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_ \-]*", c or ""):
                alphaish += 1
        return (alphaish / max(len(cols), 1)) >= 0.6 and not has_long_digits

    data_count = 0

    if not no_header:
        try:
            first = next(lines_iter)
        except StopIteration:
            return
        if _looks_like_header(first):
            yield first
        else:
            # Treat as first data line (subject to filter).
            if (not filter_str) or (filter_str in first):
                yield first
                data_count = 1
    for line in lines_iter:
        if filter_str and filter_str not in line:
            continue
        if data_count >= limit:
            break
        yield line
        data_count += 1


def _sample_uniprot(lines_iter: Iterator[str], limit: int, _no_header: bool = False, filter_str: str = None) -> Iterator[str]:
    return sample_uniprot_records(lines_iter, limit)


FILE_TYPE_HANDLERS: Dict[str, FileTypeHandler] = {
    "vcf": FileTypeHandler(detector=is_vcf_file, sampler=_sample_vcf),
    "uniprot": FileTypeHandler(
        detector=is_uniprot_file,
        sampler=_sample_uniprot,
        post_processors=[ensure_file_terminator],
        download_full=False,
    ),
    "default": FileTypeHandler(detector=lambda _p: True, sampler=_sample_default),
}


def detect_file_type(path_or_url: str) -> str:
    """Detect file type using handlers."""
    for type_name, handler in FILE_TYPE_HANDLERS.items():
        if type_name != 'default' and handler.detector(path_or_url):
            return type_name
    return 'default'


def get_file_handler(file_type: str) -> FileTypeHandler:
    """Get the handler dict for a file type."""
    return FILE_TYPE_HANDLERS.get(file_type, FILE_TYPE_HANDLERS['default'])


def register_file_type(
    type_name: str,
    detector: Callable[[str], bool],
    sampler: Callable[[Iterator[str], int, bool], Iterator[str]],
    post_processors: Optional[List[Callable[[Path], None]]] = None,
    download_full: bool = False,
):
    """Register a new file type handler.
    
    detector: function(path_or_url) -> bool
    sampler: function(lines_iter, limit, no_header) -> Iterator[str]
    post_processors: list of post-processing callables
    download_full: bool, whether to download full file before sampling
    """
    FILE_TYPE_HANDLERS[type_name] = FileTypeHandler(
        detector=detector,
        sampler=sampler,
        post_processors=post_processors or [],
        download_full=download_full,
    )


def _post_process_and_normalize(target_path: Path, handler: FileTypeHandler) -> None:
    for post_proc in handler.post_processors:
        try:
            post_proc(target_path)
        except Exception:
            pass

    try:
        _maybe_normalize_downloaded_file(target_path)
    except Exception:
        pass


def iter_lines(path: Path) -> Iterator[str]:
    """Iterate over lines in a file, handling gzip."""
    with open_maybe_gzip(path, "rt") as f:
        for line in f:
            yield line


def sample_local_file(local_path: Path, out_path: Path, limit: int, no_header: bool, filter_str: str = None) -> Tuple[int, Path]:
    target_path = _resolve_out_path(out_path, local_path.name or "sample.out")
    file_type = detect_file_type(str(local_path))
    handler = get_file_handler(file_type)
    
    data_rows = 0
    with open_maybe_gzip(target_path, "wt") as out:
        lines = iter_lines(local_path)
        for line in handler.sampler(lines, limit, no_header, filter_str):
            out.write(line)
            data_rows += 1 
    _post_process_and_normalize(target_path, handler)
    
    return data_rows, target_path


def stream_sample_http(src: str, out_path: Path, limit: int, no_header: bool, timeout: float, filter_str: str = None) -> Tuple[int, Path]:
    parsed = urlparse(src)
    assert parsed.scheme in {"http", "https"}

    file_type = detect_file_type(src)
    handler = get_file_handler(file_type)

    if handler.download_full:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.dat.gz') as tmp_file:
            tmp_gz_path = Path(tmp_file.name)
            with _urlopen_with_retry(src, timeout=timeout) as resp:
                shutil.copyfileobj(resp, tmp_file)

        tmp_dat_path = tmp_gz_path.with_suffix('.dat')
        with gzip.open(tmp_gz_path, 'rb') as gz_f, open(tmp_dat_path, 'wb') as dat_f:
            shutil.copyfileobj(gz_f, dat_f)
        tmp_gz_path.unlink()
        # Now sample from the decompressed temp file
        data_rows, target_path = sample_local_file(tmp_dat_path, out_path, limit, no_header)
        tmp_dat_path.unlink()
        return data_rows, target_path

    is_gz = parsed.path.endswith(".gz")
    is_zip = parsed.path.endswith(".zip")
    data_rows = 0
    suggested_name = Path(parsed.path).name or "sample.out"

    if is_zip:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_zip = Path(tmpdir) / Path(parsed.path).name
            with _urlopen_with_retry(src, timeout=timeout) as resp, tmp_zip.open("wb") as outb:
                shutil.copyfileobj(resp, outb)

            with zipfile.ZipFile(tmp_zip, "r") as zf:
                candidate = None
                for name in zf.namelist():
                    # skip directories
                    if name.endswith("/"):
                        continue
                    suffix = Path(name).suffix.lower()
                    if suffix in {".txt", ".tsv", ".csv", ".gtf", ".gff", ".gff3", ".bed"} or True:
                        candidate = name
                        break

                if not candidate:
                    print("No suitable file found inside ZIP archive.")
                    return 0, out_path

                target_path = _resolve_out_path(out_path, Path(candidate).name)

                with zf.open(candidate) as member:
                    if Path(candidate).suffix.lower() == ".gz":
                        with gzip.GzipFile(fileobj=io.BytesIO(member.read())) as gf:
                            reader = io.TextIOWrapper(gf, encoding="utf-8", errors="replace")
                            with open_maybe_gzip(target_path, "wt") as out:
                                for line in handler.sampler(reader, limit, no_header, filter_str):
                                    out.write(line)
                                    data_rows += 1
                    else:
                        reader = io.TextIOWrapper(member, encoding="utf-8", errors="replace")
                        with open_maybe_gzip(target_path, "wt") as out:
                            for line in handler.sampler(reader, limit, no_header, filter_str):
                                out.write(line)
                                data_rows += 1

        _post_process_and_normalize(target_path, handler)
        return data_rows, target_path

    # Non-zip HTTP/HTTPS
    target_path = _resolve_out_path(out_path, suggested_name)

    class _PrependStream(io.RawIOBase):
        def __init__(self, prefix: bytes, raw):
            self._prefix = prefix or b""
            self._pos = 0
            self._raw = raw

        def readable(self):
            return True

        def read(self, size: int = -1) -> bytes:
            if size == 0:
                return b""
            if size < 0:
                head = self._prefix[self._pos :]
                self._pos = len(self._prefix)
                tail = self._raw.read()
                return head + (tail or b"")
            if self._pos < len(self._prefix):
                take = min(size, len(self._prefix) - self._pos)
                chunk = self._prefix[self._pos : self._pos + take]
                self._pos += take
                if take == size:
                    return chunk
                rest = self._raw.read(size - take)
                return chunk + (rest or b"")
            return self._raw.read(size)

    with _urlopen_with_retry(src, timeout=timeout) as resp:
        # Peek at the first bytes to avoid crashing on fake .gz URLs (HTML error pages, etc.).
        prefix = resp.read(4096)
        raw = _PrependStream(prefix, resp)

        content_type = (getattr(resp, "headers", None) or {}).get("Content-Type", "")
        looks_html = prefix.lstrip().startswith(b"<!") or prefix.lstrip().lower().startswith(b"<html")
        looks_gzip = prefix[:2] == b"\x1f\x8b"


        if looks_html or (isinstance(content_type, str) and content_type.lower().startswith("text/html")):
            raise RuntimeError(
                f"URL did not return a data file (got HTML). URL: {src}"
            )

        use_gzip = looks_gzip or (isinstance(content_type, str) and "gzip" in content_type.lower())
        if is_gz and not looks_gzip:
            # Some servers mislabel; allow plain text fallback when it's not HTML.
            use_gzip = False

        if use_gzip:
            reader = io.TextIOWrapper(gzip.GzipFile(fileobj=raw), encoding="utf-8", errors="replace")
        else:
            reader = io.TextIOWrapper(raw, encoding="utf-8", errors="replace")

        with open_maybe_gzip(target_path, "wt") as out:
            for line in handler.sampler(reader, limit, no_header, filter_str):
                out.write(line)
                data_rows += 1  # Approximate count

    _post_process_and_normalize(target_path, handler)
    
    return data_rows, target_path


def main() -> None:
    args = parse_args()

    parsed = urlparse(args.input)
    out_path = Path(args.output)
    is_tar = args.input.endswith('.tar') or args.input.endswith('.tar.gz')
    if is_tar:
        with tempfile.TemporaryDirectory() as tmpdir:
            archive_path = Path(tmpdir) / Path(args.input).name
            if parsed.scheme in {"http", "https"}:
                with _urlopen_with_retry(args.input, timeout=args.timeout) as resp, open(archive_path, "wb") as out:
                    shutil.copyfileobj(resp, out)
            else:
                shutil.copy(args.input, archive_path)

            extract_dir = Path(tmpdir) / "extracted"
            extract_dir.mkdir(parents=True, exist_ok=True)
            with tarfile.open(archive_path, "r:*") as tar:
                tar.extractall(path=extract_dir)
            looks_like_dir = str(out_path).endswith(os.sep) or out_path.is_dir() or out_path.suffix == ""

            if looks_like_dir:
                target_dir = out_path
                target_dir.mkdir(parents=True, exist_ok=True)
                extracted = 0

                # Detect and drop the leading archive root (e.g., "pwm/") to avoid nested dirs
                all_files = [m for m in extract_dir.rglob("*") if m.is_file()]
                common_prefix = None
                if all_files:
                    parts_lists = [m.relative_to(extract_dir).parts for m in all_files]
                    first_parts = [p[0] for p in parts_lists if p]
                    if first_parts and len(set(first_parts)) == 1:
                        common_prefix = first_parts[0]

                for member in sorted(all_files):
                    rel = member.relative_to(extract_dir)
                    if common_prefix and rel.parts and rel.parts[0] == common_prefix:
                        rel = Path(*rel.parts[1:]) if len(rel.parts) > 1 else Path(member.name)
                    dest = target_dir / rel
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy(member, dest)
                    extracted += 1
                    if extracted >= args.limit:
                        break
                target_path = target_dir
                data_rows = extracted
            else:
                text_file = None
                for f in extract_dir.rglob("*"):
                    if f.is_file():
                        mime, _ = mimetypes.guess_type(str(f))
                        if mime and (mime.startswith("text") or mime == "application/octet-stream"):
                            text_file = f
                            break
                if not text_file:
                    print("No text file found in archive.")
                    return
                target_path = _resolve_out_path(out_path, text_file.name)
                data_rows = 0
                with open(text_file, "rt", encoding="utf-8", errors="replace") as src, open(target_path, "wt", encoding="utf-8") as out:
                    for line in src:
                        if data_rows >= args.limit:
                            break
                        out.write(line)
                        data_rows += 1
        # If this looks like a UniProt .dat sample, ensure terminator
        try:
            if not looks_like_dir and ('uniprot' in args.input.lower() or 'uniprot' in target_path.name.lower() or args.input.lower().endswith('.dat') or args.input.lower().endswith('.dat.gz')):
                ensure_file_terminator(target_path)
        except Exception:
            pass
        try:
            if not looks_like_dir:
                _maybe_normalize_downloaded_file(target_path)
        except Exception:
            pass
        print(f"Wrote {data_rows} rows to {target_path}")
    else:
        try:
            if parsed.scheme in {"http", "https"}:
                data_rows, target_path = stream_sample_http(args.input, out_path, args.limit, args.no_header, timeout=args.timeout, filter_str=args.filter)
                print(f"Wrote {data_rows} rows to {target_path}")
            else:
                data_rows, target_path = sample_local_file(Path(args.input), out_path, args.limit, args.no_header, filter_str=args.filter)
                print(f"Wrote {data_rows} rows to {target_path}")
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            raise SystemExit(2)



if __name__ == "__main__":
    main()

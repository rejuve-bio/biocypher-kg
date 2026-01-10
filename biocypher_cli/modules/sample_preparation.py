"""Sample preparation utilities for BioCypher CLI"""
import gzip
import logging
import os
import subprocess
import sys
import tempfile
import yaml
from pathlib import Path
from typing import Dict, List, Optional, Union
from urllib.parse import urlparse

from rich.console import Console
from rich.panel import Panel
from questionary import text as qtext

from .normalization import normalize_file_format as _normalize_file_format

PROJECT_ROOT = Path(__file__).parent.parent.parent

logger = logging.getLogger(__name__)
console = Console()


def find_url_for_filename(config_path: str, filename: str) -> Optional[str]:
    """
    Search all URLs in the data_source_config YAML for a URL ending with the given filename.
    Case-insensitive compare so sample suffix casing mismatches do not break lookup.
    Handles URLs as strings, lists, or dicts.
    """
    config_path = Path(config_path)
    if not config_path.exists():
        return None

    raw_name = filename.strip()

    def _variants(name: str) -> set:
        name_l = name.lower()
        variants = {name_l, Path(name_l).name, Path(name_l).stem}
        if name_l.startswith("sample_"):
            variants.add(name_l[len("sample_"):])
        for suffix in ("_sample", "-sample"):
            if suffix in name_l:
                variants.add(name_l.replace(suffix, ""))
        if "_" in name_l:
            parts = name_l.split("_", 1)
            if len(parts) == 2 and parts[1]:
                variants.add(parts[1])
        return {v.strip() for v in variants if v}

    needles = _variants(raw_name)

    def _matches(url: str) -> bool:
        from urllib.parse import urlparse

        url_l = url.strip().lower()
        url_name = Path(urlparse(url_l).path).name
        url_variants = _variants(url_name) | {url_name}
        return bool(needles & url_variants) or any(url_l.endswith(n) for n in needles)

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    for entry in config.values():
        urls = entry.get("url")
        if not urls:
            continue
        if isinstance(urls, str):
            if _matches(urls):
                return urls.strip()
        elif isinstance(urls, list):
            for u in urls:
                if isinstance(u, str) and _matches(u):
                    return u.strip()
                elif isinstance(u, dict):
                    for suburl in u.values():
                        if isinstance(suburl, str) and _matches(suburl):
                            return suburl.strip()
        elif isinstance(urls, dict):
            for suburl in urls.values():
                if isinstance(suburl, str) and _matches(suburl):
                    return suburl.strip()
                elif isinstance(suburl, list):
                    for u in suburl:
                        if isinstance(u, str) and _matches(u):
                            return u.strip()
    return None

def check_and_prepare_samples(
    adapters_config_path: str,
    selected_adapters: List[str] = None,
    limit: int = 1000,
    return_skipped: bool = False,
) -> List[str]:
    """Ensure sample files referenced by the adapters config exist.
    If a sample is missing, attempt to find a download URL in an organism-specific
    data source config (e.g. `config/hsa/hsa_data_source_config.yaml`) and run
    `scripts/download_and_sample.py` to create a sampled file.
    and run `scripts/download_and_sample.py` to create a sampled file. Returns list of created files.
    """
    created: List[str] = []
    skipped: List[str] = []
    try:
        config_path = Path(adapters_config_path)
        if not config_path.exists():
            console.print(f"[yellow]Adapters config not found: {config_path}[/]")
            return created
        with open(config_path) as f:
            adapters = yaml.safe_load(f) or {}
    except Exception as e:
        console.print(f"[red]Failed to read adapters config: {e}[/]")
        return created

    def _choose_data_source_config(adapters_cfg: Path) -> Path:
        """Pick the best data source config for this adapters config."""
        cfg_dir = PROJECT_ROOT / "config"

        # Prefer organism-specific config based on adapters config location/name
        cfg_str = str(adapters_cfg).lower()
        if "/hsa/" in cfg_str or cfg_str.endswith("hsa_adapters_config.yaml") or cfg_str.endswith("hsa_adapters_config_sample.yaml"):
            candidate = cfg_dir / "hsa" / "hsa_data_source_config.yaml"
            if candidate.exists():
                return candidate
        if "/dmel/" in cfg_str or cfg_str.endswith("dmel_adapters_config.yaml") or cfg_str.endswith("dmel_adapters_config_sample.yaml"):
            candidate = cfg_dir / "dmel" / "dmel_data_source_config.yaml"
            if candidate.exists():
                return candidate

        # Fallbacks for older layouts
        for candidate in (
            cfg_dir / "data_source_config.yaml",
            cfg_dir / "hsa" / "hsa_data_source_config.yaml",
            cfg_dir / "dmel" / "dmel_data_source_config.yaml",
        ):
            if candidate.exists():
                return candidate

        return cfg_dir / "data_source_config.yaml"

    data_source = {}
    ds_path = _choose_data_source_config(config_path)
    try:
        if ds_path.exists():
            with open(ds_path) as f:
                data_source = yaml.safe_load(f) or {}
    except Exception:
        data_source = {}

    def _candidate_names(name: str) -> List[str]:
        names = set()
        cur = name.lower()
        names.add(cur)

        # Add a variant that strips common sample suffixes but keeps extensions (e.g., foo_sample.csv.gz -> foo.csv.gz)
        for suffix in ("_sample", "-sample"):
            if cur.endswith(suffix):
                parts = cur.split(".")
                if len(parts) > 1:
                    stem = ".".join(parts[:-1])
                    ext = parts[-1]
                    if stem.endswith(suffix):
                        names.add(stem[: -len(suffix)] + "." + ext)


        for marker in ("_sample.", "-sample."):
            if marker in cur:
                names.add(cur.replace(marker, "."))

        def strip_ext(val: str) -> str:
            while True:
                base, ext = os.path.splitext(val)
                if ext.lower() in {".gz", ".gzip", ".bz2", ".zip", ".tsv", ".csv", ".txt", ".bed", ".gtf", ".gff", ".vcf"}:
                    val = base
                else:
                    break
            return val

        stripped = strip_ext(cur)
        names.add(stripped)
        names.add(Path(stripped).stem)

        for variant in list(names):
            if variant.endswith("_sample"):
                names.add(variant[: -len("_sample")])
            if variant.endswith("-sample"):
                names.add(variant[: -len("-sample")])

        for candidate in list(names):
            if candidate.startswith("sample_"):
                names.add(candidate[len("sample_"):])

        return [n for n in names if n]

    def _extract_url(entry, basename: str = None, prefer_dir: bool = False) -> Optional[str]:
        """Normalize url extraction across dict/list/str entries with optional basename hints."""
        if not entry:
            return None

        url_field = entry.get("url") if isinstance(entry, dict) else entry

        def _pick_from_dict(url_dict: Dict[str, str]) -> Optional[str]:
            if prefer_dir:
                for key in ("pwm", "annotation"):
                    if key in url_dict:
                        return url_dict.get(key)

            if isinstance(basename, str):
                lower_name = basename.lower()
                for k, v in url_dict.items():
                    if isinstance(k, str) and k.lower() in lower_name:
                        return v

            vals = list(url_dict.values())
            return vals[0] if vals else None

        if isinstance(url_field, list):
            return url_field[0] if url_field else None
        if isinstance(url_field, dict):
            return _pick_from_dict(url_field)
        if isinstance(url_field, str):
            return url_field
        return None

    def _is_file_sane(path: Path) -> tuple[bool, str]:
        """Level-1 file sanity checks: size>0 and at least one non-blank line.
        Supports plain text and gzip (.gz) files.
        Returns (True, "") when sane, or (False, reason).
        """
        try:
            if not path.exists():
                return False, "file missing"

            if path.is_dir():
                for child in path.rglob('*'):
                    if child.is_file():
                        sane, reason = _is_file_sane(child)
                        if sane:
                            return True, "directory contains at least one sane file"
                return False, "directory contains no sane files"

            size = path.stat().st_size
            if size == 0:
                return False, "file size is 0 bytes"

            open_fn = gzip.open if path.suffix == ".gz" else open
            with open_fn(path, "rt", errors="ignore") as fh:
                for line in fh:
                    stripped = line.strip()
                    if not stripped:
                        continue
                    lower = stripped.lower()
                    if lower.startswith("<!doctype") or lower.startswith("<html"):
                        return False, "file looks like HTML"

                    # Special-case: RNAcentral GO/Rfam annotations must be 3-column TSV:
                    #   <URS..._taxid>\tGO:...\tRfam:RF....
                    if "rnacentral_rfam_annotations" in path.name.lower():
                        parts = stripped.split("\t")
                        if len(parts) != 3:
                            return False, f"RNAcentral annotations expected 3 columns, got {len(parts)}"
                        if not parts[1].startswith("GO:"):
                            return False, f"RNAcentral annotations expected GO term in column 2, got '{parts[1]}'"
                        if not (parts[2].startswith("Rfam:") or parts[2].startswith("RF")):
                            return False, f"RNAcentral annotations expected Rfam:RF... in column 3, got '{parts[2]}'"

                    return True, ""
            return False, "no non-blank lines found"
        except Exception as e:
            return False, f"exception while validating file: {e}"

    for name, conf in (adapters.items() if isinstance(adapters, dict) else []):
        if selected_adapters and name not in selected_adapters:
            continue
        args = {}
        try:
            args = conf.get("adapter", {}).get("args", {}) or {}
        except Exception:
            args = {}

        # Apply general file format normalization for all file paths
        file_fields = []
        for k, v in args.items():
            if isinstance(v, str) and ("samples" in v or k.lower().find("file") != -1 or k.lower().find("path") != -1):
                file_fields.append(v)
                file_path = Path(v)
                if not file_path.is_absolute():
                    file_path = PROJECT_ROOT / file_path
                if file_path.exists():
                    _normalize_file_format(file_path)

        for fp in file_fields:
            out_path = Path(fp)
            if not out_path.is_absolute():
                out_path = PROJECT_ROOT / out_path

            looks_like_dir = out_path.suffix == "" or str(fp).endswith(os.sep)

            if out_path.exists():
                sane, reason = _is_file_sane(out_path)
                if sane:
                    continue
                else:
                    console.print(f"[yellow]Existing sample {out_path} failed sanity check: {reason}. Will attempt to recreate.[/]")

            basename = Path(fp).name
            def _lookup_url(basename: str, adapter_name: str, looks_like_dir: bool) -> Optional[str]:
                cfg_path = str(ds_path)

                # Prefer exact adapter-specific entry before heuristic filename matching.
                entry = data_source.get(adapter_name) or data_source.get(adapter_name.lower())
                if not entry:
                    # Check for substring matches with adapter_name
                    for ds_key, ds_entry in data_source.items():
                        if ds_key in adapter_name or adapter_name in ds_key:
                            entry = ds_entry
                            break
                if entry:
                    if looks_like_dir:
                        url_local = _extract_url(entry, basename=basename, prefer_dir=True)
                        if url_local:
                            return url_local

                    url_local = _extract_url(entry, basename=basename)
                    if url_local:
                        return url_local

                url_local = find_url_for_filename(cfg_path, basename)
                if url_local:
                    return url_local

                for cand in _candidate_names(basename):
                    cand_no_ext = Path(cand).stem
                    url_local = find_url_for_filename(cfg_path, cand)
                    if url_local:
                        return url_local
                    for ds_key, entry in data_source.items():
                        key_norm = ds_key.lower()
                        if (
                            key_norm == cand_no_ext
                            or key_norm == cand
                            or cand.startswith(key_norm)
                            or key_norm.startswith(cand)
                            or key_norm in cand
                            or cand in key_norm
                        ):
                            url_local = _extract_url(entry, basename=basename, prefer_dir=looks_like_dir)
                            if url_local:
                                return url_local

                return None

            url = _lookup_url(basename, name, looks_like_dir)

            if not url and looks_like_dir:
                # Directory target with no URL even after lookup; ensure it exists and skip download
                out_path.mkdir(parents=True, exist_ok=True)
                continue

            if not url:
                console.print(Panel.fit(f"[yellow]Sample for adapter '{name}' is missing: {out_path}\nSearched for URL in: {ds_path}[/]"))
                val = qtext(f"Enter input URL or local path to download for '{name}' (leave blank to skip):").unsafe_ask()
                if not val:
                    console.print(f"[dim]Skipping adapter {name} (no input provided).[/]")
                    skipped.append(name)
                    continue
                url = val

            # run download_and_sample.py
            # UniProt samples are record-based and can be huge; keep them small by default.
            effective_limit = limit
            url_l = str(url).lower()
            out_l = str(out_path).lower()
            if "uniprot" in url_l or "uniprot" in out_l or url_l.endswith(".dat") or url_l.endswith(".dat.gz"):
                effective_limit = min(effective_limit, 200)

            cmd = [
                sys.executable,
                str(PROJECT_ROOT / "scripts" / "download_and_sample.py"),
                "--input",
                str(url),
                "--output",
                str(out_path),
                "--limit",
                str(effective_limit),
            ]
            timeout_env = os.environ.get("BIOCYPHER_DOWNLOAD_TIMEOUT")
            if timeout_env:
                cmd.extend(["--timeout", timeout_env])
            console.print(Panel.fit(f"[blue]Preparing sample for '{name}' by running download_and_sample.py[/]"))
            env = os.environ.copy()
            env['PYTHONPATH'] = str(PROJECT_ROOT)
            try:
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, cwd=str(PROJECT_ROOT), env=env)
                output_lines: List[str] = []
                for line in proc.stdout:
                    console.print(f"[dim]{line.rstrip()}[/]")
                    output_lines.append(line)
                proc.wait()

                def _handle_result(returncode, out_path, attempt_url):
                    if returncode == 0 and out_path.exists():
                        sane, reason = _is_file_sane(out_path)
                        if sane:
                            console.print(f"[green]Wrote sample file: {out_path}[/]")
                            created.append(str(out_path))
                            return True
                        else:
                            console.print(f"[red]Sample created but failed sanity check ({reason}): {out_path}[/]")
                            return False
                    else:
                        console.print(f"[red]Failed to create sample for adapter {name} using {attempt_url}[/]")
                        return False

                success = _handle_result(proc.returncode, out_path, url)
                if not success:
                    out_joined = "".join(output_lines).lower()
                    is_timeout = ("timeouterror" in out_joined) or ("timed out" in out_joined)
                    if is_timeout:
                        console.print(
                            f"[yellow]Timed out while preparing sample for {name}. Skipping this adapter. "
                            f"(Tip: increase BIOCYPHER_DOWNLOAD_TIMEOUT to retry.)[/]"
                        )
                        skipped.append(name)
                        continue

                    alt_val = qtext(f"Enter alternate input URL or local path for '{name}' (leave blank to skip):").unsafe_ask()
                    if not alt_val:
                        console.print(f"[dim]Skipping adapter {name} (no alternative provided).[/]")
                        skipped.append(name)
                    else:
                        retry_cmd = [sys.executable, str(PROJECT_ROOT / "scripts" / "download_and_sample.py"), "--input", str(alt_val), "--output", str(out_path), "--limit", str(limit)]
                        timeout_env = os.environ.get("BIOCYPHER_DOWNLOAD_TIMEOUT")
                        if timeout_env:
                            retry_cmd.extend(["--timeout", timeout_env])
                        console.print(Panel.fit(f"[blue]Retrying sample preparation for '{name}' with provided input[/]"))
                        try:
                            proc2 = subprocess.Popen(retry_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, cwd=str(PROJECT_ROOT), env=env)
                            for line in proc2.stdout:
                                console.print(f"[dim]{line.rstrip()}[/]")
                            proc2.wait()
                            if not _handle_result(proc2.returncode, out_path, alt_val):
                                skipped.append(name)
                        except Exception as e:
                            console.print(f"[red]Retry failed for {name}: {e}[/]")
                            skipped.append(name)
            except Exception as e:
                console.print(f"[red]Error running download_and_sample for {name}: {e}[/]")
                skipped.append(name)

    if return_skipped:
        return created, skipped
    return created
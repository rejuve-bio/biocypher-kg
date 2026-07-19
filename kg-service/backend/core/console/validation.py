"""Validate a proposed build before it runs.

Two layers:
  1. static  — in-process, fast: mode/writer/config-existence/adapter-name checks.
  2. authoritative — shells out to `create_knowledge_graph.py --check-only`
     (runs no adapters) to verify every declared input path exists, exactly as a
     real build would. This is the source of truth for ``missing_paths``.
"""
from __future__ import annotations

import logging
import re
import subprocess
from pathlib import Path
from typing import Optional

from backend.core.config import settings
from backend.core.console import config_introspect as ci
from backend.core.console.job_models import BuildRequest
from backend.core.console.job_runner import build_argv, check_only_argv

logger = logging.getLogger(__name__)

_LOG_PREFIX_RE = re.compile(r"^(?:INFO|ERROR|WARNING|DEBUG)\s+--\s+")
_ADAPTER_RE = re.compile(r"^\[(.+)\]$")
_ARG_RE = re.compile(r"^(.+?):\s+(.+)$")


def _strip_log_prefix(line: str) -> str:
    return _LOG_PREFIX_RE.sub("", line).rstrip()


def _parse_missing_paths(output: str) -> dict[str, dict[str, str]]:
    """Parse the grouped missing-path report emitted by _report_missing_paths."""
    missing: dict[str, dict[str, str]] = {}
    current: Optional[str] = None
    for raw_line in output.splitlines():
        line = _strip_log_prefix(raw_line).strip()
        if not line:
            continue
        m_adapter = _ADAPTER_RE.match(line)
        if m_adapter:
            current = m_adapter.group(1)
            missing.setdefault(current, {})
            continue
        if current is None or line.startswith("Pre-flight") or line.startswith("Fix"):
            continue
        m_arg = _ARG_RE.match(line)
        if m_arg:
            missing[current][m_arg.group(1).strip()] = m_arg.group(2).strip()
    return {name: args for name, args in missing.items() if args}


def _resolve(path_str: str) -> Path:
    p = Path(path_str)
    return p if p.is_absolute() else (settings.repo_root_path / p)


def validate_build(req: BuildRequest, run_check_only: bool = True) -> dict:
    """Validate ``req``. Returns the response dict described in the plan."""
    static_errors: list[str] = []
    static_warnings: list[str] = []
    missing_paths: dict[str, dict[str, str]] = {}

    # --- writer ---
    if req.writer_type.lower() not in ci.WRITER_TYPES:
        static_errors.append(
            f"Unknown writer_type '{req.writer_type}'. Known: {ci.WRITER_TYPES}"
        )

    # --- mode resolution ---
    adapters_config_abs: Optional[str] = None
    schema_config_abs: Optional[str] = None
    adapters_dict: Optional[dict] = None
    num_adapters: Optional[int] = None

    if req.species and req.species.lower() == "all":
        # All-species run: each species uses its own config, so there's no single
        # adapters config to introspect. The CLI validates per species at run time.
        if req.dataset not in ("sample", "full"):
            static_errors.append("dataset must be 'sample' or 'full' for an all-species run.")
        static_warnings.append(
            "All-species run: adapters and input-path validation are skipped here; "
            "each species runs sequentially with its own config, and species without "
            f"the '{req.dataset}' dataset are skipped."
        )
    elif req.species:
        try:
            adapters_config_abs = str(ci.resolve_adapters_config_path(req.species, req.dataset))
            schema_config_abs = str(ci.resolve_schema_config_path(req.species, req.dataset))
            adapters_dict, _ = ci.load_adapters_dict(req.species, req.dataset)
        except ci.ConfigError as exc:
            static_errors.append(str(exc))
    elif req.adapters_config and req.schema_config:
        ac, sc = _resolve(req.adapters_config), _resolve(req.schema_config)
        if not ac.exists():
            static_errors.append(f"adapters_config not found: {ac}")
        else:
            adapters_config_abs = str(ac)
            try:
                loader = ci._load_yaml_with_includes()
                loaded = loader(str(ac)) or {}
                loaded.pop("input_dir", None)
                adapters_dict = loaded
            except Exception as exc:  # noqa: BLE001 - surface any parse error to the user
                static_errors.append(f"Could not parse adapters_config: {exc}")
        if not sc.exists():
            static_errors.append(f"schema_config not found: {sc}")
        else:
            schema_config_abs = str(sc)
    else:
        static_errors.append(
            "Provide either 'species' (+ 'dataset') or both 'adapters_config' and "
            "'schema_config'."
        )

    # --- include_adapters existence (case-insensitive, matching the CLI) ---
    if adapters_dict is not None:
        num_adapters = len(adapters_dict)
        if req.include_adapters:
            known = {k.lower() for k in adapters_dict}
            unknown = [a for a in req.include_adapters if a.lower() not in known]
            if unknown:
                static_errors.append(
                    f"Unknown adapters (not in config): {unknown}"
                )
            else:
                num_adapters = len(
                    [k for k in adapters_dict if k.lower() in
                     {a.lower() for a in req.include_adapters}]
                )

    # --- dbSNP requirement for non-sample species-mode runs ---
    # The pipeline calls _load_dbsnp unconditionally in species mode and hard-exits
    # for a non-sample run unless BOTH the cache root and a common|full variant are
    # set (from the request or species_config.yaml). Enforce that up front.
    if req.species and req.dataset != "sample":
        try:
            entry = ci._dataset_entry(req.species, req.dataset)
        except ci.ConfigError:
            entry = {}
        eff_root = (req.dbsnp_cache_root or entry.get("dbsnp_cache_root") or "").strip()
        eff_variant = (req.dbsnp_variant or entry.get("dbsnp_variant") or "").strip()
        if not eff_root:
            static_errors.append(
                "dbSNP cache root is required for non-sample runs — provide the dbSNP "
                "cache path (or set dbsnp_cache_root in species_config.yaml)."
            )
        if not eff_variant:
            static_errors.append(
                "dbSNP variant is required for non-sample runs — choose 'common' or 'full'."
            )
        elif eff_variant not in ("common", "full"):
            static_errors.append(
                f"dbSNP variant must be 'common' or 'full' (got '{eff_variant}')."
            )

    # --- authoritative path check via --check-only ---
    ran_check_only = False
    if run_check_only and adapters_config_abs and not static_errors:
        argv = check_only_argv(adapters_config_abs, req.include_adapters, req.input_dir)
        try:
            result = subprocess.run(
                argv, cwd=str(settings.repo_root_path),
                capture_output=True, text=True, timeout=60,
            )
            ran_check_only = True
            combined = (result.stdout or "") + "\n" + (result.stderr or "")
            if result.returncode != 0:
                missing_paths = _parse_missing_paths(combined)
                if not missing_paths:
                    static_errors.append(
                        "Pre-flight check failed (see build tooling). "
                        f"stderr tail: {combined.strip()[-500:]}"
                    )
        except subprocess.TimeoutExpired:
            static_warnings.append("Pre-flight --check-only timed out; skipped.")
        except (OSError, ValueError) as exc:
            static_warnings.append(f"Could not run --check-only ({exc}); skipped.")

    # --- assemble ---
    cmd_preview = build_argv(req, req.output_dir or "<assigned-at-launch>")
    valid = not static_errors and not missing_paths
    return {
        "valid": valid,
        "static_errors": static_errors,
        "static_warnings": static_warnings,
        "missing_paths": missing_paths,
        "checked_paths": ran_check_only,
        "resolved": {
            "adapters_config": adapters_config_abs,
            "schema_config": schema_config_abs,
            "num_adapters": num_adapters,
            "cmd_preview": cmd_preview,
        },
    }

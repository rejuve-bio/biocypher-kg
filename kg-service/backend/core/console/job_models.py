"""Data models for the Console: the build request and the tracked build job."""
from __future__ import annotations

import dataclasses
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


TERMINAL_STATUSES = {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED}


class BuildRequest(BaseModel):
    """A proposed knowledge-graph build (species mode or manual adapters/schema config)."""
    # species mode
    species: Optional[str] = None
    dataset: str = "sample"

    # manual mode (paths confined under REPO_ROOT)
    adapters_config: Optional[str] = None
    schema_config: Optional[str] = None

    # common options
    include_adapters: Optional[list[str]] = None
    writer_type: str = "metta"
    output_dir: Optional[str] = None
    input_dir: Optional[str] = None
    dbsnp_cache_root: Optional[str] = None
    dbsnp_variant: Optional[str] = None

    # toggleable flags (defaults match the CLI)
    write_properties: bool = True
    add_provenance: bool = True
    include_taxon_id: bool = True
    include_curie: bool = False
    skip_preflight: bool = False
    generate_data_source_schemas: bool = True


@dataclasses.dataclass
class BuildJob:
    id: str
    status: JobStatus
    params: dict
    cmd: list[str]
    cwd: str
    output_dir: str
    log_path: str
    kind: str = "build"              # "build" | "load-neo4j" | "load-mork"
    total_adapters: Optional[int] = None
    pid: Optional[int] = None
    return_code: Optional[int] = None
    created_at: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    error: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        d = dataclasses.asdict(self)
        d["status"] = self.status.value
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "BuildJob":
        d = dict(d)
        d["status"] = JobStatus(d["status"])
        field_names = {f.name for f in dataclasses.fields(cls)}
        return cls(**{k: v for k, v in d.items() if k in field_names})

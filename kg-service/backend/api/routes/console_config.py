"""Console configuration introspection + editing endpoints."""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend.core.console import config_introspect as ci

router = APIRouter(prefix="/api/console", tags=["Console"])


class ConfigWrite(BaseModel):
    content: str


@router.get("/species")
def get_species():
    """List species and their datasets, with config-file existence flags."""
    try:
        return {"species": ci.list_species_and_datasets()}
    except ci.ConfigError as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/species/{species}/datasets/{dataset}/adapters")
def get_adapters(species: str, dataset: str):
    """List adapters declared for a species/dataset."""
    try:
        return ci.list_adapters(species, dataset)
    except ci.ConfigError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.get("/species/{species}/datasets/{dataset}/schema")
def get_schema(species: str, dataset: str):
    """Shallow schema view (node/edge type names + per-source schema files)."""
    try:
        return ci.list_schema(species, dataset)
    except ci.ConfigError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.get("/species/{species}/datasets/{dataset}/config/{kind}")
def get_config(species: str, dataset: str, kind: str):
    """Raw text of the adapters or schema config for editing (kind: adapters|schema)."""
    try:
        return ci.read_config_text(species, dataset, kind)
    except ci.ConfigError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@router.put("/species/{species}/datasets/{dataset}/config/{kind}")
def put_config(species: str, dataset: str, kind: str, body: ConfigWrite):
    """Validate and atomically save an edited config file (history is git's job)."""
    try:
        return ci.save_config_text(species, dataset, kind, body.content)
    except ci.ConfigError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/writers")
def get_writers():
    return {"writers": ci.list_writers()}


@router.get("/flags")
def get_flags():
    return {"flags": ci.list_flags()}

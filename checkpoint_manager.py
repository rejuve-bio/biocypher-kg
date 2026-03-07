"""
Checkpoint Manager for BioCypher-KG pipeline.

Stores and loads pipeline state so that interrupted runs can be resumed
from the last successfully completed adapter rather than starting over.

Checkpoint file: <output_dir>/kg_checkpoint.json
"""

import json
import os
import shutil
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

from biocypher._logger import logger

CHECKPOINT_FILENAME = "kg_checkpoint.json"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _serialize(obj):
    """Make Counter / defaultdict / set JSON-serialisable."""
    if isinstance(obj, (Counter, dict)):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, set):
        return list(obj)
    return obj


def _deserialize_nodes_count(raw: dict) -> Counter:
    return Counter(raw)


def _deserialize_nodes_props(raw: dict) -> defaultdict:
    result = defaultdict(set)
    for k, v in raw.items():
        result[k] = set(v)
    return result


def _deserialize_edges_count(raw: dict) -> Counter:
    return Counter(raw)


def _deserialize_datasets_dict(raw: dict) -> dict:
    result = {}
    for ds_name, ds_data in raw.items():
        result[ds_name] = {
            **ds_data,
            "nodes": set(ds_data.get("nodes", [])),
            "edges": set(ds_data.get("edges", [])),
        }
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class CheckpointManager:
    """
    Manages a JSON checkpoint file inside the pipeline output directory.

    Checkpoint schema
    -----------------
    {
        "pipeline_id": "<output_dir>::<adapters_config>",
        "created_at": "<ISO timestamp>",
        "updated_at": "<ISO timestamp>",
        "completed_adapters": ["adapter_a", "adapter_b", ...],
        "failed_adapter": "<adapter_name> | null",
        "nodes_count": {...},
        "nodes_props":  {...},    # lists (serialised sets)
        "edges_count":  {...},
        "datasets_dict": {...}
    }
    """

    def __init__(self, output_dir: Path, pipeline_id: str):
        self.output_dir = Path(output_dir)
        self.pipeline_id = pipeline_id
        self.checkpoint_path = self.output_dir / CHECKPOINT_FILENAME
        self._state: Optional[dict] = None

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def exists(self) -> bool:
        return self.checkpoint_path.exists()

    def load(self) -> bool:
        """
        Load an existing checkpoint.
        Returns True when a usable checkpoint was found, False otherwise.
        """
        if not self.exists():
            return False
        try:
            with open(self.checkpoint_path, "r") as f:
                self._state = json.load(f)
            if self._state.get("pipeline_id") != self.pipeline_id:
                logger.warning(
                    "Checkpoint pipeline_id mismatch — ignoring stale checkpoint.\n"
                    f"  Checkpoint: {self._state.get('pipeline_id')}\n"
                    f"  Current:    {self.pipeline_id}"
                )
                self._state = None
                return False
            logger.info(
                f"Checkpoint loaded: {len(self._state['completed_adapters'])} adapter(s) already done."
            )
            return True
        except (json.JSONDecodeError, KeyError) as exc:
            logger.warning(f"Could not parse checkpoint file ({exc}). Starting fresh.")
            self._state = None
            return False

    def save(
        self,
        completed_adapters: list,
        nodes_count: Counter,
        nodes_props: defaultdict,
        edges_count: Counter,
        datasets_dict: dict,
        failed_adapter: Optional[str] = None,
    ):
        """Atomically write the checkpoint file."""
        now = datetime.utcnow().isoformat()
        state = {
            "pipeline_id": self.pipeline_id,
            "created_at": self._state.get("created_at", now) if self._state else now,
            "updated_at": now,
            "completed_adapters": completed_adapters,
            "failed_adapter": failed_adapter,
            "nodes_count": _serialize(nodes_count),
            "nodes_props": _serialize(nodes_props),
            "edges_count": _serialize(edges_count),
            "datasets_dict": _serialize(datasets_dict),
        }
        # Write atomically via a temp file
        tmp = self.checkpoint_path.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(state, f, indent=2)
        shutil.move(str(tmp), str(self.checkpoint_path))
        self._state = state

    def delete(self):
        """Remove checkpoint after a successful full run."""
        if self.checkpoint_path.exists():
            self.checkpoint_path.unlink()
            logger.info("Checkpoint file removed (pipeline completed successfully).")

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    @property
    def completed_adapters(self) -> list:
        if self._state is None:
            return []
        return self._state.get("completed_adapters", [])

    def restore_accumulators(self):
        """Return (nodes_count, nodes_props, edges_count, datasets_dict) from checkpoint."""
        if self._state is None:
            return Counter(), defaultdict(set), Counter(), {}
        return (
            _deserialize_nodes_count(self._state.get("nodes_count", {})),
            _deserialize_nodes_props(self._state.get("nodes_props", {})),
            _deserialize_edges_count(self._state.get("edges_count", {})),
            _deserialize_datasets_dict(self._state.get("datasets_dict", {})),
        )


# ---------------------------------------------------------------------------
# Interactive prompt
# ---------------------------------------------------------------------------

def prompt_resume_or_restart(checkpoint_manager: CheckpointManager) -> bool:
    """
    Ask the user interactively whether to resume or start over.

    Returns True  → resume from checkpoint
            False → start over (checkpoint will be deleted)
    """
    completed = checkpoint_manager.completed_adapters
    failed = checkpoint_manager._state.get("failed_adapter")
    updated_at = checkpoint_manager._state.get("updated_at", "unknown")

    print("\n" + "=" * 60)
    print("  CHECKPOINT DETECTED")
    print("=" * 60)
    print(f"  Last saved : {updated_at}")
    print(f"  Completed  : {len(completed)} adapter(s)")
    if completed:
        for a in completed:
            print(f"               ✓ {a}")
    if failed:
        print(f"  Failed on  : ✗ {failed}")
    print("=" * 60)

    while True:
        answer = input("\nResume from checkpoint? [Y]es / [N]o (start over): ").strip().lower()
        if answer in ("y", "yes", ""):
            logger.info("Resuming from checkpoint.")
            return True
        if answer in ("n", "no"):
            checkpoint_manager.delete()
            logger.info("Starting over — checkpoint deleted.")
            return False
        print("Please enter Y or N.")
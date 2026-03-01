"""YAML loader with support for a simple `!include` directive.

This module adds a custom SafeLoader that understands `!include <relative/or/absolute/path>`.
The include path is resolved relative to the YAML file that contains the directive.

Example:
    # config/test_config.yaml
    !include hsa/hsa_adapters_config_sample.yaml

    # python
    from config.yaml_loader import load_yaml_with_includes
    cfg = load_yaml_with_includes("config/test_config.yaml")
"""
from __future__ import annotations

from pathlib import Path
from typing import TextIO, Union, Any

import yaml


class IncludeLoader(yaml.SafeLoader):
    """A SafeLoader that tracks the directory of the YAML being parsed."""

    def __init__(self, stream: TextIO):
        self._root = Path(getattr(stream, "name", Path.cwd())).resolve().parent
        super().__init__(stream)


def _include_constructor(loader: IncludeLoader, node: yaml.Node) -> Any:
    include_value = loader.construct_scalar(node)
    include_path = Path(include_value)

    if not include_path.is_absolute():
        include_path = (loader._root / include_path).resolve()

    if not include_path.exists():
        raise FileNotFoundError(
            f"Included file not found: {include_path} (from {loader._root})"
        )

    with include_path.open("r", encoding="utf-8") as handle:
        return yaml.load(handle, IncludeLoader)


yaml.add_constructor("!include", _include_constructor, Loader=IncludeLoader)


def load_yaml_with_includes(stream: Union[str, Path, TextIO]) -> Any:
    """Load YAML with `!include` support.

    Args:
        stream: path-like or open file handle.

    Returns:
        Parsed YAML content.
    """

    if isinstance(stream, (str, Path)):
        with Path(stream).open("r", encoding="utf-8") as handle:
            return yaml.load(handle, IncludeLoader)

    return yaml.load(stream, IncludeLoader)

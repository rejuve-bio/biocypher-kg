"""Configuration management"""
import yaml
from pathlib import Path
from typing import List, Dict, Optional, Union
from questionary import select, confirm, checkbox, text
from .utils import console, PROJECT_ROOT, get_file_selection, find_config_files, find_aux_files, get_available_adapters, PathValidator
from .adapters import build_custom_adapter_config

def configuration_workflow(organism: str) -> Optional[Dict[str, Union[str, List[str]]]]:
    config_files = find_config_files(organism)
    aux_files = find_aux_files(organism)
    selections = {}
    
    default_output = str(PROJECT_ROOT / f"output_{'human' if organism == 'human' else 'fly'}")
    while True:
        selections["--output-dir"] = text("Enter output directory:", default=default_output, validate=PathValidator).unsafe_ask()
        if confirm(f"Use '{selections['--output-dir']}' as output directory?", default=True).unsafe_ask(): break
    
    while True:
        choice = select("Adapter configuration method:", choices=[{"name": "📁 Use existing config", "value": "existing"}, {"name": "🛠️ Create custom adapter", "value": "custom"}, "🔙 Back"], pointer="→").unsafe_ask()
        if choice == "🔙 Back": return None
        elif choice == "existing":
            result = get_file_selection("Select adapters config:", config_files, allow_multiple=False, allow_custom=True)
            if result: selections["--adapters-config"] = result; break
        elif choice == "custom":
            custom_config = build_custom_adapter_config()
            if custom_config: selections["--adapters-config"] = custom_config; break
    
    if choice == "existing":
        adapters = get_available_adapters(selections["--adapters-config"])
        if adapters:
            selected_adapters = checkbox("Select adapters to include:", choices=adapters, instruction="(Space to select, Enter to confirm)").unsafe_ask()
            if selected_adapters: selections["--include-adapters"] = selected_adapters
    
    while True:
        result = get_file_selection("Select dbSNP rsIDs file:", aux_files, allow_multiple=False, allow_custom=True)
        if result is None: continue
        selections["--dbsnp-rsids"] = result; break
    
    while True:
        result = get_file_selection("Select dbSNP positions file:", aux_files, allow_multiple=False, allow_custom=True)
        if result is None: continue
        selections["--dbsnp-pos"] = result; break
    
    selections["--writer-type"] = select("Select output format:", choices=["neo4j", "metta", "prolog", "parquet", "KGX","networkx"], default="neo4j").unsafe_ask()
    selections["--add-provenance"] = not confirm("Skip adding provenance?", default=False).unsafe_ask()
    selections["--write-properties"] = confirm("Write properties?", default=True).unsafe_ask()
    return selections

def build_command_from_selections(selections: Dict[str, Union[str, List[str]]]) -> List[str]:
    cmd = ["python3", str(PROJECT_ROOT / "create_knowledge_graph.py")]
    cmd.extend(["--output-dir", selections["--output-dir"]])
    cmd.extend(["--adapters-config", selections["--adapters-config"]])
    if "--include-adapters" in selections:
        for adapter in selections["--include-adapters"]: cmd.extend(["--include-adapters", adapter])
    cmd.extend(["--dbsnp-rsids", selections["--dbsnp-rsids"]])
    cmd.extend(["--dbsnp-pos", selections["--dbsnp-pos"]])
    cmd.extend(["--writer-type", selections["--writer-type"]])
    if not selections["--add-provenance"]: cmd.append("--no-add-provenance")
    if not selections["--write-properties"]: cmd.append("--no-write-properties")
    return cmd
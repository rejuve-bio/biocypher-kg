"""Utility functions and validators"""
import yaml
import logging
import platform
import shutil
import re
from pathlib import Path
from typing import List, Dict, Optional, Union
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from questionary import Validator, ValidationError, select, checkbox, text, confirm

logger = logging.getLogger(__name__)
console = Console()

# Get project root
PROJECT_ROOT = Path(__file__).parent.parent.parent
USER_ADAPTERS_DIR = PROJECT_ROOT / "adapters" / "user_adapters"
USER_ADAPTERS_DIR.mkdir(parents=True, exist_ok=True)

class PathValidator(Validator):
    def validate(self, document):
        if not document.text:
            raise ValidationError(message="Please enter a path", cursor_position=len(document.text))
        path = Path(document.text)
        if not path.exists():
            raise ValidationError(message="Path does not exist", cursor_position=len(document.text))

class YamlValidator(Validator):
    def validate(self, document):
        if not document.text:
            raise ValidationError(message="Please enter YAML content", cursor_position=len(document.text))
        try:
            yaml.safe_load(document.text)
        except yaml.YAMLError as e:
            raise ValidationError(message=f"Invalid YAML: {str(e)}", cursor_position=len(document.text))

class PythonClassValidator(Validator):
    def validate(self, document):
        if not document.text:
            raise ValidationError(message="Please enter a valid Python class name", cursor_position=len(document.text))
        if not re.match(r'^[A-Z][a-zA-Z0-9]*$', document.text):
            raise ValidationError(message="Class name must be PascalCase", cursor_position=len(document.text))

def find_config_files(organism: str = None) -> Dict[str, str]:
    config_dir = PROJECT_ROOT / "config"
    files = {
        "Human - Sample Adapters": str(config_dir / "adapters_config_sample.yaml"),
        "Human - Full Adapters": str(config_dir / "adapters_config.yml"),
        "Fly - Sample Adapters": str(config_dir / "dmel" / "dmel_adapters_config_sample.yaml"),
        "Fly - Full Adapters": str(config_dir / "dmel" / "dmel_adapters_config.yaml"),
        # "Fly - Sample Adapters": str(config_dir / "dmel_adapters_config_sample.yaml"),
        # "Fly - Full Adapters": str(config_dir / "dmel_adapters_config.yml"),
        "Biocypher Config": str(config_dir / "biocypher_config.yml"),
        "Docker Config": str(config_dir / "biocypher_docker_config.yml"),
        "Data Source Config": str(config_dir / "data_source_config.yml"),
        "Download Config": str(config_dir / "download.yml"),
    }
    if organism == "human":
        return {k: v for k, v in files.items() if k.startswith("Human") or "Config" in k}
    elif organism == "fly":
        return {k: v for k, v in files.items() if k.startswith("Fly") or "Config" in k}
    return files

def find_aux_files(organism: str = None) -> Dict[str, str]:
    aux_dir = PROJECT_ROOT / "aux_files"
    files = {
        "Human - Tissues Ontology Map": str(aux_dir / "abc_tissues_to_ontology_map.pkl"),
        "Human - Gene Mapping": str(aux_dir / "gene_mapping.pkl"),
        "Human - Variant Data": str(aux_dir / "variant_data.pkl"),
        "Fly - dbSNP rsIDs": str(aux_dir / "sample_dbsnp_rsids.pkl"),
        "Fly - dbSNP Positions": str(aux_dir / "sample_dbsnp_pos.pkl"),
    }
    if organism == "human":
        return {k: v for k, v in files.items() if k.startswith("Human")}
    elif organism == "fly":
        return {k: v for k, v in files.items() if k.startswith("Fly")}
    return files

def get_available_adapters(config_path: str) -> List[str]:
    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
            if not config: return []
            adapters = []
            for key, value in config.items():
                if isinstance(value, dict) and ('adapter' in value or 'module' in value):
                    adapters.append(key)
                else:
                    adapters.append(key)
            return sorted(adapters)
    except Exception as e:
        logger.error(f"Error loading adapters from {config_path}: {e}")
        return []

def get_file_selection(prompt: str, options: Dict[str, str], allow_multiple: bool = True, allow_custom: bool = True, back_option: bool = True):
    choices = list(options.keys())
    if allow_custom: choices.append("📤 Enter custom path")
    if back_option: choices.append("🔙 Back")
    
    while True:
        if allow_multiple:
            selected = checkbox(prompt, choices=choices, instruction="(Use space to select, enter to confirm)").unsafe_ask()
        else:
            selected = select(prompt, choices=choices).unsafe_ask()
        
        if selected == "🔙 Back": return None
        if not isinstance(selected, list): selected = [selected]
        
        result = []
        for item in selected:
            if item == "📤 Enter custom path":
                custom_path = text("Please enter the full path:", validate=PathValidator).unsafe_ask()
                if custom_path: result.append(custom_path)
            elif item != "🔙 Back":
                result.append(options[item])
        
        if result: return result if allow_multiple else result[0]

def display_config_summary(config: Dict[str, Union[str, List[str]]]) -> None:
    table = Table(title="\nConfiguration Summary", show_header=True, header_style="bold magenta")
    table.add_column("Option", style="cyan"); table.add_column("Value", style="green")
    for key, value in config.items():
        if isinstance(value, list): value = ", ".join(value)
        table.add_row(key, str(value))
    console.print(Panel.fit(table, style="blue"))

def view_system_status() -> None:
    table = Table(title="System Status", show_header=True)
    table.add_column("Component", style="cyan"); table.add_column("Status", style="green")
    required_dirs = [PROJECT_ROOT / "config", PROJECT_ROOT / "aux_files", USER_ADAPTERS_DIR]
    for d in required_dirs:
        status = "✅ Found" if d.exists() else "❌ Missing"
        table.add_row(str(d), status)
    table.add_row("Python Version", platform.python_version())
    total, used, free = shutil.disk_usage("/")
    table.add_row("Disk Space", f"Total: {total // (2**30)}GB, Free: {free // (2**30)}GB")
    console.print(Panel.fit(table))

def show_help() -> None:
    help_text = """
    [bold]BioCypher Knowledge Graph Generator Help[/]
    [underline]Main Features:[/]
    - 🚀 Generate knowledge graphs for Human or Drosophila melanogaster
    - ⚡ Quick start with default configurations
    - 🛠️ Full customization options for advanced users
    - 🐍 Create custom adapters through CLI
    [underline]Workflow:[/]
    1. Select organism (Human or Fly)
    2. Choose default or custom configuration
    3. For custom: Configure each parameter
    4. Review configuration summary
    5. Execute generation
    [underline]Custom Adapters:[/]
    - Create new adapters in adapters/user_adapters/
    - Choose between writing from scratch or using templates
    - Automatically generates config entries
    [underline]Navigation:[/]
    - Use arrow keys to move between options
    - Press Enter to confirm selections
    - Most screens support going back with the '🔙 Back' option
    [underline]Troubleshooting:[/]
    - Ensure all required directories exist
    - Check file permissions if you encounter errors
    - Use the detailed logs option to diagnose problems
    """
    console.print(Panel.fit(help_text, title="Help & Documentation"))

#!/usr/bin/env python3
"""
User-friendly BioCypher_KG CLI_Tool with support for Human and Drosophila melanogaster
"""
import subprocess
import sys
import yaml
import logging
import json
import pickle
import click
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Union
from rich.console import Console
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TaskProgressColumn,
    TimeRemainingColumn,
)
from rich.panel import Panel
from rich.style import Style
from rich.table import Table
from questionary import select, confirm, checkbox, text
import questionary
from questionary import Validator, ValidationError
import time
import platform
import shutil
import tempfile
import os


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

console = Console()
PROJECT_ROOT = Path(__file__).parent.parent


def find_uploaded_files() -> Dict[str, str]:
    common_dirs = [
        PROJECT_ROOT / ".tmp_cli",
        PROJECT_ROOT / "uploads",
        Path("/app/uploads")
    ]

    found = {}
    for folder in common_dirs:
        if folder.exists():
            for file in folder.iterdir():
                if file.is_file():
                    found[file.name] = str(file.resolve())
    return found


def _load_adapters_yaml_template() -> str:
    """
    Load a template for the adapters YAML.
    Prefer config/adapters_config_sample.yaml, then adapters_config.yml.
    Fallback to a minimal working stub if neither exists or is empty.
    """
    candidates = [
        PROJECT_ROOT / "config" / "adapters_config_sample.yaml",
    ]
    for p in candidates:
        try:
            if p.exists() and p.is_file():
                content = p.read_text()
                if content and content.strip():
                    return content
        except Exception:
            pass
    return (
        "# Paste your adapters configuration here. Example:\n"
        "gencode_gene:\n"
        "  adapter:\n"
        "    module: biocypher_metta.adapters.gencode_gene_adapter\n"
        "    cls: GencodeGeneAdapter\n"
        "  args:\n"
        "    filepath: /path/to/gencode.annotation.gtf.gz\n"
        "  outdir: gencode/gene\n"
        "  nodes: true\n"
        "  edges: false\n"
    )

class PathValidator(Validator):
    def validate(self, document):
        if not document.text:
            raise ValidationError(
                message="Please enter a path",
                cursor_position=len(document.text),
            )
        path = Path(document.text)
        if not path.exists():
            raise ValidationError(
                message="Path does not exist",
                cursor_position=len(document.text),
            )

def find_config_files(organism: str = None) -> Dict[str, str]:
    """Find available config files with friendly names"""
    config_dir = PROJECT_ROOT / "config"
    files = {
        # Human configs
        "Human - Sample Adapters": str(config_dir / "adapters_config_sample.yaml"),
        "Human - Full Adapters": str(config_dir / "adapters_config.yml"),
        # Fly configs
        "Fly - Sample Adapters": str(config_dir / "dmel_adapters_config_sample.yaml"),
        "Fly - Full Adapters": str(config_dir / "dmel_adapters_config.yml"),
        # Common configs
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
    """Return friendly-name -> path for known aux files."""
    aux = PROJECT_ROOT / "aux_files"
    options = {
        "Human - dbSNP rsIDs": str(aux / "sample_dbsnp_rsids.pkl"),
        "Human - dbSNP Positions": str(aux / "sample_dbsnp_pos.pkl"),
    }
    # Keep any existing logic if present
    # ...existing code...
    return options

def get_available_adapters(config_path: str) -> List[str]:
    """Get list of available adapters from config file"""
    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
            if not config:
                logger.warning(f"Config file {config_path} is empty")
                return []
            
            adapters = []
            for key, value in config.items():
                if isinstance(value, dict):
                    if 'adapter' in value or 'module' in value:
                        adapters.append(key)
                else:
                    adapters.append(key)
            
            return sorted(adapters)
    except Exception as e:
        logger.error(f"Error loading adapters from {config_path}: {e}")
        return []

def build_default_human_command() -> List[str]:
    """Default command for human data"""
    return [
        "python3", str(PROJECT_ROOT / "create_knowledge_graph.py"),
        "--output-dir", str(PROJECT_ROOT / "output_human"),
        "--adapters-config", str(PROJECT_ROOT / "config/adapters_config_sample.yaml"),
        "--dbsnp-rsids", str(PROJECT_ROOT / "aux_files/abc_tissues_to_ontology_map.pkl"),
        "--dbsnp-pos", str(PROJECT_ROOT / "aux_files/abc_tissues_to_ontology_map.pkl"),
        "--writer-type", "neo4j",
        "--no-add-provenance"
    ]

def build_default_fly_command() -> List[str]:
    """Default command for Drosophila melanogaster"""
    return [
        "python3", str(PROJECT_ROOT / "create_knowledge_graph.py"),
        "--output-dir", str(PROJECT_ROOT / "output_fly"),
        "--adapters-config", str(PROJECT_ROOT / "config/dmel_adapters_config_sample.yaml"),
        "--dbsnp-rsids", str(PROJECT_ROOT / "aux_files/sample_dbsnp_rsids.pkl"),
        "--dbsnp-pos", str(PROJECT_ROOT / "aux_files/sample_dbsnp_pos.pkl"),
        "--writer-type", "neo4j",
        "--no-add-provenance"
    ]

def get_file_selection(
    prompt: str,
    options: Dict[str, str],
    allow_multiple: bool = False,
    allow_custom: bool = True,
    back_option: bool = True
) -> Optional[Union[str, List[str]]]:
    """Generic selector supporting custom paths, multiple choices, and back navigation"""
    choices = list(options.keys())
    
    if allow_custom:
        choices.append("📤 Enter custom path")
    
    if back_option:
        choices.append("🔙 Back")
    
    while True:
        if allow_multiple:
            selected = checkbox(
                prompt,
                choices=choices,
                instruction="(Use space to select, enter to confirm)",
            ).unsafe_ask()
        else:
            selected = select(
                prompt,
                choices=choices,
            ).unsafe_ask()
        
        if selected == "🔙 Back":
            return None
        
        if not isinstance(selected, list):
            selected = [selected]
        
        result = []
        for item in selected:
            if item == "📤 Enter custom path":
                custom_path = text(
                    "Please enter the full path:",
                    validate=PathValidator
                ).unsafe_ask()
                if custom_path:
                    result.append(custom_path)
            elif item != "🔙 Back":
                result.append(options[item])
        
        if result:
            return result if allow_multiple else result[0]
def display_config_summary(config: Dict[str, Union[str, List[str]]]) -> None:
    """Display a summary of the configuration"""
    table = Table(title="\nConfiguration Summary", show_header=True, header_style="bold magenta")
    table.add_column("Option", style="cyan")
    table.add_column("Value", style="green")
    
    for key, value in config.items():
        if isinstance(value, list):
            value = ", ".join(value)
        table.add_row(key, str(value))
    
    console.print(Panel.fit(table, style="blue"))

def run_generation(cmd: List[str], show_logs: bool) -> None:
    """Execute the generation process with enhanced progress tracking"""
    # Ensure output directory exists (create_knowledge_graph expects existing dir)
    try:
        if "--output-dir" in cmd:
            idx = cmd.index("--output-dir")
            out_dir = Path(cmd[idx + 1])
            out_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        console.print(f"[yellow]Warning: could not ensure output dir exists: {e}[/]")
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeRemainingColumn(),
        transient=False,
    ) as progress:
        task = progress.add_task("[cyan]Generating knowledge graph...", total=100)
        
        if show_logs:
            console.print("\n[bold]Running with logs visible:[/]\n")
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=str(PROJECT_ROOT),
                bufsize=1,
                universal_newlines=True
            )
            
            for i in range(10):
                progress.update(task, advance=10)
                for line in process.stdout:
                    if "progress" in line.lower():
                        pass
                    console.print(line, end='')
                time.sleep(0.5)
            
            process.wait()
        else:
            for i in range(10):
                progress.update(task, advance=10)
                time.sleep(0.5)
            
            process = subprocess.run(
                cmd,
                cwd=str(PROJECT_ROOT),
                capture_output=True,
                text=True
            )
        
        progress.stop()
        
        if process.returncode == 0:
            console.print(Panel.fit(
                "[bold green]✔ Knowledge Graph generation completed successfully![/]",
                subtitle="Thank you for using BioCypher!",
                style="green"
            ))
        else:
            console.print(Panel.fit(
                "[bold red]✖ KG generation failed[/]",
                style="red"
            ))
            if not show_logs and confirm("Show error details?", default=False).unsafe_ask():
                console.print(f"\n[red]Error output:[/]\n{process.stderr}")

def build_command_from_selections(selections: Dict[str, Union[str, List[str]]]) -> List[str]:
    """Build the command list from user selections"""
    cmd = ["python3", str(PROJECT_ROOT / "create_knowledge_graph.py")]
    cmd.extend(["--output-dir", selections["--output-dir"]])
    cmd.extend(["--adapters-config", selections["--adapters-config"]])
    
    if "--include-adapters" in selections:
        for adapter in selections["--include-adapters"]:
            cmd.extend(["--include-adapters", adapter])
    
    cmd.extend(["--dbsnp-rsids", selections["--dbsnp-rsids"]])
    cmd.extend(["--dbsnp-pos", selections["--dbsnp-pos"]])
    cmd.extend(["--writer-type", selections["--writer-type"]])
    
    if not selections["--add-provenance"]:
        cmd.append("--no-add-provenance")
    if not selections["--write-properties"]:
        cmd.append("--no-write-properties")
    
    return cmd

def _print_paste_instructions(mode: str, suffix: Optional[str], preview: Optional[str] = None) -> bool:
    """
    Print detailed, step-by-step paste instructions and require user confirmation
    before opening the editor. Returns True if user confirmed, False otherwise.
    """
    if mode == "yaml":
        header = "Paste adapters YAML."
        body = (
            "- Each top-level key is an adapter name.\n"
            "- Required per adapter: adapter.module, adapter.cls, args, outdir, nodes, edges.\n\n"
            "Example:\n"
            "gencode_gene:\n"
            "  adapter:\n"
            "    module: biocypher_metta.adapters.gencode_gene_adapter\n"
            "    cls: GencodeGeneAdapter\n"
            "  args:\n"
            "    filepath: /path/to/gencode.annotation.gtf.gz\n"
            "  outdir: gencode/gene\n"
            "  nodes: true\n"
            "  edges: false\n"
        )
    elif mode == "pkl-json-dict" and (suffix or "").endswith("_rsids.pkl"):
        header = "Paste JSON for dbSNP rsIDs mapping."
        body = 'Format: {"rs123": {"chr": "chr1", "pos": 10177}, ...}\n- chr may be "1" or "chr1"; it will be normalized.\n'
    elif mode == "pkl-json-dict" and (suffix or "").endswith("_pos.pkl"):
        header = "Paste JSON for position→rsID mapping."
        body = 'Format: {"chr1_10177": "rs123", ...}\n- Key must look like "chr<chrom>_<pos>".\n'
    else:
        header = "Paste content"
        body = "The CLI will validate and save the pasted content.\n"

    steps = (
        "Steps before opening the editor:\n"
        "  1) Paste the content according to the format above.\n"
        "  2) Save and close the editor (example: Ctrl+X then Y in nano; :wq in vi).\n"
        "  3) The CLI will validate and save the pasted content to a temporary file.\n"
    )

    preview_text = ""
    if preview:
        preview_text = "\n--- Sample/template (first 10 lines) ---\n" + "\n".join(preview.splitlines()[:10]) + "\n--------------------------------------\n"

    console.print(Panel.fit(f"{header}\n\n{body}\n{steps}\n{preview_text}", style="blue"))

    open_now = confirm("Open editor now to paste content?", default=True).unsafe_ask()
    return bool(open_now)

def _save_pasted_yaml(content: str, suffix: str = ".yaml") -> str:
    data = yaml.safe_load(content)
    if not isinstance(data, dict) or not data:
        raise ValueError("Top-level YAML must be a non-empty mapping")
    for name, cfg in data.items():
        if isinstance(cfg, dict) and "adapter" in cfg:
            a = cfg["adapter"]
            if not isinstance(a, dict) or "module" not in a or "cls" not in a:
                raise ValueError(f"Adapter '{name}' must define adapter.module and adapter.cls")
    tmp_dir = PROJECT_ROOT / ".tmp_cli"
    tmp_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    path = tmp_dir / f"pasted_{ts}{suffix}"
    path.write_text(content)
    return str(path)

def _save_pasted_json_as_pkl(content: str, suffix: str = ".pkl") -> str:
    data = json.loads(content)
    if not isinstance(data, dict):
        raise ValueError("Top-level JSON must be an object (mapping)")
    # Validate by suffix conventions
    if suffix.endswith("_rsids.pkl"):
        for k, v in data.items():
            if not isinstance(v, dict) or "chr" not in v or "pos" not in v:
                raise ValueError(f"Entry '{k}' must map to {{'chr': 'chrN', 'pos': int}}")
            chr_val = str(v["chr"])
            if not chr_val.startswith("chr"):
                chr_val = "chr" + chr_val
            v["chr"] = chr_val
            v["pos"] = int(v["pos"])
    if suffix.endswith("_pos.pkl"):
        key_re = re.compile(r"^chr[0-9XYM]+_[0-9]+$", re.IGNORECASE)
        rs_re = re.compile(r"^rs[0-9]+$", re.IGNORECASE)
        for k, v in data.items():
            if not key_re.match(k):
                raise ValueError(f"Key '{k}' must look like 'chr<chrom>_<pos>'")
            if not isinstance(v, str) or not rs_re.match(v):
                raise ValueError(f"Value for '{k}' must be an rsID like 'rs123'")
    tmp_dir = PROJECT_ROOT / ".tmp_cli"
    tmp_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    path = tmp_dir / f"pasted_{ts}{suffix}"
    with open(path, "wb") as f:
        pickle.dump(data, f)
    return str(path)

def provide_data_input(
    kind: str,
    known_options: Dict[str, str],
    *,
    allow_multiple: bool = False,
    paste_mode: Optional[str] = None,
    paste_title: Optional[str] = None,
    paste_template: Optional[str] = None,
    file_suffix: Optional[str] = None,
) -> Optional[Union[str, List[str]]]:
    """
    Unified UX per item:
    - Pick from known files
    - Upload a file
    - Paste now in editor
    """
    methods = []
    if known_options:
        methods.append("Pick from known files")
    methods += ["Upload a file", "Paste now in editor", "Back"]
    method = select(f"How do you want to provide {kind}?", choices=methods).unsafe_ask()

    if method == "Back":
        return None

    if method == "Pick from known files":
        choices = list(known_options.keys())
        if allow_multiple:
            chosen = checkbox(f"Select {kind}:", choices=choices).unsafe_ask()
            return [known_options[c] for c in chosen]
        chosen = select(f"Select {kind}:", choices=choices).unsafe_ask()
        return known_options[chosen]

    if method == "Upload a file":
        available_files = find_uploaded_files()
        if available_files:
            console.print("[blue]Found available uploaded files:[/]")
            choice = select(
                "Pick a file or enter a custom path:",
                list(available_files.keys()) + ["Enter custom path"]
            ).unsafe_ask()
            if choice == "Enter custom path":
                src_path_str = text(
                    "Enter the path to the file you want to upload:",
                    validate=PathValidator()
                ).unsafe_ask()
            else:
                src_path_str = available_files[choice]
        else:
            console.print("[yellow]No files found in upload folders.[/]")
            src_path_str = text(
                "Enter the path to the file you want to upload:",
                validate=PathValidator()
            ).unsafe_ask()

        src_path = Path(src_path_str).expanduser()

        tmp_dir = PROJECT_ROOT / ".tmp_cli"
        tmp_dir.mkdir(exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")

        try:
            if paste_mode == "pkl-json-dict" and src_path.suffix.lower() in {".json", ".txt"}:
                content = src_path.read_text()
                data = json.loads(content)
                if not isinstance(data, dict):
                    raise ValueError("Top-level JSON must be an object (mapping)")
                dest = tmp_dir / f"uploaded_{ts}{file_suffix or '.pkl'}"
                with open(dest, "wb") as f:
                    pickle.dump(data, f)
                console.print(f"[green]Uploaded and converted to[/] {dest}")
                return str(dest)

            dest = tmp_dir / f"uploaded_{ts}{src_path.suffix or (file_suffix or '')}"
            shutil.copy2(src_path, dest)
            console.print(f"[green]Uploaded to[/] {dest}")
            return str(dest)
        except Exception as e:
            console.print(f"[red]Upload failed: {e}[/]")
            return None

    if method == "Paste now in editor":
        initial = paste_template or (_load_adapters_yaml_template() if paste_mode == "yaml" else "")
        proceed = _print_paste_instructions(paste_mode or "", file_suffix, preview=initial)
        if not proceed:
            console.print("[yellow]Paste cancelled by user.[/]")
            return None

        console.print(Panel.fit(paste_title or "Paste content in the editor. Save & close.", style="blue"))
        editor_prefill = _make_editor_prefill(paste_mode or "", initial)
        console.print("[italic]Opening your $EDITOR (fallback: nano/vi) for paste...[/]")
        content = _open_editor_with_prefill(editor_prefill)
        if not content or not content.strip():
            console.print("[red]No content provided.[/]")
            return None
        try:
            if paste_mode == "yaml":
                return _save_pasted_yaml(content, file_suffix or ".yaml")
            elif paste_mode == "pkl-json-dict":
                return _save_pasted_json_as_pkl(content, file_suffix or ".pkl")
            else:
                console.print("[red]Unsupported paste mode.[/]")
                return None
        except Exception as e:
            console.print(f"[red]Invalid content: {e}[/]")
            return None

    return None



def _make_editor_prefill(mode: str, template: Optional[str]) -> str:
    if mode == "yaml":
        instr = (
            "# INSTRUCTIONS: Paste adapters YAML below. Lines starting with '#' are comments.\n"
            "# 1) Each top-level key is an adapter name.\n"
            "# 2) Required fields per adapter: adapter.module, adapter.cls, args, outdir, nodes, edges.\n"
            "# 3) Save and close the editor to continue (example: Ctrl+X then Y in nano; :wq in vi).\n"
            "# Example adapter block (remove comments when editing):\n"
            "# gencode_gene:\n"
            "#   adapter:\n"
            "#     module: biocypher_metta.adapters.gencode_gene_adapter\n"
            "#     cls: GencodeGeneAdapter\n"
            "#   args:\n"
            "#     filepath: /path/to/gencode.annotation.gtf.gz\n"
            "#   outdir: gencode/gene\n"
            "#   nodes: true\n"
            "#   edges: false\n\n"
        )
    elif mode == "pkl-json-dict":
        instr = (
            "# INSTRUCTIONS: Paste JSON mapping below. Lines starting with '#' are comments.\n"
            "# For rsids (_rsids.pkl) format: {\"rs123\": {\"chr\": \"chr1\", \"pos\": 10177}, ...}\n"
            "# For positions (_pos.pkl) format: {\"chr1_10177\": \"rs123\", ...}\n"
            "# Save and close the editor to continue.\n\n"
        )
    else:
        instr = "# Paste content below. Save & close when done.\n\n"

    body = (template or "").rstrip() + "\n"
    return instr + body

def _open_editor_with_prefill(prefill: str) -> Optional[str]:
    try:
        fd, tmp_path = tempfile.mkstemp(suffix=".tmp", prefix="biocypher_", text=True)
        os.close(fd)
        Path(tmp_path).write_text(prefill)
        editor = os.environ.get("VISUAL") or os.environ.get("EDITOR") or "nano"
        if shutil.which(editor) is None:
            if shutil.which("nano"):
                editor = "nano"
            elif shutil.which("vi"):
                editor = "vi"
        try:
            ret = subprocess.call([editor, tmp_path])
        except FileNotFoundError:
            console.print(f"[red]Editor '{editor}' not found. Please install an editor or set $EDITOR/$VISUAL.[/]")
            try:
                os.unlink(tmp_path)
            except Exception:
                pass
            return None
        if ret != 0:
            console.print(f"[yellow]Editor exited with code {ret} ({editor}). Reading buffer anyway...[/]")
        content = Path(tmp_path).read_text()
        try:
            os.unlink(tmp_path)
        except Exception:
            pass
        return content
    except Exception as e:
        console.print(f"[red]Failed to open editor: {e}[/]")
        return None


def configuration_workflow(organism: str) -> Optional[Dict[str, Union[str, List[str]]]]:
    selections: Dict[str, Union[str, List[str]]] = {}

    default_output = str(PROJECT_ROOT / f"output_{'human' if organism == 'human' else 'fly'}")
    selections["--output-dir"] = text("Enter output directory:", default=default_output).unsafe_ask()

    adapters_path = provide_data_input(
        "adapters config",
        known_options=find_config_files(organism),
        paste_mode="yaml",
        paste_title="Adapters YAML",
        file_suffix=".yaml",
    )
    if not adapters_path:
        return None
    selections["--adapters-config"] = adapters_path

    try:
        adapter_names = get_available_adapters(adapters_path)
        if adapter_names:
            chosen = checkbox("Select adapters to include:", choices=adapter_names).unsafe_ask()
            selections["--include-adapters"] = chosen
    except Exception:
        pass

    rsids_path = provide_data_input(
        "dbSNP rsIDs file",
        known_options=find_aux_files(organism),
        paste_mode="pkl-json-dict",
        paste_title="Paste JSON for rsID → {chr, pos}",
        file_suffix="_rsids.pkl",
    )
    if rsids_path:
        selections["--dbsnp-rsids"] = rsids_path

    pos_path = provide_data_input(
        "dbSNP positions file",
        known_options=find_aux_files(organism),
        paste_mode="pkl-json-dict",
        paste_title="Paste JSON for chr_pos → rsID",
        file_suffix="_pos.pkl",
    )
    if pos_path:
        selections["--dbsnp-pos"] = pos_path

    # Writer type
    selections["--writer-type"] = select(
        "Select output format:",
        choices=["neo4j", "metta", "prolog"],
        default="neo4j"
    ).unsafe_ask()
    
    # Additional options
    selections["--add-provenance"] = not confirm("Skip adding provenance?", default=False).unsafe_ask()
    selections["--write-properties"] = confirm("Write properties?", default=True).unsafe_ask()
    
    return selections

def main_menu() -> None:
    """Main menu with enhanced options"""
    console.print(Panel.fit(
        "[bold green]🔬 BioCypher Knowledge Graph Generator[/]",
        subtitle="Supporting Human and Drosophila melanogaster",
        style="green"
    ))
    
  
    required_dirs = [PROJECT_ROOT / "config", PROJECT_ROOT / "aux_files"]
    missing = [d for d in required_dirs if not d.exists()]
    if missing:
        console.print(Panel.fit(
            "[red]❌ Missing required directories:[/]\n" + "\n".join(f"- {d}" for d in missing),
            title="Error",
            style="red"
        ))
        return
    
    while True:
        choice = select(
            "Main Menu",
            choices=[
                "🚀 Generate Knowledge Graph",
                "⚙️ Configuration Options",
                "📊 View System Status",
                "❓ Help & Documentation",
                "🚪 Exit"
            ],
            qmark=">",
            pointer="→"
        ).unsafe_ask()
        
        if choice == "🚀 Generate Knowledge Graph":
            generate_kg_workflow()
        elif choice == "⚙️ Configuration Options":
            config_options_workflow()
        elif choice == "📊 View System Status":
            view_system_status()
        elif choice == "❓ Help & Documentation":
            show_help()
        elif choice == "🚪 Exit":
            console.print("[italic]Thank you for using BioCypher. Goodbye![/]")
            sys.exit(0)

def generate_kg_workflow() -> None:
    """Complete KG generation workflow with organism selection"""
    # select organism
    organism = select(
        "Select organism to generate KG for:",
        choices=[
            {"name": "🧬 Human", "value": "human"},
            {"name": "🪰 Drosophila melanogaster (Fly)", "value": "fly"},
            "🔙 Back"
        ],
        qmark=">",
        pointer="→"
    ).unsafe_ask()
    
    if organism == "🔙 Back":
        return
    
    #select configuration type
    config_type = select(
        f"Select configuration type for {organism}:",
        choices=[
            "⚡ Default Configuration",
            "🛠️ Custom Configuration",
            "🔙 Back"
        ],
        qmark=">",
        pointer="→"
    ).unsafe_ask()
    
    if config_type == "🔙 Back":
        return
    
    if config_type == "⚡ Default Configuration":
        if organism == "human":
            cmd = build_default_human_command()
            console.print(Panel.fit(
                "[bold]Using default human configuration[/]\n"
                "Output will be saved to 'output_human' directory",
                style="blue"
            ))
        else:
            cmd = build_default_fly_command()
            console.print(Panel.fit(
                "[bold]Using default fly configuration[/]\n"
                "Output will be saved to 'output_fly' directory",
                style="blue"
            ))
    else:  # Custom Configuration
        selections = configuration_workflow(organism)
        if not selections:
            return
        
        display_config_summary(selections)
        cmd = build_command_from_selections(selections)
    

    console.print(Panel.fit(
        "[bold]Ready to generate knowledge graph[/]",
        style="blue"
    ))
    
    show_logs = confirm(
        "Show detailed logs during generation?",
        default=False
    ).unsafe_ask()
    
    if confirm("Start knowledge graph generation?", default=True).unsafe_ask():
        run_generation(cmd, show_logs)
        
def config_options_workflow() -> None:
    """Configuration options submenu"""
    while True:
        choice = select(
            "Configuration Options",
            choices=[
                "🔍 View Available Config Files",
                "📂 View Available Auxiliary Files",
                "🔙 Back to Main Menu" 
            ],
            qmark=">",
            pointer="→"
        ).unsafe_ask()
        
        if choice == "🔍 View Available Config Files":
            config_files = find_config_files()
            table = Table(title="Available Config Files", show_header=True)
            table.add_column("Name", style="cyan")
            table.add_column("Path", style="green")
            for name, path in config_files.items():
                table.add_row(name, path)
            console.print(table)
        elif choice == "📂 View Available Auxiliary Files":
            aux_files = find_aux_files()
            table = Table(title="Available Auxiliary Files", show_header=True)
            table.add_column("Name", style="cyan")
            table.add_column("Path", style="green")
            for name, path in aux_files.items():
                table.add_row(name, path)
            console.print(table)
        elif choice == "🔙 Back to Main Menu":
            return

def view_system_status() -> None:
    """Display system status information"""
    table = Table(title="System Status", show_header=True)
    table.add_column("Component", style="cyan")
    table.add_column("Status", style="green")
    
    # Check directories
    required_dirs = [PROJECT_ROOT / "config", PROJECT_ROOT / "aux_files"]
    for d in required_dirs:
        status = "✅ Found" if d.exists() else "❌ Missing"
        table.add_row(str(d), status)
    
    # Check Python version
    table.add_row("Python Version", platform.python_version())
    
    # Check disk space
    total, used, free = shutil.disk_usage("/")
    table.add_row("Disk Space", f"Total: {total // (2**30)}GB, Free: {free // (2**30)}GB")
    
    console.print(Panel.fit(table))


def show_help() -> None:
    """Display help information"""
    help_text = """
    [bold]BioCypher Knowledge Graph Generator Help[/]
    
    [underline]Main Features:[/]
    - 🚀 Generate knowledge graphs for Human or Drosophila melanogaster
    - ⚡ Quick start with default configurations
    - 🛠️ Full customization options for advanced users
    
    [underline]Workflow:[/]
    1. Select organism (Human or Fly)
    2. Choose default or custom configuration
    3. For custom: Configure each parameter
    4. Review configuration summary
    5. Execute generation
    
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

if __name__ == "__main__":
    try:
        main_menu()
    except KeyboardInterrupt:
        console.print("\n[italic]Operation cancelled by user. Exiting...[/]")
        sys.exit(0)
    except questionary.ValidationError as e:
        console.print(f"[red]Error: {e.message}[/]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Unexpected error: {str(e)}[/]")
        sys.exit(1)




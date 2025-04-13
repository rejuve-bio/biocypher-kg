#!/usr/bin/env python3
"""
User-friendly BioCypher KG Generator with advanced CLI features
"""
import subprocess
import sys
from pathlib import Path
from typing import List, Dict, Optional
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

console = Console()

# Get project root
PROJECT_ROOT = Path(__file__).parent.parent    

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

def find_config_files() -> Dict[str, str]:
    """Find available config files with friendly names"""
    config_dir = PROJECT_ROOT / "config"
    return {
        "Sample Adapters Config": str(config_dir / "adapters_config_sample.yaml"),
        "Adapter Config": str(config_dir / "adapters_config.yml"),
        "Biocypher Config": str(config_dir / "biocypher_config.yml"),
        "Docker Config": str(config_dir / "biocypher_docker_config.yml"),
        "Data Source Config": str(config_dir / "data_source_config.yml"),
        "Download Config": str(config_dir / "download.yml"),
    }

def find_aux_files() -> Dict[str, str]:
    """Find available auxiliary files with friendly names"""
    aux_dir = PROJECT_ROOT / "aux_files"
    return {
        "Tissues Ontology Map": str(aux_dir / "abc_tissues_to_ontology_map.pkl"),
        "Gene Mapping": str(aux_dir / "gene_mapping.pkl"),
        "Variant Data": str(aux_dir / "variant_data.pkl"),
    }

def build_default_command() -> List[str]:
    """Your proven command with parallel paths"""
    return [
        "python3", str(PROJECT_ROOT / "create_knowledge_graph.py"),
        "--output-dir", str(PROJECT_ROOT / "output_cli"),
        "--adapters-config", str(PROJECT_ROOT / "config/adapters_config_sample.yaml"),
        "--dbsnp-rsids", str(PROJECT_ROOT / "aux_files/abc_tissues_to_ontology_map.pkl"),
        "--dbsnp-pos", str(PROJECT_ROOT / "aux_files/abc_tissues_to_ontology_map.pkl"),
        "--writer-type", "neo4j",
        "--no-add-provenance"
    ]

def get_file_selection(
    prompt: str,
    options: Dict[str, str],
    allow_multiple: bool = False,
    allow_custom: bool = True,
    back_option: bool = True
) -> Optional[str]:
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

def display_config_summary(config: Dict[str, str]) -> None:
    """Display a summary of the configuration"""
    table = Table(title="\nConfiguration Summary", show_header=True, header_style="bold magenta")
    table.add_column("Option", style="cyan")
    table.add_column("Value", style="green")
    
    for key, value in config.items():
        table.add_row(key, str(value))
    
    console.print(Panel.fit(table, style="blue"))

def run_generation(cmd: List[str], show_logs: bool) -> None:
    """Execute the generation process with enhanced progress tracking"""
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
            
            # Simulate progress
            for i in range(10):
                progress.update(task, advance=10)
                for line in process.stdout:
                    if "progress" in line.lower():  # progress detection
                        # Extract percentage from line 
                        pass
                    console.print(line, end='')
                time.sleep(0.5)
            
            process.wait()
        else:
            # Simulate progress for silent mode
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
                subtitle="Thank you for using BioCypher",
                style="green"
            ))
        else:
            console.print(Panel.fit(
                "[bold red]✖ KG generation failed[/]",
                style="red"
            ))
            if not show_logs and confirm("Show error details?", default=False).unsafe_ask():
                console.print(f"\n[red]Error output:[/]\n{process.stderr}")

def configuration_workflow() -> Optional[Dict[str, str]]:
    """Interactive configuration workflow with back navigation"""
    config_files = find_config_files()
    aux_files = find_aux_files()
    
    selections = {}
    
    # Output directory
    while True:
        selections["--output-dir"] = text(
            "Enter output directory:",
            default=str(PROJECT_ROOT / "output_cli"),
            validate=PathValidator
        ).unsafe_ask()
        
        if confirm(f"Use '{selections['--output-dir']}' as output directory?", default=True).unsafe_ask():
            break
    
    # Adapters config
    while True:
        result = get_file_selection(
            "Select adapters config:",
            config_files,
            allow_multiple=False,
            allow_custom=True
        )
        if result is None:
            continue  # User chose to go back
        selections["--adapters-config"] = result
        break
    
    # dbSNP rsIDs file
    while True:
        result = get_file_selection(
            "Select dbSNP rsIDs file:",
            aux_files,
            allow_multiple=False,
            allow_custom=True
        )
        if result is None:
            continue
        selections["--dbsnp-rsids"] = result
        break
    
    # dbSNP positions file
    while True:
        result = get_file_selection(
            "Select dbSNP positions file:",
            aux_files,
            allow_multiple=False,
            allow_custom=True
        )
        if result is None:
            continue
        selections["--dbsnp-pos"] = result
        break
    
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

def build_command_from_selections(selections: Dict[str, str]) -> List[str]:
    """Build the command list from user selections"""
    cmd = ["python3", str(PROJECT_ROOT / "create_knowledge_graph.py")]
    cmd.extend(["--output-dir", selections["--output-dir"]])
    cmd.extend(["--adapters-config", selections["--adapters-config"]])
    cmd.extend(["--dbsnp-rsids", selections["--dbsnp-rsids"]])
    cmd.extend(["--dbsnp-pos", selections["--dbsnp-pos"]])
    cmd.extend(["--writer-type", selections["--writer-type"]])
    
    if not selections["--add-provenance"]:
        cmd.append("--no-add-provenance")
    if not selections["--write-properties"]:
        cmd.append("--no-write-properties")
    
    return cmd

def main_menu() -> None:
    """Main menu with enhanced options"""
    console.print(Panel.fit(
        "[bold green]🔬 BioCypher Knowledge Graph Generator[/]",
        subtitle="Advanced CLI Interface",
        style="green"
    ))
    
    # Verify required directories
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
    """Complete KG generation workflow"""
    # Configuration selection
    use_default = confirm(
        "Use default configuration?",
        default=True,
        auto_enter=False
    ).unsafe_ask()
    
    if use_default:
        cmd = build_default_command()
        console.print(Panel.fit(
            "[bold]Using default configuration[/]\n"
            "This will use sample files and default output location.",
            style="blue"
        ))
    else:
        selections = configuration_workflow()
        if not selections:  # User backed out
            return
        
        display_config_summary(selections)
        cmd = build_command_from_selections(selections)
    
    # Execution phase
    console.print(Panel.fit(
        "[bold]Configuration complete. Ready to generate.[/]",
        style="blue"
    ))
    
    show_logs = confirm(
        "Show detailed logs during generation?",
        default=False,
        auto_enter=False
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
    import platform
    table.add_row("Python Version", platform.python_version())
    
    # Check disk space
    import shutil
    total, used, free = shutil.disk_usage("/")
    table.add_row("Disk Space", f"Total: {total // (2**30)}GB, Free: {free // (2**30)}GB")
    
    console.print(Panel.fit(table))

def show_help() -> None:
    """Display help information"""
    help_text = """
    [bold]BioCypher Knowledge Graph Generator Help[/]
    
    [underline]Main Features:[/]
    - 🚀 Generate knowledge graphs from biological data
    - ⚙️ Flexible configuration options
    - 📊 Real-time progress tracking
    
    [underline]Navigation:[/]
    - Use arrow keys to move between options
    - Press Enter to confirm selections
    - Most screens support going back with the '🔙 Back' option
    
    [underline]Configuration:[/]
    - You can use default configurations or customize all parameters
    - Configuration files are located in the 'config' directory
    - Auxiliary files are in the 'aux_files' directory
    
    [underline]Troubleshooting:[/]
    - Ensure all required directories exist
    - Check file permissions if you encounter errors
    - Use the detailed logs option to diagnose problems
    """
    console.print(Panel.fit(help_text, title="Help & Documentation"))

if __name__ == "__main__":
    import time  # For progress simulation
    
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
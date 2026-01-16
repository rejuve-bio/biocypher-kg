#!/usr/bin/env python3
"""
User-friendly BioCypher_KG CLI_Tool with support for Human and Drosophila melanogaster
"""
import subprocess
import sys
import logging
from pathlib import Path
from typing import List
from questionary import select, confirm
from rich.panel import Panel
from rich.table import Table

# Import from modules
from modules.utils import *
from modules.config import *
from modules.adapters import *

logger = logging.getLogger(__name__)

def build_default_human_command() -> List[str]:
    return [
        sys.executable, str(PROJECT_ROOT / "create_knowledge_graph.py"),
        "--output-dir", str(PROJECT_ROOT / "output_human"),
        "--adapters-config", str(PROJECT_ROOT / "config/hsa/hsa_adapters_config_sample.yaml"),
        "--schema-config", str(PROJECT_ROOT / "config/hsa/hsa_schema_config.yaml"),
        "--dbsnp-rsids", str(PROJECT_ROOT / "aux_files/hsa/sample_dbsnp_rsids.pkl"),
        "--dbsnp-pos", str(PROJECT_ROOT / "aux_files/hsa/sample_dbsnp_pos.pkl"),
        "--writer-type", "neo4j", "--no-add-provenance"
    ]

def build_default_fly_command() -> List[str]:
    return [
        sys.executable, str(PROJECT_ROOT / "create_knowledge_graph.py"),
        "--output-dir", str(PROJECT_ROOT / "output_fly"),
        "--adapters-config", str(PROJECT_ROOT / "config/dmel/dmel_adapters_config_sample.yaml"),
        "--schema-config", str(PROJECT_ROOT / "config/dmel/dmel_schema_config.yaml"),
        "--dbsnp-rsids", str(PROJECT_ROOT / "aux_files/hsa/sample_dbsnp_rsids.pkl"),
        "--dbsnp-pos", str(PROJECT_ROOT / "aux_files/hsa/sample_dbsnp_pos.pkl"),
        "--writer-type", "neo4j", "--no-add-provenance"
    ]

def run_generation(cmd: List[str], show_logs: bool) -> None:
    try:
        console.print("\n[bold]Starting knowledge graph generation...[/]\n")
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=str(PROJECT_ROOT), bufsize=1, universal_newlines=True)
        while True:
            stdout_line = process.stdout.readline()
            stderr_line = process.stderr.readline()
            if stdout_line:
                if stdout_line.startswith("INFO --"): console.print(stdout_line.strip())
                else: console.print(f"[dim]{stdout_line.strip()}[/]")
            if stderr_line:
                if stderr_line.startswith("ERROR --"): console.print(f"[red]{stderr_line.strip()}[/]")
                else: console.print(f"[yellow]{stderr_line.strip()}[/]")
            if process.poll() is not None: break
        if process.returncode == 0:
            console.print(Panel.fit("[bold green]✔ Knowledge Graph generation completed successfully![/]", style="green"))
        else:
            console.print(Panel.fit("[bold red]✖ KG generation failed[/]", style="red"))
    except Exception as e:
        console.print(Panel.fit(f"[bold red]✖ Execution failed: {str(e)}[/]", style="red"))

def generate_kg_workflow() -> None:
    organism = select("Select organism to generate KG for:", choices=[{"name": "🧬 Human", "value": "human"}, {"name": "🪰 Drosophila melanogaster (Fly)", "value": "fly"}, "🔙 Back"], qmark=">", pointer="→").unsafe_ask()
    if organism == "🔙 Back": return
    config_type = select(f"Select configuration type for {organism}:", choices=["⚡ Default Configuration", "🛠️ Custom Configuration", "🔙 Back"], qmark=">", pointer="→").unsafe_ask()
    if config_type == "🔙 Back": return
    if config_type == "⚡ Default Configuration":
        if organism == "human": cmd = build_default_human_command()
        else: cmd = build_default_fly_command()
        console.print(Panel.fit(f"[bold]Using default {organism} configuration[/]\nOutput will be saved to 'output_{organism}' directory", style="blue"))
    else:
        selections = configuration_workflow(organism)
        if not selections: return
        display_config_summary(selections)
        cmd = build_command_from_selections(selections)
    console.print(Panel.fit("[bold]Ready to generate knowledge graph[/]", style="blue"))
    show_logs = confirm("Show detailed logs during generation?", default=False).unsafe_ask()
    if confirm("Start knowledge graph generation?", default=True).unsafe_ask(): run_generation(cmd, show_logs)
        
def config_options_workflow() -> None:
    while True:
        choice = select("Configuration Options", choices=["🔍 View Available Config Files", "📂 View Available Auxiliary Files", "🔙 Back to Main Menu"], qmark=">", pointer="→").unsafe_ask()
        if choice == "🔍 View Available Config Files":
            config_files = find_config_files()
            table = Table(title="Available Config Files", show_header=True)
            table.add_column("Name", style="cyan"); table.add_column("Path", style="green")
            for name, path in config_files.items(): table.add_row(name, path)
            console.print(table)
        elif choice == "📂 View Available Auxiliary Files":
            aux_files = find_aux_files()
            table = Table(title="Available Auxiliary Files", show_header=True)
            table.add_column("Name", style="cyan"); table.add_column("Path", style="green")
            for name, path in aux_files.items(): table.add_row(name, path)
            console.print(table)
        elif choice == "🔙 Back to Main Menu": return

def main_menu() -> None:
    console.print(Panel.fit("[bold green]🔬 BioCypher Knowledge Graph Generator[/]", subtitle="Supporting Human and Drosophila melanogaster", style="green"))
    required_dirs = [PROJECT_ROOT / "config", PROJECT_ROOT / "aux_files"]
    missing = [d for d in required_dirs if not d.exists()]
    if missing:
        console.print(Panel.fit("[red]❌ Missing required directories:[/]\n" + "\n".join(f"- {d}" for d in missing), title="Error", style="red"))
        return
    while True:
        choice = select("Main Menu", choices=["🚀 Generate Knowledge Graph", "⚙️ Configuration Options", "📊 View System Status", "❓ Help & Documentation", "🚪 Exit"], qmark=">", pointer="→").unsafe_ask()
        if choice == "🚀 Generate Knowledge Graph": generate_kg_workflow()
        elif choice == "⚙️ Configuration Options": config_options_workflow()
        elif choice == "📊 View System Status": view_system_status()
        elif choice == "❓ Help & Documentation": show_help()
        elif choice == "🚪 Exit":
            console.print("[italic]Thank you for using BioCypher. Goodbye![/]")
            sys.exit(0)

if __name__ == "__main__":
    try: main_menu()
    except KeyboardInterrupt: console.print("\n[italic]Operation cancelled by user. Exiting...[/]"); sys.exit(0)
    except questionary.ValidationError as e: console.print(f"[red]Error: {e.message}[/]"); sys.exit(1)
    except Exception as e: console.print(f"[red]Unexpected error: {str(e)}[/]"); sys.exit(1)
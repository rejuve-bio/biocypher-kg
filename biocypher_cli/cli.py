#!/usr/bin/env python3
"""
User-friendly BioCypher_KG CLI_Tool with support for Human and Drosophila melanogaster
"""
import subprocess
import sys
import logging
from pathlib import Path
from typing import List
from questionary import select, confirm, ValidationError
from rich.panel import Panel
from rich.table import Table

# Import from modules
from .modules.utils import *
from .modules.config import *
from .modules.adapters import *

from .modules.sample_preparation import check_and_prepare_samples


logger = logging.getLogger(__name__)

def build_default_human_command() -> List[str]:
    return [
        "python3", str(PROJECT_ROOT / "create_knowledge_graph.py"),
        "--output-dir", str(PROJECT_ROOT / "output_human"),
        "--adapters-config", str(PROJECT_ROOT / "config/hsa/hsa_adapters_config_sample.yaml"),
        "--dbsnp-rsids", str(PROJECT_ROOT / "aux_files/hsa/sample_dbsnp_rsids.pkl"),
        "--dbsnp-pos", str(PROJECT_ROOT / "aux_files/hsa/sample_dbsnp_pos.pkl"),
        "--schema-config", str(PROJECT_ROOT / "config/hsa/hsa_schema_config.yaml"),
        "--writer-type", "neo4j", "--no-add-provenance"
    ]

def build_default_fly_command() -> List[str]:
    return [
        "python3", str(PROJECT_ROOT / "create_knowledge_graph.py"),
        "--output-dir", str(PROJECT_ROOT / "output_fly"),
        "--adapters-config", str(PROJECT_ROOT / "config/dmel/dmel_adapters_config_sample.yaml"),
        "--dbsnp-rsids", str(PROJECT_ROOT / "aux_files/hsa/sample_dbsnp_rsids.pkl"),
        "--dbsnp-pos", str(PROJECT_ROOT / "aux_files/hsa/sample_dbsnp_pos.pkl"),
        "--schema-config", str(PROJECT_ROOT / "config/dmel/dmel_schema_config.yaml"),
        "--writer-type", "neo4j", "--no-add-provenance"
    ]

def run_generation(cmd: List[str], show_logs: bool) -> None:
    try:
        console.print("\n[bold]Starting knowledge graph generation...[/]\n")
        # Merge stderr into stdout to avoid pipe deadlocks and ensure we always show errors.
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=str(PROJECT_ROOT),
            bufsize=1,
            universal_newlines=True,
        )

        output_lines: List[str] = []
        log_hint: str = ""

        for raw_line in iter(process.stdout.readline, ''):
            if raw_line is None:
                break
            line = raw_line.rstrip("\n")
            if not line:
                continue
            output_lines.append(line)

            # Capture BioCypher log file location if printed.
            if "Logging into" in line and "biocypher-" in line:
                log_hint = line.strip()

            if show_logs:
                if line.startswith("INFO --"):
                    console.print(line)
                elif line.startswith("ERROR --"):
                    console.print(f"[red]{line}[/]")
                else:
                    console.print(f"[dim]{line}[/]")

        process.wait()

        if process.returncode == 0:
            console.print(Panel.fit("[bold green]✔ Knowledge Graph generation completed successfully![/]", style="green"))
        else:
            tail = "\n".join(output_lines[-25:]) if output_lines else "(no output captured)"
            detail = f"returncode={process.returncode}"
            if log_hint:
                detail += f"\n{log_hint}"
            logger.error("KG generation failed (%s). Last output lines:\n%s", detail, tail)
            if show_logs:
                console.print(Panel.fit(f"[bold red]✖ KG generation failed[/]\n{detail}", style="red"))
            else:
                console.print(Panel.fit(f"[bold red]✖ KG generation failed[/]\n{detail}\n\nLast output:\n{tail}", style="red"))
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

    # Check/prepare samples before running
    try:
        if config_type == "⚡ Default Configuration":
            if "--adapters-config" in cmd:
                idx = cmd.index("--adapters-config")
                if idx + 1 < len(cmd):
                    created, skipped = check_and_prepare_samples(cmd[idx + 1], None, return_skipped=True)
                    if created:
                        console.print(Panel.fit(f"[green]Prepared {len(created)} sample file(s).[/]", style="green"))
                    if skipped:
                        console.print(Panel.fit(f"[yellow]Skipped {len(skipped)} adapter(s) due to sample preparation issues.[/]", style="yellow"))
        else:
            created, skipped = check_and_prepare_samples(
                selections.get("--adapters-config"),
                selections.get("--include-adapters"),
                return_skipped=True,
            )
            if created:
                console.print(Panel.fit(f"[green]Prepared {len(created)} sample file(s).[/]", style="green"))
            if skipped:
                console.print(Panel.fit(f"[yellow]Skipped {len(skipped)} adapter(s) due to sample preparation issues.[/]", style="yellow"))
    except Exception as e:
        logger.exception("Sample preparation failed")
        console.print(f"[yellow]Warning: sample preparation step failed: {e}[/]")

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
    except ValidationError as e: console.print(f"[red]Error: {e.message}[/]"); sys.exit(1)
    except Exception as e: console.print(f"[red]Unexpected error: {str(e)}[/]"); sys.exit(1)
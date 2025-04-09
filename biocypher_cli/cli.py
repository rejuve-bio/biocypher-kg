#!/usr/bin/env python3
"""
User-friendly BioCypher KG Generator with controlled logging
"""
import subprocess
from pathlib import Path
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from questionary import select, confirm

console = Console()

# Get project root
PROJECT_ROOT = Path(__file__).parent.parent

def find_config_files():
    """Find available config files with friendly names"""
    config_dir = PROJECT_ROOT / "config"
    return {
        "Sample Adapters_config": str(config_dir / "adapters_config_sample.yaml"),
        "Adapter Config": str(config_dir / "adapters_config.yml"),
        "Biocypher Config":str(config_dir/"biocypher_config.yml"),
        "biocypher_docker_config":str(config_dir/" biocypher_docker_config.yml"),
        "data_source_config.yml":str(config_dir/" data_source_config.ymll"),
        "download.yml":str(config_dir/" download.yml"),
        "biocypher_docker_config":str(config_dir/" biocypher_docker_config.yml"),
        
    }

def find_aux_files():
    """Find available auxiliary files with friendly names"""
    aux_dir = PROJECT_ROOT / "aux_files"
    return {
        "tissues_ontology_map": str(aux_dir / "abc_tissues_to_ontology_map.pkl"),
        
    }

def build_default_command():
    """Default command configuration"""
    configs = find_config_files()
    aux_files = find_aux_files()
    return [
        "python3", str(PROJECT_ROOT / "create_knowledge_graph.py"),
        "--output-dir", str(PROJECT_ROOT / "output"),
        "--adapters-config", configs["adapters_config"],
        "--dbsnp-rsids", aux_files["tissues_ontology_map"],
        "--dbsnp-pos", aux_files["tissues_ontology_map"],
        "--writer-type", "neo4j",
        "--no-add-provenance"
    ]

def run_generation(cmd, show_logs):
    """Execute the generation process with optional log display"""
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        transient=True,
    ) as progress:
        task = progress.add_task("Generating knowledge graph...", total=None)
        
        if show_logs:
            console.print("\n[bold]Running with logs visible:[/]\n")
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=str(PROJECT_ROOT)
            )
            
            for line in process.stdout:
                console.print(line, end='')
            process.wait()
        else:
            process = subprocess.run(
                cmd,
                cwd=str(PROJECT_ROOT),
                capture_output=True,
                text=True
            )
        
        progress.stop()
        
        if process.returncode == 0:
            console.print("[bold green]✅ KG generation completed successfully![/]")
        else:
            console.print("[bold red]❌ KG generation failed[/]")
            if not show_logs and confirm("Show error details?", default=False).ask():
                console.print(f"\n[red]Error output:[/]\n{process.stderr}")

def main():
    console.print("[bold green]🔬 BioCypher KG Generator[/]", justify="center")
    
    # Verify required directories
    required_dirs = [PROJECT_ROOT / "config", PROJECT_ROOT / "aux_files"]
    missing = [d for d in required_dirs if not d.exists()]
    if missing:
        console.print("[red]❌ Missing directories:[/]")
        for d in missing:
            console.print(f"- {d}")
        return

    # Configuration selection
    use_default = confirm("Use default configuration?", default=True).ask()
    
    if use_default:
        cmd = build_default_command()
    else:
        config_files = find_config_files()
        aux_files = find_aux_files()
        
        selections = {
            "--output-dir": str(PROJECT_ROOT / "output"),
            "--adapters-config": select(
                "Select adapters config:",
                choices=list(config_files.keys()),
            ).ask(),
            "--dbsnp-rsids": select(
                "Select dbSNP rsIDs file:",
                choices=list(aux_files.keys()),
            ).ask(),
            "--dbsnp-pos": select(
                "Select dbSNP positions file:",
                choices=list(aux_files.keys()),
            ).ask(),
            "--writer-type": select(
                "Select output format:",
                choices=["neo4j", "Metta", "Prolog"],
                default="neo4j"
            ).ask()
        }
        
        cmd = ["python3", str(PROJECT_ROOT / "create_knowledge_graph.py")]
        cmd.extend(["--output-dir", selections["--output-dir"]])
        cmd.extend(["--adapters-config", config_files[selections["--adapters-config"]]])
        cmd.extend(["--dbsnp-rsids", aux_files[selections["--dbsnp-rsids"]]])
        cmd.extend(["--dbsnp-pos", aux_files[selections["--dbsnp-pos"]]])
        cmd.extend(["--writer-type", selections["--writer-type"]])
        
        if not confirm("Add provenance?", default=False).ask():
            cmd.append("--no-add-provenance")

    # Execution phase
    console.print("\n[bold]Configuration complete. Ready to generate.[/]")
    show_logs = confirm("Show detailed logs during generation?", default=False).ask()
    
    if confirm("Start knowledge graph generation?", default=True).ask():
        run_generation(cmd, show_logs)

if __name__ == "__main__":
    main()
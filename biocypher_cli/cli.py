#!/usr/bin/env python3
"""
Interactive BioCypher KG Generator (Fixed Parallel Paths)
"""
import subprocess
from pathlib import Path
from rich.console import Console
from questionary import select, confirm, path

console = Console()

# Get project root (where create_knowledge_graph.py lives)
PROJECT_ROOT = Path(__file__).parent.parent

def build_default_command():
    """Your proven command with parallel paths"""
    return [
        "python3", str(PROJECT_ROOT / "create_knowledge_graph.py"),
        "--output-dir", str(PROJECT_ROOT / "output"),
        "--adapters-config", str(PROJECT_ROOT / "config/adapters_config_sample.yaml"),
        "--dbsnp-rsids", str(PROJECT_ROOT / "aux_files/abc_tissues_to_ontology_map.pkl"),
        "--dbsnp-pos", str(PROJECT_ROOT / "aux_files/abc_tissues_to_ontology_map.pkl"),
        "--writer-type", "neo4j",
        "--no-add-provenance"
    ]

def main():
    console.print("[bold green]🔬 BioCypher KG Generator[/]", justify="center")
    
    # Verify critical files exist
    required_files = [
        PROJECT_ROOT / "create_knowledge_graph.py",
        PROJECT_ROOT / "config/adapters_config_sample.yaml",
        PROJECT_ROOT / "aux_files/abc_tissues_to_ontology_map.pkl"
    ]
    
    missing = [f for f in required_files if not f.exists()]
    if missing:
        console.print("[red]❌ Missing files:[/]")
        for f in missing:
            console.print(f"- {f}")
        return

    # Mode selection
    use_default = confirm("Use default configuration?", default=True).ask()
    
    if use_default:
        cmd = build_default_command()
    else:
        # Interactive path selection with project-root-relative defaults
        components = {
            "--output-dir": path("Output directory:",
                                default=str(PROJECT_ROOT / "output")).ask(),
            "--adapters-config": path("Adapters config:",
                                    default=str(PROJECT_ROOT / "config/adapters_config_sample.yaml"),
                                    only_files=True).ask(),
            "--dbsnp-rsids": path("dbSNP rsIDs file:",
                                 default=str(PROJECT_ROOT / "aux_files/abc_tissues_to_ontology_map.pkl"),
                                 only_files=True).ask(),
            "--writer-type": select("Output format:",
                                  choices=["neo4j", "csv", "rdf"],
                                  default="neo4j").ask()
        }
        
        cmd = ["python3", str(PROJECT_ROOT / "create_knowledge_graph.py")]
        for flag, value in components.items():
            cmd.extend([flag, str(Path(value))])
        
        if not confirm("Add provenance?", default=False).ask():
            cmd.append("--no-add-provenance")

    console.print(f"\n[bold]Command to execute:[/]\n{' '.join(cmd)}")
    
    if confirm("Execute now?", default=True).ask():
        try:
            subprocess.run(cmd, check=True, cwd=str(PROJECT_ROOT))  # Run from project root
            console.print("[bold green]✅ KG generation complete![/]")
        except subprocess.CalledProcessError as e:
            console.print(f"[bold red]❌ Failed with code {e.returncode}[/]")
        except Exception as e:
            console.print(f"[bold red]❌ Unexpected error: {e}[/]")

if __name__ == "__main__":
    main()
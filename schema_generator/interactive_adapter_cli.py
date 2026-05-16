import os
import sys
import yaml
import json
import subprocess
from rich.console import Console
from rich.prompt import Prompt, Confirm
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from pathlib import Path


project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

try:
    import questionary
    HAS_QUESTIONARY = True
except ImportError:
    HAS_QUESTIONARY = False

console = Console()

def get_yaml_files(directory):
    return [f for f in os.listdir(directory) if f.endswith('.yaml') or f.endswith('.yml')]

def get_adapters_from_config(config_path):
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return list(config.keys()) if config else []

def list_processors():
    proc_dir = Path("biocypher_metta/processors")
    if not proc_dir.exists():
        return []
    return [f.stem for f in proc_dir.glob("*.py") if f.name != "__init__.py"]

def run_command(command, description):
    console.print(f"[bold blue]Running:[/bold blue] {description}...")
    
    try:
        result = subprocess.run(command, capture_output=False, text=True)
        
        if result.returncode == 0:
            console.print(f"[bold green]Success:[/bold green] {description} completed.")
            return True, ""
        else:
            console.print(f"[bold red]Error:[/bold red] {description} failed.")
            return False, f"Process exited with code {result.returncode}"
    except Exception as e:
        console.print(f"[bold red]Exception during command:[/bold red] {e}")
        return False, str(e)

def register_adapter(adapter_name, adapter_config_obj):
    """Register the adapter in the main species config files."""
    config_files = [
        "config/hsa/hsa_adapters_config_sample.yaml",
        "config/hsa/hsa_adapters_config.yaml"
    ]
    
    registered_any = False
    for config_path in config_files:
        path = Path(config_path)
        if not path.exists():
            console.print(f"[yellow]Skipping {config_path} (not found).[/yellow]")
            continue
            
        try:
            with open(path, 'r') as f:
                full_config = yaml.safe_load(f) or {}
            
            if adapter_name in full_config:
                if not Confirm.ask(f"Adapter '{adapter_name}' already exists in {config_path}. Overwrite?", default=False):
                    continue
            
            full_config[adapter_name] = adapter_config_obj[adapter_name]
            
            with open(path, 'w') as f:
                yaml.dump(full_config, f, sort_keys=False)
            
            console.print(f"[bold green]✓ Registered '{adapter_name}' in {config_path}.[/bold green]")
            registered_any = True
        except Exception as e:
            console.print(f"[bold red]Error registering in {config_path}:[/bold red] {e}")
            
    return registered_any

def get_manual_config():
    console.print(Panel("[bold yellow]Manual YAML Configuration[/bold yellow]\n"
                       "Paste your configuration below following this template:\n\n"
                       "adapter_name:\n"
                       "  adapter:\n"
                       "    module: biocypher_metta.adapters.module_name_auto\n"
                       "    cls: ClassName \n"
                       "    args:\n"
                       "      filepath: ./path/to/data\n"
                       "      arg2: value 2\n"
                       "  outdir: subdirectory_name\n"
                       "  nodes: true\n"
                       "  edges: false", 
                       title="Template", border_style="green"))
    
    console.print("[cyan]Enter your config (Press Ctrl+D or type 'END' on a new line to finish):[/cyan]")
    lines = []
    while True:
        try:
            line = input()
            if line.strip() == "END":
                break
            lines.append(line)
        except EOFError:
            break
    
    yaml_str = "\n".join(lines)
    if not yaml_str.strip():
        return None, None, None

    try:
        config = yaml.safe_load(yaml_str)
        if not config or not isinstance(config, dict):
            raise ValueError("Invalid YAML structure")
        
        adapter_name = list(config.keys())[0]
        config_path = f"data_source_schemas/adapter_configs/{adapter_name}.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(config, f, sort_keys=False)
        
        return config_path, adapter_name, config
    except Exception as e:
        console.print(f"[bold red]Error parsing YAML:[/bold red] {e}")
        return None, None, None

def get_guided_config():
    console.print("\n[bold yellow]Guided Configuration[/bold yellow]")
    adapter_name = Prompt.ask("Adapter name (e.g., 'gencode_gene')")
    module_path = Prompt.ask(f"Module path", default=f"biocypher_metta.adapters.{adapter_name}_auto")
    class_name = Prompt.ask("class name", default=''.join(x.capitalize() for x in adapter_name.split('_')) + "Adapter")
    
    args = {}
    console.print("\n[bold cyan]Enter adapter arguments[/bold cyan]")
    while True:
        arg_name = Prompt.ask("Argument name (leave empty to finish)", default="")
        if not arg_name:
            break
        arg_value = Prompt.ask(f"Value for {arg_name}")
        args[arg_name] = arg_value
        
    outdir = Prompt.ask("output subdirectory", default=adapter_name.split('_')[0] if '_' in adapter_name else adapter_name)
    nodes = Confirm.ask("process nodes", default=True)
    edges = Confirm.ask("process edges", default=True)
    
    config = {
        adapter_name: {
            "adapter": {
                "module": module_path,
                "cls": class_name,
                "args": args
            },
            "outdir": outdir,
            "nodes": nodes,
            "edges": edges
        }
    }
    
    config_path = f"data_source_schemas/adapter_configs/{adapter_name}.yaml"
    with open(config_path, 'w') as f:
        yaml.dump(config, f, sort_keys=False)
        
    return config_path, adapter_name, config

def main():
    console.print(Panel.fit(
        "[bold cyan]BioCypher Robust Adapter Wizard[/bold cyan]\n"
        "[dim]Interactive adapter generation and KG pipeline[/dim]",
        border_style="cyan"
    ))

    # 1. Choose Configuration Method
    console.print("\n[bold yellow]Step 1: Adapter Configuration Method[/bold yellow]")
    choice = questionary.select(
        "How would you like to provide the adapter configuration?",
        choices=[
            "1. Write the YAML directly",
            "2. Use guided configuration",
            "3. Select from existing config (hsa_adapters_config_sample.yaml)"
        ]
    ).ask() if HAS_QUESTIONARY else Prompt.ask("Choose method", choices=["1", "2", "3"], default="1")

    selected_config = None
    selected_adapter = None
    adapter_config_obj = None

    if choice in ["1", "1. Write the YAML directly"]:
        selected_config, selected_adapter, adapter_config_obj = get_manual_config()
    elif choice in ["2", "2. Use guided configuration"]:
        selected_config, selected_adapter, adapter_config_obj = get_guided_config()
    else:
        # Original flow
        config_dir = "config/hsa"
        configs = [f for f in get_yaml_files(config_dir) if "adapters" in f.lower()]
        
        table = Table(title="Available Adapter Configurations")
        table.add_column("ID", justify="right", style="cyan", no_wrap=True)
        table.add_column("Filename", style="magenta")
        
        for i, f in enumerate(configs):
            table.add_row(str(i+1), f)
        
        console.print(table)
        config_idx = int(Prompt.ask("Choose an adapter config", choices=[str(i+1) for i in range(len(configs))])) - 1
        selected_config = os.path.join(config_dir, configs[config_idx])

        # 2. Choose Adapter
        adapters = get_adapters_from_config(selected_config)
        if not adapters:
            console.print("[bold red]No adapters found in config![/bold red]")
            return

        # Show table for reference
        table = Table(title=f"Adapters in {configs[config_idx]}")
        table.add_column("ID", justify="right", style="cyan", no_wrap=True)
        table.add_column("Adapter Name", style="green")
        
        for i, a in enumerate(adapters):
            table.add_row(str(i+1), a)
        
        console.print(table)
        
        # Use searchable selector if available, otherwise fallback to number input
        if HAS_QUESTIONARY and len(adapters) > 10:
            console.print("\n[cyan] Tip: Start typing to filter adapters[/cyan]")
            selected_adapter = questionary.select(
                "Choose an adapter to generate:",
                choices=adapters,
                use_shortcuts=False,
                use_arrow_keys=True,
            ).ask()
            
            if selected_adapter is None:
                console.print("[yellow]Cancelled[/yellow]")
                return
        else:
            adapter_idx = int(Prompt.ask("Choose an adapter to generate", choices=[str(i+1) for i in range(len(adapters))])) - 1
            selected_adapter = adapters[adapter_idx]
            
        # Load the config object for registration later
        with open(selected_config, 'r') as f:
            full_cfg = yaml.safe_load(f)
            adapter_config_obj = {selected_adapter: full_cfg.get(selected_adapter)}

    if not selected_config or not selected_adapter:
        console.print("[bold red]No adapter selected or config generation failed![/bold red]")
        return

    console.print("\n[bold yellow]Step 2: Define Adapter Specification & Logic[/bold yellow]")

    # Read nodes/edges flags from the selected adapter config
    with open(selected_config) as f:
        import yaml as _yaml
        _all_adapters = _yaml.safe_load(f)
    _adapter_cfg = _all_adapters.get(selected_adapter, {})
    _gen_nodes = _adapter_cfg.get('nodes', True)
    _gen_edges = _adapter_cfg.get('edges', True)

    if _gen_nodes and not _gen_edges:
        # Node-only adapter
        console.print("[dim]This adapter generates [bold]nodes only[/bold] (edges: False in config)[/dim]")
        source_type = Prompt.ask("Node entity type", default="gene")
        source_id   = Prompt.ask("Source ID column index, name, or composite (e.g. 0, 'gene_id', or '0,1,2')", default="0")
        target_type = source_type   # not used
        target_id   = None
    elif _gen_edges and not _gen_nodes:
        # Edge-only adapter
        console.print("[dim]This adapter generates [bold]edges only[/bold] (nodes: False in config)[/dim]")
        source_type = Prompt.ask("Source entity type", default="protein")
        source_default = "filename" if source_type.lower() == "filename" else "0"
        source_id   = Prompt.ask("Source ID column index, name, or composite (e.g. 0, 'protein_id', or '0,1,2')", default=source_default)
        
        target_type = Prompt.ask("Target entity type", default="biological_process")
        target_default = "filename" if target_type.lower() == "filename" else "1"
        target_id   = Prompt.ask("Target ID column index, name, or composite (e.g. 1, 'go_id', or '3,4')", default=target_default)
    else:
        # Both nodes and edges
        console.print("[dim]This adapter generates [bold]both nodes and edges[/bold][/dim]")
        source_type = Prompt.ask("Source entity type", default="protein")
        source_default = "filename" if source_type.lower() == "filename" else "0"
        source_id   = Prompt.ask("Source ID column index, name, or composite (e.g. 0, 'protein_id', or '0,1,2')", default=source_default)
        
        target_type = Prompt.ask("Target entity type", default="biological_process")
        target_default = "filename" if target_type.lower() == "filename" else "1"
        target_id   = Prompt.ask("Target ID column index, name, or composite (e.g. 1, 'go_id', or '3,4')", default=target_default)
    
 
    console.print(Panel(
        "[bold yellow]Pro-Tip: How to write a 'High-Power' Logic Recipe[/bold yellow]\n"
        "To get the most accurate adapter, describe your [cyan]Additional Logic[/cyan] as steps:\n\n"
        "1. [bold]Processors[/bold]: Any specific ones? (e.g., 'Use HGNCProcessor to get Ensembl ID')\n"
        "2. [bold]Filtering[/bold]: Any specific criteria? (e.g., 'Only include rows where score > 0.5')\n"
        "3. [bold]Fallbacks[/bold]: Backup plan? (e.g., 'If Ensembl ID missing, use HGNC symbol')\n\n"
        "[dim][italic]Example: 'Filter rows with p-value < 0.05. If gene_id is missing, skip the row.'[/italic][/dim]",
        title="[bold cyan]Logic Recipe Template[/bold cyan]",
        border_style="cyan"
    ))
    
    console.print("\n[bold yellow]Enter Additional Rules u want to add (Step-by-Step)[/bold yellow]")
    console.print("[dim]Enter each extra rule (e.g. filters, fallbacks). Press Enter on empty to finish.[/dim]")
    recipe_steps = [f"Source ID: {source_id}", f"Target ID: {target_id}"]
    while True:
        step = Prompt.ask(f"Rule {len(recipe_steps)-1}")
        if not step:
            break
        recipe_steps.append(f"- {step}")
    
    logic_recipe = "\n".join(recipe_steps) if recipe_steps else "Direct mapping"
    
    # 4. Properties
    properties_dict = {}
    if Confirm.ask("Do you want to explicitly define property types?", default=False):
        console.print("[dim]Enter properties as 'name:type' (e.g., 'evidence:str'). Press Enter on empty to finish.[/dim]")
        props = []
        while True:
            p = Prompt.ask("Property (name:type)")
            if not p:
                break
            if ":" in p:
                prop_name, prop_type = p.split(":", 1)
                properties_dict[prop_name.strip()] = prop_type.strip()
                props.append(p)
            else:
                console.print("[bold red]Invalid format![/bold red] Please use 'name:type' (e.g. score:float)")
        
        if props:
            logic_recipe += f" MANDATORY PROPERTY MAPPING REQUIREMENTS: {', '.join(props)}."

    # 6. Processors (Enhanced to support multiple processors)
    use_processors = Confirm.ask("Do you want to use Processors?", default=False)
    processor_info = {}
    selected_processors = []
    if use_processors:
        processors = list_processors()
        if processors:
            table = Table(title="Available Processors")
            table.add_column("ID", justify="right", style="cyan")
            table.add_column("Processor Name", style="magenta")
            for i, p in enumerate(processors):
                table.add_row(str(i+1), p)
            console.print(table)
            
            console.print("\n[bold yellow]Multiple Processor Selection[/bold yellow]")
            console.print("[dim]You can select multiple processors. Enter processor IDs separated by commas (e.g., '5,6')[/dim]")
            
            proc_input = Prompt.ask("Choose processor(s)", default="1")
            proc_indices = [int(x.strip()) - 1 for x in proc_input.split(',')]
            
            for proc_idx in proc_indices:
                if 0 <= proc_idx < len(processors):
                    selected_processor = processors[proc_idx]
                    
                    # Ask which ID needs processing for each processor
                    console.print(Panel(
                        f"[bold cyan]Processor Configuration[/bold cyan]\n"
                        f"Selected Processor: [yellow]{selected_processor}[/yellow]\n\n"
                        f"[dim]Which ID should be processed?[/dim]",
                        border_style="cyan"
                    ))
                    
                    processor_target = Prompt.ask(
                        f"Apply {selected_processor} to",
                        choices=["source", "target", "both"],
                        default="source"
                    )
                    
                    selected_processors.append({
                        "processor_name": selected_processor,
                        "processor_target": processor_target
                    })
                    
                    logic_recipe += f" Use the '{selected_processor}' processor to convert {processor_target} ID(s)."
            
            # Store multiple processors info
            if selected_processors:
                processor_info = {
                    "processors": selected_processors,
                    "count": len(selected_processors)
                }
        else:
            console.print("[dim]No processors found in biocypher_metta/processors.[/dim]")

    # 7. Generate Adapter Specification
    spec_path = f"data_source_schemas/adapter_specs/{selected_adapter}_specification.yaml"
    spec_cmd = [
        "uv", "run", "python3", "-m", "schema_generator.llm_adapter_specification_generator",
        "--adapter-config", selected_config,
        "--adapter-name", selected_adapter,
        "--output", spec_path,
        "--logic-recipe", logic_recipe,
        "--source-type", source_type,
        "--target-type", target_type,
        "--source-id", source_id,
    ]
    
    if target_id is not None:
        spec_cmd.extend(["--target-id", target_id])
    
    # Add properties if user provided them
    if properties_dict:
        spec_cmd.extend(["--properties", json.dumps(properties_dict)])
    
    # Add processor info if user selected one or more
    if processor_info:
        if "processors" in processor_info:
            # Multiple processors - send the whole dict
            spec_cmd.extend([
                "--processors", json.dumps(processor_info)
            ])
        else:
            # Single processor (backward compatibility)
            spec_cmd.extend([
                "--processor-name", processor_info["processor_name"],
                "--processor-target", processor_info["processor_target"]
            ])
    
    success, output = run_command(spec_cmd, f"Generating specification for {selected_adapter}")
    if not success:
        console.print(Panel(output, title="[bold red]Adapter Specification Generation Failed[/bold red]", border_style="red"))
        return

    console.print("\n[bold yellow]Step 3: Review File Structure & Columns[/bold yellow]")
    
    # Get filepath from adapter config
    with open(selected_config) as f:
        import yaml as _yaml
        _all_adapters = _yaml.safe_load(f)
    _adapter_cfg = _all_adapters.get(selected_adapter, {})
    filepath = _adapter_cfg.get('adapter', {}).get('args', {}).get('filepath')
    
    if filepath:
        try:
            from schema_generator.source_inspector import SourceInspector
            inspector = SourceInspector(filepath)
            inspection = inspector.inspect()
            
            # Display file metadata
            console.print(Panel(
                f"[bold cyan]File Inspection Results[/bold cyan]\n"
                f"[dim]File:[/dim] {filepath}\n"
                f"[dim]Compression:[/dim] {inspection['compression']}\n"
                f"[dim]Delimiter:[/dim] {repr(inspection['delimiter'])}\n"
                f"[dim]Has Header:[/dim] {inspection['has_header']}\n"
                f"[dim]Columns Detected:[/dim] {len(inspection['headers'])}",
                border_style="cyan"
            ))
            
          
            if not inspection['headers']:
                console.print("[bold yellow]⚠ No columns detected in file![/bold yellow]")
                console.print("[dim]The file inspection may have failed. Check the file format and try again.[/dim]")
        
        except Exception as e:
            console.print(f"[bold yellow]⚠ Could not inspect file: {e}[/bold yellow]")
            console.print("[dim]Proceeding with specification generation...[/dim]")
    
    console.print("\n[bold yellow]Step 4: Review & Edit Adapter Specification[/bold yellow]")
    if os.path.exists(spec_path):
        from rich.syntax import Syntax
        with open(spec_path, 'r') as f:
            spec_content = f.read()
        
        console.print(Panel(
            Syntax(spec_content, "yaml", theme="monokai", line_numbers=True),
            title=f"[bold cyan]Generated Specification: {os.path.basename(spec_path)}[/bold cyan]",
            border_style="cyan"
        ))

        if not Confirm.ask("Does this specification look correct?", default=True):
            if Confirm.ask("Would you like to edit the specification manually now?", default=True):
                editor = os.environ.get('EDITOR', 'nano')
                console.print(f"[bold blue]Opening editor ({editor})...[/bold blue] Please save and close when finished.")
                subprocess.run([editor, spec_path])
                
                # Re-verify after edit
                console.print("[bold green]Adapter specification updated.[/bold green]")
                if not Confirm.ask("Proceed with the updated specification?", default=True):
                    console.print("[bold red]Aborting generation based on user request.[/bold red]")
                    return
    else:
        console.print("[bold red]Error: Adapter specification file was not generated![/bold red]")
        return
        
    _module = _adapter_cfg.get('adapter', {}).get('module', f"biocypher_metta.adapters.{selected_adapter}_auto")
    adapter_output = _module.replace('.', '/') + ".py"
    
    adapter_cmd = [
        "uv", "run", "python3", "-m", "schema_generator.llm_adapter_generator",
        "--specification", spec_path,
        "--adapters-config", selected_config,
        "--adapter-name", selected_adapter,
        "--output", adapter_output
    ]
    
    success, output = run_command(adapter_cmd, f"Generating adapter code for {selected_adapter}")
    if not success:
        console.print(Panel(output, title="[bold red]Adapter Generation Failed[/bold red]", border_style="red"))
        return

    console.print(f"\n[bold green]Adapter generated at:[/bold green] {adapter_output}")
    
    if Confirm.ask(f"Do you want to register '{selected_adapter}' in the main production config files?", default=True):
        if register_adapter(selected_adapter, adapter_config_obj):
            if "data_source_schemas/adapter_configs" in selected_config and os.path.exists(selected_config):
                try:
                    os.remove(selected_config)
                    console.print(f"[dim]Removed temporary config: {selected_config}[/dim]")
                except Exception as e:
                    console.print(f"[yellow]Note: Could not remove temporary config: {e}[/yellow]")

    if Confirm.ask("Do you want to run the Knowledge Graph creation process now?", default=False):
        species = Prompt.ask("Species", default="hsa")
        dataset = Prompt.ask("Dataset", default="sample")
        writer_type = questionary.select(
            "Writer type:",
            choices=["neo4j", "metta", "rdf", "csv"]
        ).ask() if HAS_QUESTIONARY else Prompt.ask("Writer type", choices=["neo4j", "metta", "rdf", "csv"], default="neo4j")
        
        output_dir = f"out_{_adapter_cfg.get('outdir', selected_adapter)}"
        
        kg_cmd = [
            "uv", "run", "python3", "create_knowledge_graph.py",
            "--species", species,
            "--dataset", dataset,
            "--output-dir", output_dir,
            "--include-adapters", selected_adapter,
            "--no-checkpoint",
            "--writer-type", writer_type
        ]
        run_command(kg_cmd, f"Running KG creation for {selected_adapter} ({writer_type})")

if __name__ == "__main__":
    main()

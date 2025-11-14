"""Adapter creation and management"""
import yaml
import re
import time
from pathlib import Path
from typing import Optional
from questionary import select, confirm, text
from .utils import console, USER_ADAPTERS_DIR, YamlValidator, PythonClassValidator, PROJECT_ROOT

def save_temp_adapter_config(config: dict) -> str:
    temp_dir = PROJECT_ROOT / "config" / "temp_adapters"
    temp_dir.mkdir(exist_ok=True)
    temp_file = temp_dir / f"adapter_{int(time.time())}.yaml"
    with open(temp_file, 'w') as f:
        yaml.dump(config, f, sort_keys=False, default_flow_style=False)
    console.print(f"[green]Adapter configuration saved to: {temp_file}[/]")
    return str(temp_file)

def create_user_adapter_file() -> Optional[str]:
    console.print("\n[bold]Create New Adapter Python File[/]", style="blue")
    adapter_name = text("Enter adapter name (snake_case, e.g. 'gencode_gene'):", validate=lambda x: True if re.match(r'^[a-z][a-z0-9_]*$', x) else "Must be snake_case").unsafe_ask()
    class_name = text("Enter class name (PascalCase, e.g. 'GencodeAdapter'):", validate=PythonClassValidator).unsafe_ask()
    
    choice = select("How would you like to proceed?", choices=[{"name": "✏️ Write code from scratch", "value": "scratch"}, {"name": "📋 Use basic template", "value": "template"}, "🔙 Back"]).unsafe_ask()
    if choice == "🔙 Back": return None
    
    if choice == "template":
        content = f'''"""
{adapter_name.title()} Adapter for BioCypher
"""
from biocypher import Adapter
from typing import Generator

class {class_name}(Adapter):
    def __init__(self, write_properties, add_provenance, filepath=None):
        self.filepath = filepath
        self.write_properties = write_properties
        self.add_provenance = add_provenance
        self.source = "YOUR_SOURCE"
        self.version = "1.0"
        super().__init__(write_properties, add_provenance)

    def get_nodes(self) -> Generator[tuple, None, None]:
        pass

    def get_edges(self) -> Generator[tuple, None, None]:
        pass
'''
    else:
        content = text("Enter your adapter Python code:", multiline=True, default='from biocypher import Adapter\n\nclass ' + class_name + '(Adapter):\n    pass').unsafe_ask()
    
    filename = f"{adapter_name}_adapter.py"
    save_path = USER_ADAPTERS_DIR / filename
    try:
        with open(save_path, 'w') as f: f.write(content)
        console.print(f"[green]✔ Adapter saved to: {save_path}[/]")
        return str(save_path)
    except Exception as e:
        console.print(f"[red]✖ Error saving file: {str(e)}[/]")
        return None

def get_yaml_input_adapter_config() -> str:
    console.print("\n[bold]Enter your adapter configuration in YAML format:[/]", style="blue")
    console.print("Example format:\n", style="dim")
    console.print('adapter_name:\n  adapter:\n    module: module.path\n    cls: ClassName\n    args:\n      arg1: value1\n      arg2: value2\n  outdir: output/path\n  nodes: true\n  edges: false', style="dim")
    
    yaml_content = text("Paste your YAML configuration below:", multiline=True, validate=YamlValidator).unsafe_ask()
    try:
        config = yaml.safe_load(yaml_content)
        if not config: raise ValueError("Empty YAML configuration")
        return save_temp_adapter_config(config)
    except Exception as e:
        console.print(f"[red]Error parsing YAML: {str(e)}[/]")
        return None

def build_custom_adapter_config() -> str:
    console.print("\n[bold]Building Custom Adapter Configuration[/]", style="blue")
    choice = select("Adapter configuration method:", choices=[{"name": "✍️ Write YAML directly", "value": "yaml"}, {"name": "🛠️ Use guided configuration", "value": "guided"}, {"name": "🐍 Create new Python adapter", "value": "new_adapter"}, "🔙 Back"], pointer="→").unsafe_ask()
    if choice == "🔙 Back": return None
    elif choice == "yaml": return get_yaml_input_adapter_config()
    elif choice == "new_adapter":
        adapter_path = create_user_adapter_file()
        if not adapter_path: return None
        adapter_name = Path(adapter_path).stem.replace('_adapter', '')
        module_path = f"adapters.user_adapters.{adapter_name}_adapter"
        class_name = ''.join([x.title() for x in adapter_name.split('_')]) + 'Adapter'
        config = {adapter_name: {"adapter": {"module": module_path, "cls": class_name, "args": {"filepath": "path/to/your/data"}}, "outdir": adapter_name, "nodes": True, "edges": False}}
        return save_temp_adapter_config(config)
    
    adapter_name = text("Adapter name (e.g., 'gencode_gene'):").unsafe_ask()
    module = text("Module path (e.g., 'biocypher_metta.adapters.gencode_gene_adapter'):").unsafe_ask()
    cls = text("Class name (e.g., 'GencodeGeneAdapter'):").unsafe_ask()
    args = {}
    console.print("\n[bold]Enter adapter arguments:[/]", style="blue")
    while True:
        arg_name = text("Argument name (leave empty to finish):").unsafe_ask()
        if not arg_name: break
        arg_value = text(f"Value for '{arg_name}':").unsafe_ask()
        args[arg_name] = str(Path(arg_value))
    outdir = text("\nOutput subdirectory (e.g., 'gencode/gene'):").unsafe_ask()
    nodes = confirm("Process nodes?", default=True).unsafe_ask()
    edges = confirm("Process edges?", default=False).unsafe_ask()
    config = {adapter_name: {"adapter": {"module": module, "cls": cls, "args": args}, "outdir": outdir, "nodes": nodes, "edges": edges}}
    return save_temp_adapter_config(config)
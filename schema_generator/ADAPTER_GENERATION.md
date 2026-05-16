# BioCypher Robust Adapter Wizard

An automated pipeline for generating BioCypher Knowledge Graph adapters using LLMs (OpenRouter). This tool streamlines the process of transforming raw biological data files into production-ready Python adapters with minimal manual coding.

## Overview

The Robust Adapter Wizard automates the following workflow:
1.  **Data Inspection**: Detects delimiters, headers, and samples data structure from local files.
2.  **Semantic Mapping**: Uses LLM to infer the meaning of data columns and propose relationship properties.
3.  **Specification Generation**: Creates a "Logic Blueprint" (YAML) that defines join logic, ID normalization, and implementation steps.
4.  **Code Synthesis**: Generates a syntactically valid Python adapter class that inherits from the BioCypher base `Adapter`.
5.  **Auto-Registration**: Optionally adds the new adapter to your species configuration files.

## Prerequisites

- **Python Environment**: Managed via `uv` or `pip`.
- **API Keys**: Ensure you have a `.env` file in the project root with at least one of the following:
    ```bash
    OPENROUTER_API_KEY=your_key_here
    
    ```
- **Dependencies**: Install required packages:
    ```bash
    pip install rich questionary pyyaml requests pydantic
    ```

## Usage

Run the interactive wizard from the project root:

```bash
uv run python3 schema_generator/interactive_adapter_cli.py
```

### Wizard Steps

1.  **Select Configuration**: 
    - Choose an existing adapter from `hsa_adapters_config_sample.yaml`.
    - Provide a manual YAML configuration.
    - Select from all available YAML files in the `config/` directory.
2.  **Logic Recipe**: (Optional) Provide high-level "Logic Recipes" to guide the LLM on specific transformation requirements (e.g., "Strip version suffixes from Ensembl IDs").
3.  **Entity/Edge Selection**: Confirm which nodes or edges you wish to generate from the data source.
4.  **Generation**: The tool will automatically:
    - Inspect the source file.
    - Generate a technical specification (`data_source_schemas/adapter_specs/`).
    - Synthesize the Python adapter code (`biocypher_metta/adapters/`).
5.  **Review & Register**: Review the generated coverage and register the adapter in your configuration.

## Key Components

- **`interactive_adapter_cli.py`**: The main user interface.
- **`llm_adapter_generator.py`**: Orchestrates the code generation process.
- **`logic_inference.py`**: Handles complex joining logic for auxiliary files and biological identifiers.
- **`code_fixer.py`**: A defensive layer that automatically detects and repairs syntax errors or hallucinations in the generated code.
- **`source_inspector.py`**: Deterministic analysis of file structure and data types.

## Debugging

If generation fails, the tool saves diagnostic data to the `debug_traces/` directory. These files contain the full prompts and raw LLM responses used during the failed attempt, allowing for detailed troubleshooting of logic or context errors.
- if u got 0 output, make sure to check the generated adapter is used the correct processor method if any.


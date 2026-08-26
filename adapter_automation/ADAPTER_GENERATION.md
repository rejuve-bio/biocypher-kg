# BioCypher Adapter Automation

An automated pipeline for generating BioCypher Knowledge Graph adapters using LLMs (OpenRouter). This tool streamlines the process of transforming raw biological data files into production-ready Python adapters with minimal manual coding.

## Overview

The Adapter automates the following workflow:
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
uv run python3 adapter_automation/interactive_adapter_cli.py
```

### Steps

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
- **`logic_inference.py`**: Handles joining logic for auxiliary files and biological identifiers.
- **`code_fixer.py`**: A defensive layer that automatically detects and repairs syntax errors or hallucinations in the generated code.
- **`source_inspector.py`**: Deterministic analysis of file structure and data types.

## Debugging

If generation fails, the tool saves diagnostic data to the `debug_traces/` directory. These files contain the full prompts and raw LLM responses used during the failed attempt, allowing for detailed troubleshooting of logic or context errors.
- If you get no output, check that the generated adapter uses the correct processor method and any required column filters.

## Supported Adapters in the Current Pipeline

Most generated adapters produce the exact same output as the original manual implementations:

- **Gencode_gene**
- **Gencode_transcripts**
- **Transcribes_to**
- **Gencode_exon**
- **Exon_part_of_transcript**
- **Exon_part_of_gene**
- **Reactome_pathway**
- **Reactome_reaction**
- **Reactome_reaction_to_pathway**
- **Reactome_input_role_protein_to_reaction_or_pathway**
- **Reactome_output_role_protein_to_reaction_or_pathway**
- **Reactome_catalyst_role_protein_to_reaction_or_pathway**
- **Reactome_negative_role_protein_to_reaction_or_pathway**
- **Reactome_positive_role_protein_to_reaction_or_pathway**
- **Genes_pathways**
- **Gene_or_gene_product_reaction**
- **Reactome_ppi**
- **Pathway_to_biological_process**
- **Gaf_biological_process_gene_product**
- **Gaf_molecular_function_gene_product**
- **Gaf_cellular_component_gene_product_part_of**
- **Gaf_cellular_component_gene_product_located_in**
- **Gaf_biological_process_gene**
- **Gaf_molecular_function_gene**
- **Gaf_cellular_component_gene_part_of**
- **Gaf_cellular_component_gene_located_in**
- **T-link**
- **Tflink_protein_protein**
- **String**
- **Tadmap**
- **Tadmap_gene**
- **Roadmap_chromatin_state**
- **Roadmap_h3_mark**
- **Roadmap_dhs**
- **Gtex_eqtl**
- **Gtex_expression**
- **Cadd**
- **Refseq_closest_gene**
- **Topld_eur**
- **Dgv_variant**
- **Epd_promoter**
- **Peregrine_enhancer**
- **Enhancer_atlas_enhancer**
- **PolyPhen2**
- **TFBS adapters** (Transcription_factor_binding_site)
- **Snp_to_upstream_gene**
- **Snp_to_downstream_gene**
- **Snp_to_in_gene**
- **Promoter_ccre**
- **Encode_re2g**
- **Encode_re2g_gene_associates**

### Adapters with Known Caveats (Produces correct output but prone to hallucination/edge cases)
- **ABC**: Selection of the processor method sometimes hallucinates.
- **Chebi_small_molecule_to_pathways**: Prefixes are not added correctly during execution.
- **Chebi_small_molecule_to_reactions**: Prefixes are not added correctly during execution.
- **Parent_pathway_of**: Prefixes are not added correctly during execution.
- **Child_pathway_of**: Prefixes are not added correctly during execution.
- **Rna_central_non_coding_rna** (and related `biological_process`, `molecular_function`, `cellular_component`): Users should avoid providing files unrelated to the specific edge or node adapter being generated.
- **Epd_promoter_regulates_gene**: Selection of the processor method sometimes hallucinates.
- **Hpo_gene_phenotype**: Selection of the processor method sometimes hallucinates.
- **Hpo_gene_disease**: Selection of the processor method sometimes hallucinates.
- **Dbsuper_super_enhancer** & **Dbsuper_super_enhancer_regulates_gene**: Selection of the processor method sometimes hallucinates.
- **Dbsnp_snps**: Selection of the processor method sometimes hallucinates.
- **Gene_tfbs_association**: Produces correct output, but target ID uses a composite key that the current pipeline cannot fully handle.
- **Promoter_ccre_associates_with_gene_nearest_gene** (and `coding_only`): LLM sometimes hallucinates the usage of the additional argument `edge_type: nearest`.
- **Promoter_ccre_associates_with_gene_eqtl**: LLM sometimes hallucinates when filtering specific columns.
- **Proximal_enhancer_ccre** (and related `associates_with_gene`, `coding_only`): LLM sometimes hallucinates the usage of the additional argument `element_filter: proximal`.
- **Distal_enhancer_ccre** (and related `associates_with_gene`, `coding_only`): LLM sometimes hallucinates the usage of the additional argument `element_filter: distal`.
- **Alliance_gene_disease_biomarker_orthology** (and related `implicated_via_orthology`, `is_implicated_in`, `is_marker_for`): LLM sometimes hallucinates when filtering specific columns.
- **Coexpression**: Selection of the processor method sometimes hallucinates.

## Adapters Not Supported in the Current Pipeline

The following adapters are currently unsupported, primarily due to structural complexity or overlap/join operations that exceed current pipeline capabilities:

- **dgv_variant_gene_overlap**
- **dgv_variant_ncrna_overlap**
- **Dgv_variant_promoter_overlap**
- **dbvar_variant_gene_overlap**
- **dbvar_variant_ncrna_overlap**
- **Dbvar_variant_promoter_overlap**
  *(Reason: Require overlap/join operations exceeding current capabilities)*
- **Peregrine_enhancer_regulates**
  *(Reason: Uses three data files plus one mapping file. LLM may generate incorrect specs; requires manual verification)*
- **Enhancer_atlas_enhancer_regulates**
  *(Reason: Source file structure is too complex)*
- **BGEE**
  *(Reason: Ontology adapter with multiple yields. Target typing becomes ambiguous and prefixes are incorrectly removed)*
- **Uniprotkb_sprot_translates_to**
  *(Reason: Source file structure is too complex)*
- **Ontology adapter**

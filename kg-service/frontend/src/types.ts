export interface DatasetEntry {
  name: string;
  adapters_config: string;
  schema_config: string;
  dbsnp_cache_root: string;
  dbsnp_variant: string;
  adapters_config_exists: boolean;
  schema_config_exists: boolean;
}

export interface SpeciesEntry {
  species: string;
  datasets: DatasetEntry[];
}

export interface AdapterInfo {
  name: string;
  module: string | null;
  cls: string | null;
  nodes: boolean;
  edges: boolean;
  outdir: string | null;
  source_id: string | null;
  provenance: unknown;
  args: Record<string, unknown>;
  declared_paths: string[];
}

export interface AdaptersResponse {
  species: string;
  dataset: string;
  input_dir: string | null;
  adapters_config: string;
  count: number;
  adapters: AdapterInfo[];
}

export interface FlagInfo {
  name: string;
  default: boolean;
  help: string;
}

export interface BuildRequest {
  species?: string | null;
  dataset: string;
  adapters_config?: string | null;
  schema_config?: string | null;
  include_adapters?: string[] | null;
  writer_type: string;
  output_dir?: string | null;
  input_dir?: string | null;
  dbsnp_cache_root?: string | null;
  dbsnp_variant?: string | null;
  write_properties: boolean;
  add_provenance: boolean;
  include_taxon_id: boolean;
  include_curie: boolean;
  skip_preflight: boolean;
  generate_data_source_schemas: boolean;
}

export interface ValidationResult {
  valid: boolean;
  static_errors: string[];
  static_warnings: string[];
  missing_paths: Record<string, Record<string, string>>;
  checked_paths: boolean;
  resolved: {
    adapters_config: string | null;
    schema_config: string | null;
    num_adapters: number | null;
    cmd_preview: string[];
  };
}

export type JobStatus =
  | "queued"
  | "running"
  | "succeeded"
  | "failed"
  | "cancelled";

export interface BuildJob {
  id: string;
  status: JobStatus;
  kind?: string; // "build" | "load-neo4j" | "load-mork"
  params: Record<string, unknown>;
  cmd: string[];
  cwd: string;
  output_dir: string;
  log_path: string;
  total_adapters?: number | null;
  pid: number | null;
  return_code: number | null;
  created_at: string | null;
  started_at: string | null;
  finished_at: string | null;
  error: string | null;
  // present on GET /builds/{id}
  checkpoint?: CheckpointInfo | null;
  resumable?: boolean;
  retryable?: boolean; // failed/cancelled load job → can be re-run
}

export interface CheckpointInfo {
  completed_adapters: string[];
  completed_count: number;
  failed_adapter: string | null;
  updated_at: string | null;
}

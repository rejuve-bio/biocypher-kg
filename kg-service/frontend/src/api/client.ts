import type {
  AdaptersResponse,
  BuildJob,
  BuildRequest,
  FlagInfo,
  SpeciesEntry,
  ValidationResult,
} from "../types";

const BASE = "/api/console";

async function json<T>(p: Promise<Response>): Promise<T> {
  const res = await p;
  if (!res.ok) {
    let detail: unknown;
    try {
      detail = (await res.json())?.detail;
    } catch {
      detail = await res.text();
    }
    throw new ApiError(res.status, detail);
  }
  return res.json() as Promise<T>;
}

export class ApiError extends Error {
  constructor(public status: number, public detail: unknown) {
    super(typeof detail === "string" ? detail : `Request failed (${status})`);
  }
}

export const api = {
  listSpecies: () =>
    json<{ species: SpeciesEntry[] }>(fetch(`${BASE}/species`)).then(
      (d) => d.species,
    ),

  listAdapters: (species: string, dataset: string) =>
    json<AdaptersResponse>(
      fetch(`${BASE}/species/${species}/datasets/${dataset}/adapters`),
    ),

  listWriters: () =>
    json<{ writers: string[] }>(fetch(`${BASE}/writers`)).then((d) => d.writers),

  listFlags: () =>
    json<{ flags: FlagInfo[] }>(fetch(`${BASE}/flags`)).then((d) => d.flags),

  validate: (req: BuildRequest) =>
    json<ValidationResult>(
      fetch(`${BASE}/builds/validate`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(req),
      }),
    ),

  createBuild: (req: BuildRequest) =>
    json<{ id: string; status: string; job: BuildJob }>(
      fetch(`${BASE}/builds`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(req),
      }),
    ),

  listBuilds: () =>
    json<{ builds: BuildJob[] }>(fetch(`${BASE}/builds`)).then((d) => d.builds),

  getBuild: (id: string) => json<BuildJob>(fetch(`${BASE}/builds/${id}`)),

  getLogs: (id: string, tail = 500) =>
    json<{ id: string; status: string; return_code: number | null; lines: string[] }>(
      fetch(`${BASE}/builds/${id}/logs?tail=${tail}`),
    ),

  cancelBuild: (id: string) =>
    json<{ id: string; action: string }>(
      fetch(`${BASE}/builds/${id}`, { method: "DELETE" }),
    ),

  resumeBuild: (id: string) =>
    json<{ id: string; status: string; resumed_from: string }>(
      fetch(`${BASE}/builds/${id}/resume`, { method: "POST" }),
    ),

  loadBuild: (id: string, target: "neo4j" | "mork") =>
    json<{ id: string; status: string; kind: string }>(
      fetch(`${BASE}/builds/${id}/load/${target}`, { method: "POST" }),
    ),

  retryLoad: (id: string) =>
    json<{ id: string; status: string; kind: string }>(
      fetch(`${BASE}/builds/${id}/retry`, { method: "POST" }),
    ),

  getConfig: (species: string, dataset: string, kind: "adapters" | "schema") =>
    json<ConfigFile>(
      fetch(`${BASE}/species/${species}/datasets/${dataset}/config/${kind}`),
    ),

  saveConfig: (
    species: string,
    dataset: string,
    kind: "adapters" | "schema",
    content: string,
  ) =>
    json<ConfigFile>(
      fetch(`${BASE}/species/${species}/datasets/${dataset}/config/${kind}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ content }),
      }),
    ),

  listOutput: (id: string) =>
    json<{
      output_dir: string;
      exists: boolean;
      count: number;
      truncated?: boolean;
      files: { path: string; size: number }[];
    }>(fetch(`${BASE}/builds/${id}/output`)),

  getGraphInfo: (id: string) =>
    json<{ present: boolean; summary?: GraphInfoSummary }>(
      fetch(`${BASE}/builds/${id}/graph-info`),
    ),

  getConfigSnapshot: (id: string) =>
    json<{ present: boolean; manifest?: ConfigManifest }>(
      fetch(`${BASE}/builds/${id}/config-snapshot`),
    ),

  outputDownloadUrl: (id: string, path: string) =>
    `${BASE}/builds/${id}/output/download?path=${encodeURIComponent(path)}`,
};

export interface ConfigFile {
  species: string;
  dataset: string;
  kind: string;
  path: string;
  content: string;
}

// Observatory endpoints query the live Neo4j graph (mounted at /api, not /api/console).
const OBS = "/api";

export const obs = {
  summary: () => json<GraphSummary>(fetch(`${OBS}/summary`)),
  entities: (label: string, limit: number, offset: number) =>
    json<EntitiesPage>(
      fetch(`${OBS}/entities/${encodeURIComponent(label)}?limit=${limit}&offset=${offset}`),
    ),
};

export interface GraphSummary {
  node_count: number;
  edge_count: number;
  last_updated_at: string | null;
  dataset_count: number;
  top_entities: { name: string; count: number }[];
  top_connections: { name: string; count: number }[];
  schema?: { node_types: string[]; relationship_types: string[] };
}

export interface EntitiesPage {
  label: string;
  total: number;
  limit: number;
  offset: number;
  entities: Record<string, unknown>[];
}

export interface ConfigManifest {
  species: string;
  dataset: string;
  writer_type: string | null;
  source: { adapters_config: string; schema_config: string };
  sha256: { adapters_config: string; schema_config: string };
  config_hash: string;
  recorded_at: string;
}

export interface GraphInfoSummary {
  node_count: number | null;
  edge_count: number | null;
  dataset_count: number | null;
  last_updated_at: string | null;
  kg_format: string | null;
  data_size: string | null;
  top_entities: { name: string; count: number }[] | null;
  top_connections: { name: string; count: number }[] | null;
  datasets?: DatasetComposition[] | null;
}

export interface DatasetComposition {
  name: string;
  version?: string | null;
  url?: string | null;
  nodes: string[];
  edges: string[];
}

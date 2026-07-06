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

  outputDownloadUrl: (id: string, path: string) =>
    `${BASE}/builds/${id}/output/download?path=${encodeURIComponent(path)}`,
};

export interface GraphInfoSummary {
  node_count: number | null;
  edge_count: number | null;
  dataset_count: number | null;
  last_updated_at: string | null;
  kg_format: string | null;
  data_size: string | null;
  top_entities: { name: string; count: number }[] | null;
  top_connections: { name: string; count: number }[] | null;
}

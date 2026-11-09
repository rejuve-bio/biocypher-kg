import type {
  AdaptersResponse,
  BuildJob,
  BuildRequest,
  FlagInfo,
  SpeciesEntry,
  ValidationResult,
} from "../types";

const BASE = "/api/console";

/** Fired when the console asks for its password (on a server, see App's SignIn). */
export const SIGN_IN_EVENT = "kg-console-sign-in";

async function json<T>(p: Promise<Response>): Promise<T> {
  const res = await p;
  if (res.status === 401) window.dispatchEvent(new Event(SIGN_IN_EVENT));
  if (!res.ok) {
    // Read the body exactly once. A non-JSON error body (proxy/timeout page,
    // empty 5xx) must not crash the error path with "body stream already read".
    const body = await res.text().catch(() => "");
    let detail: unknown = body || `Request failed (${res.status})`;
    try {
      const parsed = JSON.parse(body);
      if (parsed && typeof parsed === "object" && "detail" in parsed) {
        detail = (parsed as { detail: unknown }).detail;
      }
    } catch {
      // Body was not JSON; keep the raw text (or the status fallback).
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

/** Human-readable byte size, e.g. 1536 -> "1.5 KB". */
export function formatBytes(bytes: number | null | undefined): string {
  if (!bytes || bytes <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.min(units.length - 1, Math.floor(Math.log(bytes) / Math.log(1024)));
  const v = bytes / Math.pow(1024, i);
  return `${v >= 10 || i === 0 ? Math.round(v) : v.toFixed(1)} ${units[i]}`;
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

  adaptersBySource: (species: string, dataset: string) =>
    json<AdaptersBySource>(
      fetch(`${BASE}/species/${species}/datasets/${dataset}/adapters-by-source`),
    ),

  listWriters: () =>
    json<{ writers: string[] }>(fetch(`${BASE}/writers`)).then((d) => d.writers),

  listFlags: () =>
    json<{ flags: FlagInfo[] }>(fetch(`${BASE}/flags`)).then((d) => d.flags),

  buildLimits: () =>
    json<{ cpu_count: number; max_workers: number }>(fetch(`${BASE}/build/limits`)),

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

  deleteBuild: (id: string, purgeOutput = false) =>
    json<{ id: string; action: string; output_purged?: boolean }>(
      fetch(`${BASE}/builds/${id}?purge_output=${purgeOutput}`, { method: "DELETE" }),
    ),

  // ── Source-data download + local disk ──
  startDownload: (req: {
    species: string;
    source?: string | null;
    sources?: string[];
    no_checksum?: boolean;
  }) =>
    json<{ id: string; status: string; kind: string; job: BuildJob }>(
      fetch(`${BASE}/downloads`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(req),
      }),
    ),

  // ── dbSNP cache (prepared by the console into the data volume) ──
  dbsnpStatus: (root?: string) =>
    json<DbsnpStatus>(
      fetch(`${BASE}/dbsnp/status${root ? `?root=${encodeURIComponent(root)}` : ""}`),
    ),

  prepareDbsnp: (variant: "common" | "full") =>
    json<{ id: string; status: string; kind: string; root: string }>(
      fetch(`${BASE}/dbsnp/prepare`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ variant }),
      }),
    ),

  listDownloads: () =>
    json<{ downloads: BuildJob[] }>(fetch(`${BASE}/downloads`)).then((d) => d.downloads),

  dataStatus: (species: string) =>
    json<DataStatus>(fetch(`${BASE}/species/${encodeURIComponent(species)}/data-status`)),

  diskUsage: () => json<DiskUsage>(fetch(`${BASE}/disk`)),

  /** Each adapter's known full size for a species+dataset (learned from builds). */
  sizes: (species: string, dataset: string) =>
    json<KnownSizes>(
      fetch(`${BASE}/sizes?species=${encodeURIComponent(species)}&dataset=${encodeURIComponent(dataset)}`),
    ),

  resumeBuild: (id: string) =>
    json<{ id: string; status: string; resumed_from: string }>(
      fetch(`${BASE}/builds/${id}/resume`, { method: "POST" }),
    ),

  // One load job, or for an all-species build one per species ("jobs") plus the
  // species skipped because they have no database ("skipped").
  loadBuild: (id: string, target: "neo4j" | "mork") =>
    json<{
      id?: string;
      status?: string;
      kind?: string;
      jobs?: { id: string; species: string }[];
      skipped?: { species: string; reason: string }[];
    }>(
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

  getConfigEntries: (species: string, dataset: string, kind: "adapters" | "schema") =>
    json<ConfigEntries>(
      fetch(`${BASE}/species/${species}/datasets/${dataset}/config/${kind}/entries`),
    ),

  getConfigEntry: (species: string, dataset: string, kind: "adapters" | "schema", name: string) =>
    json<{ name: string; data: Record<string, unknown> }>(
      fetch(`${BASE}/species/${species}/datasets/${dataset}/config/${kind}/entries/${encodeURIComponent(name)}`),
    ),

  saveConfigEntry: (
    species: string,
    dataset: string,
    kind: "adapters" | "schema",
    name: string,
    data: Record<string, unknown>,
  ) =>
    json<{ name: string; data: Record<string, unknown>; warnings?: string[] }>(
      fetch(`${BASE}/species/${species}/datasets/${dataset}/config/${kind}/entries/${encodeURIComponent(name)}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ data }),
      }),
    ),

  deleteConfigEntry: (species: string, dataset: string, kind: "adapters" | "schema", name: string) =>
    json<{ deleted: string }>(
      fetch(`${BASE}/species/${species}/datasets/${dataset}/config/${kind}/entries/${encodeURIComponent(name)}`, {
        method: "DELETE",
      }),
    ),

  biolinkCategories: () =>
    json<{ categories: string[] }>(fetch(`${BASE}/biolink/categories`)).then((d) => d.categories),

  dataSources: (species: string) =>
    json<DataSources>(fetch(`${BASE}/species/${encodeURIComponent(species)}/data-sources`)),

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

  // Signing in, when the console is on a server (it then asks for a password).
  session: () =>
    json<{ required: boolean; signed_in: boolean; on_server: boolean }>(fetch(`${BASE}/session`)),
  /** The server's folders for Browse… (no path: the places it can see). */
  listFolders: (path?: string | null) =>
    json<FolderListing>(
      fetch(`${BASE}/settings/folders${path ? `?path=${encodeURIComponent(path)}` : ""}`),
    ),
  login: (password: string) =>
    json<{ signed_in: boolean }>(
      fetch(`${BASE}/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ password }),
      }),
    ),
  logout: () => json<{ signed_in: boolean }>(fetch(`${BASE}/logout`, { method: "POST" })),

  exploreTargets: () =>
    json<{ targets: ExploreTarget[] }>(fetch(`${BASE}/explore/targets`)).then((d) => d.targets),

  exploreSchema: (species: string, db: "neo4j" | "mork") =>
    json<ExploreSchema>(
      fetch(`${BASE}/explore/schema?species=${encodeURIComponent(species)}&db=${db}`),
    ),

  // ── Runtime settings (DB connections + build config) ──
  getSettings: () => json<ConsoleSettings>(fetch(`${BASE}/settings`)),

  saveSettings: (patch: SettingsPatch) =>
    json<ConsoleSettings>(
      fetch(`${BASE}/settings`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(patch),
      }),
    ),

  dataLocations: () => json<DataLocationsInfo>(fetch(`${BASE}/settings/data-locations`)),

  /** Subfolders the console can read (no path: where to start). kind "dbsnp" marks caches. */
  /** Where a folder chosen in Finder is (browsers don't tell pages; the server finds it). */
  locateFolder: (body: ChosenFolder & { kind?: FolderKind; species?: string }) =>
    json<{ matches: string[] }>(
      fetch(`${BASE}/settings/folders/locate`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      }),
    ),

  // ── Per-species Neo4j / MORK instances the console manages through Docker ──
  instances: () => json<InstancesStatus>(fetch(`${BASE}/instances`)),

  instanceAction: (species: string, kind: InstanceKind, action: InstanceAction) =>
    json<{ species: string; kind: string; action: string }>(
      fetch(`${BASE}/instances/${encodeURIComponent(species)}/${kind}/${action}`, {
        method: "POST",
      }),
    ),

  // ── What each species uses, and connecting your own databases ──
  connections: (check = true) =>
    json<{ species: SpeciesConnections[] }>(fetch(`${BASE}/connections?check=${check}`)),

  connectExisting: (species: string, kind: InstanceKind, body: ExistingDatabase) =>
    json<SpeciesConnections>(
      fetch(`${BASE}/connections/${encodeURIComponent(species)}/${kind}`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      }),
    ),

  disconnect: (species: string, kind: InstanceKind) =>
    json<SpeciesConnections>(
      fetch(`${BASE}/connections/${encodeURIComponent(species)}/${kind}`, { method: "DELETE" }),
    ),

  /** Login of a Neo4j the console runs (for Neo4j Browser). Never your own database's. */
  consoleLogin: (species: string) =>
    json<{ user: string; password: string }>(
      fetch(`${BASE}/connections/${encodeURIComponent(species)}/neo4j/login`),
    ),

  testConnection: (req: TestConnectionRequest) =>
    json<TestResult>(
      fetch(`${BASE}/settings/test`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(req),
      }),
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
  warnings?: string[];
}

export interface DataSource {
  key: string;
  name: string;
  urls: string[];
}

export interface SourceAdapter {
  name: string;
  nodes: boolean;
  edges: boolean;
  needs_dbsnp: boolean;
}

export interface AdapterGroup {
  source: string;
  name: string;
  downloadable: boolean;
  needs_dbsnp: boolean;
  local_paths: string[];
  present: boolean | null;
  bytes: number | null;
  urls: string[];
  website: string | null;
  // Where its data was found: "yours" (your read-only folder) or "console" (downloaded).
  location: DataLocation | null;
  adapters: SourceAdapter[];
  adapter_count: number;
  node_count: number;
  edge_count: number;
}

export type DataLocation = "yours" | "console";

export interface DbsnpVariantInfo {
  present: boolean; // there and really a dbSNP cache
  bytes: number;
  built_at: string | null;
  problem?: string | null; // why a file that's there can't be used
  // The cache root a build should use for this variant (your read-only cache first).
  root?: string;
}

export interface DbsnpStatus {
  root: string | null; // where Prepare writes; null until the data folder is set
  readonly_root?: string | null; // a cache you already have, read-only
  variants: Record<"common" | "full", DbsnpVariantInfo>;
  job_id: string | null;
  job_variant: string | null;
  custom?: { root: string; variants: Record<"common" | "full", DbsnpVariantInfo> };
}

export interface AdaptersBySource {
  species: string;
  dataset: string;
  input_dir: string;
  groups: AdapterGroup[];
}

export interface DataStatusSource {
  key: string;
  name: string;
  present: boolean;
  bytes: number;
  location?: DataLocation | null;
}

export interface DataStatus {
  species: string;
  input_dir: string | null; // downloads go here; null until the data folder is set
  input_origin?: "console";
  readonly_dir?: string | null; // your data: read first, never written
  exists: boolean;
  manifest_present: boolean;
  source_count: number;
  present_count: number;
  total_bytes: number;
  sources: DataStatusSource[];
}

export interface DiskUsage {
  data_root: string | null;
  available: boolean;
  free_bytes?: number;
  total_bytes_fs?: number;
  used_bytes?: number;
  categories?: Record<string, number>;
}

/** One adapter's full size, learned from a build that wasn't cut short by a size
 * limit. null for a kind the adapter doesn't write. */
export interface KnownSize {
  nodes: number | null;
  edges: number | null;
  job_id: string;
  built_at: string | null;
}

export interface KnownSizes {
  species: string;
  dataset: string;
  adapters: Record<string, KnownSize>;
}

export interface DataSources {
  species: string;
  count: number;
  sources: DataSource[];
}

export interface EntrySummary {
  name: string;
  // adapters
  nodes?: boolean;
  edges?: boolean;
  module?: string | null;
  cls?: string | null;
  // schema
  represented_as?: string;
  input_label?: string | null;
  biolink_category?: string | null;
  is_a?: string | string[] | null;
  // true for entries inherited from the base (primer) schema: shared + read-only here
  inherited?: boolean;
}

export interface ConfigEntries {
  species: string;
  dataset: string;
  kind: "adapters" | "schema";
  path: string;
  count: number;
  entries: EntrySummary[];
  // base (primer) schema entries this species does not override (schema views only)
  inherited?: EntrySummary[];
}

// Observatory endpoints query the live Neo4j graph (mounted at /api, not /api/console).
const OBS = "/api";

export const obs = {
  summary: () => json<GraphSummary>(fetch(`${OBS}/summary`)),
  schema: () => json<DetailedSchema>(fetch(`${OBS}/schema`)),
  entities: (label: string, limit: number, offset: number) =>
    json<EntitiesPage>(
      fetch(`${OBS}/entities/${encodeURIComponent(label)}?limit=${limit}&offset=${offset}`),
    ),
};

export interface DetailedSchema {
  nodes: { data: { id: string; properties: string[] } }[];
  edges: { data: { source: string; target: string; possible_connections: string[] } }[];
}

export interface ExploreTarget {
  species: string;
  explicit: boolean;
  neo4j_uri: string | null;
  mork_url: string | null;
}

export interface SpeciesEndpoint {
  neo4j_uri?: string | null;
  mork_url?: string | null;
}

export interface ConsoleSettings {
  neo4j: { uri: string | null; user: string | null; database: string | null; password_set: boolean };
  mork: { url: string | null };
  build: {
    // Required, no defaults: null until chosen in Settings.
    data_root: string | null;
    output_root: string | null; // the Builds folder
    neo4j_memory: string | null; // memory for each Neo4j the console runs, e.g. "2G"
    missing: string[]; // what a build still needs set, e.g. ["Builds folder"]
    max_concurrent_builds: number | null;
    max_output_builds: number | null;
    max_build_history: number | null;
    limits?: Record<"max_concurrent_builds" | "max_output_builds" | "max_build_history", [number, number]>;
  };
  species: Record<string, SpeciesEndpoint>;
  available_species: string[];
}

export interface SettingsPatch {
  neo4j?: { uri?: string | null; user?: string | null; password?: string | null; database?: string | null };
  mork?: { url?: string | null };
  build?: {
    data_root?: string | null;
    output_root?: string | null;
    neo4j_memory?: string | null;
    max_concurrent_builds?: number | null;
    max_output_builds?: number | null;
    max_build_history?: number | null;
  };
  // A null value removes that species' overrides.
  species?: Record<string, SpeciesEndpoint | null>;
  // Folders that already hold data; null or "" clears an override.
  data?: { input_dirs?: Record<string, string | null>; dbsnp_root?: string | null };
}

export type InstanceKind = "neo4j" | "mork";
export type InstanceAction = "setup" | "stop" | "remove";

/** What a folder field is for: a dbSNP cache, a species' source data, the folder the
 * console keeps its data in, or the Builds folder. The server checks the chosen
 * folder really is that. */
export type FolderKind = "dbsnp" | "source" | "writable" | "builds";

/** One level of the server's folders, for Browse… on a server. */
export interface FolderListing {
  path: string | null; // null: the places the console can see
  parent: string | null; // null at a place: back goes to the places
  writable: boolean;
  folders: { name: string; path: string }[];
}

/** A folder chosen in Finder, as the browser describes it: its name and (a sample
 * of) what's in it. count is how many entries it has, when the browser knows. */
export interface ChosenFolder {
  name: string;
  entries: { name: string; dir: boolean; size?: number }[];
  count: number | null;
}

/** Where a species' database comes from: run by the console, or your own. */
export type ConnSource = "console" | "existing" | null;

export interface ConnInfo {
  source: ConnSource;
  address: string | null;
  unencrypted?: boolean; // your own database on another computer, without encryption
  // Present when checked: does it answer (and, for Neo4j, does the login work)?
  ok?: boolean;
  error?: string | null;
  reason?: "auth" | "unreachable" | "database" | "address" | "other" | null;
  // Neo4j only (MORK has no login).
  user?: string;
  database?: string;
  password_set?: boolean;
}

export interface SpeciesConnections {
  species: string;
  neo4j: ConnInfo;
  mork: ConnInfo;
}

export interface ExistingDatabase {
  address: string;
  user?: string;
  password?: string;
  database?: string;
}

export interface InstanceInfo {
  name: string; // container name, e.g. neo4j-hsa
  exists: boolean;
  running: boolean;
  state: string | null;
  managed: boolean; // created by the console (vs. up.sh / by hand)
  url: string; // how the console connects (bolt://neo4j-hsa:7687)
  connected: boolean; // is the species' connection setting pointed at this instance?
  open_url: string; // where to open it on this machine (Neo4j Browser)
  progress: { state: "pulling" | "creating" | "starting" | "done" | "error"; message: string } | null;
}

export interface InstancesStatus {
  docker: boolean; // can the console reach Docker to manage instances?
  error?: string;
  species: { species: string; neo4j: InstanceInfo; mork: InstanceInfo }[];
}

export interface SpeciesDataLocation {
  species: string;
  // Where downloads go: <data folder>/input/<species>; null until the data folder is set.
  path: string | null;
  origin: "console";
  exists: boolean;
  // A folder you chose: read first, never written.
  readonly: string | null;
  readonly_exists: boolean;
  // Does your folder hold this species' data (the files its adapters read)?
  readonly_check: { found: string[]; total: number; problem: string | null } | null;
  override: string | null;
  default: string | null;
  config_dir: string | null;
}

export interface DataLocationsInfo {
  in_docker: boolean;
  species: SpeciesDataLocation[];
  dbsnp: {
    path: string | null; // where Prepare writes; null until the data folder is set
    override: string | null;
    default: string | null;
    readonly_root: string | null;
    variants: Record<"common" | "full", DbsnpVariantInfo>;
    readonly_variants: Record<"common" | "full", DbsnpVariantInfo> | null;
    readonly_problem: string | null; // why your cache can't be used
  };
}

export interface TestConnectionRequest {
  target: "neo4j" | "mork";
  uri?: string;
  user?: string;
  password?: string;
  database?: string;
  url?: string;
}

export interface TestResult {
  reachable: boolean;
  error?: string;
}

export interface ExploreSchema {
  species: string;
  db: "neo4j" | "mork";
  reachable: boolean;
  error?: string;
  schema?: DetailedSchema;
  node_types?: number;
  edge_types?: number;
  node_count?: number;
  edge_count?: number;
  atom_count?: number;
  atom_count_capped?: boolean; // counted from a bounded read: a lower bound
  version?: string | null;
  sampled?: boolean;
  top_entities?: { name: string; count: number }[];
  top_connections?: { name: string; count: number }[];
}

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
  schema?: DetailedSchema | null;
  // What each adapter wrote; complete = false when a size limit cut it short.
  adapter_counts?: Record<string, { nodes: number | null; edges: number | null; complete: boolean }> | null;
}

export interface DatasetComposition {
  name: string;
  version?: string | null;
  url?: string | null;
  nodes: string[];
  edges: string[];
}

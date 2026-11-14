import { lazy, Suspense, useEffect, useRef, useState } from "react";
import { useParams, useNavigate, Link } from "react-router-dom";
import { api, type GraphInfoSummary, type ConfigManifest } from "../api/client";
import type { BuildJob } from "../types";
import JobStatusBadge from "../components/JobStatusBadge";
import { sizeSetting } from "../sizes";
import ConfirmDialog, { type Confirmable } from "../components/ConfirmDialog";

// Lazy so Cytoscape ships in its own chunk, only on build pages that show a schema.
const SchemaGraph = lazy(() => import("../components/SchemaGraph"));

const TERMINAL = new Set(["succeeded", "failed", "cancelled"]);

type OutputFile = { path: string; size: number };

function fmtNum(n: number | null): string {
  return n == null ? "—" : n.toLocaleString();
}

function humanSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let n = bytes / 1024;
  let i = 0;
  while (n >= 1024 && i < units.length - 1) {
    n /= 1024;
    i++;
  }
  return `${n.toFixed(1)} ${units[i]}`;
}

// A MORK load isn't surgical like Neo4j's: if anything changed, the loader clears
// MORK's annotation space and imports only this build's files.
const MORK_REPLACES =
  "Replaces MORK's data with this build only. Skipped if nothing changed.";

export default function BuildDetail() {
  const { id = "" } = useParams();
  const navigate = useNavigate();
  const [job, setJob] = useState<BuildJob | null>(null);
  const [lines, setLines] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [files, setFiles] = useState<OutputFile[] | null>(null);
  const [graphInfo, setGraphInfo] = useState<GraphInfoSummary | null>(null);
  const [snapshot, setSnapshot] = useState<ConfigManifest | null>(null);
  const [pending, setPending] = useState<Confirmable | null>(null);
  const [fileFilter, setFileFilter] = useState("");
  const [notice, setNotice] = useState<string | null>(null);
  const [refresh, setRefresh] = useState(0); // restarts polling (e.g. after loads start)
  const logRef = useRef<HTMLDivElement>(null);
  const stick = useRef(true);

  useEffect(() => {
    let alive = true;
    let timer: ReturnType<typeof setTimeout>;

    const tick = async () => {
      try {
        const [j, l] = await Promise.all([api.getBuild(id), api.getLogs(id, 2000)]);
        if (!alive) return;
        setJob(j);
        setLines(l.lines);
        // Refresh file list every poll so downloads appear incrementally.
        const [out, gi] = await Promise.all([
          api.listOutput(id).catch(() => null),
          api.getGraphInfo(id).catch(() => null),
        ]);
        if (!alive) return;
        if (out?.exists) setFiles(out.files);
        if (gi?.present) setGraphInfo(gi.summary ?? null);
        // Keep polling while the build runs, and while any of its loads does: a
        // finished build whose load is still going must show the load finishing.
        const loadActive = Object.values(j.loads ?? {}).some(
          (l) => l.status === "queued" || l.status === "running",
        );
        if (!TERMINAL.has(j.status) || loadActive) {
          timer = setTimeout(tick, 2000);
        }
      } catch (e) {
        if (alive) setError(String(e));
      }
    };
    tick();
    return () => {
      alive = false;
      clearTimeout(timer);
    };
  }, [id, refresh]);

  // Auto-scroll the log to the bottom unless the user scrolled up.
  useEffect(() => {
    const el = logRef.current;
    if (el && stick.current) el.scrollTop = el.scrollHeight;
  }, [lines]);

  // Fetch the recorded config snapshot once a build has succeeded.
  useEffect(() => {
    if (!job || (job.kind ?? "build") !== "build" || job.status !== "succeeded") return;
    api.getConfigSnapshot(id).then((r) => setSnapshot(r.manifest ?? null)).catch(() => {});
  }, [id, job?.status, job?.kind]);

  async function onCancel() {
    try {
      await api.cancelBuild(id);
    } catch (e) {
      setError(String(e));
    }
  }

  async function onResume() {
    try {
      const res = await api.resumeBuild(id);
      navigate(`/builds/${res.id}`);
    } catch (e) {
      setError(String(e));
    }
  }

  async function onLoad(target: "neo4j" | "mork") {
    try {
      const res = await api.loadBuild(id, target);
      if (res.jobs) {
        // All species: one load per species, shown on this page as they run.
        const skipped = (res.skipped ?? []).map((s) => `${s.species} (${s.reason})`);
        setNotice(
          `loading ${res.jobs.map((j) => j.species).join(", ")}` +
            (skipped.length ? ` · skipped: ${skipped.join(", ")}` : ""),
        );
        setRefresh((n) => n + 1);
      } else if (res.id) {
        navigate(`/builds/${res.id}`);
      }
    } catch (e) {
      setNotice(e instanceof Error ? e.message : String(e));
    }
  }

  async function onRetry() {
    try {
      const res = await api.retryLoad(id);
      navigate(`/builds/${res.id}`);
    } catch (e) {
      setError(String(e));
    }
  }

  function renderLoadBtn(target: "neo4j" | "mork") {
    const T = target === "neo4j" ? "Neo4j" : "MORK";
    // Every load of this target (one per species for an all-species build).
    const mine = Object.entries(job?.loads ?? {})
      .filter(([key, l]) => (l.target ?? key) === target)
      .map(([, l]) => l);
    const inProgress = mine.some((l) => l.status === "running" || l.status === "queued");
    const loaded = mine.length > 0 && mine.every((l) => l.status === "succeeded");
    const each = allSpecies ? " each species" : "";
    return (
      <button
        className="secondary"
        onClick={() =>
          setPending({
            title: loaded ? `Reload${each} ${T}?` : `Load${each} into ${T}?`,
            message:
              (allSpecies
                ? `Each species goes into its own ${T}. Species without one are skipped. `
                : "") +
              (target === "mork"
                ? MORK_REPLACES
                : loaded
                  ? `Re-syncs your live ${T}. Skipped if nothing changed.`
                  : `Writes to your live ${T}. Only changed datasets are updated.`),
            confirmLabel: loaded ? `Reload ${T}` : `Load ${T}`,
            run: () => onLoad(target),
          })
        }
        disabled={inProgress}
        title={
          loaded
            ? target === "mork"
              ? "loaded · reload replaces MORK's data"
              : `loaded · reload re-syncs ${T}`
            : undefined
        }
      >
        {inProgress ? `Loading ${T}…` : loaded ? `Reload${each} ${T}` : `Load${each} to ${T}`}
      </button>
    );
  }

  if (error) return <div className="alert err">{error}</div>;
  if (!job) return <div className="muted">Loading…</div>;

  const active = !TERMINAL.has(job.status);
  const isBuild = (job.kind ?? "build") === "build";
  const allSpecies = String((job.params as { species?: string })?.species ?? "").toLowerCase() === "all";
  // What each adapter wrote (graph_info "adapter_counts"), marking the ones a size
  // limit stopped early. The rest are full sizes, which Percent builds use.
  const adapterCounts = Object.entries(graphInfo?.adapter_counts ?? {});
  const cutShort = adapterCounts.filter(([, c]) => !c.complete).length;
  const writer = (job.params as { writer_type?: string })?.writer_type;
  const canLoadNeo4j = isBuild && job.status === "succeeded" && writer === "neo4j";
  const canLoadMork = isBuild && job.status === "succeeded" && writer === "metta";

  return (
    <>
      <div className="card">
        <div className="row" style={{ justifyContent: "space-between" }}>
          <div className="row">
            <JobStatusBadge status={job.status} />
            {!isBuild && (
              <span className="tag edge">
                {job.kind === "dbsnp" ? "dbSNP cache" : (job.kind ?? "").replace("load-", "load → ")}
              </span>
            )}
            <span className="mono muted">{job.id}</span>
          </div>
          <div className="row">
            <Link className="link" to="/history">
              ← builds
            </Link>
            {active && (
              <button
                className="danger"
                onClick={() =>
                  setPending({
                    title: "Stop this build?",
                    message: "Finished adapters are kept, so you can resume later.",
                    confirmLabel: "Stop build",
                    cancelLabel: "Keep building",
                    tone: "danger",
                    run: onCancel,
                  })
                }
              >
                Cancel
              </button>
            )}
            {job.resumable && (
              <button className="secondary" onClick={onResume}>
                Resume
              </button>
            )}
            {job.retryable && (
              <button
                className="secondary"
                onClick={() =>
                  setPending({
                    title: "Retry this load?",
                    message: (job.kind ?? "").includes("mork")
                      ? MORK_REPLACES
                      : "Writes to your live Neo4j. Only changed datasets are updated.",
                    confirmLabel: "Retry load",
                    run: onRetry,
                  })
                }
              >
                Retry load
              </button>
            )}
            {canLoadNeo4j && renderLoadBtn("neo4j")}
            {canLoadMork && renderLoadBtn("mork")}
          </div>
        </div>
        <div className="row" style={{ marginTop: 12, gap: 24 }}>
          <Meta label="Started" value={fmt(job.started_at)} />
          <Meta label="Finished" value={fmt(job.finished_at)} />
          {isBuild && <Meta label="Build size" value={sizeSetting(job.params)} />}
        </div>
        {isBuild && job.output_dir && (
          <div style={{ marginTop: 12 }}>
            <Meta label="Saved in" value={job.output_dir} />
          </div>
        )}
        {isBuild && job.loads && Object.keys(job.loads).length > 0 && (
          <div className="row" style={{ marginTop: 14, gap: 8, alignItems: "center" }}>
            <span className="muted" style={{ fontSize: 12 }}>Loaded into</span>
            {Object.entries(job.loads).map(([key, l]) => (
              <span
                key={key}
                className={loadPillClass(l.status)}
                title={`${l.status} · open log`}
                onClick={() => navigate(`/builds/${l.job_id}`)}
              >
                {(l.target ?? key) === "neo4j" ? "Neo4j" : "MORK"}
                {allSpecies && l.species ? ` ${l.species}` : ""} {loadIcon(l.status)}
              </span>
            ))}
          </div>
        )}
        {isBuild && snapshot && (
          <div className="row" style={{ marginTop: 12, gap: 8, alignItems: "center" }}>
            <span className="muted" style={{ fontSize: 12 }}>Config</span>
            <span
              className="mono"
              title={`adapters: ${snapshot.source.adapters_config}\nschema:   ${snapshot.source.schema_config}`}
            >
              {snapshot.config_hash.slice(0, 12)}
            </span>
            <a className="link" href={api.outputDownloadUrl(id, "build_config/manifest.json")}>
              snapshot
            </a>
          </div>
        )}
        {job.total_adapters != null &&
          (job.checkpoint || job.status === "running") && (
            <div style={{ marginTop: 12 }}>
              <div className="muted" style={{ fontSize: 12, marginBottom: 4 }}>
                {job.checkpoint?.completed_count ?? 0}/{job.total_adapters} adapters
              </div>
              <div className="progress">
                <div
                  className="progress-fill"
                  style={{
                    width: `${Math.min(
                      100,
                      Math.round(
                        (100 * (job.checkpoint?.completed_count ?? 0)) /
                          job.total_adapters,
                      ),
                    )}%`,
                  }}
                />
              </div>
            </div>
          )}
        {job.checkpoint && (
          <div className="alert ok" style={{ marginTop: 10 }}>
            checkpoint · <strong>{job.checkpoint.completed_count}</strong> adapter
            {job.checkpoint.completed_count === 1 ? "" : "s"} done
            {job.checkpoint.failed_adapter ? ` · failed on ${job.checkpoint.failed_adapter}` : ""}
            {job.resumable ? " · resume continues here" : ""}
          </div>
        )}
        {job.error && <div className="alert err" style={{ marginTop: 10 }}>{job.error}</div>}
        {notice && <div className="alert ok mono" style={{ marginTop: 10 }}>{notice}</div>}
        <details style={{ marginTop: 10 }}>
          <summary className="muted">command</summary>
          <div className="mono" style={{ marginTop: 6 }}>{job.cmd.join(" ")}</div>
        </details>
      </div>

      <div className="card">
        <h2>Log {active ? "· live" : ""}</h2>
        <div
          className="logbox"
          ref={logRef}
          onScroll={(e) => {
            const el = e.currentTarget;
            stick.current = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
          }}
        >
          {lines.length ? lines.join("\n") : "no output yet"}
        </div>
      </div>

      {(graphInfo || (files && files.length > 0)) && (
        <div className="card">
          <h2>Results {active ? "· updating live" : ""}</h2>
          {graphInfo && (
            <div className="row" style={{ gap: 24, marginBottom: 14 }}>
              <Meta label="Nodes" value={fmtNum(graphInfo.node_count)} />
              <Meta label="Edges" value={fmtNum(graphInfo.edge_count)} />
              <Meta label="Datasets" value={graphInfo.dataset_count ?? "—"} />
              <Meta label="Format" value={graphInfo.kg_format ?? "—"} />
              <Meta label="Size" value={graphInfo.data_size ?? "—"} />
            </div>
          )}
          {graphInfo?.top_entities && graphInfo.top_entities.length > 0 && (
            <div style={{ marginBottom: 14 }}>
              <div className="muted" style={{ fontSize: 12, marginBottom: 6 }}>
                Top node types
              </div>
              <div className="chips">
                {graphInfo.top_entities.slice(0, 12).map((e) => (
                  <span className="pill" key={e.name}>
                    {e.name} <strong>{e.count.toLocaleString()}</strong>
                  </span>
                ))}
              </div>
            </div>
          )}
          {graphInfo?.top_connections && graphInfo.top_connections.length > 0 && (
            <div style={{ marginBottom: 14 }}>
              <div className="muted" style={{ fontSize: 12, marginBottom: 6 }}>
                Top edge types
              </div>
              <div className="chips">
                {graphInfo.top_connections.slice(0, 12).map((e) => (
                  <span className="pill" key={e.name}>
                    {e.name} <strong>{e.count.toLocaleString()}</strong>
                  </span>
                ))}
              </div>
            </div>
          )}
          {graphInfo?.schema && graphInfo.schema.nodes.length > 0 && (
            <div style={{ margin: "6px 0 18px" }}>
              <div className="muted" style={{ fontSize: 12, marginBottom: 6 }}>
                Schema
              </div>
              <Suspense fallback={<div className="muted">Loading schema…</div>}>
                <SchemaGraph
                  schema={graphInfo.schema}
                  topEntities={graphInfo.top_entities}
                  topConnections={graphInfo.top_connections}
                />
              </Suspense>
            </div>
          )}
          {adapterCounts.length > 0 && (
            <div style={{ marginBottom: 14 }}>
              <div className="muted" style={{ fontSize: 12, marginBottom: 6 }}>
                Rows by adapter
                {cutShort > 0 ? ` · ${cutShort} cut by size limit` : " · full sizes, now known"}
              </div>
              <table>
                <thead>
                  <tr>
                    <th>Adapter</th>
                    <th style={{ textAlign: "right" }}>Nodes</th>
                    <th style={{ textAlign: "right" }}>Edges</th>
                  </tr>
                </thead>
                <tbody>
                  {adapterCounts.map(([name, c]) => (
                    <tr key={name}>
                      <td className="mono" style={{ whiteSpace: "nowrap" }}>
                        {name}
                        {!c.complete && (
                          <span className="muted" title="stopped early by the size limit">
                            {" "}· cut
                          </span>
                        )}
                      </td>
                      <td className="mono" style={{ textAlign: "right" }}>{c.nodes == null ? "—" : fmtNum(c.nodes)}</td>
                      <td className="mono" style={{ textAlign: "right" }}>{c.edges == null ? "—" : fmtNum(c.edges)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          {graphInfo?.datasets && graphInfo.datasets.length > 0 && (
            <div style={{ marginBottom: 14 }}>
              <div className="muted" style={{ fontSize: 12, marginBottom: 6 }}>
                Composition by adapter
              </div>
              <table>
                <thead>
                  <tr>
                    <th>Adapter</th>
                    <th>Node types</th>
                    <th>Edge types</th>
                  </tr>
                </thead>
                <tbody>
                  {graphInfo.datasets.map((d) => (
                    <tr key={d.name}>
                      <td className="mono" style={{ whiteSpace: "nowrap", verticalAlign: "top" }}>
                        {d.name}
                        {d.version ? <span className="muted"> {d.version}</span> : null}
                      </td>
                      <td style={{ verticalAlign: "top" }}>
                        {d.nodes?.length ? (
                          <span className="chips">
                            {d.nodes.map((n) => (
                              <span className="tag node" key={n}>{n}</span>
                            ))}
                          </span>
                        ) : (
                          <span className="muted">—</span>
                        )}
                      </td>
                      <td style={{ verticalAlign: "top" }}>
                        {d.edges?.length ? (
                          <span className="chips">
                            {d.edges.map((e) => (
                              <span className="tag edge" key={e}>{e}</span>
                            ))}
                          </span>
                        ) : (
                          <span className="muted">—</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          {files && files.length > 0 && (() => {
            const fq = fileFilter.trim().toLowerCase();
            const shown = fq
              ? files.filter((f) => f.path.toLowerCase().includes(fq))
              : files;
            return (
              <>
                <div
                  className="row"
                  style={{ justifyContent: "space-between", marginBottom: 6 }}
                >
                  <span className="muted" style={{ fontSize: 12 }}>
                    Output files ({fq ? `${shown.length} of ${files.length}` : files.length})
                  </span>
                  <input
                    type="text"
                    value={fileFilter}
                    onChange={(e) => setFileFilter(e.target.value)}
                    placeholder="filter files"
                    style={{ minWidth: 220 }}
                  />
                </div>
                <table>
                  <tbody>
                    {shown.map((f) => (
                      <tr key={f.path}>
                        <td className="mono">{f.path}</td>
                        <td className="muted" style={{ whiteSpace: "nowrap" }}>
                          {humanSize(f.size)}
                        </td>
                        <td style={{ textAlign: "right" }}>
                          <a className="link" href={api.outputDownloadUrl(id, f.path)}>
                            download
                          </a>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {!shown.length && (
                  <span className="muted">no match for “{fileFilter}”</span>
                )}
              </>
            );
          })()}
        </div>
      )}
      <ConfirmDialog pending={pending} onClose={() => setPending(null)} />
    </>
  );
}

function Meta({ label, value }: { label: string; value: unknown }) {
  return (
    <div>
      <div className="muted" style={{ fontSize: 12 }}>
        {label}
      </div>
      <div className="mono">{String(value)}</div>
    </div>
  );
}

function loadPillClass(status: string): string {
  if (status === "succeeded") return "loadpill ok";
  if (status === "failed" || status === "cancelled") return "loadpill err";
  return "loadpill run"; // queued / running
}
function loadIcon(status: string): string {
  if (status === "succeeded") return "ok";
  if (status === "failed" || status === "cancelled") return "failed";
  return "…";
}

function fmt(iso: string | null): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString();
  } catch {
    return iso;
  }
}

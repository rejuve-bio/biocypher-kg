import { useEffect, useRef, useState } from "react";
import { useParams, useNavigate, Link } from "react-router-dom";
import { api, type GraphInfoSummary, type ConfigManifest } from "../api/client";
import type { BuildJob } from "../types";
import JobStatusBadge from "../components/JobStatusBadge";
import ConfirmDialog, { type Confirmable } from "../components/ConfirmDialog";

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
        if (!TERMINAL.has(j.status)) {
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
  }, [id]);

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
      navigate(`/builds/${res.id}`);
    } catch (e) {
      setError(String(e));
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
    const st = job?.loads?.[target]?.status;
    const inProgress = st === "running" || st === "queued";
    const loaded = st === "succeeded";
    return (
      <button
        className="secondary"
        onClick={() =>
          setPending({
            title: loaded ? `Reload ${T}?` : `Load into ${T}?`,
            message: loaded
              ? `${T} already holds this build. Reloading re-runs the surgical update on your live ${T} database, and it is skipped automatically if nothing has changed.`
              : `This writes to your live ${T} database. Only datasets that changed are updated (a surgical load); everything else is left in place.`,
            confirmLabel: loaded ? `Reload ${T}` : `Load ${T}`,
            run: () => onLoad(target),
          })
        }
        disabled={inProgress}
        title={
          loaded
            ? `Already loaded to ${T}. Reload re-syncs (surgical: skipped if unchanged) — useful if ${T} was reset.`
            : inProgress
              ? `Loading into ${T}…`
              : undefined
        }
      >
        {inProgress ? `⇪ Loading ${T}…` : loaded ? `⟲ Reload ${T}` : `⇪ Load to ${T}`}
      </button>
    );
  }

  if (error) return <div className="alert err">{error}</div>;
  if (!job) return <div className="muted">Loading…</div>;

  const active = !TERMINAL.has(job.status);
  const isBuild = (job.kind ?? "build") === "build";
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
              <span className="tag edge">{(job.kind ?? "").replace("load-", "load→")}</span>
            )}
            <span className="mono muted">{job.id}</span>
          </div>
          <div className="row">
            <Link className="link" to="/history">
              ← All builds
            </Link>
            {active && (
              <button
                className="danger"
                onClick={() =>
                  setPending({
                    title: "Stop this running build?",
                    message:
                      "The build in progress will be stopped now. Adapters that already finished are kept as a checkpoint, so you can resume later instead of starting from scratch.",
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
                ⟲ Resume
              </button>
            )}
            {job.retryable && (
              <button
                className="secondary"
                onClick={() =>
                  setPending({
                    title: "Retry this load?",
                    message: `This re-runs the failed load into your live ${
                      (job.kind ?? "").includes("mork") ? "MORK" : "Neo4j"
                    } database. Only changed datasets are applied (a surgical update).`,
                    confirmLabel: "Retry load",
                    run: onRetry,
                  })
                }
              >
                ⟲ Retry load
              </button>
            )}
            {canLoadNeo4j && renderLoadBtn("neo4j")}
            {canLoadMork && renderLoadBtn("mork")}
          </div>
        </div>
        <div className="row" style={{ marginTop: 12, gap: 24 }}>
          <Meta label="Return code" value={job.return_code ?? "—"} />
          <Meta label="Started" value={fmt(job.started_at)} />
          <Meta label="Finished" value={fmt(job.finished_at)} />
          <Meta label="PID" value={job.pid ?? "—"} />
        </div>
        {isBuild && job.loads && Object.keys(job.loads).length > 0 && (
          <div className="row" style={{ marginTop: 14, gap: 8, alignItems: "center" }}>
            <span className="muted" style={{ fontSize: 12 }}>Loaded into</span>
            {Object.entries(job.loads).map(([target, l]) => (
              <span
                key={target}
                className={loadPillClass(l.status)}
                title={`${l.status} — view load log`}
                onClick={() => navigate(`/builds/${l.job_id}`)}
              >
                {target === "neo4j" ? "Neo4j" : "MORK"} {loadIcon(l.status)}
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
                Progress: {job.checkpoint?.completed_count ?? 0}/{job.total_adapters}{" "}
                adapters
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
            Checkpoint: <strong>{job.checkpoint.completed_count}</strong> adapter
            {job.checkpoint.completed_count === 1 ? "" : "s"} completed
            {job.checkpoint.failed_adapter
              ? ` · failed on ${job.checkpoint.failed_adapter}`
              : ""}
            {job.resumable ? " — Resume continues from here." : ""}
          </div>
        )}
        {job.error && <div className="alert err" style={{ marginTop: 10 }}>{job.error}</div>}
        <details style={{ marginTop: 10 }}>
          <summary className="muted">Command &amp; output dir</summary>
          <div className="mono" style={{ marginTop: 6 }}>{job.cmd.join(" ")}</div>
          <div className="mono muted" style={{ marginTop: 6 }}>→ {job.output_dir}</div>
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
          {lines.length ? lines.join("\n") : "(no output yet)"}
        </div>
      </div>

      {(graphInfo || (files && files.length > 0)) && (
        <div className="card">
          <h2>Results {active ? "· updating live" : ""}</h2>
          <div className="alert ok" style={{ marginBottom: 14 }}>
            📁 Files were written to <span className="mono">{job.output_dir}</span>
            <div className="muted" style={{ fontSize: 12, marginTop: 4 }}>
              They're already on disk there — the download links below are just a
              convenience (useful when the Console runs on a remote server).
            </div>
          </div>
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
                    placeholder="🔍 Filter files…"
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
                  <span className="muted">No files match “{fileFilter}”.</span>
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
  if (status === "succeeded") return "✓";
  if (status === "failed" || status === "cancelled") return "⟲";
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

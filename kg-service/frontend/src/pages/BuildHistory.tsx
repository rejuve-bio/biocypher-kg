import { useEffect, useState, type MouseEvent } from "react";
import { useNavigate } from "react-router-dom";
import { api } from "../api/client";
import type { BuildJob, JobStatus } from "../types";
import JobStatusBadge from "../components/JobStatusBadge";

const ACTIVE = new Set(["queued", "running"]);
const TARGET_LABEL: Record<string, string> = { neo4j: "Neo4j", mork: "MORK" };

function loadPillClass(status: JobStatus): string {
  if (status === "succeeded") return "loadpill ok";
  if (status === "failed" || status === "cancelled") return "loadpill err";
  return "loadpill run"; // queued / running
}
function loadIcon(status: JobStatus): string {
  if (status === "succeeded") return "✓";
  if (status === "failed" || status === "cancelled") return "⟲";
  return "…";
}

export default function BuildHistory() {
  const navigate = useNavigate();
  const [builds, setBuilds] = useState<BuildJob[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    const load = () =>
      api
        .listBuilds()
        .then((b) => alive && setBuilds(b))
        .catch((e) => alive && setError(String(e)));
    load();
    // Poll while any build is active (also refreshes each build's load status).
    const timer = setInterval(load, 3000);
    return () => {
      alive = false;
      clearInterval(timer);
    };
  }, []);

  async function onResume(e: MouseEvent, jobId: string) {
    e.stopPropagation(); // don't trigger the row's navigate-to-detail
    try {
      const res = await api.resumeBuild(jobId);
      navigate(`/builds/${res.id}`);
    } catch (err) {
      setError(String(err));
    }
  }

  if (error) return <div className="alert err">{error}</div>;

  return (
    <div className="card">
      <h2>Build history</h2>
      {!builds.length && <div className="muted">No builds yet.</div>}
      {builds.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>Status</th>
              <th>Species / Dataset</th>
              <th>Writer</th>
              <th>Loaded</th>
              <th>Created</th>
              <th>ID</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {builds.map((b) => {
              const p = b.params as Record<string, unknown>;
              const loads = b.loads ?? {};
              const loadEntries = Object.entries(loads);
              return (
                <tr
                  key={b.id}
                  className="clickable"
                  onClick={() => navigate(`/builds/${b.id}`)}
                >
                  <td>
                    <JobStatusBadge status={b.status} />
                    {ACTIVE.has(b.status) ? " ⏳" : ""}
                  </td>
                  <td>
                    {String(p.species ?? "—")} / {String(p.dataset ?? "—")}
                  </td>
                  <td>{String(p.writer_type ?? "—")}</td>
                  <td>
                    {loadEntries.length ? (
                      <span className="row" style={{ gap: 6 }}>
                        {loadEntries.map(([target, l]) => (
                          <span
                            key={target}
                            className={loadPillClass(l.status)}
                            title={`Load → ${TARGET_LABEL[target] ?? target}: ${l.status} — click for log`}
                            onClick={(e) => {
                              e.stopPropagation();
                              navigate(`/builds/${l.job_id}`);
                            }}
                          >
                            {TARGET_LABEL[target] ?? target} {loadIcon(l.status)}
                          </span>
                        ))}
                      </span>
                    ) : (
                      <span className="muted" style={{ fontSize: 12 }}>
                        —
                      </span>
                    )}
                  </td>
                  <td className="muted">{fmt(b.created_at)}</td>
                  <td className="mono muted">{b.id.slice(0, 8)}</td>
                  <td>
                    {b.resumable ? (
                      <button
                        className="secondary btn-sm"
                        onClick={(e) => onResume(e, b.id)}
                        title="Resume from checkpoint"
                      >
                        ⟲ Resume
                      </button>
                    ) : (b.status === "failed" || b.status === "cancelled") ? (
                      <span
                        className="muted"
                        style={{ fontSize: 11 }}
                        title="No checkpoint was written (stopped before the first adapter completed)"
                      >
                        no checkpoint
                      </span>
                    ) : null}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}
    </div>
  );
}

function fmt(iso: string | null): string {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString();
  } catch {
    return iso;
  }
}

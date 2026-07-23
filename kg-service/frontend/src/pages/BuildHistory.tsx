import { useEffect, useState, type MouseEvent } from "react";
import { useNavigate } from "react-router-dom";
import { api } from "../api/client";
import type { BuildJob } from "../types";
import JobStatusBadge from "../components/JobStatusBadge";

const ACTIVE = new Set(["queued", "running"]);

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
    // Poll while any build is active.
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

  async function onRetry(e: MouseEvent, jobId: string) {
    e.stopPropagation();
    try {
      const res = await api.retryLoad(jobId);
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
              <th>Created</th>
              <th>ID</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {builds.map((b) => {
              const p = b.params as Record<string, unknown>;
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
                    {b.kind && b.kind !== "build" && (
                      <span className="tag edge" style={{ marginRight: 6 }}>
                        {b.kind.replace("load-", "load→")}
                      </span>
                    )}
                    {String(p.species ?? "—")} / {String(p.dataset ?? "—")}
                  </td>
                  <td>{String(p.writer_type ?? "—")}</td>
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
                    ) : b.retryable ? (
                      <button
                        className="secondary btn-sm"
                        onClick={(e) => onRetry(e, b.id)}
                        title="Re-run this load"
                      >
                        ⟲ Retry
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

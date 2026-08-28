import { useEffect, useState } from "react";
import { obs, type GraphSummary, type EntitiesPage } from "../api/client";

const PAGE = 50;

function cell(v: unknown): string {
  if (v == null) return "";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

function msgOf(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

export default function ExploreGraph() {
  const [summary, setSummary] = useState<GraphSummary | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [label, setLabel] = useState<string | null>(null);
  const [offset, setOffset] = useState(0);
  const [page, setPage] = useState<EntitiesPage | null>(null);
  const [pageErr, setPageErr] = useState<string | null>(null);

  useEffect(() => {
    obs
      .summary()
      .then(setSummary)
      .catch((e) => setError(msgOf(e)));
  }, []);

  useEffect(() => {
    if (!label) return;
    let alive = true;
    setPageErr(null);
    obs
      .entities(label, PAGE, offset)
      .then((p) => alive && setPage(p))
      .catch((e) => alive && setPageErr(msgOf(e)));
    return () => {
      alive = false;
    };
  }, [label, offset]);

  function selectLabel(l: string) {
    setLabel(l);
    setOffset(0);
    setPage(null);
  }

  if (error) {
    return (
      <div className="card">
        <h2>Explore graph</h2>
        <div className="alert err">Could not reach Neo4j: {error}</div>
        <div className="muted" style={{ fontSize: 13 }}>
          Exploration reads the live Neo4j graph. Make sure the Console is connected to a
          Neo4j instance with a loaded graph.
        </div>
      </div>
    );
  }

  if (!summary) return <div className="muted">Loading…</div>;

  const cols = page
    ? Array.from(new Set(page.entities.flatMap((e) => Object.keys(e)))).sort((a, b) =>
        a === "id" ? -1 : b === "id" ? 1 : a.localeCompare(b),
      )
    : [];
  const shown = page?.entities.length ?? 0;

  return (
    <>
      <div className="card">
        <h2>Explore graph</h2>
        <div className="row" style={{ gap: 24, marginBottom: 16 }}>
          <Meta label="Nodes" value={summary.node_count.toLocaleString()} />
          <Meta label="Edges" value={summary.edge_count.toLocaleString()} />
          <Meta label="Datasets" value={summary.dataset_count} />
          <Meta label="Last updated" value={fmt(summary.last_updated_at)} />
        </div>

        <div className="muted" style={{ fontSize: 12, marginBottom: 6 }}>
          Node types (click to browse)
        </div>
        <div className="chips" style={{ marginBottom: 14 }}>
          {summary.top_entities.map((e) => (
            <button
              key={e.name}
              className={`pill${label === e.name ? " pill-active" : ""}`}
              onClick={() => selectLabel(e.name)}
              title={`Browse ${e.name} nodes`}
            >
              {e.name} <strong>{e.count.toLocaleString()}</strong>
            </button>
          ))}
        </div>

        {summary.top_connections.length > 0 && (
          <>
            <div className="muted" style={{ fontSize: 12, marginBottom: 6 }}>
              Edge types
            </div>
            <div className="chips">
              {summary.top_connections.map((e) => (
                <span className="pill" key={e.name}>
                  {e.name} <strong>{e.count.toLocaleString()}</strong>
                </span>
              ))}
            </div>
          </>
        )}
      </div>

      {label && (
        <div className="card">
          <div className="row" style={{ justifyContent: "space-between" }}>
            <h2 style={{ margin: 0 }}>
              {label}{" "}
              {page && <span className="muted">· {page.total.toLocaleString()} total</span>}
            </h2>
            <div className="row">
              <button
                className="secondary btn-sm"
                disabled={offset === 0}
                onClick={() => setOffset(Math.max(0, offset - PAGE))}
              >
                ← Prev
              </button>
              <span className="muted" style={{ fontSize: 12 }}>
                {shown ? `${offset + 1}–${offset + shown}` : "0"}
                {page ? ` of ${page.total.toLocaleString()}` : ""}
              </span>
              <button
                className="secondary btn-sm"
                disabled={!page || offset + shown >= page.total}
                onClick={() => setOffset(offset + PAGE)}
              >
                Next →
              </button>
            </div>
          </div>

          {pageErr && <div className="alert err" style={{ marginTop: 10 }}>{pageErr}</div>}

          {page && page.entities.length > 0 && (
            <div style={{ overflowX: "auto", marginTop: 12 }}>
              <table>
                <thead>
                  <tr>
                    {cols.map((c) => (
                      <th key={c}>{c}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {page.entities.map((e, i) => (
                    <tr key={i}>
                      {cols.map((c) => (
                        <td key={c} className="mono" style={{ whiteSpace: "nowrap" }}>
                          {cell(e[c])}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          {page && page.entities.length === 0 && (
            <div className="muted" style={{ marginTop: 12 }}>No nodes on this page.</div>
          )}
        </div>
      )}
    </>
  );
}

function Meta({ label, value }: { label: string; value: unknown }) {
  return (
    <div>
      <div className="muted" style={{ fontSize: 12 }}>{label}</div>
      <div className="mono">{String(value)}</div>
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

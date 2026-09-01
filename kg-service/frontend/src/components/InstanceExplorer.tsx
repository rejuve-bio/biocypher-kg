import { useEffect, useState } from "react";
import NeighborhoodGraph from "./NeighborhoodGraph";
import { api, obs, type Neighborhood } from "../api/client";

type Db = "neo4j" | "mork";

type Instance = { id: string; properties: Record<string, unknown> };
type Loaded = { instances: Instance[]; total: number | null; hasMore: boolean; error?: string };

const NAME_KEYS = ["gene_name", "term_name", "transcript_name", "name", "label", "symbol"];

function subtitle(props: Record<string, unknown>): string | null {
  for (const k of NAME_KEYS) {
    const v = props[k];
    if (typeof v === "string" && v.trim()) return v;
  }
  return null;
}

async function loadInstances(db: Db, type: string, count: number): Promise<Loaded> {
  if (db === "mork") {
    const r = await api.morkInstances(type, count);
    if (!r.reachable) return { instances: [], total: null, hasMore: false, error: r.error || "MORK unreachable" };
    return {
      instances: (r.instances || []).map((i) => ({ id: i.id, properties: i.properties })),
      total: null,
      hasMore: Boolean(r.has_more),
    };
  }
  const r = await obs.entities(type, count, 0);
  const instances = (r.entities || [])
    .map((e) => ({ id: String(e.id ?? ""), properties: e }))
    .filter((i) => i.id);
  return { instances, total: r.total ?? null, hasMore: r.total != null ? count < r.total : instances.length >= count };
}

function neighborFetcher(db: Db): (type: string, id: string) => Promise<Neighborhood> {
  return db === "mork" ? (t, i) => api.morkNeighbors(t, i, 50) : (t, i) => obs.neighbors(t, i, 50);
}

const PAGE = 50;

export default function InstanceExplorer({ db, type, onClose }: { db: Db; type: string; onClose: () => void }) {
  const [count, setCount] = useState(PAGE);
  const [data, setData] = useState<Loaded | null>(null);
  const [loading, setLoading] = useState(true);
  const [seed, setSeed] = useState<{ type: string; id: string } | null>(null);

  useEffect(() => {
    setCount(PAGE);
    setSeed(null);
  }, [db, type]);

  useEffect(() => {
    let alive = true;
    setLoading(true);
    loadInstances(db, type, count)
      .then((d) => {
        if (!alive) return;
        setData(d);
        // Auto-seed the graph with the first instance for immediate context.
        setSeed((prev) => prev ?? (d.instances[0] ? { type, id: d.instances[0].id } : null));
      })
      .finally(() => alive && setLoading(false));
    return () => {
      alive = false;
    };
  }, [db, type, count]);

  return (
    <div className="card" style={{ marginTop: 14 }}>
      <div className="row" style={{ justifyContent: "space-between", alignItems: "center" }}>
        <div className="row" style={{ gap: 10, alignItems: "baseline" }}>
          <h3 style={{ margin: 0 }}>Instances of <span className="mono">{type}</span></h3>
          <span className="muted" style={{ fontSize: 12 }}>
            {data?.total != null ? `${data.total.toLocaleString()} total` : data ? `${data.instances.length}${data.hasMore ? "+" : ""} shown` : ""}
          </span>
        </div>
        <button className="secondary btn-sm" onClick={onClose}>Close</button>
      </div>

      {data?.error && <div className="alert warn" style={{ marginTop: 10 }}>{data.error}</div>}

      <div className="explorer-grid">
        <div className="instance-list">
          {loading && !data ? (
            <div className="muted" style={{ padding: 12 }}>Loading…</div>
          ) : (
            <>
              {(data?.instances || []).map((inst) => {
                const sub = subtitle(inst.properties);
                const active = seed?.id === inst.id;
                return (
                  <button
                    key={inst.id}
                    className={`instance-row ${active ? "active" : ""}`}
                    onClick={() => setSeed({ type, id: inst.id })}
                  >
                    <span className="mono" style={{ fontSize: 12, wordBreak: "break-all" }}>{inst.id}</span>
                    {sub && <span className="muted" style={{ fontSize: 11 }}>{sub}</span>}
                  </button>
                );
              })}
              {data && data.instances.length === 0 && (
                <div className="muted" style={{ padding: 12 }}>No instances found.</div>
              )}
              {data?.hasMore && (
                <button
                  className="secondary btn-sm"
                  style={{ margin: 8 }}
                  disabled={loading}
                  onClick={() => setCount((c) => c + PAGE)}
                >
                  {loading ? "Loading…" : "Load more"}
                </button>
              )}
            </>
          )}
        </div>

        <div className="instance-graph">
          {seed ? (
            <NeighborhoodGraph seed={seed} fetchNeighbors={neighborFetcher(db)} height={520} />
          ) : (
            <div className="muted" style={{ padding: 24 }}>Select an instance to explore its neighborhood.</div>
          )}
        </div>
      </div>
    </div>
  );
}

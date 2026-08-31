import { useEffect, useState } from "react";
import SchemaGraph from "../components/SchemaGraph";
import { api, obs, type GraphSummary, type DetailedSchema, type MorkSummary } from "../api/client";

type Target = "neo4j" | "mork";

function msgOf(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

export default function ExploreGraph() {
  const [target, setTarget] = useState<Target>("neo4j");

  const [summary, setSummary] = useState<GraphSummary | null>(null);
  const [schema, setSchema] = useState<DetailedSchema | null>(null);
  const [neoErr, setNeoErr] = useState<string | null>(null);

  const [mork, setMork] = useState<MorkSummary | null>(null);
  const [morkErr, setMorkErr] = useState<string | null>(null);

  useEffect(() => {
    if (target !== "neo4j") return;
    setNeoErr(null);
    setSummary(null);
    Promise.all([obs.summary(), obs.schema()])
      .then(([s, sc]) => {
        setSummary(s);
        setSchema(sc);
      })
      .catch((e) => setNeoErr(msgOf(e)));
  }, [target]);

  useEffect(() => {
    if (target !== "mork") return;
    setMorkErr(null);
    setMork(null);
    api.morkSummary().then(setMork).catch((e) => setMorkErr(msgOf(e)));
  }, [target]);

  return (
    <div className="card">
      <div className="row" style={{ gap: 12 }}>
        <h2 style={{ margin: 0 }}>Explore</h2>
        <span className="group-actions">
          <button
            className={`linkbtn ${target === "neo4j" ? "seg-on" : ""}`}
            onClick={() => setTarget("neo4j")}
          >
            Neo4j
          </button>
          <button
            className={`linkbtn ${target === "mork" ? "seg-on" : ""}`}
            onClick={() => setTarget("mork")}
          >
            MORK
          </button>
        </span>
        <span className="muted" style={{ fontSize: 12 }}>live database</span>
      </div>

      {target === "neo4j" && (
        <>
          {neoErr && (
            <div className="alert err" style={{ marginTop: 12 }}>
              Could not reach Neo4j: {neoErr}. Exploration reads the live Neo4j graph.
            </div>
          )}
          {summary && schema && (
            <>
              <div className="row" style={{ gap: 24, margin: "12px 0 2px" }}>
                <Meta label="Node types" value={schema.nodes.length} />
                <Meta label="Edge types" value={summary.top_connections?.length ?? 0} />
                <Meta label="Nodes" value={(summary.node_count ?? 0).toLocaleString()} />
                <Meta label="Edges" value={(summary.edge_count ?? 0).toLocaleString()} />
              </div>
              {schema.nodes.length ? (
                <SchemaGraph
                  schema={schema}
                  topEntities={summary.top_entities}
                  topConnections={summary.top_connections}
                />
              ) : (
                <div className="muted" style={{ marginTop: 12 }}>The Neo4j graph is empty.</div>
              )}
            </>
          )}
          {!summary && !neoErr && <div className="muted" style={{ marginTop: 12 }}>Loading…</div>}
        </>
      )}

      {target === "mork" && (
        <>
          {morkErr && (
            <div className="alert err" style={{ marginTop: 12 }}>Could not reach MORK: {morkErr}</div>
          )}
          {mork && !mork.reachable && (
            <div className="alert warn" style={{ marginTop: 12 }}>
              MORK is not reachable{mork.error ? `: ${mork.error}` : ""}.
            </div>
          )}
          {mork && mork.reachable && (
            <>
              <div className="row" style={{ gap: 24, margin: "12px 0 2px" }}>
                <Meta label="Atoms" value={mork.atom_count != null ? mork.atom_count.toLocaleString() : "—"} />
                <Meta label="Datasets" value={mork.dataset_count ?? "—"} />
                <Meta label="Version" value={mork.latest_version ?? "—"} />
              </div>
              <p className="muted" style={{ fontSize: 13, lineHeight: 1.55, marginTop: 12 }}>
                MORK is an atomspace of MeTTa atoms, not a labeled property graph, so it is shown
                as counts and versions rather than a node and edge schema.
              </p>
              {mork.versions && mork.versions.length > 0 && (
                <div style={{ marginTop: 12 }}>
                  <div className="muted" style={{ fontSize: 12, marginBottom: 6 }}>Versions</div>
                  <div className="chips">
                    {mork.versions.slice(0, 24).map((v, i) => (
                      <span className="pill" key={i}>
                        {typeof v === "object" ? JSON.stringify(v) : String(v)}
                      </span>
                    ))}
                  </div>
                </div>
              )}
            </>
          )}
          {!mork && !morkErr && <div className="muted" style={{ marginTop: 12 }}>Loading…</div>}
        </>
      )}
    </div>
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

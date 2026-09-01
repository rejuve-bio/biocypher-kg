import { useEffect, useState } from "react";
import SchemaGraph from "../components/SchemaGraph";
import InstanceExplorer from "../components/InstanceExplorer";
import PatternPanel from "../components/PatternPanel";
import AtomTree from "../components/AtomTree";
import { api, obs, type GraphSummary, type DetailedSchema, type MorkSummary, type MorkSchema } from "../api/client";

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
  const [morkSchema, setMorkSchema] = useState<MorkSchema | null>(null);
  const [morkErr, setMorkErr] = useState<string | null>(null);

  // Instance exploration: which node type is being browsed (null = schema map only).
  const [browseType, setBrowseType] = useState<string | null>(null);
  const [morkTool, setMorkTool] = useState<"none" | "query" | "atoms">("none");

  // A type from one database may not exist in the other, so reset on switch.
  useEffect(() => {
    setBrowseType(null);
    setMorkTool("none");
  }, [target]);

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
    setMorkSchema(null);
    api.morkSummary().then(setMork).catch((e) => setMorkErr(msgOf(e)));
    api.morkSchema().then(setMorkSchema).catch(() => setMorkSchema(null));
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
                <>
                  <SchemaGraph
                    schema={schema}
                    topEntities={summary.top_entities}
                    topConnections={summary.top_connections}
                    onBrowseType={setBrowseType}
                  />
                  {browseType && (
                    <InstanceExplorer db="neo4j" type={browseType} onClose={() => setBrowseType(null)} />
                  )}
                </>
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
                <Meta
                  label="Node types"
                  value={morkSchema?.node_type_count ?? (morkSchema?.schema?.nodes.length ?? "—")}
                />
                <Meta label="Edge types" value={morkSchema?.edge_type_count ?? "—"} />
                <Meta label="Atoms" value={mork.atom_count != null ? mork.atom_count.toLocaleString() : "—"} />
                <Meta label="Datasets" value={mork.dataset_count ?? "—"} />
                <Meta label="Version" value={mork.latest_version?.version ?? "—"} />
              </div>

              {morkSchema?.schema && morkSchema.schema.nodes.length > 0 ? (
                <>
                  {morkSchema.sampled && (
                    <div className="muted" style={{ fontSize: 12, marginTop: 10 }}>
                      Schema derived from a live sample of the atomspace; very rare types may be missing.
                    </div>
                  )}
                  <SchemaGraph schema={morkSchema.schema} showCounts={false} onBrowseType={setBrowseType} />
                  {browseType && (
                    <InstanceExplorer db="mork" type={browseType} onClose={() => setBrowseType(null)} />
                  )}
                </>
              ) : (
                <p className="muted" style={{ fontSize: 13, lineHeight: 1.55, marginTop: 12 }}>
                  MORK is an atomspace of MeTTa atoms. The node and edge schema is derived from the atom
                  structure. This space has no recognizable node/edge atoms yet.
                </p>
              )}

              <div className="row" style={{ gap: 8, marginTop: 16, alignItems: "center" }}>
                <span className="muted" style={{ fontSize: 12 }}>Power tools:</span>
                <button
                  className={`linkbtn ${morkTool === "query" ? "seg-on" : ""}`}
                  onClick={() => setMorkTool((t) => (t === "query" ? "none" : "query"))}
                >
                  Pattern query
                </button>
                <button
                  className={`linkbtn ${morkTool === "atoms" ? "seg-on" : ""}`}
                  onClick={() => setMorkTool((t) => (t === "atoms" ? "none" : "atoms"))}
                >
                  Raw atoms
                </button>
              </div>
              {morkTool === "query" && (
                <div className="card" style={{ marginTop: 12 }}>
                  <h3 style={{ marginTop: 0 }}>Pattern query</h3>
                  <PatternPanel />
                </div>
              )}
              {morkTool === "atoms" && (
                <div className="card" style={{ marginTop: 12 }}>
                  <h3 style={{ marginTop: 0 }}>Raw atoms</h3>
                  <AtomTree />
                </div>
              )}

              {mork.versions && mork.versions.length > 0 && (
                <div style={{ marginTop: 14 }}>
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

import { useEffect, useState } from "react";
import SchemaGraph from "../components/SchemaGraph";
import { api, type ExploreTarget, type ExploreSchema } from "../api/client";

type Db = "neo4j" | "mork";

function msgOf(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

function Meta({ label, value }: { label: string; value: unknown }) {
  return (
    <div>
      <div className="muted" style={{ fontSize: 12 }}>{label}</div>
      <div className="mono">{String(value)}</div>
    </div>
  );
}

// One species' live schema for the selected database.
function SpeciesSchema({ species, db, compact }: { species: string; db: Db; compact?: boolean }) {
  const [data, setData] = useState<ExploreSchema | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let alive = true;
    setLoading(true);
    setData(null);
    api
      .exploreSchema(species, db)
      .then((d) => alive && setData(d))
      .catch((e) => alive && setData({ species, db, reachable: false, error: msgOf(e) }))
      .finally(() => alive && setLoading(false));
    return () => {
      alive = false;
    };
  }, [species, db]);

  if (loading) return <div className="muted" style={{ padding: 12 }}>Loading {species}…</div>;
  if (!data) return null;
  if (!data.reachable) {
    return (
      <div className="alert warn" style={{ marginTop: 8 }}>
        {species}: {data.error || "unreachable"}
      </div>
    );
  }

  const stats: [string, unknown][] =
    db === "neo4j"
      ? [
          ["Node types", data.node_types ?? "—"],
          ["Edge types", data.edge_types ?? "—"],
          ["Nodes", data.node_count != null ? data.node_count.toLocaleString() : "—"],
          ["Edges", data.edge_count != null ? data.edge_count.toLocaleString() : "—"],
        ]
      : [
          ["Node types", data.node_types ?? "—"],
          ["Edge types", data.edge_types ?? "—"],
          ["Atoms", data.atom_count != null ? data.atom_count.toLocaleString() : "—"],
          ["Version", data.version ?? "—"],
        ];

  return (
    <div>
      <div className="row" style={{ gap: compact ? 16 : 24, margin: "8px 0 2px" }}>
        {stats.map(([l, v]) => (
          <Meta key={l} label={l} value={v} />
        ))}
      </div>
      {data.schema && data.schema.nodes.length ? (
        <>
          {db === "mork" && data.sampled && (
            <div className="muted" style={{ fontSize: 12, margin: "6px 0" }}>
              Schema sampled from the live atomspace; very rare types may be missing.
            </div>
          )}
          <SchemaGraph
            schema={data.schema}
            topEntities={data.top_entities}
            topConnections={data.top_connections}
            showCounts={db === "neo4j"}
            height={compact ? 380 : 640}
          />
        </>
      ) : (
        <div className="muted" style={{ marginTop: 8 }}>No schema for {species} in {db}.</div>
      )}
    </div>
  );
}

export default function ExploreGraph() {
  const [db, setDb] = useState<Db>("neo4j");
  const [species, setSpecies] = useState<string>("all");
  const [targets, setTargets] = useState<ExploreTarget[] | null>(null);
  const [targetsErr, setTargetsErr] = useState<string | null>(null);

  useEffect(() => {
    api.exploreTargets().then(setTargets).catch((e) => setTargetsErr(msgOf(e)));
  }, []);

  // The backend returns only configured instances: one entry per species in
  // per-species mode, or a single "default" entry for a lone global instance. The
  // explicit filter is a belt-and-suspenders guard so a non-explicit entry never
  // sneaks in alongside explicit ones.
  const explicitTargets = (targets ?? []).filter((t) => t.explicit);
  const shown = explicitTargets.length > 0 ? explicitTargets : (targets ?? []);
  const speciesList = shown.map((t) => t.species);

  return (
    <div className="card">
      <div className="row" style={{ gap: 12, flexWrap: "wrap", alignItems: "center" }}>
        <h2 style={{ margin: 0 }}>Explore</h2>
        <span className="group-actions">
          <button className={`linkbtn ${db === "neo4j" ? "seg-on" : ""}`} onClick={() => setDb("neo4j")}>
            Neo4j
          </button>
          <button className={`linkbtn ${db === "mork" ? "seg-on" : ""}`} onClick={() => setDb("mork")}>
            MORK
          </button>
        </span>
        <span className="muted" style={{ fontSize: 12 }}>live schema</span>
        <span style={{ flex: 1 }} />
        <span className="muted" style={{ fontSize: 12 }}>Species</span>
        <select value={species} onChange={(e) => setSpecies(e.target.value)}>
          <option value="all">All species</option>
          {speciesList.map((s) => (
            <option key={s} value={s}>{s}</option>
          ))}
        </select>
      </div>

      {targetsErr && (
        <div className="alert err" style={{ marginTop: 12 }}>Could not load species: {targetsErr}</div>
      )}
      {!targets && !targetsErr && <div className="muted" style={{ marginTop: 12 }}>Loading…</div>}

      {targets && speciesList.length === 0 && (
        <div className="muted" style={{ marginTop: 12 }}>No species configured.</div>
      )}

      {targets && species !== "all" && (
        <div style={{ marginTop: 8 }}>
          <SpeciesSchema species={species} db={db} />
        </div>
      )}

      {targets && species === "all" && speciesList.length > 0 && (
        <div className="species-grid">
          {speciesList.map((s) => (
            <div className="species-card" key={s}>
              <div className="row" style={{ justifyContent: "space-between", alignItems: "center" }}>
                <h3 style={{ margin: 0, textTransform: "uppercase", letterSpacing: "0.03em" }}>{s}</h3>
                <button className="linkbtn" onClick={() => setSpecies(s)}>focus →</button>
              </div>
              <SpeciesSchema species={s} db={db} compact />
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

import { useEffect, useRef, useState } from "react";
import cytoscape from "cytoscape";
import {
  obs,
  type GraphSummary,
  type DetailedSchema,
  type EntitiesPage,
} from "../api/client";

type Selected =
  | { kind: "node"; id: string; count: number; props: string[] }
  | { kind: "edge"; rel: string; source: string; target: string; count: number }
  | null;

const PAGE = 25;

function cssVar(name: string, fallback: string): string {
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return v || fallback;
}

function cell(v: unknown): string {
  if (v == null) return "";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

function msgOf(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

export default function ExploreGraph() {
  const [schema, setSchema] = useState<DetailedSchema | null>(null);
  const [summary, setSummary] = useState<GraphSummary | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<Selected>(null);
  const [theme, setTheme] = useState(() =>
    document.documentElement.dataset.theme === "light" ? "light" : "dark",
  );

  const [offset, setOffset] = useState(0);
  const [pageData, setPageData] = useState<EntitiesPage | null>(null);
  const [pageErr, setPageErr] = useState<string | null>(null);

  const cyEl = useRef<HTMLDivElement | null>(null);
  const cyRef = useRef<cytoscape.Core | null>(null);

  useEffect(() => {
    Promise.all([obs.schema(), obs.summary()])
      .then(([s, sum]) => {
        setSchema(s);
        setSummary(sum);
      })
      .catch((e) => setError(msgOf(e)));
  }, []);

  useEffect(() => {
    const root = document.documentElement;
    const mo = new MutationObserver(() =>
      setTheme(root.dataset.theme === "light" ? "light" : "dark"),
    );
    mo.observe(root, { attributes: true, attributeFilter: ["data-theme"] });
    return () => mo.disconnect();
  }, []);

  // Build (and rebuild on theme change) the schema graph.
  useEffect(() => {
    if (!schema || !summary || !cyEl.current) return;

    const nodeCounts = new Map(
      summary.top_entities.map((e) => [e.name.toLowerCase(), e.count]),
    );
    const edgeCounts = new Map(
      summary.top_connections.map((e) => [e.name.toLowerCase(), e.count]),
    );
    const ids = new Set(schema.nodes.map((n) => n.data.id));
    const maxN = Math.max(1, ...schema.nodes.map((n) => nodeCounts.get(n.data.id) ?? 0));

    const els: cytoscape.ElementDefinition[] = schema.nodes.map((n) => {
      const c = nodeCounts.get(n.data.id) ?? 0;
      return {
        data: {
          id: n.data.id,
          label: n.data.id,
          count: c,
          props: n.data.properties,
          w: 36 + 56 * Math.sqrt(c / maxN),
        },
      };
    });

    let maxE = 1;
    const edgeDefs: cytoscape.ElementDefinition[] = [];
    for (const e of schema.edges) {
      for (const rel of e.data.possible_connections) {
        const c = edgeCounts.get(rel) ?? 0;
        maxE = Math.max(maxE, c);
        const { source: s, target: t } = e.data;
        for (const end of [s, t]) {
          if (!ids.has(end)) {
            els.push({ data: { id: end, label: end, count: 0, props: [], w: 36 } });
            ids.add(end);
          }
        }
        edgeDefs.push({
          data: { id: `${s}__${rel}__${t}`, source: s, target: t, label: rel, rel, count: c },
        });
      }
    }
    for (const ed of edgeDefs) ed.data!.ew = 1.3 + 4.5 * ((ed.data!.count as number) / maxE);
    els.push(...edgeDefs);

    const accent = cssVar("--accent", "#10b981");
    const accent2 = cssVar("--accent-2", "#06b6d4");
    const textc = cssVar("--text", "#e8eef8");
    const mutedc = cssVar("--muted", "#94a3b8");
    const panel2 = cssVar("--panel-2", "#273449");

    cyRef.current?.destroy();
    const cy = cytoscape({
      container: cyEl.current,
      elements: els,
      wheelSensitivity: 0.2,
      style: [
        {
          selector: "node",
          style: {
            "background-color": accent,
            "background-opacity": 0.92,
            width: "data(w)",
            height: "data(w)",
            label: "data(label)",
            color: textc,
            "font-size": 11,
            "font-weight": 600,
            "text-valign": "center",
            "text-halign": "center",
            "text-wrap": "wrap",
            "text-max-width": "78px",
            "border-width": 2,
            "border-color": panel2,
          },
        },
        {
          selector: "node:selected",
          style: { "border-width": 4, "border-color": accent2 },
        },
        {
          selector: "edge",
          style: {
            width: "data(ew)",
            "line-color": accent2,
            "line-opacity": 0.5,
            "target-arrow-color": accent2,
            "target-arrow-shape": "triangle",
            "arrow-scale": 1.1,
            "curve-style": "bezier",
            label: "data(label)",
            "font-size": 8,
            color: mutedc,
            "text-rotation": "autorotate",
            "text-background-color": panel2,
            "text-background-opacity": 0.85,
            "text-background-padding": 2,
          },
        },
        {
          selector: "edge:selected",
          style: { "line-color": accent, "target-arrow-color": accent, "line-opacity": 1, color: textc },
        },
      ] as cytoscape.StylesheetStyle[],
    });
    cyRef.current = cy;

    // Run the force layout, then frame the graph once it settles.
    const layout = cy.layout({
      name: "cose",
      animate: true,
      animationDuration: 600,
      padding: 40,
      nodeDimensionsIncludeLabels: true,
    } as cytoscape.LayoutOptions);
    layout.one("layoutstop", () => cy.fit(undefined, 40));
    layout.run();

    cy.on("tap", "node", (evt) => {
      const d = evt.target.data();
      setSelected({ kind: "node", id: d.id, count: d.count, props: d.props || [] });
      setOffset(0);
      setPageData(null);
      setPageErr(null);
    });
    cy.on("tap", "edge", (evt) => {
      const d = evt.target.data();
      setSelected({ kind: "edge", rel: d.rel, source: d.source, target: d.target, count: d.count });
    });
    cy.on("tap", (evt) => {
      if (evt.target === cy) setSelected(null);
    });

    return () => {
      cy.destroy();
      cyRef.current = null;
    };
  }, [schema, summary, theme]);

  // Load records for a selected node type.
  useEffect(() => {
    if (!selected || selected.kind !== "node") return;
    let alive = true;
    setPageErr(null);
    obs
      .entities(selected.id, PAGE, offset)
      .then((p) => alive && setPageData(p))
      .catch((e) => alive && setPageErr(msgOf(e)));
    return () => {
      alive = false;
    };
  }, [selected, offset]);

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
  if (!schema || !summary) return <div className="muted">Loading…</div>;

  const cols =
    pageData && pageData.entities.length
      ? Array.from(new Set(pageData.entities.flatMap((e) => Object.keys(e)))).sort((a, b) =>
          a === "id" ? -1 : b === "id" ? 1 : a.localeCompare(b),
        )
      : [];
  const shown = pageData?.entities.length ?? 0;

  return (
    <>
      <div className="card">
        <div className="row" style={{ justifyContent: "space-between" }}>
          <h2 style={{ margin: 0 }}>Explore graph</h2>
          <div className="row">
            <button className="secondary btn-sm" onClick={() => cyRef.current?.fit(undefined, 40)}>
              Fit
            </button>
            <button
              className="secondary btn-sm"
              onClick={() => {
                const cy = cyRef.current;
                if (!cy) return;
                const l = cy.layout({ name: "cose", animate: true } as cytoscape.LayoutOptions);
                l.one("layoutstop", () => cy.fit(undefined, 40));
                l.run();
              }}
            >
              Re-layout
            </button>
          </div>
        </div>

        <div className="row" style={{ gap: 24, margin: "12px 0 2px" }}>
          <Meta label="Node types" value={schema.nodes.length} />
          <Meta label="Edge types" value={summary.top_connections.length} />
          <Meta label="Nodes" value={summary.node_count.toLocaleString()} />
          <Meta label="Edges" value={summary.edge_count.toLocaleString()} />
        </div>

        <div className="explore-grid">
          <div ref={cyEl} className="cy-canvas" />
          <div className="cy-panel">
            {selected?.kind === "node" && (
              <>
                <div className="row" style={{ gap: 8, alignItems: "baseline" }}>
                  <span className="tag node">node</span>
                  <span className="mono" style={{ fontSize: 15, fontWeight: 700 }}>{selected.id}</span>
                </div>
                <div className="muted" style={{ fontSize: 12, marginTop: 6 }}>
                  {selected.count.toLocaleString()} nodes
                </div>
                <div className="muted" style={{ fontSize: 12, margin: "12px 0 6px" }}>
                  Properties ({selected.props.length})
                </div>
                <div className="chips">
                  {selected.props.length ? (
                    selected.props.map((p) => (
                      <span className="pill" key={p}>{p}</span>
                    ))
                  ) : (
                    <span className="muted">none</span>
                  )}
                </div>
                <div className="muted" style={{ fontSize: 12, marginTop: 14 }}>
                  Records shown below ↓
                </div>
              </>
            )}
            {selected?.kind === "edge" && (
              <>
                <div className="row" style={{ gap: 8, alignItems: "baseline" }}>
                  <span className="tag edge">edge</span>
                  <span className="mono" style={{ fontSize: 15, fontWeight: 700 }}>{selected.rel}</span>
                </div>
                <div className="muted" style={{ fontSize: 12, marginTop: 6 }}>
                  {selected.count.toLocaleString()} relationships
                </div>
                <div style={{ marginTop: 14, fontSize: 13 }}>
                  <span className="mono">{selected.source}</span>
                  <span className="muted"> → </span>
                  <span className="mono">{selected.target}</span>
                </div>
              </>
            )}
            {!selected && (
              <>
                <div style={{ fontWeight: 700, marginBottom: 8 }}>Schema</div>
                <p className="muted" style={{ fontSize: 13, lineHeight: 1.55, margin: 0 }}>
                  Each circle is a node type (size by count); each arrow is a relationship type
                  and its direction. Click a node or arrow to inspect it, drag to rearrange,
                  scroll to zoom.
                </p>
                <div className="chips" style={{ marginTop: 14 }}>
                  <span className="pill"><span className="dot-node" /> node type</span>
                  <span className="pill"><span className="dot-edge" /> relationship</span>
                </div>
              </>
            )}
          </div>
        </div>
      </div>

      {selected?.kind === "node" && (
        <div className="card">
          <div className="row" style={{ justifyContent: "space-between" }}>
            <h2 style={{ margin: 0 }}>
              {selected.id}{" "}
              {pageData && <span className="muted">· {pageData.total.toLocaleString()} total</span>}
            </h2>
            <div className="row">
              <button className="secondary btn-sm" disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - PAGE))}>
                ← Prev
              </button>
              <span className="muted" style={{ fontSize: 12 }}>
                {shown ? `${offset + 1}–${offset + shown}` : "0"}
                {pageData ? ` of ${pageData.total.toLocaleString()}` : ""}
              </span>
              <button className="secondary btn-sm" disabled={!pageData || offset + shown >= pageData.total} onClick={() => setOffset(offset + PAGE)}>
                Next →
              </button>
            </div>
          </div>
          {pageErr && <div className="alert err" style={{ marginTop: 10 }}>{pageErr}</div>}
          {pageData && pageData.entities.length > 0 && (
            <div style={{ overflowX: "auto", marginTop: 12 }}>
              <table>
                <thead>
                  <tr>{cols.map((c) => <th key={c}>{c}</th>)}</tr>
                </thead>
                <tbody>
                  {pageData.entities.map((e, i) => (
                    <tr key={i}>
                      {cols.map((c) => (
                        <td key={c} className="mono" style={{ whiteSpace: "nowrap" }}>{cell(e[c])}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
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

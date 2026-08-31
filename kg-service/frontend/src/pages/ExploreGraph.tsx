import { useEffect, useRef, useState } from "react";
import cytoscape from "cytoscape";
import { api, type GraphInfoSummary } from "../api/client";
import type { BuildJob } from "../types";

type Selected =
  | { kind: "node"; id: string; count: number; props: string[]; color: string }
  | { kind: "edge"; rel: string; source: string; target: string; count: number; color: string }
  | null;

// Generated categorical palette: golden-angle hues give good separation for any count.
// Comma-separated hsl() is required — Cytoscape's color parser rejects the space form.
function goldenColor(i: number, light: number): string {
  const hue = ((i * 137.508) % 360).toFixed(0);
  return `hsl(${hue}, 68%, ${light}%)`;
}

function cssVar(name: string, fallback: string): string {
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return v || fallback;
}

function msgOf(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

function buildLabel(b: BuildJob): string {
  const p = b.params as Record<string, unknown>;
  const when = b.finished_at || b.created_at;
  const t = when ? new Date(when).toLocaleString() : "";
  return `${p.species ?? "?"}/${p.dataset ?? "?"} · ${p.writer_type ?? "?"} · ${t}`;
}

export default function ExploreGraph() {
  const [builds, setBuilds] = useState<BuildJob[]>([]);
  const [buildId, setBuildId] = useState("");
  const [info, setInfo] = useState<GraphInfoSummary | null>(null);
  const [noInfo, setNoInfo] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<Selected>(null);
  const [theme, setTheme] = useState(() =>
    document.documentElement.dataset.theme === "light" ? "light" : "dark",
  );

  const cyEl = useRef<HTMLDivElement | null>(null);
  const cyRef = useRef<cytoscape.Core | null>(null);

  useEffect(() => {
    api
      .listBuilds()
      .then((b) => {
        const done = b.filter((j) => j.status === "succeeded" && (j.kind ?? "build") === "build");
        setBuilds(done);
        if (done.length) setBuildId(done[0].id);
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

  useEffect(() => {
    if (!buildId) return;
    setSelected(null);
    setNoInfo(false);
    api
      .getGraphInfo(buildId)
      .then((r) => {
        if (r.present && r.summary) setInfo(r.summary);
        else {
          setInfo(null);
          setNoInfo(true);
        }
      })
      .catch((e) => setError(msgOf(e)));
  }, [buildId]);

  useEffect(() => {
    if (!info || !info.schema || !cyEl.current) return;

    const nodeTypes = info.schema.nodes.map((n) => n.data.id);
    const nodeColor: Record<string, string> = {};
    nodeTypes.forEach((t, i) => (nodeColor[t] = goldenColor(i, 55)));

    const relSet = new Set<string>();
    for (const e of info.schema.edges) for (const r of e.data.possible_connections) relSet.add(r);
    const relTypes = Array.from(relSet).sort();
    const relColor: Record<string, string> = {};
    relTypes.forEach((r, i) => (relColor[r] = goldenColor(i, 62)));

    const nodeCount = new Map((info.top_entities ?? []).map((e) => [e.name.toLowerCase(), e.count]));
    const relCount = new Map((info.top_connections ?? []).map((e) => [e.name.toLowerCase(), e.count]));

    const ids = new Set(nodeTypes);
    const els: cytoscape.ElementDefinition[] = info.schema.nodes.map((n) => ({
      data: {
        id: n.data.id,
        label: n.data.id,
        props: n.data.properties,
        count: nodeCount.get(n.data.id.toLowerCase()) ?? 0,
        color: nodeColor[n.data.id],
      },
    }));
    for (const e of info.schema.edges) {
      const { source: s, target: t } = e.data;
      for (const end of [s, t]) {
        if (!ids.has(end)) {
          els.push({ data: { id: end, label: end, props: [], count: 0, color: goldenColor(ids.size, 55) } });
          ids.add(end);
        }
      }
      for (const rel of e.data.possible_connections) {
        els.push({
          data: {
            id: `${s}__${rel}__${t}`,
            source: s,
            target: t,
            label: rel,
            rel,
            count: relCount.get(rel.toLowerCase()) ?? 0,
            color: relColor[rel],
          },
        });
      }
    }

    const textc = cssVar("--text", "#e8eef8");
    const mutedc = cssVar("--muted", "#94a3b8");
    const panel2 = cssVar("--panel-2", "#273449");
    const accent2 = cssVar("--accent-2", "#06b6d4");

    cyRef.current?.destroy();
    const cy = cytoscape({
      container: cyEl.current,
      elements: els,
      wheelSensitivity: 0.2,
      style: [
        {
          selector: "node",
          style: {
            "background-color": "data(color)",
            width: 62,
            height: 62,
            label: "data(label)",
            color: textc,
            "font-size": 11,
            "font-weight": 600,
            "text-valign": "center",
            "text-halign": "center",
            "text-wrap": "wrap",
            "text-max-width": "80px",
            "text-outline-width": 2,
            "text-outline-color": panel2,
            "text-outline-opacity": 0.6,
            "border-width": 3,
            "border-color": panel2,
            "overlay-opacity": 0,
          },
        },
        { selector: "node:selected", style: { "border-width": 4, "border-color": accent2 } },
        {
          selector: "edge",
          style: {
            width: 3,
            "line-color": "data(color)",
            "line-opacity": 0.8,
            "target-arrow-color": "data(color)",
            "target-arrow-shape": "triangle",
            "arrow-scale": 1.2,
            "curve-style": "bezier",
            label: "data(label)",
            "font-size": 9,
            color: mutedc,
            "text-rotation": "autorotate",
            "text-background-color": panel2,
            "text-background-opacity": 0.85,
            "text-background-padding": 2,
            "overlay-opacity": 0,
          },
        },
        { selector: "edge:selected", style: { width: 5, "line-opacity": 1, color: textc } },
      ] as cytoscape.StylesheetStyle[],
    });
    cyRef.current = cy;

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
      setSelected({ kind: "node", id: d.id, count: d.count, props: d.props || [], color: d.color });
    });
    cy.on("tap", "edge", (evt) => {
      const d = evt.target.data();
      setSelected({ kind: "edge", rel: d.rel, source: d.source, target: d.target, count: d.count, color: d.color });
    });
    cy.on("tap", (evt) => {
      if (evt.target === cy) setSelected(null);
    });

    return () => {
      cy.destroy();
      cyRef.current = null;
    };
  }, [info, theme]);

  if (error) {
    return (
      <div className="card">
        <h2>Explore graph</h2>
        <div className="alert err">{error}</div>
      </div>
    );
  }

  return (
    <div className="card">
      <div className="row" style={{ justifyContent: "space-between", flexWrap: "wrap", gap: 10 }}>
        <div className="row" style={{ gap: 10 }}>
          <h2 style={{ margin: 0 }}>Explore graph</h2>
          <select value={buildId} onChange={(e) => setBuildId(e.target.value)} style={{ minWidth: 260 }}>
            {!builds.length && <option value="">No succeeded builds</option>}
            {builds.map((b) => (
              <option key={b.id} value={b.id}>
                {buildLabel(b)}
              </option>
            ))}
          </select>
        </div>
        <div className="row">
          <button className="secondary btn-sm" onClick={() => cyRef.current?.fit(undefined, 40)}>Fit</button>
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

      {noInfo && (
        <div className="alert warn" style={{ marginTop: 12 }}>
          This build has no graph_info.json to explore.
        </div>
      )}

      {info && (
        <>
          <div className="row" style={{ gap: 24, margin: "12px 0 2px" }}>
            <Meta label="Node types" value={info.schema?.nodes.length ?? 0} />
            <Meta label="Edge types" value={info.top_connections?.length ?? 0} />
            <Meta label="Nodes" value={(info.node_count ?? 0).toLocaleString()} />
            <Meta label="Edges" value={(info.edge_count ?? 0).toLocaleString()} />
          </div>

          <div className="explore-grid">
            <div ref={cyEl} className="cy-canvas" />
            <div className="cy-panel">
              {selected?.kind === "node" && (
                <>
                  <div className="row" style={{ gap: 8, alignItems: "center" }}>
                    <span className="type-dot" style={{ background: selected.color }} />
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
                      selected.props.map((p) => <span className="pill" key={p}>{p}</span>)
                    ) : (
                      <span className="muted">none</span>
                    )}
                  </div>
                </>
              )}
              {selected?.kind === "edge" && (
                <>
                  <div className="row" style={{ gap: 8, alignItems: "center" }}>
                    <span className="type-dot" style={{ background: selected.color }} />
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
                    Each circle is a node type, each arrow a relationship type and its direction.
                    Colors distinguish types. Click a node or arrow to inspect it, drag to
                    rearrange, scroll to zoom.
                  </p>
                </>
              )}
            </div>
          </div>
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

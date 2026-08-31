import { useEffect, useRef, useState } from "react";
import cytoscape from "cytoscape";
import type { DetailedSchema } from "../api/client";

type Count = { name: string; count: number };

type Props = {
  schema: DetailedSchema;
  topEntities?: Count[] | null;
  topConnections?: Count[] | null;
  height?: number;
};

type Selected =
  | { kind: "node"; id: string; count: number; props: string[]; color: string }
  | { kind: "edge"; rel: string; source: string; target: string; count: number; color: string }
  | null;

// Spread force layout so a dense schema (many node types) stays legible.
const LAYOUT = {
  name: "cose",
  animate: true,
  animationDuration: 600,
  padding: 40,
  nodeDimensionsIncludeLabels: true,
  idealEdgeLength: 150,
  nodeRepulsion: 16000,
  gravity: 0.2,
  numIter: 1500,
} as cytoscape.LayoutOptions;

// Comma-separated hsl() is required — Cytoscape rejects the space-separated form.
function goldenColor(i: number, light: number): string {
  const hue = ((i * 137.508) % 360).toFixed(0);
  return `hsl(${hue}, 68%, ${light}%)`;
}

function cssVar(name: string, fallback: string): string {
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return v || fallback;
}

export default function SchemaGraph({ schema, topEntities, topConnections, height = 660 }: Props) {
  const [selected, setSelected] = useState<Selected>(null);
  const [legend, setLegend] = useState<{ label: string; color: string }[]>([]);
  const [theme, setTheme] = useState(() =>
    document.documentElement.dataset.theme === "light" ? "light" : "dark",
  );
  const cyEl = useRef<HTMLDivElement | null>(null);
  const cyRef = useRef<cytoscape.Core | null>(null);

  useEffect(() => {
    const root = document.documentElement;
    const mo = new MutationObserver(() =>
      setTheme(root.dataset.theme === "light" ? "light" : "dark"),
    );
    mo.observe(root, { attributes: true, attributeFilter: ["data-theme"] });
    return () => mo.disconnect();
  }, []);

  useEffect(() => {
    if (!cyEl.current) return;
    setSelected(null);

    const nodeTypes = schema.nodes.map((n) => n.data.id);
    const nodeColor: Record<string, string> = {};
    nodeTypes.forEach((t, i) => (nodeColor[t] = goldenColor(i, 55)));
    setLegend(nodeTypes.map((t) => ({ label: t, color: nodeColor[t] })));

    const relSet = new Set<string>();
    for (const e of schema.edges) for (const r of e.data.possible_connections) relSet.add(r);
    const relColor: Record<string, string> = {};
    Array.from(relSet).sort().forEach((r, i) => (relColor[r] = goldenColor(i, 62)));

    const nodeCount = new Map((topEntities ?? []).map((e) => [e.name.toLowerCase(), e.count]));
    const relCount = new Map((topConnections ?? []).map((e) => [e.name.toLowerCase(), e.count]));

    const ids = new Set(nodeTypes);
    const els: cytoscape.ElementDefinition[] = schema.nodes.map((n) => ({
      data: {
        id: n.data.id,
        label: n.data.id,
        props: n.data.properties,
        count: nodeCount.get(n.data.id.toLowerCase()) ?? 0,
        color: nodeColor[n.data.id],
      },
    }));
    for (const e of schema.edges) {
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
            "line-opacity": 0.5,
            "target-arrow-color": "data(color)",
            "target-arrow-shape": "triangle",
            "arrow-scale": 1.2,
            "curve-style": "bezier",
            // Labels hidden by default (the main source of clutter); shown on focus.
            label: "data(label)",
            "text-opacity": 0,
            "font-size": 9,
            color: textc,
            "text-rotation": "autorotate",
            "text-background-color": panel2,
            "text-background-opacity": 0.9,
            "text-background-padding": 2,
            "overlay-opacity": 0,
          },
        },
        { selector: "node.faded", style: { opacity: 0.14 } },
        { selector: "edge.faded", style: { opacity: 0.06 } },
        { selector: "node.hl", style: {} },
        {
          selector: "edge.hl",
          style: { "line-opacity": 1, width: 4, "text-opacity": 1 },
        },
      ] as cytoscape.StylesheetStyle[],
    });
    cyRef.current = cy;

    const layout = cy.layout(LAYOUT);
    layout.one("layoutstop", () => cy.fit(undefined, 40));
    layout.run();

    const focus = (hood: cytoscape.CollectionReturnValue) => {
      cy.batch(() => {
        cy.elements().removeClass("hl").addClass("faded");
        hood.removeClass("faded").addClass("hl");
      });
    };
    const clear = () => cy.batch(() => cy.elements().removeClass("hl faded"));

    cy.on("tap", "node", (evt) => {
      const n = evt.target;
      focus(n.closedNeighborhood());
      const d = n.data();
      setSelected({ kind: "node", id: d.id, count: d.count, props: d.props || [], color: d.color });
    });
    cy.on("tap", "edge", (evt) => {
      const e = evt.target;
      focus(e.connectedNodes().union(e));
      const d = e.data();
      setSelected({ kind: "edge", rel: d.rel, source: d.source, target: d.target, count: d.count, color: d.color });
    });
    cy.on("tap", (evt) => {
      if (evt.target === cy) {
        clear();
        setSelected(null);
      }
    });

    return () => {
      cy.destroy();
      cyRef.current = null;
    };
  }, [schema, topEntities, topConnections, theme]);

  return (
    <div>
      <div className="row" style={{ justifyContent: "space-between", marginBottom: 8 }}>
        <span className="muted" style={{ fontSize: 12 }}>
          Click a node or arrow to focus it and its neighbors. Click empty space to reset.
          Drag to rearrange, scroll to zoom.
        </span>
        <div className="row">
          <button className="secondary btn-sm" onClick={() => cyRef.current?.fit(undefined, 40)}>Fit</button>
          <button
            className="secondary btn-sm"
            onClick={() => {
              const cy = cyRef.current;
              if (!cy) return;
              const l = cy.layout(LAYOUT);
              l.one("layoutstop", () => cy.fit(undefined, 40));
              l.run();
            }}
          >
            Re-layout
          </button>
        </div>
      </div>

      <div className="cy-stage">
        <div ref={cyEl} className="cy-canvas" style={{ height, width: "100%" }} />

        {legend.length > 0 && (
          <div className="cy-legend">
            <div className="muted" style={{ fontSize: 11, fontWeight: 700, letterSpacing: "0.04em", marginBottom: 6 }}>
              NODE TYPES
            </div>
            {legend.map((t) => (
              <div className="legend-row" key={t.label}>
                <span className="type-dot" style={{ width: 10, height: 10, background: t.color }} />
                <span style={{ fontSize: 12 }}>{t.label}</span>
              </div>
            ))}
          </div>
        )}

        {selected && (
          <div className="cy-overlay">
            {selected.kind === "node" ? (
              <>
                <div className="row" style={{ gap: 8, alignItems: "center" }}>
                  <span className="type-dot" style={{ background: selected.color }} />
                  <span className="mono" style={{ fontSize: 15, fontWeight: 700 }}>{selected.id}</span>
                </div>
                <div className="muted" style={{ fontSize: 12, marginTop: 6 }}>{selected.count.toLocaleString()} nodes</div>
                <div className="muted" style={{ fontSize: 12, margin: "12px 0 6px" }}>Properties ({selected.props.length})</div>
                <div className="chips">
                  {selected.props.length ? selected.props.map((p) => <span className="pill" key={p}>{p}</span>) : <span className="muted">none</span>}
                </div>
              </>
            ) : (
              <>
                <div className="row" style={{ gap: 8, alignItems: "center" }}>
                  <span className="type-dot" style={{ background: selected.color }} />
                  <span className="mono" style={{ fontSize: 15, fontWeight: 700 }}>{selected.rel}</span>
                </div>
                <div className="muted" style={{ fontSize: 12, marginTop: 6 }}>{selected.count.toLocaleString()} relationships</div>
                <div style={{ marginTop: 14, fontSize: 13 }}>
                  <span className="mono">{selected.source}</span>
                  <span className="muted"> → </span>
                  <span className="mono">{selected.target}</span>
                </div>
              </>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

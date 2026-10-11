import { useEffect, useMemo, useState } from "react";
import yaml from "js-yaml";
import { api, type EntrySummary } from "../api/client";
import { colorForName } from "../palette";

// Per-type color (hashed from the name) for the little dot next to each entry.
const dotStyle = (name: string) => ({ ["--dot"]: colorForName(name) } as React.CSSProperties);

type Kind = "adapters" | "schema";
type Data = Record<string, unknown>;

// Marker shown next to entries inherited from the base (primer) schema.
const BASE_TAG: React.CSSProperties = {
  fontSize: 9, letterSpacing: 0.3, opacity: 0.6, marginLeft: 6,
  border: "1px solid currentColor", borderRadius: 3, padding: "0 3px", textTransform: "uppercase",
  flex: "none", whiteSpace: "nowrap",
};

// Render one entry as the YAML that will be written to the file (name as the
// top-level key), matching the on-disk block.
function entryToYaml(name: string, data: Data): string {
  try {
    return yaml.dump({ [name]: data }, { indent: 2, lineWidth: -1, noRefs: true, sortKeys: false });
  } catch {
    return "# could not render YAML";
  }
}

function msgOf(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

// Coerce a text field back to a YAML-ish scalar so we don't turn 9606 into "9606".
function coerce(s: string): unknown {
  const t = s.trim();
  if (t === "true" || t === "True") return true;
  if (t === "false" || t === "False") return false;
  if (/^-?\d+$/.test(t)) return parseInt(t, 10);
  if (/^-?\d+\.\d+$/.test(t)) return parseFloat(t);
  return s;
}

/** Editable list of key → scalar rows (adapter args). */
function KeyValueRows({ obj, onChange }: { obj: Data; onChange: (o: Data) => void }) {
  const rows = Object.entries(obj);
  const setKey = (i: number, key: string) => {
    const next: Data = {};
    rows.forEach(([k, v], j) => (next[j === i ? key : k] = v));
    onChange(next);
  };
  const setVal = (k: string, val: string) => onChange({ ...obj, [k]: coerce(val) });
  const remove = (k: string) => {
    const next = { ...obj };
    delete next[k];
    onChange(next);
  };
  const add = () => onChange({ ...obj, "": "" });
  return (
    <div className="kv-rows">
      {rows.map(([k, v], i) => (
        <div className="kv-row" key={i}>
          <input className="input mono" value={k} placeholder="key" onChange={(e) => setKey(i, e.target.value)} />
          <input className="input mono" value={v == null ? "" : String(v)} placeholder="value" onChange={(e) => setVal(k, e.target.value)} />
          <button className="secondary btn-sm" onClick={() => remove(k)} title="remove">✕</button>
        </div>
      ))}
      <button className="secondary btn-sm" onClick={add}>+ add</button>
    </div>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className="gc-field">
      <span className="muted" style={{ fontSize: 12 }}>{label}</span>
      {children}
    </label>
  );
}

// ── is_a hierarchy tree (schema) ──────────────────────────────────────────────
type TNode = { e: EntrySummary; children: TNode[] };

function firstParent(e: EntrySummary, names: Set<string>): string | null {
  const p = e.is_a;
  const v = Array.isArray(p) ? p[0] : p; // multiple inheritance -> primary parent
  return typeof v === "string" && v !== e.name && names.has(v) ? v : null;
}

function buildTree(items: EntrySummary[]): TNode[] {
  const names = new Set(items.map((i) => i.name));
  const childrenOf = new Map<string, EntrySummary[]>();
  const roots: EntrySummary[] = [];
  for (const e of items) {
    const p = firstParent(e, names);
    if (p) {
      const arr = childrenOf.get(p) ?? [];
      arr.push(e);
      childrenOf.set(p, arr);
    } else roots.push(e);
  }
  const build = (e: EntrySummary): TNode => ({ e, children: (childrenOf.get(e.name) ?? []).map(build) });
  const byName = (a: TNode, b: TNode) => a.e.name.localeCompare(b.e.name);
  const sortRec = (n: TNode): TNode => ({ ...n, children: n.children.map(sortRec).sort(byName) });
  return roots.map(build).map(sortRec).sort(byName);
}

function TreeItem({ node, selected, onOpen }: { node: TNode; selected: string | null; onOpen: (n: string) => void }) {
  const [open, setOpen] = useState(true);
  const has = node.children.length > 0;
  return (
    <div className="gc-tnode">
      <div className={`gc-tree-row ${selected === node.e.name ? "active" : ""} ${has ? "is-parent" : ""}`}>
        {has ? (
          <button className={`gc-caret ${open ? "open" : ""}`} onClick={() => setOpen((o) => !o)} aria-label="toggle">▸</button>
        ) : (
          <span className="gc-dot" style={dotStyle(node.e.name)} />
        )}
        <button className="gc-tree-name" onClick={() => onOpen(node.e.name)} title={node.e.name} style={node.e.inherited ? { opacity: 0.65 } : undefined}>{node.e.name}</button>
        {node.e.inherited && <span style={BASE_TAG}>base</span>}
        {has && <span className="gc-tree-count">{node.children.length}</span>}
      </div>
      {open && has && (
        <div className="gc-tree-children">
          {node.children.map((c) => <TreeItem key={c.e.name} node={c} selected={selected} onOpen={onOpen} />)}
        </div>
      )}
    </div>
  );
}

function NavContent({ items, kind, filtering, selected, onOpen }: { items: EntrySummary[]; kind: Kind; filtering: boolean; selected: string | null; onOpen: (n: string) => void }) {
  if (items.length === 0) return <div className="muted" style={{ padding: 8, fontSize: 12 }}>none</div>;
  // Hierarchy tree for schema (when not searching); flat list for adapters / search.
  if (kind === "schema" && !filtering) {
    return <>{buildTree(items).map((n) => <TreeItem key={n.e.name} node={n} selected={selected} onOpen={onOpen} />)}</>;
  }
  return (
    <>
      {items.map((e) => (
        <div key={e.name} className={`gc-tree-row ${selected === e.name ? "active" : ""}`}>
          <span className="gc-dot" style={dotStyle(e.name)} />
          <button className="gc-tree-name" onClick={() => onOpen(e.name)} title={e.name} style={e.inherited ? { opacity: 0.65 } : undefined}>{e.name}</button>
          {e.inherited && <span style={BASE_TAG}>base</span>}
        </div>
      ))}
    </>
  );
}

export default function GuidedConfig({ species, dataset, kind }: { species: string; dataset: string; kind: Kind }) {
  const [entries, setEntries] = useState<EntrySummary[] | null>(null);
  const [inherited, setInherited] = useState<EntrySummary[]>([]);
  const [filter, setFilter] = useState("");
  const [selected, setSelected] = useState<string | null>(null);
  const [data, setData] = useState<Data | null>(null);
  const [dirty, setDirty] = useState(false);
  const [loading, setLoading] = useState(false);
  const [biolink, setBiolink] = useState<string[]>([]);
  const [msg, setMsg] = useState<{ kind: "ok" | "err" | "warn"; text: string } | null>(null);

  useEffect(() => {
    setSelected(null);
    setData(null);
    setEntries(null);
    setInherited([]);
    setMsg(null);
    api.getConfigEntries(species, dataset, kind)
      .then((r) => { setEntries(r.entries); setInherited(r.inherited ?? []); })
      .catch((e) => setMsg({ kind: "err", text: msgOf(e) }));
  }, [species, dataset, kind]);

  useEffect(() => {
    if (kind === "schema" && biolink.length === 0) api.biolinkCategories().then(setBiolink).catch(() => {});
  }, [kind, biolink.length]);

  const openEntry = (name: string) => {
    const fromBase = inherited.some((e) => e.name === name);
    setSelected(name);
    setData(null);
    setDirty(false);
    setMsg(null);
    setLoading(true);
    api
      // Inherited types live in the base schema; fetch them from there, read-only.
      .getConfigEntry(fromBase ? "base" : species, fromBase ? "base" : dataset, kind, name)
      .then((r) => setData(r.data))
      .catch((e) => setMsg({ kind: "err", text: msgOf(e) }))
      .finally(() => setLoading(false));
  };

  const patch = (up: (d: Data) => void) => {
    setData((prev) => {
      if (!prev) return prev;
      const next = structuredClone(prev);
      up(next);
      return next;
    });
    setDirty(true);
  };

  const save = async () => {
    if (!selected || !data) return;
    setMsg(null);
    try {
      const res = await api.saveConfigEntry(species, dataset, kind, selected, data);
      setDirty(false);
      const warns = res.warnings ?? [];
      if (warns.length) {
        setMsg({ kind: "warn", text: `Saved ${selected}, but ${warns.length} warning${warns.length > 1 ? "s" : ""}:\n- ${warns.join("\n- ")}` });
      } else {
        setMsg({ kind: "ok", text: `Saved ${selected}. Only that entry's lines changed.` });
      }
      api.getConfigEntries(species, dataset, kind)
        .then((r) => { setEntries(r.entries); setInherited(r.inherited ?? []); })
        .catch(() => {});
    } catch (e) {
      setMsg({ kind: "err", text: msgOf(e) });
    }
  };

  const addEntry = () => {
    const name = window.prompt(`New ${kind === "adapters" ? "adapter" : "type"} name:`)?.trim();
    if (!name) return;
    if (entries?.some((e) => e.name === name)) {
      setMsg({ kind: "err", text: `An entry named "${name}" already exists.` });
      return;
    }
    const seed: Data =
      kind === "adapters"
        ? { adapter: { module: "", cls: "", args: {} }, outdir: "", nodes: true, edges: false }
        : { represented_as: "node", input_label: name, is_a: "", biolink_category: "", properties: {} };
    setEntries((prev) => [...(prev ?? []), { name }]);
    setSelected(name);
    setData(seed);
    setDirty(true);
  };

  const removeEntry = async () => {
    if (!selected) return;
    if (!window.confirm(`Delete entry "${selected}" from the ${kind} config?`)) return;
    try {
      await api.deleteConfigEntry(species, dataset, kind, selected);
      setEntries((prev) => (prev ?? []).filter((e) => e.name !== selected));
      setSelected(null);
      setData(null);
      setMsg({ kind: "ok", text: `Deleted ${selected}.` });
    } catch (e) {
      setMsg({ kind: "err", text: msgOf(e) });
    }
  };

  // Own (editable) entries plus the base/primer entries this species inherits (read-only).
  const allEntries = useMemo(() => [...(entries ?? []), ...inherited], [entries, inherited]);
  const shown = useMemo(
    () => allEntries.filter((e) => e.name.toLowerCase().includes(filter.toLowerCase())),
    [allEntries, filter],
  );
  const selIsInherited = !!selected && inherited.some((e) => e.name === selected);
  const isEdgeEntry = (e: EntrySummary) =>
    kind === "adapters" ? !!e.edges && !e.nodes : e.represented_as === "edge";
  const nodeGroup = shown.filter((e) => !isEdgeEntry(e));
  const edgeGroup = shown.filter((e) => isEdgeEntry(e));

  const filtering = filter.trim().length > 0;

  return (
    <div className="gc-guided">
      <div className="gc-toolbar">
        <input className="input" placeholder="Search entries…" value={filter} onChange={(e) => setFilter(e.target.value)} />
        <button className="secondary btn-sm" onClick={addEntry}>+ Add {kind === "adapters" ? "adapter" : "type"}</button>
      </div>

      <div className="gc-cols">
        <div className="gc-nav-col">
          <div className="gc-nav-head">
            <span className="type-dot dot-node" /> {kind === "adapters" ? "Node adapters" : "Node types"}
            <span className="muted">{nodeGroup.length}</span>
          </div>
          <div className="gc-nav-body">
            {entries === null ? <div className="muted" style={{ padding: 8 }}>Loading…</div>
              : <NavContent items={nodeGroup} kind={kind} filtering={filtering} selected={selected} onOpen={openEntry} />}
          </div>
        </div>

        <div className="gc-nav-col">
          <div className="gc-nav-head">
            <span className="type-dot dot-edge" /> {kind === "adapters" ? "Edge adapters" : "Edge types"}
            <span className="muted">{edgeGroup.length}</span>
          </div>
          <div className="gc-nav-body">
            {entries === null ? <div className="muted" style={{ padding: 8 }}>Loading…</div>
              : <NavContent items={edgeGroup} kind={kind} filtering={filtering} selected={selected} onOpen={openEntry} />}
          </div>
        </div>

        <div className="gc-form">
        {!selected ? (
          <div className="muted" style={{ padding: 20 }}>Select an entry to edit, or add a new one.</div>
        ) : loading || !data ? (
          <div className="muted" style={{ padding: 20 }}>Loading {selected}…</div>
        ) : selIsInherited ? (
          <>
            <div className="row" style={{ justifyContent: "space-between", alignItems: "center" }}>
              <h3 className="mono" style={{ margin: 0 }}>{selected}</h3>
              <span className="muted" style={{ fontSize: 12 }}>from base schema · read-only</span>
            </div>
            <div className="alert" style={{ marginTop: 8 }}>
              Inherited from the base (shared) schema. To change it, choose “Base (shared)” in the Species selector.
            </div>
            <pre className="gc-raw mono" style={{ marginTop: 10 }}>{entryToYaml(selected, data)}</pre>
          </>
        ) : (
          <>
            <div className="row" style={{ justifyContent: "space-between", alignItems: "center" }}>
              <h3 className="mono" style={{ margin: 0 }}>{selected}</h3>
              <div className="row">
                <button className="secondary btn-sm" onClick={removeEntry}>Delete</button>
                <button className="suggested btn-sm" disabled={!dirty} onClick={save}>Save entry</button>
              </div>
            </div>

            {kind === "adapters" ? <AdapterForm data={data} patch={patch} /> : <SchemaForm data={data} patch={patch} biolink={biolink} />}

            <details style={{ marginTop: 14 }}>
              <summary className="muted" style={{ fontSize: 12 }}>YAML (exactly as it will be written to the file)</summary>
              <pre className="gc-raw mono">{entryToYaml(selected, data)}</pre>
            </details>
          </>
        )}

        {msg && <div className={`alert ${msg.kind}`} style={{ marginTop: 12, whiteSpace: "pre-wrap" }}>{msg.text}</div>}
        </div>
      </div>
    </div>
  );
}

function AdapterForm({ data, patch }: { data: Data; patch: (up: (d: Data) => void) => void }) {
  const adapter = (data.adapter ?? {}) as Data;
  const args = (adapter.args ?? {}) as Data;
  return (
    <div className="gc-fields">
      <div className="row" style={{ gap: 18 }}>
        <label className="chip">
          <input type="checkbox" checked={!!data.nodes} onChange={(e) => patch((d) => { d.nodes = e.target.checked; })} /> nodes
        </label>
        <label className="chip">
          <input type="checkbox" checked={!!data.edges} onChange={(e) => patch((d) => { d.edges = e.target.checked; })} /> edges
        </label>
      </div>
      <Field label="Adapter module">
        <input className="input mono" value={String(adapter.module ?? "")} onChange={(e) => patch((d) => { ((d.adapter ??= {}) as Data).module = e.target.value; })} />
      </Field>
      <Field label="Adapter class">
        <input className="input mono" value={String(adapter.cls ?? "")} onChange={(e) => patch((d) => { ((d.adapter ??= {}) as Data).cls = e.target.value; })} />
      </Field>
      <Field label="Output dir">
        <input className="input mono" value={String(data.outdir ?? "")} onChange={(e) => patch((d) => { d.outdir = e.target.value; })} />
      </Field>
      <div>
        <span className="muted" style={{ fontSize: 12 }}>Arguments</span>
        <KeyValueRows obj={args} onChange={(o) => patch((d) => { ((d.adapter ??= {}) as Data).args = o; })} />
      </div>
    </div>
  );
}

function SchemaForm({ data, patch, biolink }: { data: Data; patch: (up: (d: Data) => void) => void; biolink: string[] }) {
  const props = (data.properties ?? {}) as Data;
  const mixins = Array.isArray(data.mixins) ? (data.mixins as string[]) : [];
  return (
    <div className="gc-fields">
      <Field label="Represented as">
        <select value={String(data.represented_as ?? "node")} onChange={(e) => patch((d) => { d.represented_as = e.target.value; })}>
          <option value="node">node</option>
          <option value="edge">edge</option>
        </select>
      </Field>
      <Field label="Input label">
        <input className="input mono" value={String(data.input_label ?? "")} onChange={(e) => patch((d) => { d.input_label = e.target.value; })} />
      </Field>
      <Field label="is_a">
        <input className="input mono" value={String(data.is_a ?? "")} onChange={(e) => patch((d) => { d.is_a = e.target.value; })} />
      </Field>
      <Field label="Biolink category">
        <input className="input mono" list="biolink-cats" value={String(data.biolink_category ?? "")} onChange={(e) => patch((d) => { d.biolink_category = e.target.value; })} />
        <datalist id="biolink-cats">{biolink.map((c) => <option key={c} value={c} />)}</datalist>
      </Field>
      <label className="chip" style={{ width: "fit-content" }}>
        <input type="checkbox" checked={!!data.inherit_properties} onChange={(e) => patch((d) => { d.inherit_properties = e.target.checked; })} /> inherit_properties
      </label>
      <Field label="Mixins (comma-separated)">
        <input className="input mono" value={mixins.join(", ")} onChange={(e) => patch((d) => { d.mixins = e.target.value.split(",").map((s) => s.trim()).filter(Boolean); })} />
      </Field>
      <div>
        <span className="muted" style={{ fontSize: 12 }}>Properties</span>
        <PropertiesEditor obj={props} onChange={(o) => patch((d) => { d.properties = o; })} />
      </div>
    </div>
  );
}

/** Editor for schema properties: name → { type, biolink, ...preserved }. */
function PropertiesEditor({ obj, onChange }: { obj: Data; onChange: (o: Data) => void }) {
  const rows = Object.entries(obj);
  const val = (v: unknown) => (v && typeof v === "object" ? (v as Data) : {});
  const setName = (i: number, name: string) => {
    const next: Data = {};
    rows.forEach(([k, v], j) => (next[j === i ? name : k] = v));
    onChange(next);
  };
  const setField = (k: string, field: string, value: string) => onChange({ ...obj, [k]: { ...val(obj[k]), [field]: value } });
  const remove = (k: string) => {
    const next = { ...obj };
    delete next[k];
    onChange(next);
  };
  const add = () => onChange({ ...obj, "": { type: "str", biolink: "" } });
  return (
    <div className="kv-rows">
      <div className="prop-head muted">
        <span>name</span><span>type</span><span>biolink</span><span></span>
      </div>
      {rows.map(([k, v], i) => {
        const o = val(v);
        return (
          <div className="prop-row" key={i}>
            <input className="input mono" value={k} placeholder="name" onChange={(e) => setName(i, e.target.value)} />
            <input className="input mono" value={String(o.type ?? "")} placeholder="str" onChange={(e) => setField(k, "type", e.target.value)} />
            <input className="input mono" list="biolink-cats" value={String(o.biolink ?? "")} placeholder="biolink slot" onChange={(e) => setField(k, "biolink", e.target.value)} />
            <button className="secondary btn-sm" onClick={() => remove(k)} title="remove">✕</button>
          </div>
        );
      })}
      <button className="secondary btn-sm" onClick={add}>+ add property</button>
    </div>
  );
}

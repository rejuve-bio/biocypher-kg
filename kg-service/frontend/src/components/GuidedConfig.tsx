import { useEffect, useMemo, useState } from "react";
import { api, type EntrySummary } from "../api/client";

type Kind = "adapters" | "schema";
type Data = Record<string, unknown>;

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

export default function GuidedConfig({ species, dataset, kind }: { species: string; dataset: string; kind: Kind }) {
  const [entries, setEntries] = useState<EntrySummary[] | null>(null);
  const [filter, setFilter] = useState("");
  const [selected, setSelected] = useState<string | null>(null);
  const [data, setData] = useState<Data | null>(null);
  const [dirty, setDirty] = useState(false);
  const [loading, setLoading] = useState(false);
  const [biolink, setBiolink] = useState<string[]>([]);
  const [msg, setMsg] = useState<{ kind: "ok" | "err"; text: string } | null>(null);

  useEffect(() => {
    setSelected(null);
    setData(null);
    setEntries(null);
    setMsg(null);
    api.getConfigEntries(species, dataset, kind).then((r) => setEntries(r.entries)).catch((e) => setMsg({ kind: "err", text: msgOf(e) }));
  }, [species, dataset, kind]);

  useEffect(() => {
    if (kind === "schema" && biolink.length === 0) api.biolinkCategories().then(setBiolink).catch(() => {});
  }, [kind, biolink.length]);

  const openEntry = (name: string) => {
    setSelected(name);
    setData(null);
    setDirty(false);
    setMsg(null);
    setLoading(true);
    api
      .getConfigEntry(species, dataset, kind, name)
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
      await api.saveConfigEntry(species, dataset, kind, selected, data);
      setDirty(false);
      setMsg({ kind: "ok", text: `Saved ${selected}. Only that entry's lines changed.` });
      api.getConfigEntries(species, dataset, kind).then((r) => setEntries(r.entries)).catch(() => {});
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

  const shown = useMemo(
    () => (entries ?? []).filter((e) => e.name.toLowerCase().includes(filter.toLowerCase())),
    [entries, filter],
  );

  return (
    <div className="gc-grid">
      <div className="gc-list">
        <input className="input" placeholder="Search entries…" value={filter} onChange={(e) => setFilter(e.target.value)} style={{ marginBottom: 8 }} />
        <button className="secondary btn-sm" style={{ marginBottom: 8, width: "100%" }} onClick={addEntry}>+ Add {kind === "adapters" ? "adapter" : "type"}</button>
        <div className="gc-entries">
          {entries === null ? (
            <div className="muted" style={{ padding: 10 }}>Loading…</div>
          ) : (
            shown.map((e) => (
              <button key={e.name} className={`gc-entry ${selected === e.name ? "active" : ""}`} onClick={() => openEntry(e.name)}>
                <span className="mono" style={{ fontSize: 12 }}>{e.name}</span>
                <span className="muted" style={{ fontSize: 11 }}>
                  {kind === "adapters"
                    ? `${e.nodes ? "nodes" : ""}${e.nodes && e.edges ? " · " : ""}${e.edges ? "edges" : ""}`
                    : e.represented_as}
                </span>
              </button>
            ))
          )}
          {entries !== null && shown.length === 0 && <div className="muted" style={{ padding: 10 }}>No matches.</div>}
        </div>
      </div>

      <div className="gc-form">
        {!selected ? (
          <div className="muted" style={{ padding: 20 }}>Select an entry to edit, or add a new one.</div>
        ) : loading || !data ? (
          <div className="muted" style={{ padding: 20 }}>Loading {selected}…</div>
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
              <summary className="muted" style={{ fontSize: 12 }}>Raw entry (everything, including fields not shown above)</summary>
              <pre className="gc-raw mono">{JSON.stringify(data, null, 2)}</pre>
            </details>
          </>
        )}

        {msg && <div className={`alert ${msg.kind}`} style={{ marginTop: 12 }}>{msg.text}</div>}
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

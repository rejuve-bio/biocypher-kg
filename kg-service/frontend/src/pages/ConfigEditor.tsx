import { useEffect, useState } from "react";
import CodeMirror from "@uiw/react-codemirror";
import { yaml } from "@codemirror/lang-yaml";
import { api } from "../api/client";
import type { SpeciesEntry } from "../types";
import ConfirmDialog, { type Confirmable } from "../components/ConfirmDialog";
import GuidedConfig from "../components/GuidedConfig";

type Kind = "adapters" | "schema";
type Mode = "guided" | "yaml";

export default function ConfigEditor() {
  const [species, setSpecies] = useState<SpeciesEntry[]>([]);
  const [sel, setSel] = useState("");
  const [dataset, setDataset] = useState("");
  const [kind, setKind] = useState<Kind>("adapters");
  const [mode, setMode] = useState<Mode>("guided");
  const [content, setContent] = useState("");
  const [path, setPath] = useState("");
  const [dirty, setDirty] = useState(false);
  const [loading, setLoading] = useState(false);
  const [msg, setMsg] = useState<{ kind: "ok" | "err" | "warn"; text: string } | null>(null);
  const [cmTheme, setCmTheme] = useState<"light" | "dark">(
    () => (document.documentElement.dataset.theme === "light" ? "light" : "dark"),
  );
  const [pending, setPending] = useState<Confirmable | null>(null);

  // Keep the editor theme in sync with the app's light/dark toggle.
  useEffect(() => {
    const root = document.documentElement;
    const obs = new MutationObserver(() =>
      setCmTheme(root.dataset.theme === "light" ? "light" : "dark"),
    );
    obs.observe(root, { attributes: true, attributeFilter: ["data-theme"] });
    return () => obs.disconnect();
  }, []);

  useEffect(() => {
    api
      .listSpecies()
      .then((s) => {
        setSpecies(s);
        if (s.length) setSel(s[0].species);
      })
      .catch((e) => setMsg({ kind: "err", text: msgOf(e) }));
  }, []);

  const datasets = species.find((s) => s.species === sel)?.datasets ?? [];

  useEffect(() => {
    if (sel === "base") {
      // The base (shared) primer schema has no datasets and no adapters.
      setDataset("base");
      setKind("schema");
    } else {
      setDataset(datasets.length ? datasets[0].name : "");
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sel, species]);

  useEffect(() => {
    if (mode !== "yaml") return;
    if (!sel) return;
    if (sel !== "base" && (!dataset || !datasets.some((d) => d.name === dataset))) return;
    let alive = true;
    setLoading(true);
    setMsg(null);
    api
      .getConfig(sel, dataset, kind)
      .then((r) => {
        if (!alive) return;
        setContent(r.content);
        setPath(r.path);
        setDirty(false);
      })
      .catch((e) => {
        if (!alive) return;
        setContent("");
        setPath("");
        setMsg({ kind: "err", text: msgOf(e) });
      })
      .finally(() => alive && setLoading(false));
    return () => {
      alive = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sel, dataset, kind, mode]);

  async function save() {
    setMsg(null);
    try {
      const r = await api.saveConfig(sel, dataset, kind, content);
      setContent(r.content);
      setDirty(false);
      const warns = r.warnings ?? [];
      if (warns.length) {
        setMsg({ kind: "warn", text: `Saved, but ${warns.length} warning${warns.length > 1 ? "s" : ""}:\n- ${warns.join("\n- ")}` });
      } else {
        setMsg({ kind: "ok", text: "Saved. Revert with git if needed." });
      }
    } catch (e) {
      setMsg({ kind: "err", text: msgOf(e) });
    }
  }

  return (
    <div className="card">
      <h2>Edit config</h2>
      <div className="row">
        <label className="field">
          Species
          <select value={sel} onChange={(e) => setSel(e.target.value)}>
            <option value="base">Base (shared)</option>
            {species.map((s) => (
              <option key={s.species} value={s.species}>{s.species}</option>
            ))}
          </select>
        </label>
        {sel !== "base" && (
          <label className="field">
            Dataset
            <select value={dataset} onChange={(e) => setDataset(e.target.value)}>
              {datasets.map((d) => (
                <option key={d.name}>{d.name}</option>
              ))}
            </select>
          </label>
        )}
        <label className="field">
          File
          <select value={kind} onChange={(e) => setKind(e.target.value as Kind)}>
            <option value="adapters" disabled={sel === "base"}>adapters config</option>
            <option value="schema">schema config</option>
          </select>
        </label>
        <span style={{ flex: 1 }} />
        <span className="group-actions" style={{ alignSelf: "end" }}>
          <button className={`linkbtn ${mode === "guided" ? "seg-on" : ""}`} onClick={() => setMode("guided")}>Guided</button>
          <button className={`linkbtn ${mode === "yaml" ? "seg-on" : ""}`} onClick={() => setMode("yaml")}>YAML</button>
        </span>
      </div>

      {mode === "guided" && sel && dataset && (
        <GuidedConfig species={sel} dataset={dataset} kind={kind} />
      )}

      {mode === "yaml" && (
        <>
          {path && (
            <div className="field-hint" style={{ marginTop: 10 }}>
              <span className="mono">{path}</span>
              {dirty ? " · unsaved changes" : ""}
            </div>
          )}

          <div className="cm-wrap" style={{ marginTop: 10 }}>
            <CodeMirror
              value={content}
              height="480px"
              theme={cmTheme}
              extensions={[yaml()]}
              editable={!loading}
              onChange={(v) => {
                setContent(v);
                setDirty(true);
              }}
              basicSetup={{ lineNumbers: true, foldGutter: true, highlightActiveLine: true }}
              placeholder={loading ? "Loading…" : "Select a config to edit…"}
            />
          </div>

          {msg && (
            <div className={`alert ${msg.kind}`} style={{ marginTop: 10, whiteSpace: "pre-wrap" }}>
              {msg.text}
            </div>
          )}

          <div className="row" style={{ marginTop: 12 }}>
            <button
              className="primary"
              disabled={!dirty || loading || !content}
              onClick={() =>
                setPending({
                  title: "Save config changes?",
                  message: `This overwrites ${path}. New builds of ${sel}/${dataset} will use the updated config. The file is validated on save, and you can revert it with git if needed.`,
                  confirmLabel: "Save",
                  run: save,
                })
              }
            >
              💾 Save
            </button>
            <span className="field-hint">Validated on save; invalid YAML is rejected and nothing is overwritten.</span>
          </div>
        </>
      )}

      <ConfirmDialog pending={pending} onClose={() => setPending(null)} />
    </div>
  );
}

function msgOf(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

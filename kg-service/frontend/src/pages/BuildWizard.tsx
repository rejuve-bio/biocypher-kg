import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import { api, ApiError } from "../api/client";
import type {
  AdapterInfo,
  BuildRequest,
  FlagInfo,
  SpeciesEntry,
  ValidationResult,
} from "../types";

export default function BuildWizard() {
  const navigate = useNavigate();

  const [species, setSpecies] = useState<SpeciesEntry[]>([]);
  const [writers, setWriters] = useState<string[]>([]);
  const [flags, setFlags] = useState<FlagInfo[]>([]);
  const [loadError, setLoadError] = useState<string | null>(null);

  const [selSpecies, setSelSpecies] = useState<string>("");
  const [selDataset, setSelDataset] = useState<string>("");
  const [adapters, setAdapters] = useState<AdapterInfo[]>([]);
  const [adaptersError, setAdaptersError] = useState<string | null>(null);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [writerType, setWriterType] = useState<string>("metta");
  const [outputDir, setOutputDir] = useState<string>("");
  const [adapterFilter, setAdapterFilter] = useState<string>("");
  const [flagValues, setFlagValues] = useState<Record<string, boolean>>({});

  const [validating, setValidating] = useState(false);
  const [validation, setValidation] = useState<ValidationResult | null>(null);
  const [submitting, setSubmitting] = useState(false);

  // Initial load: species, writers, flags.
  useEffect(() => {
    Promise.all([api.listSpecies(), api.listWriters(), api.listFlags()])
      .then(([sp, w, fl]) => {
        setSpecies(sp);
        setWriters(w);
        setFlags(fl);
        setFlagValues(Object.fromEntries(fl.map((f) => [f.name, f.default])));
        if (sp.length) setSelSpecies(sp[0].species);
      })
      .catch((e) => setLoadError(String(e)));
  }, []);

  const datasets = useMemo(
    () => species.find((s) => s.species === selSpecies)?.datasets ?? [],
    [species, selSpecies],
  );

  // Pick a default dataset (prefer 'sample', prefer ones whose config exists).
  useEffect(() => {
    if (!datasets.length) {
      setSelDataset("");
      return;
    }
    const usable = datasets.filter((d) => d.adapters_config_exists);
    // Only auto-select a dataset whose config actually exists. If none do, leave
    // it unset so the adapters section shows a friendly notice instead of erroring.
    const preferred = usable.find((d) => d.name === "sample") ?? usable[0];
    setSelDataset(preferred ? preferred.name : "");
  }, [datasets]);

  // Load adapters when species/dataset change.
  useEffect(() => {
    setValidation(null);
    setAdaptersError(null);
    if (!selSpecies || !selDataset) {
      setAdapters([]);
      setSelected(new Set());
      return;
    }
    api
      .listAdapters(selSpecies, selDataset)
      .then((r) => {
        setAdapters(r.adapters);
        setSelected(new Set(r.adapters.map((a) => a.name))); // default: all
      })
      .catch((e) => {
        // Scope the failure to this section — don't blow away the whole wizard.
        setAdapters([]);
        setSelected(new Set());
        setAdaptersError(String(e));
      });
  }, [selSpecies, selDataset]);

  function toggleAdapter(name: string) {
    setSelected((prev) => {
      const next = new Set(prev);
      next.has(name) ? next.delete(name) : next.add(name);
      return next;
    });
    setValidation(null);
  }

  const nodeAdapters = useMemo(() => adapters.filter((a) => !a.edges), [adapters]);
  const edgeAdapters = useMemo(() => adapters.filter((a) => a.edges), [adapters]);

  function setGroup(list: AdapterInfo[], on: boolean) {
    setSelected((prev) => {
      const next = new Set(prev);
      list.forEach((a) => (on ? next.add(a.name) : next.delete(a.name)));
      return next;
    });
    setValidation(null);
  }

  function buildRequest(): BuildRequest {
    const allSelected = selected.size === adapters.length;
    return {
      species: selSpecies,
      dataset: selDataset,
      // omit include_adapters when everything is selected (means "all")
      include_adapters: allSelected ? null : Array.from(selected),
      writer_type: writerType,
      // empty => let the server assign a default per-build output folder
      output_dir: outputDir.trim() || null,
      write_properties: flagValues.write_properties ?? true,
      add_provenance: flagValues.add_provenance ?? true,
      include_taxon_id: flagValues.include_taxon_id ?? true,
      include_curie: flagValues.include_curie ?? false,
      skip_preflight: flagValues.skip_preflight ?? false,
      generate_data_source_schemas:
        flagValues.generate_data_source_schemas ?? true,
    };
  }

  async function onValidate() {
    setValidating(true);
    setValidation(null);
    try {
      setValidation(await api.validate(buildRequest()));
    } catch (e) {
      setLoadError(String(e));
    } finally {
      setValidating(false);
    }
  }

  async function onBuild() {
    setSubmitting(true);
    try {
      const res = await api.createBuild(buildRequest());
      navigate(`/builds/${res.id}`);
    } catch (e) {
      if (e instanceof ApiError && e.detail && typeof e.detail === "object") {
        const v = (e.detail as { validation?: ValidationResult }).validation;
        if (v) setValidation(v);
      } else {
        setLoadError(String(e));
      }
    } finally {
      setSubmitting(false);
    }
  }

  if (loadError) {
    return (
      <div className="alert err">
        Failed to load Console data: {loadError}
        <div className="muted" style={{ marginTop: 8 }}>
          Is the API running (uvicorn backend.api.main:app) and REPO_ROOT correct?
        </div>
      </div>
    );
  }

  const nSelected = selected.size;

  const q = adapterFilter.trim().toLowerCase();
  const applyFilter = (list: AdapterInfo[]) =>
    q ? list.filter((a) => a.name.toLowerCase().includes(q)) : list;

  const renderItem = (a: AdapterInfo) => (
    <label className="adapter-item" key={a.name} title={a.module ?? ""}>
      <input
        type="checkbox"
        checked={selected.has(a.name)}
        onChange={() => toggleAdapter(a.name)}
      />
      <span className="name">{a.name}</span>
    </label>
  );

  const renderGroup = (title: string, kind: "node" | "edge", list: AdapterInfo[]) => {
    if (!list.length) return null;
    const sel = list.filter((a) => selected.has(a.name)).length;
    return (
      <div className="adapter-group">
        <div className="group-head">
          <span className={`group-dot ${kind}`} />
          <span className="group-title">{title}</span>
          <span className="group-count">
            {sel}/{list.length}
          </span>
          <span className="group-actions">
            <button className="linkbtn" onClick={() => setGroup(list, true)}>
              all
            </button>
            <button className="linkbtn" onClick={() => setGroup(list, false)}>
              none
            </button>
          </span>
        </div>
        <div className="adapter-grid">{list.map(renderItem)}</div>
      </div>
    );
  };

  return (
    <>
      <div className="card">
        <h2>1 · Species &amp; Dataset</h2>
        <div className="row">
          <label className="field">
            Species
            <select
              value={selSpecies}
              onChange={(e) => setSelSpecies(e.target.value)}
            >
              {species.map((s) => (
                <option key={s.species} value={s.species}>
                  {s.species}
                </option>
              ))}
            </select>
          </label>
          <label className="field">
            Dataset
            <select
              value={selDataset}
              onChange={(e) => setSelDataset(e.target.value)}
            >
              {datasets.map((d) => (
                <option
                  key={d.name}
                  value={d.name}
                  disabled={!d.adapters_config_exists}
                >
                  {d.name}
                  {d.adapters_config_exists ? "" : " (config missing)"}
                </option>
              ))}
            </select>
          </label>
        </div>
      </div>

      <div className="card">
        <h2>
          2 · Adapters ({nSelected}/{adapters.length})
        </h2>
        <div className="row" style={{ marginBottom: 10 }}>
          <input
            type="text"
            value={adapterFilter}
            onChange={(e) => setAdapterFilter(e.target.value)}
            placeholder="🔍 Filter adapters…"
            style={{ flex: 1, minWidth: 200 }}
          />
          <button onClick={() => setSelected(new Set(adapters.map((a) => a.name)))}>
            Select all
          </button>
          <button onClick={() => setSelected(new Set())}>Clear</button>
        </div>
        {!selDataset && (
          <div className="alert warn">
            No dataset with config files is available for this species yet.
          </div>
        )}
        {adaptersError && (
          <div className="alert err">Could not load adapters: {adaptersError}</div>
        )}
        {renderGroup("Nodes", "node", applyFilter(nodeAdapters))}
        {renderGroup("Edges", "edge", applyFilter(edgeAdapters))}
        {adapters.length > 0 && q &&
          !applyFilter(nodeAdapters).length &&
          !applyFilter(edgeAdapters).length && (
            <span className="muted">No adapters match “{adapterFilter}”.</span>
          )}
        {!adapters.length && selDataset && !adaptersError && (
          <span className="muted">No adapters loaded.</span>
        )}
      </div>

      <div className="card">
        <h2>3 · Output format &amp; options</h2>
        <div className="row" style={{ marginBottom: 12, alignItems: "flex-start" }}>
          <label className="field">
            Writer
            <select
              value={writerType}
              onChange={(e) => setWriterType(e.target.value)}
            >
              {writers.map((w) => (
                <option key={w} value={w}>
                  {w}
                </option>
              ))}
            </select>
          </label>
          <label className="field" style={{ flex: 1, minWidth: 280 }}>
            Output directory (optional)
            <input
              type="text"
              value={outputDir}
              onChange={(e) => setOutputDir(e.target.value)}
              placeholder="Leave empty for a default per-build folder"
              style={{ width: "100%" }}
            />
            <span className="field-hint">
              Absolute path or relative to the repo root. In Docker, any path under
              the mounted DATA_ROOT writes straight to your host. Blank = default
              per-build folder.
            </span>
          </label>
        </div>
        <div className="chips">
          {flags.map((f) => (
            <label className="chip" key={f.name} title={f.help}>
              <input
                type="checkbox"
                checked={flagValues[f.name] ?? f.default}
                onChange={(e) =>
                  setFlagValues((prev) => ({ ...prev, [f.name]: e.target.checked }))
                }
              />
              {f.name}
            </label>
          ))}
        </div>
      </div>

      <div className="card">
        <h2>4 · Validate &amp; Build</h2>
        <div className="row">
          <button
            className="secondary"
            onClick={onValidate}
            disabled={validating || !selDataset}
          >
            {validating ? "Validating…" : "Validate"}
          </button>
          <button
            className="primary"
            onClick={onBuild}
            disabled={submitting || !nSelected}
          >
            {submitting ? "Launching…" : "Launch build"}
          </button>
        </div>
        {validation && <ValidationPanel result={validation} />}
      </div>
    </>
  );
}

function ValidationPanel({ result }: { result: ValidationResult }) {
  return (
    <div style={{ marginTop: 14 }}>
      {result.valid ? (
        <div className="alert ok">
          ✓ Valid{result.checked_paths ? " — all input paths exist" : ""} (
          {result.resolved.num_adapters} adapter
          {result.resolved.num_adapters === 1 ? "" : "s"})
        </div>
      ) : (
        <div className="alert err">✗ Not valid — see below</div>
      )}

      {result.static_errors.map((e, i) => (
        <div className="alert err" key={`e${i}`}>
          {e}
        </div>
      ))}
      {result.static_warnings.map((w, i) => (
        <div className="alert warn" key={`w${i}`}>
          ⚠ {w}
        </div>
      ))}

      {Object.keys(result.missing_paths).length > 0 && (
        <div className="alert err">
          <strong>Missing input files:</strong>
          {Object.entries(result.missing_paths).map(([adapter, args]) => (
            <div key={adapter} style={{ marginTop: 6 }}>
              <div className="mono">[{adapter}]</div>
              {Object.entries(args).map(([arg, path]) => (
                <div className="mono muted" key={arg}>
                  &nbsp;&nbsp;{arg}: {path}
                </div>
              ))}
            </div>
          ))}
        </div>
      )}

      <details style={{ marginTop: 8 }}>
        <summary className="muted">Command preview</summary>
        <div className="mono" style={{ marginTop: 6 }}>
          {result.resolved.cmd_preview.join(" ")}
        </div>
      </details>
    </div>
  );
}

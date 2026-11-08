import { useEffect, useMemo, useRef, useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { MOD, usePageHints } from "../components/Hints";
import {
  api,
  ApiError,
  formatBytes,
  type DataStatus,
  type KnownSize,
  type AdapterGroup,
  type DbsnpStatus,
} from "../api/client";
import {
  compact,
  datesText,
  PERCENT_CHOICES,
  preview,
  ROW_CHOICES,
  sourceSize,
  type SizeUnit,
} from "../sizes";
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
  const [adapterGroups, setAdapterGroups] = useState<AdapterGroup[] | null>(null);
  const [activeSource, setActiveSource] = useState<string | null>(null);
  const [activeDownloadSource, setActiveDownloadSource] = useState<string | null>(null);
  const [adaptersError, setAdaptersError] = useState<string | null>(null);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [writerType, setWriterType] = useState<string>("metta");
  const [maxWorkers, setMaxWorkers] = useState<number>(1);
  const [workerCap, setWorkerCap] = useState<number>(8);
  // Size: rows per adapter (0 = no limit), or a percent of each adapter's known size.
  const [sizeUnit, setSizeUnit] = useState<SizeUnit>("rows");
  const [limit, setLimit] = useState<number>(0);
  const [percent, setPercent] = useState<number>(10);
  // Each adapter's full size, learned from earlier builds (sizes.py on the server).
  const [known, setKnown] = useState<Record<string, KnownSize>>({});
  // Where builds are saved (Settings > Builds folder), shown in step 3.
  const [buildsFolder, setBuildsFolder] = useState<string | null>(null);
  // What Settings still needs before any build (folders have no defaults).
  const [missing, setMissing] = useState<string[]>([]);
  const [dataFolder, setDataFolder] = useState<string | null>(null);
  // Local source-data status + a planned-build estimate (scope before building).
  const [dataStatus, setDataStatus] = useState<DataStatus | null>(null);
  const [downloadId, setDownloadId] = useState<string | null>(null);
  const [downloadStatus, setDownloadStatus] = useState<string | null>(null);
  const [dataError, setDataError] = useState<string | null>(null);
  // First-run onboarding (dismissible, remembered per browser).
  const [showHelp, setShowHelp] = useState<boolean>(() => {
    try {
      return localStorage.getItem("kg-console-onboarded") !== "1";
    } catch {
      return true;
    }
  });
  const [adapterFilter, setAdapterFilter] = useState<string>("");
  const [dbsnpVariant, setDbsnpVariant] = useState<string>("common");
  const [dbsnpCacheRoot, setDbsnpCacheRoot] = useState<string>("");
  // dbSNP cache the console prepares into the data folder; status is polled while a
  // prepare job runs. dbsnpOwnPath reveals the "use an existing cache" path field.
  const [dbsnp, setDbsnp] = useState<DbsnpStatus | null>(null);
  const [dbsnpPoke, setDbsnpPoke] = useState(0);
  const [dbsnpBusy, setDbsnpBusy] = useState(false);
  const [dbsnpOwnPath, setDbsnpOwnPath] = useState(false);
  const [flagValues, setFlagValues] = useState<Record<string, boolean>>({});

  const [validating, setValidating] = useState(false);
  const [validation, setValidation] = useState<ValidationResult | null>(null);
  const [submitting, setSubmitting] = useState(false);

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
    // Worker cap is a progressive enhancement; keep the default if the endpoint is absent.
    api.buildLimits().then((lim) => setWorkerCap(Math.max(1, lim.max_workers))).catch(() => {});
    api
      .getSettings()
      .then((s) => {
        setBuildsFolder(s.build.output_root);
        setDataFolder(s.build.data_root);
        setMissing(s.build.missing);
      })
      .catch(() => {});
  }, []);

  const isAll = selSpecies === "all";

  const datasets = useMemo(() => {
    if (isAll) {
      // All-species: union of dataset names; each species uses its own config.
      const names = Array.from(
        new Set(species.flatMap((s) => s.datasets.map((d) => d.name))),
      );
      return names.map((name) => ({
        name,
        adapters_config: "",
        schema_config: "",
        dbsnp_cache_root: "",
        dbsnp_variant: "",
        adapters_config_exists: true,
        schema_config_exists: true,
      }));
    }
    return species.find((s) => s.species === selSpecies)?.datasets ?? [];
  }, [species, selSpecies, isAll]);

  // Pick a default dataset (prefer 'sample', prefer ones whose config exists).
  useEffect(() => {
    if (!datasets.length) {
      setSelDataset("");
      return;
    }
    const usable = datasets.filter((d) => d.adapters_config_exists);
    // Leave unset when no config exists so the UI shows a notice instead of erroring.
    const preferred = usable.find((d) => d.name === "sample") ?? usable[0];
    setSelDataset(preferred ? preferred.name : "");
  }, [datasets]);

  // Prefill dbSNP fields from the selected dataset's species_config defaults.
  useEffect(() => {
    const d = datasets.find((x) => x.name === selDataset);
    setDbsnpCacheRoot(d?.dbsnp_cache_root ?? "");
    setDbsnpVariant(d?.dbsnp_variant || "common");
  }, [selDataset, datasets]);

  useEffect(() => {
    // Answers for a species/dataset you've already switched away from are dropped:
    // a late one would replace the list (selecting all of it), so the build would
    // run adapters you can't see.
    let current = true;
    setValidation(null);
    setAdaptersError(null);
    // Clear the old list right away, so nothing from the last choice can be launched.
    setAdapters([]);
    setAdapterGroups(null);
    setSelected(new Set());
    // "all" has no single config to introspect: every adapter of each species runs.
    if (isAll || !selSpecies || !selDataset) {
      return;
    }
    api
      .listAdapters(selSpecies, selDataset)
      .then((r) => {
        if (!current) return;
        setAdapters(r.adapters);
        setSelected(new Set(r.adapters.map((a) => a.name))); // default: all
      })
      .catch((e) => {
        if (!current) return;
        // Scope the failure to this section; don't blow away the whole wizard.
        setAdapters([]);
        setSelected(new Set());
        setAdaptersError(String(e));
      });
    // Source grouping drives the picker; fall back to the flat list on failure.
    api
      .adaptersBySource(selSpecies, selDataset)
      .then((r) => {
        if (!current) return;
        setAdapterGroups(r.groups);
        // Default the detail pane to the first source that has a schema to show.
        setActiveSource(
          (r.groups.find((g) => g.adapter_count > 0) ?? r.groups[0])?.source ?? null,
        );
      })
      .catch(() => {
        if (!current) return;
        setAdapterGroups(null);
        setActiveSource(null);
      });
    return () => {
      current = false;
    };
  }, [selSpecies, selDataset]);

  // Local data presence + a scope estimate for the chosen species/dataset.
  useEffect(() => {
    let current = true;
    setDataStatus(null);
    setKnown({});
    if (isAll || !selSpecies) return;
    api.dataStatus(selSpecies).then((d) => current && setDataStatus(d)).catch(() => current && setDataStatus(null));
    if (selDataset)
      api.sizes(selSpecies, selDataset).then((r) => current && setKnown(r.adapters)).catch(() => current && setKnown({}));
    return () => {
      current = false;
    };
  }, [selSpecies, selDataset, isAll]);

  // Poll a running download until it finishes, then refresh the data status.
  useEffect(() => {
    if (!downloadId) return;
    let alive = true;
    const tick = async () => {
      try {
        const j = await api.getBuild(downloadId);
        if (!alive) return;
        setDownloadStatus(j.status);
        if (["succeeded", "failed", "cancelled"].includes(j.status)) {
          if (selSpecies) {
            api.dataStatus(selSpecies).then(setDataStatus).catch(() => {});
            // Refresh per-source presence so the source cards update after a fetch.
            if (selDataset)
              api
                .adaptersBySource(selSpecies, selDataset)
                .then((r) => setAdapterGroups(r.groups))
                .catch(() => {});
          }
          setActiveDownloadSource(null);
          return; // terminal: stop polling
        }
      } catch {
        /* transient; keep polling */
      }
      if (alive) setTimeout(tick, 2500);
    };
    tick();
    return () => {
      alive = false;
    };
  }, [downloadId, selSpecies, selDataset]);

  // dbSNP cache status for non-sample runs. Re-checks a typed cache path (debounced)
  // so the UI can say whether it's usable, and polls while a prepare job is running.
  useEffect(() => {
    if (!selSpecies || !selDataset || selDataset === "sample") {
      setDbsnp(null);
      return;
    }
    let alive = true;
    let poll: number | undefined;
    const load = () =>
      api
        .dbsnpStatus(dbsnpCacheRoot.trim() || undefined)
        .then((s) => {
          if (!alive) return;
          setDbsnp(s);
          if (s.job_id) poll = window.setTimeout(load, 5000);
        })
        .catch(() => {
          if (alive) setDbsnp(null);
        });
    const debounce = window.setTimeout(load, 300);
    return () => {
      alive = false;
      window.clearTimeout(debounce);
      if (poll) window.clearTimeout(poll);
    };
  }, [selSpecies, selDataset, dbsnpCacheRoot, dbsnpPoke]);

  function dismissHelp() {
    setShowHelp(false);
    try {
      localStorage.setItem("kg-console-onboarded", "1");
    } catch {
      /* private mode: just hide for this session */
    }
  }

  async function onDownload() {
    if (!selSpecies || isAll) return;
    setDataError(null);
    try {
      const res = await api.startDownload({ species: selSpecies });
      setDownloadId(res.id);
      setDownloadStatus(res.status);
      setActiveDownloadSource(null);
    } catch (e) {
      setDataError(String(e));
    }
  }

  // Fetch just one source's data (the downloader supports --source).
  async function onDownloadSource(source: string) {
    if (!selSpecies || isAll) return;
    setDataError(null);
    try {
      const res = await api.startDownload({ species: selSpecies, source });
      setDownloadId(res.id);
      setDownloadStatus(res.status);
      setActiveDownloadSource(source);
    } catch (e) {
      setDataError(String(e));
    }
  }

  const downloadBusy = !!downloadId && (downloadStatus === "running" || downloadStatus === "queued");

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
  // Full adapter records keyed by name, so a source group can reuse the node/edge
  // renderers (which want AdapterInfo) from the lighter grouped payload.
  const adapterByName = useMemo(
    () => new Map(adapters.map((a) => [a.name, a])),
    [adapters],
  );

  function setGroup(list: AdapterInfo[], on: boolean) {
    setSelected((prev) => {
      const next = new Set(prev);
      list.forEach((a) => (on ? next.add(a.name) : next.delete(a.name)));
      return next;
    });
    setValidation(null);
  }

  // Download just the selected sources that aren't on disk yet, as one job.
  async function onDownloadMissing(sources: string[]) {
    if (!selSpecies || isAll || !sources.length) return;
    setDataError(null);
    try {
      const res = await api.startDownload({ species: selSpecies, sources });
      setDownloadId(res.id);
      setDownloadStatus(res.status);
      setActiveDownloadSource(null);
    } catch (e) {
      setDataError(String(e));
    }
  }

  async function onPrepareDbsnp() {
    setDbsnpBusy(true);
    setDataError(null);
    try {
      await api.prepareDbsnp(dbsnpVariant === "full" ? "full" : "common");
      setDbsnpPoke((n) => n + 1);
    } catch (e) {
      setDataError(String(e));
    } finally {
      setDbsnpBusy(false);
    }
  }

  // ── Readiness: what the selected sources need before a build can run ──
  // A source counts as selected when all of its adapters are in the build.
  const selectedGroups = (adapterGroups ?? []).filter(
    (g) => g.adapter_count > 0 && g.adapters.every((a) => selected.has(a.name)),
  );
  const selectedData = selectedGroups.filter((g) => g.source !== "_shared");
  const onDisk = selectedData.filter((g) => g.present);
  const inYours = onDisk.filter((g) => g.location === "yours").length;
  const toDownload = selectedData.filter((g) => g.downloadable && !g.present);
  const byHand = selectedData.filter((g) => !g.downloadable && !g.present);
  const dbsnpNeededBy = selectedGroups.filter((g) => g.needs_dbsnp).map((g) => g.name);
  // The pipeline only loads dbSNP when a selected adapter needs it. All-species runs
  // (and the flat fallback list) can't be inspected here, so assume it's needed.
  const dbsnpNeeded =
    !!selDataset &&
    selDataset !== "sample" &&
    (isAll || !adapterGroups ? true : dbsnpNeededBy.length > 0);
  const variantKey: "common" | "full" = dbsnpVariant === "full" ? "full" : "common";
  const customRoot = dbsnpCacheRoot.trim();
  const managedInfo = dbsnp?.variants[variantKey];
  const managedReady = !!managedInfo?.present;
  const customReady = !!dbsnp?.custom?.variants[variantKey]?.present;
  // A path typed here wins; otherwise the cache that has this variant (your read-only
  // cache from Settings first, then the one the console prepared).
  const effectiveDbsnpRoot =
    customRoot || (managedReady && dbsnp ? managedInfo?.root ?? dbsnp.root : "");
  const dbsnpFromYours = managedReady && !!dbsnp?.readonly_root && managedInfo?.root === dbsnp.readonly_root;

  function buildRequest(): BuildRequest {
    const allSelected = selected.size === adapters.length;
    return {
      species: selSpecies,
      dataset: selDataset,
      // null = all adapters (always for all-species runs).
      include_adapters: isAll || allSelected ? null : Array.from(selected),
      writer_type: writerType,
      // Server decides where output goes: DATA_ROOT/<dated> if set, else repo default.
      output_dir: null,
      // dbSNP only when a selected source needs it (never for sample, which uses the
      // bundled cache).
      dbsnp_cache_root: dbsnpNeeded ? effectiveDbsnpRoot || null : null,
      dbsnp_variant: dbsnpNeeded ? variantKey : null,
      write_properties: flagValues.write_properties ?? true,
      add_provenance: flagValues.add_provenance ?? true,
      include_taxon_id: flagValues.include_taxon_id ?? true,
      include_curie: flagValues.include_curie ?? false,
      skip_preflight: flagValues.skip_preflight ?? false,
      generate_data_source_schemas:
        flagValues.generate_data_source_schemas ?? true,
      max_workers: maxWorkers,
      limit: sizeUnit === "rows" ? limit : 0,
      percent: sizeUnit === "percent" && !isAll ? percent : null,
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

  // Size: Percent needs every chosen adapter's full size; the preview shows what
  // this build will write, from the known sizes and the chosen limit.
  const chosenAdapters = isAll ? [] : Array.from(selected);
  const sizeAhead = preview(chosenAdapters, known, sizeUnit, limit, percent);
  const percentReady = !isAll && chosenAdapters.length > 0 && sizeAhead.unknown.length === 0;
  const percentWhyNot = isAll
    ? "percent: one species at a time"
    : !chosenAdapters.length
      ? "percent: pick sources first"
      : `percent: ${sizeAhead.unknown.length} adapter(s) unsized · build them once, no limit`;
  const sizePreviewText = (() => {
    if (sizeUnit === "percent" && !percentReady) return percentWhyNot;
    if (!chosenAdapters.length) return "";
    const from = sizeAhead.dates.length ? ` · sized ${datesText(sizeAhead.dates)}` : "";
    const counts = `≈ ${sizeAhead.nodes.toLocaleString()} nodes · ${sizeAhead.edges.toLocaleString()} edges`;
    if (!sizeAhead.known) return "size unknown · build once, no limit, to learn it";
    if (sizeAhead.unknown.length) {
      const cap = limit > 0 && sizeUnit === "rows" ? ` (≤ ${limit.toLocaleString()} rows each)` : "";
      return `${counts}${from} + ${sizeAhead.unknown.length} unsized${cap}`;
    }
    return `${counts}${from}`;
  })();

  // Validate and Launch in the bar along the bottom; ⌘↵ / Ctrl+Enter launches.
  const canValidate = !validating && !!selDataset;
  // Percent chosen but no longer possible (a source of unknown size was added): the
  // size line says why, and Launch waits until Rows is picked or the sizes are known.
  const sizeBlocked = sizeUnit === "percent" && !percentReady;
  const canLaunch = !submitting && (isAll || selected.size > 0) && !sizeBlocked && !missing.length;
  usePageHints([
    { keys: [], label: validating ? "Validating…" : "Validate", onClick: onValidate, disabled: !canValidate },
    { keys: [MOD, "↵"], label: submitting ? "Launching…" : "Launch build", onClick: onBuild, disabled: !canLaunch },
  ]);
  const launch = useRef({ onBuild, canLaunch });
  launch.current = { onBuild, canLaunch };
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === "Enter" && launch.current.canLaunch) {
        e.preventDefault();
        launch.current.onBuild();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  if (loadError) {
    return (
      <div className="alert err">
        The console couldn’t load what it needs: {loadError}
        <div className="muted" style={{ marginTop: 8 }}>Reload the page to try again.</div>
      </div>
    );
  }

  // Source-level selection summary (a source counts as selected when all its
  // adapters are in the build).
  const selectableGroups = (adapterGroups ?? []).filter((g) => g.adapter_count > 0);
  const selectedGroupCount = selectableGroups.filter((g) =>
    g.adapters.every((a) => selected.has(a.name)),
  ).length;

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

  // Turn an internal adapter name into a readable type: drop the source prefix and
  // the underscores (e.g. "uniprot_sprot_translates_to" -> "sprot translates to").
  function readableType(name: string, source: string): string {
    let n = name;
    if (source && source !== "_shared" && n.startsWith(source + "_")) n = n.slice(source.length + 1);
    return n.replace(/_/g, " ");
  }

  const groupAdapterInfos = (g: AdapterGroup) =>
    g.adapters.map((a) => adapterByName.get(a.name)).filter((a): a is AdapterInfo => Boolean(a));

  const groupMatches = (g: AdapterGroup) =>
    !q ||
    g.name.toLowerCase().includes(q) ||
    g.adapters.some((a) => a.name.toLowerCase().includes(q));

  // A source's full size, once every adapter of it has been built without a limit.
  const adapterNames = (g: AdapterGroup) => g.adapters.map((a) => a.name);
  const sizeLabel = (g: AdapterGroup) => {
    const size = sourceSize(adapterNames(g), known);
    return size ? compact(size.nodes + size.edges) : "";
  };
  const sizeTitle = (g: AdapterGroup) => {
    const size = sourceSize(adapterNames(g), known);
    return size
      ? `${size.nodes.toLocaleString()} nodes · ${size.edges.toLocaleString()} edges`
      : "size unknown";
  };

  // Left list: one compact selectable row per source. The checkbox includes the
  // source in the build; clicking the row opens its schema in the detail pane.
  const renderSourceListItem = (g: AdapterGroup) => {
    const empty = g.adapter_count === 0;
    const included = !empty && g.adapters.every((a) => selected.has(a.name));
    const isShared = g.source === "_shared";
    const active = activeSource === g.source;
    return (
      <div
        className={`source-item${active ? " active" : ""}${included ? " on" : ""}`}
        key={g.source}
        onClick={() => setActiveSource(g.source)}
      >
        <input
          type="checkbox"
          checked={included}
          disabled={empty}
          onClick={(e) => e.stopPropagation()}
          onChange={() => setGroup(groupAdapterInfos(g), !included)}
        />
        <span className="source-item-name">{g.name}</span>
        {!isShared && (
          <span
            className={`src-dot ${g.present ? "ok" : "missing"}`}
            title={g.present ? "downloaded" : "not downloaded"}
          />
        )}
        <span className="muted source-item-count" title={sizeTitle(g)}>{sizeLabel(g)}</span>
      </div>
    );
  };

  // Right pane: the active source's schema as node/edge columns, plus its download
  // controls (kept out of the list so navigating sources stays compact).
  const renderSourceDetail = (g: AdapterGroup) => {
    const empty = g.adapter_count === 0;
    const isShared = g.source === "_shared";
    const included = !empty && g.adapters.every((a) => selected.has(a.name));
    const downloading = activeDownloadSource === g.source && downloadBusy;
    const nodeTypes = g.adapters.filter((a) => a.nodes).map((a) => readableType(a.name, g.source));
    const edgeTypes = g.adapters.filter((a) => a.edges).map((a) => readableType(a.name, g.source));
    return (
      <div className="source-detail-pane">
        <div className="detail-head">
          <div style={{ minWidth: 0 }}>
            <div className="detail-title">{g.name}</div>
            <div className="muted detail-sub">
              {empty
                ? "download only"
                : `${g.node_count} node · ${g.edge_count} edge types · ${
                    sourceSize(adapterNames(g), known) ? sizeTitle(g) : "size unknown"
                  }`}
            </div>
          </div>
          {!empty && (
            <label className="detail-include">
              <input type="checkbox" checked={included} onChange={() => setGroup(groupAdapterInfos(g), !included)} />
              include
            </label>
          )}
        </div>

        {empty ? (
          <span className="muted">feeds other adapters · no types of its own</span>
        ) : (
          <div className="schema-cols">
            <div className="schema-col">
              <div className="schema-label node">Nodes ({nodeTypes.length})</div>
              {nodeTypes.length ? (
                <div className="schema-grid">
                  {nodeTypes.map((t) => <span className="chip-tag node" key={t}>{t}</span>)}
                </div>
              ) : (
                <span className="muted">none</span>
              )}
            </div>
            <div className="schema-col">
              <div className="schema-label edge">Edges ({edgeTypes.length})</div>
              {edgeTypes.length ? (
                <div className="schema-grid">
                  {edgeTypes.map((t) => <span className="chip-tag edge" key={t}>{t}</span>)}
                </div>
              ) : (
                <span className="muted">none</span>
              )}
            </div>
          </div>
        )}

        <div className="detail-foot">
          {!isShared && (
            <span className={`src-badge ${g.present ? "ok" : "missing"}`}>
              {g.present
                ? `${g.location === "yours" ? "in your folder" : "downloaded"}${
                    g.bytes ? ` · ${formatBytes(g.bytes)}` : ""
                  }`
                : "not downloaded"}
            </span>
          )}
          {/* Sources found in your read-only folder are never re-downloaded. */}
          {g.downloadable && g.location !== "yours" && (
            <button type="button" onClick={() => onDownloadSource(g.source)} disabled={downloadBusy || !dataFolder}>
              {downloading ? "Downloading…" : g.present ? "Re-download" : "Download"}
            </button>
          )}
          {!g.downloadable && !isShared && !g.present && (
            <span className="field-hint">
              manual · place files at {g.local_paths.slice(0, 2).join(", ") || "the input folder"}
            </span>
          )}
          {g.website && (
            <a
              href={g.website.startsWith("http") ? g.website : `https://${g.website}`}
              target="_blank"
              rel="noreferrer"
              className="src-link"
            >
              website ↗
            </a>
          )}
        </div>
      </div>
    );
  };

  return (
    <>
      {missing.length > 0 && (
        <div className="alert warn setup-needed">
          set in Settings first: {missing.join(", ")} ·{" "}
          <Link className="link" to="/settings">
            settings
          </Link>
        </div>
      )}
      {showHelp && (
        <div className="card onboarding" style={{ borderLeft: "3px solid var(--accent)" }}>
          <div className="onboarding-head">
            <div>
              <h2 style={{ margin: 0 }}>Build your own knowledge graph</h2>
              <p className="onboarding-lead">source data → graph, in four steps</p>
            </div>
            <button className="linkbtn" onClick={dismissHelp}>dismiss</button>
          </div>
          <div className="onboarding-steps">
            <div className="ob-step">
              <span className="ob-num">1</span>
              <div className="ob-title">Pick a species &amp; dataset</div>
              <div className="ob-desc">
                <b>sample</b> bundled, offline · <b>full</b> real source data
              </div>
            </div>
            <div className="ob-step">
              <span className="ob-num">2</span>
              <div className="ob-title">Pick data sources</div>
              <div className="ob-desc">choose sources · see the types each adds</div>
            </div>
            <div className="ob-step">
              <span className="ob-num">3</span>
              <div className="ob-title">Get the data</div>
              <div className="ob-desc">download what you picked · dbSNP if needed</div>
            </div>
            <div className="ob-step">
              <span className="ob-num">4</span>
              <div className="ob-title">Build &amp; explore</div>
              <div className="ob-desc">
                validate · build · load into Neo4j or MORK · <b>explore</b>
              </div>
            </div>
          </div>
        </div>
      )}
      <div className="card">
        <h2><span className="step">1</span> Species &amp; Dataset</h2>
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
              <option value="all">all species (sequential)</option>
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

        {!isAll && selSpecies && (
          <div style={{ marginTop: 14 }}>
            {dataStatus && (
              <div className="field-hint" style={{ marginTop: 4 }}>
                on disk: {dataStatus.present_count}/{dataStatus.source_count} sources
                {dataStatus.total_bytes > 0 ? ` · ${formatBytes(dataStatus.total_bytes)}` : ""}
              </div>
            )}
            {selDataset !== "sample" && dataStatus && (
              <div className="field-hint" style={{ marginTop: 4 }}>
                {dataStatus.readonly_dir && <>yours (read-only): {dataStatus.readonly_dir} · </>}
                downloads → {dataStatus.input_dir ?? "set the console data folder"} ·{" "}
                <button type="button" className="linkbtn" onClick={() => navigate("/settings")}>
                  change
                </button>
              </div>
            )}
            <div className="field-hint" style={{ marginTop: 4 }}>
              {selDataset === "sample"
                ? "sample: bundled data · nothing to download"
                : "only the sources you select are downloaded"}
            </div>
          </div>
        )}
      </div>

      <div className="card">
        <h2>
          <span className="step">2</span> Data sources{" "}
          {isAll || !adapterGroups
            ? ""
            : `(${selectedGroupCount}/${selectableGroups.length} selected)`}
        </h2>
        {isAll ? (
          <div className="alert warn">
            all species: every adapter included · no per-source picking
          </div>
        ) : (
        <>
        <p className="field-hint" style={{ margin: "0 0 12px" }}>
          check to include · click for its node and edge types
        </p>
        <div className="row" style={{ marginBottom: 12 }}>
          <input
            type="text"
            value={adapterFilter}
            onChange={(e) => setAdapterFilter(e.target.value)}
            placeholder="filter sources"
            style={{ flex: 1, minWidth: 200 }}
          />
          <button onClick={() => setSelected(new Set(adapters.map((a) => a.name)))}>
            All sources
          </button>
          <button onClick={() => setSelected(new Set())}>Clear</button>
        </div>
        {!selDataset && (
          <div className="alert warn">
            no dataset with config files for this species
          </div>
        )}
        {adaptersError && (
          <div className="alert err">adapters failed to load: {adaptersError}</div>
        )}
        {adapterGroups ? (
          (() => {
            // Only sources that actually contribute nodes/edges belong in the build
            // picker; download-only sources with no adapter are not buildable.
            const visible = adapterGroups.filter((g) => g.adapter_count > 0 && groupMatches(g));
            if (!visible.length)
              return <span className="muted">no match for “{adapterFilter}”</span>;
            const active =
              visible.find((g) => g.source === activeSource) ?? visible[0];
            return (
              <div className="source-browser">
                <div className="source-list">{visible.map(renderSourceListItem)}</div>
                {renderSourceDetail(active)}
              </div>
            );
          })()
        ) : (
          <>
            {renderGroup("Nodes", "node", applyFilter(nodeAdapters))}
            {renderGroup("Edges", "edge", applyFilter(edgeAdapters))}
          </>
        )}
        {!adapters.length && selDataset && !adaptersError && (
          <span className="muted">loading sources…</span>
        )}
        </>
        )}

        {selDataset && selDataset !== "sample" && (
          <div className="readiness">
            {!isAll && adapterGroups && (
              <div className="ready-row">
                <div className="ready-label">Data</div>
                <div className="ready-body">
                  <div>
                    <b>{selectedData.length}</b> source{selectedData.length === 1 ? "" : "s"} selected ·{" "}
                    {onDisk.length} on disk
                    {inYours > 0 && ` (${inYours} in your folder)`}
                    {toDownload.length > 0 && (
                      <>
                        {" "}· <b>{toDownload.length}</b> to download
                      </>
                    )}
                  </div>
                  {byHand.length > 0 && (
                    <div className="field-hint">
                      manual: {byHand.map((g) => g.name).join(", ")} → place files at{" "}
                      {byHand.flatMap((g) => g.local_paths).slice(0, 3).join(", ")}
                    </div>
                  )}
                  {downloadId && (
                    <div className="field-hint">
                      Download {downloadStatus}{" "}
                      <button
                        type="button"
                        className="linkbtn"
                        onClick={() => navigate(`/builds/${downloadId}`)}
                      >
                        view log
                      </button>
                    </div>
                  )}
                </div>
                <div className="ready-actions">
                  {toDownload.length > 0 ? (
                    <button
                      className="primary"
                      onClick={() => onDownloadMissing(toDownload.map((g) => g.source))}
                      disabled={downloadBusy || !dataFolder}
                    >
                      {downloadBusy ? "Downloading…" : `Download ${toDownload.length} missing`}
                    </button>
                  ) : (
                    <span className="ready-ok">on disk</span>
                  )}
                  <button type="button" className="linkbtn" onClick={onDownload} disabled={downloadBusy || !dataFolder}>
                    download all sources
                  </button>
                </div>
              </div>
            )}

            {dbsnpNeeded && (
              <div className="ready-row">
                <div className="ready-label">dbSNP</div>
                <div className="ready-body">
                  <div>
                    for {dbsnpNeededBy.length ? dbsnpNeededBy.join(", ") : "variant sources"} · maps
                    variant IDs to positions
                  </div>
                  <div className="row" style={{ gap: 10, marginTop: 6 }}>
                    <select value={variantKey} onChange={(e) => setDbsnpVariant(e.target.value)}>
                      <option value="common">common (smaller)</option>
                      <option value="full">full</option>
                    </select>
                    {customRoot ? (
                      customReady ? (
                        <span className="ready-status ok">using {customRoot}/{variantKey}</span>
                      ) : (
                        <span className="ready-status warn">
                          {dbsnp?.custom?.variants[variantKey]?.problem ??
                            `no ${variantKey} cache at ${customRoot}`}
                        </span>
                      )
                    ) : managedReady && managedInfo ? (
                      <span className="ready-status ok">
                        ready{dbsnpFromYours ? " · yours, read-only" : ""} ·{" "}
                        {formatBytes(managedInfo.bytes)}
                        {managedInfo.built_at ? ` · built ${managedInfo.built_at.slice(0, 10)}` : ""}
                      </span>
                    ) : dbsnp?.job_id ? (
                      <span className="ready-status muted">
                        preparing {dbsnp.job_variant ?? ""} cache…
                      </span>
                    ) : (
                      <span className="ready-status warn">not prepared</span>
                    )}
                  </div>
                  {dbsnpOwnPath || customRoot ? (
                    <>
                      <label className="field" style={{ marginTop: 6 }}>
                        cache path
                        <input
                          type="text"
                          value={dbsnpCacheRoot}
                          onChange={(e) => setDbsnpCacheRoot(e.target.value)}
                          placeholder="/path/to/dbsnp (holds common/ or full/)"
                        />
                      </label>
                      <button
                        type="button"
                        className="linkbtn"
                        style={{ justifySelf: "start" }}
                        onClick={() => {
                          setDbsnpCacheRoot("");
                          setDbsnpOwnPath(false);
                        }}
                      >
                        use the console’s cache
                      </button>
                    </>
                  ) : (
                    <button
                      type="button"
                      className="linkbtn"
                      style={{ justifySelf: "start" }}
                      onClick={() => setDbsnpOwnPath(true)}
                    >
                      use my own cache
                    </button>
                  )}
                  <div className="field-hint">
                    prepare once · ~30 GB from NCBI → {dbsnp?.root ?? "the data folder (not set)"} ·
                    reused by every build
                  </div>
                </div>
                <div className="ready-actions">
                  {!customRoot && !managedReady && (
                    <button
                      className="primary"
                      onClick={onPrepareDbsnp}
                      disabled={dbsnpBusy || !!dbsnp?.job_id || !dataFolder}
                    >
                      {dbsnp?.job_id ? "Preparing…" : `Prepare ${variantKey} cache`}
                    </button>
                  )}
                  {dbsnp?.job_id && (
                    <button
                      type="button"
                      className="linkbtn"
                      onClick={() => navigate(`/builds/${dbsnp.job_id}`)}
                    >
                      view log
                    </button>
                  )}
                </div>
              </div>
            )}

            {dataError && <div className="alert err">{dataError}</div>}
          </div>
        )}
      </div>

      <div className="card">
        <h2><span className="step">3</span> Output format &amp; options</h2>
        <div className="row" style={{ marginBottom: 12 }}>
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
          <label className="field">
            Workers
            <select value={maxWorkers} onChange={(e) => setMaxWorkers(Number(e.target.value))}>
              {Array.from({ length: workerCap }, (_, i) => i + 1).map((n) => (
                <option key={n} value={n}>
                  {n === 1 ? "1 · sequential" : n === workerCap ? `${n} · max` : n}
                </option>
              ))}
            </select>
          </label>
          <div className="field">
            <span className="field-label">Size</span>
            <div className="size-control">
              <span className="group-actions">
                <button
                  type="button"
                  className={`linkbtn ${sizeUnit === "rows" ? "seg-on" : ""}`}
                  onClick={() => setSizeUnit("rows")}
                >
                  rows
                </button>
                <button
                  type="button"
                  className={`linkbtn ${sizeUnit === "percent" ? "seg-on" : ""}`}
                  onClick={() => setSizeUnit("percent")}
                  disabled={!percentReady}
                  title={percentReady ? "% of each source's full size" : percentWhyNot}
                >
                  percent
                </button>
              </span>
              {sizeUnit === "rows" ? (
                <select value={limit} onChange={(e) => setLimit(Number(e.target.value))}>
                  {ROW_CHOICES.map((n) => (
                    <option key={n} value={n}>
                      {n === 0 ? "full" : `${n.toLocaleString()} rows / adapter`}
                    </option>
                  ))}
                </select>
              ) : (
                <select value={percent} onChange={(e) => setPercent(Number(e.target.value))}>
                  {PERCENT_CHOICES.map((p) => (
                    <option key={p} value={p}>
                      {p}%
                    </option>
                  ))}
                </select>
              )}
            </div>
          </div>
        </div>
        {sizePreviewText && <div className="size-preview">{sizePreviewText}</div>}
        <dl className="kv">
          <dt>workers</dt>
          <dd>parallel adapters · more = faster, more memory</dd>
          <dt>size</dt>
          <dd>rows, or % of full size · keeps each adapter&apos;s first rows</dd>
          <dt>saved in</dt>
          <dd>
            {buildsFolder ? (
              <>
                {buildsFolder.replace(/\/$/, "")}/{writerType}/{selSpecies || "species"}-
                {selDataset || "dataset"}-&lt;time&gt; ·{" "}
                <Link className="link" to="/settings">
                  change
                </Link>
              </>
            ) : (
              <span className="warn-text">
                not set ·{" "}
                <Link className="link" to="/settings">
                  set the Builds folder
                </Link>
              </span>
            )}
          </dd>
        </dl>
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
        <h2><span className="step">4</span> Validate &amp; Build</h2>
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
            disabled={!canLaunch}
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
          valid · {result.resolved.num_adapters} adapter
          {result.resolved.num_adapters === 1 ? "" : "s"}
          {result.checked_paths ? " · input paths ok" : ""}
        </div>
      ) : (
        <div className="alert err">invalid</div>
      )}

      {result.static_errors.map((e, i) => (
        <div className="alert err" key={`e${i}`}>
          {e}
        </div>
      ))}
      {result.static_warnings.map((w, i) => (
        <div className="alert warn" key={`w${i}`}>
          {w}
        </div>
      ))}

      {Object.keys(result.missing_paths).length > 0 && (
        <div className="alert err">
          <strong>missing input files</strong>
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
        <summary className="muted">command</summary>
        <div className="mono" style={{ marginTop: 6 }}>
          {result.resolved.cmd_preview.join(" ")}
        </div>
      </details>
    </div>
  );
}

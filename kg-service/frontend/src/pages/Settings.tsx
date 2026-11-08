import { useEffect, useRef, useState } from "react";
import { api, ApiError } from "../api/client";
import { MOD, usePageHints, useStatus } from "../components/Hints";
import type {
  ConnInfo,
  ConsoleSettings,
  DataLocationsInfo,
  DbsnpVariantInfo,
  ExistingDatabase,
  ChosenFolder,
  FolderKind,
  InstanceInfo,
  InstanceAction,
  InstanceKind,
  InstancesStatus,
  SettingsPatch,
  SpeciesConnections,
  SpeciesDataLocation,
} from "../api/client";

/** Settings page: each species' databases, data locations and build settings.
 *
 * Databases are per species. Each species' Neo4j and MORK is either run by the
 * console (it creates the instance and owns its login) or one you already have (you
 * give its address and, for Neo4j, its login). "In use" shows what each species
 * reads from and loads into right now, checked live. */
export default function Settings() {
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [current, setCurrent] = useState<ConsoleSettings | null>(null);

  // Build settings (kept as strings; coerced on save).
  const [dataRoot, setDataRoot] = useState("");
  const [buildsRoot, setBuildsRoot] = useState("");
  const [memory, setMemory] = useState("");
  const [maxConc, setMaxConc] = useState("");
  const [maxOut, setMaxOut] = useState("");
  const [maxHist, setMaxHist] = useState("");

  // Data already on disk: per-species source folders and a dbSNP cache root. The
  // inputs hold what the user typed; `locations` is what the server resolved.
  const [locations, setLocations] = useState<DataLocationsInfo | null>(null);
  const [inputDirs, setInputDirs] = useState<Record<string, string>>({});
  const [dbsnpRoot, setDbsnpRoot] = useState("");

  // Instances the console runs through Docker ("Run here").
  const [inst, setInst] = useState<InstancesStatus | null>(null);
  const [instErr, setInstErr] = useState<string | null>(null);
  const [instTick, setInstTick] = useState(0);
  const instWorking = useRef(false);

  // What each species uses now, with live checks ("In use").
  const [conns, setConns] = useState<SpeciesConnections[] | null>(null);
  const [connsTick, setConnsTick] = useState(0);
  const [checking, setChecking] = useState(false);

  const [saving, setSaving] = useState(false);
  const status = useStatus();

  function hydrate(s: ConsoleSettings) {
    setCurrent(s);
    // Only what you chose; a blank field shows the default it falls back to.
    setDataRoot(s.build.data_root ?? "");
    setBuildsRoot(s.build.output_root ?? "");
    setMemory(s.build.neo4j_memory ?? "");
    setMaxConc(s.build.max_concurrent_builds?.toString() ?? "");
    setMaxOut(s.build.max_output_builds?.toString() ?? "");
    setMaxHist(s.build.max_build_history?.toString() ?? "");
  }

  // Instance status; polls while a setup runs. When one finishes (or a stop/remove
  // lands), what each species uses may have changed, so check connections again.
  useEffect(() => {
    let alive = true;
    let timer: number | undefined;
    const load = () =>
      api
        .instances()
        .then((s) => {
          if (!alive) return;
          setInst(s);
          const working = s.species.some((row) =>
            [row.neo4j, row.mork].some((i) => isWorking(i)),
          );
          if (instWorking.current && !working) setConnsTick((n) => n + 1);
          instWorking.current = working;
          if (working) timer = window.setTimeout(load, 2500);
        })
        .catch(() => {
          if (alive) setInst({ docker: false, species: [] });
        });
    load();
    return () => {
      alive = false;
      if (timer) window.clearTimeout(timer);
    };
  }, [instTick]);

  // What each species uses: show it at once, then fill in the live checks.
  useEffect(() => {
    let alive = true;
    setChecking(true);
    api
      .connections(false)
      .then((r) => alive && setConns(r.species))
      .catch(() => {});
    api
      .connections(true)
      .then((r) => alive && setConns(r.species))
      .catch(() => alive && setConns((c) => c ?? []))
      .finally(() => alive && setChecking(false));
    return () => {
      alive = false;
    };
  }, [connsTick]);

  async function onInstance(name: string, kind: InstanceKind, action: InstanceAction) {
    setInstErr(null);
    try {
      await api.instanceAction(name, kind, action);
    } catch (e) {
      setInstErr(errText(e));
    }
    // Mark as working so the next idle status re-checks connections, even when the
    // action finished before we saw it in progress.
    instWorking.current = true;
    setInstTick((n) => n + 1); // reload, and poll while it works
  }

  function mergeConn(row: SpeciesConnections) {
    setConns((cur) => (cur ?? []).map((c) => (c.species === row.species ? row : c)));
    setInstTick((n) => n + 1); // the "Run here" side shows whether it's still in use
  }

  /** Use a database you already run (checked by the server before it's saved). */
  async function onConnect(name: string, kind: InstanceKind, body: ExistingDatabase) {
    mergeConn(await api.connectExisting(name, kind, body));
  }

  async function onDisconnect(name: string, kind: InstanceKind) {
    mergeConn(await api.disconnect(name, kind));
  }

  function hydrateLocations(l: DataLocationsInfo) {
    setLocations(l);
    setInputDirs(Object.fromEntries(l.species.map((s) => [s.species, s.override ?? ""])));
    setDbsnpRoot(l.dbsnp.override ?? "");
  }

  useEffect(() => {
    Promise.all([api.getSettings(), api.dataLocations()])
      .then(([s, l]) => {
        hydrate(s);
        hydrateLocations(l);
      })
      .catch((e) => setLoadError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoading(false));
  }, []);

  const numOrNull = (v: string): number | null => {
    const t = v.trim();
    if (t === "") return null;
    const n = Number(t);
    return Number.isFinite(n) ? n : null;
  };
  const strOrNull = (v: string): string | null => (v.trim() === "" ? null : v.trim());

  async function onSave() {
    setSaving(true);
    status(null);
    try {
      // Databases aren't part of this save: each species' cell saves its own.
      const patch: SettingsPatch = {
        build: {
          data_root: strOrNull(dataRoot),
          output_root: strOrNull(buildsRoot),
          neo4j_memory: memory || null,
          max_concurrent_builds: numOrNull(maxConc),
          max_output_builds: numOrNull(maxOut),
          max_build_history: numOrNull(maxHist),
        },
        // Blank folder fields clear the override, so the default applies again.
        data: {
          input_dirs: Object.fromEntries(
            Object.entries(inputDirs).map(([name, path]) => [name, strOrNull(path)]),
          ),
          dbsnp_root: strOrNull(dbsnpRoot),
        },
      };
      const updated = await api.saveSettings(patch);
      hydrate(updated);
      // Re-resolve: the saved folders change where each species' data comes from.
      hydrateLocations(await api.dataLocations());
      status({ text: "saved", tone: "ok" });
    } catch (e) {
      status({ text: errText(e), tone: "err" }, 15000);
    } finally {
      setSaving(false);
    }
  }

  /** Put the fields back to what's saved. */
  function onReset() {
    if (current) hydrate(current);
    if (locations) hydrateLocations(locations);
    status({ text: "reverted" });
  }

  // Save and Undo live in the bar along the bottom; ⌘S / Ctrl+S saves.
  usePageHints([
    { keys: [MOD, "S"], label: saving ? "Saving…" : "Save", onClick: onSave, disabled: saving || loading },
    { keys: [], label: "Undo", onClick: onReset, disabled: saving || loading },
  ]);
  const saveRef = useRef(onSave);
  saveRef.current = onSave;
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "s") {
        e.preventDefault();
        saveRef.current();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  if (loading) return <div className="muted">Loading settings…</div>;
  if (loadError) return <div className="card danger">settings failed to load: {loadError}</div>;

  const instBy = new Map((inst?.species ?? []).map((row) => [row.species, row]));
  const connBy = new Map((conns ?? []).map((row) => [row.species, row]));
  const docker = !!inst?.docker;
  // The config's species order (hsa first).
  const rowNames = (conns ?? []).length
    ? (conns ?? []).map((row) => row.species)
    : (inst?.species ?? []).map((row) => row.species);

  return (
    <div style={{ display: "grid", gap: 16, maxWidth: 860, margin: "0 auto", width: "100%" }}>
      <div>
        <h1>Settings</h1>
        <div className="muted mono">databases · folders · builds · applied on save</div>
      </div>

      {/* ── In use: what each species reads from and loads into right now ── */}
      <div className="card">
        <div className="row" style={{ alignItems: "center", gap: 10 }}>
          <h2 style={{ margin: 0 }}>In use</h2>
          <span style={{ flex: 1 }} />
          <button
            type="button"
            className="linkbtn"
            onClick={() => setConnsTick((n) => n + 1)}
            disabled={checking}
          >
            {checking ? "checking…" : "check again"}
          </button>
        </div>
        <div className="muted mono" style={{ margin: "6px 0 12px" }}>
          what each species explores and loads into
        </div>
        {!conns ? (
          <div className="muted">Loading…</div>
        ) : (
          <div className="inst-table">
            <div className="inst-row inst-head">
              <span>Species</span>
              <span>Neo4j</span>
              <span>MORK</span>
            </div>
            {conns.map((row) => (
              <div className="inst-row" key={row.species}>
                <span className="inst-species">{row.species}</span>
                <InUseCell species={row.species} kind="neo4j" conn={row.neo4j} checking={checking} />
                <InUseCell species={row.species} kind="mork" conn={row.mork} checking={checking} />
              </div>
            ))}
          </div>
        )}
      </div>

      {/* ── Species databases: run one here, or connect one you have ── */}
      <div className="card">
        <h2>Species databases</h2>
        <div className="muted mono" style={{ marginBottom: 12 }}>
          <b>run here</b> console creates it · <b>use existing</b> your own, your login
        </div>
        {!inst || !conns ? (
          <div className="muted">Loading…</div>
        ) : rowNames.length === 0 ? (
          <div className="muted">no species in the config</div>
        ) : (
          <div className="inst-table">
            <div className="inst-row inst-head">
              <span>Species</span>
              <span>Neo4j</span>
              <span>MORK</span>
            </div>
            {rowNames.map((name) => (
              <div className="inst-row" key={name}>
                <span className="inst-species">{name}</span>
                {(["neo4j", "mork"] as const).map((kind) => (
                  <DbCell
                    key={kind}
                    kind={kind}
                    info={instBy.get(name)?.[kind]}
                    conn={connBy.get(name)?.[kind]}
                    species={name}
                    docker={docker}
                    blocked={kind === "neo4j" && !current?.build.neo4j_memory ? "set Neo4j memory below first" : null}
                    onAction={(a) => onInstance(name, kind, a)}
                    onConnect={(body) => onConnect(name, kind, body)}
                    onDisconnect={() => onDisconnect(name, kind)}
                  />
                ))}
              </div>
            ))}
          </div>
        )}
        {inst && !docker && (
          <div className="field-hint warn" style={{ marginTop: 8 }}>
            can't run databases here · use existing
          </div>
        )}
        {instErr && (
          <div className="field-hint warn" style={{ marginTop: 8 }}>
            {instErr}
          </div>
        )}
      </div>

      {/* ── Build settings: one row per setting ── */}
      <div className="card">
        <h2>Build settings</h2>
        <div className="set-list">
          <FolderField
            label="Builds folder"
            value={buildsRoot}
            onChange={setBuildsRoot}
            placeholder="required"
            kind="builds"
            hint={
              strOrNull(buildsRoot) ? (
                <span className="field-hint">
                  → {strOrNull(buildsRoot)!.replace(/\/$/, "")}/&lt;writer&gt;/&lt;species&gt;-&lt;dataset&gt;-&lt;time&gt;
                  · keeps the newest, removes older
                </span>
              ) : (
                <span className="field-hint warn">required before any build</span>
              )
            }
          />
          <FolderField
            label="Console data folder"
            value={dataRoot}
            onChange={setDataRoot}
            placeholder="required"
            kind="writable"
            hint={
              <span className={`field-hint${strOrNull(dataRoot) ? "" : " warn"}`}>
                {strOrNull(dataRoot) ? "" : "required · "}downloads · dbSNP cache · load history
              </span>
            }
          />
          <label className="field">
            Neo4j memory
            <select value={memory} onChange={(e) => setMemory(e.target.value)}>
              <option value="" disabled>
                choose…
              </option>
              {["1G", "2G", "4G", "8G", "16G"].map((m) => (
                <option key={m} value={m}>
                  {m.replace("G", " GB")}
                </option>
              ))}
            </select>
            <span className={`field-hint${memory ? "" : " warn"}`}>
              {memory ? "" : "required to run Neo4j here · "}per console-run Neo4j · change restarts
              them, data kept
            </span>
          </label>
          <label className="field">
            Builds at the same time
            <NumberStepper value={maxConc} onChange={setMaxConc} min={1} />
          </label>
          <label className="field">
            Finished builds kept
            <NumberStepper value={maxOut} onChange={setMaxOut} min={1} />
          </label>
          <label className="field">
            Builds listed in History
            <NumberStepper value={maxHist} onChange={setMaxHist} min={1} />
          </label>
        </div>
      </div>

      {/* ── Data locations ── */}
      {locations && (
        <div className="card">
          <h2>Data locations</h2>
          <div className="muted mono" style={{ marginBottom: 12 }}>
            your own copies · read-only · missing data is downloaded · blank = none
          </div>
          <div className="set-list">
            {locations.species.map((loc) => (
              <FolderField
                key={loc.species}
                kind="source"
                species={loc.species}
                label={`${loc.species} source data`}
                value={inputDirs[loc.species] ?? ""}
                onChange={(v) => setInputDirs((d) => ({ ...d, [loc.species]: v }))}
                placeholder="none"
                hint={<LocationHint loc={loc} typed={inputDirs[loc.species] ?? ""} />}
              />
            ))}
            <FolderField
              label="dbSNP cache"
              value={dbsnpRoot}
              onChange={setDbsnpRoot}
              placeholder="none"
              kind="dbsnp"
              hint={<DbsnpHint info={locations.dbsnp} typed={dbsnpRoot} />}
            />
          </div>
        </div>
      )}
    </div>
  );
}

/** A themed numeric input with −/+ buttons, replacing the browser's native spinner. */
function NumberStepper({
  value,
  onChange,
  min = 1,
}: {
  value: string;
  onChange: (v: string) => void;
  min?: number;
}) {
  const num = value.trim() === "" ? null : Number(value);
  const step = (delta: number) => {
    // Empty lands on min for either button; otherwise clamp at min.
    const cur = num !== null && Number.isFinite(num) ? num : min - delta;
    onChange(String(Math.max(min, cur + delta)));
  };
  return (
    <div className="stepper">
      <button type="button" className="stepper-btn" onClick={() => step(-1)} aria-label="Decrease">
        −
      </button>
      <input
        type="number"
        min={min}
        value={value}
        onChange={(e) => onChange(e.target.value)}
      />
      <button type="button" className="stepper-btn" onClick={() => step(1)} aria-label="Increase">
        +
      </button>
    </div>
  );
}

const WORKING = new Set(["pulling", "creating", "starting"]);

function isWorking(info: InstanceInfo): boolean {
  return !!info.progress && WORKING.has(info.progress.state);
}

/** One species' Neo4j or MORK in "In use": where it comes from, its address, how
 * the console logs in, and whether that works right now. */
function InUseCell({
  species,
  kind,
  conn,
  checking,
}: {
  species: string;
  kind: InstanceKind;
  conn: ConnInfo;
  checking: boolean;
}) {
  const [shown, setShown] = useState<string | null>(null); // a console-run login, on request
  const [err, setErr] = useState<string | null>(null);
  useEffect(() => setShown(null), [conn.address]);

  if (!conn.source) {
    return (
      <div className="inst-cell">
        <div className="inst-line">
          <span className="src-dot missing" />
          <span className="inst-state muted">none</span>
        </div>
      </div>
    );
  }
  const checked = conn.ok !== undefined;
  const dot = !checked ? "missing" : conn.ok ? "ok" : "bad";
  const state = !checked ? (checking ? "checking…" : "") : conn.ok ? "working" : "not working";

  async function reveal() {
    setErr(null);
    try {
      const login = await api.consoleLogin(species);
      setShown(login.password);
    } catch (e) {
      setErr(errText(e));
    }
  }

  return (
    <div className="inst-cell">
      <div className="inst-line">
        <span className={`src-dot ${dot}`} />
        <span className={`use-source ${conn.source ?? ""}`}>
          {conn.source === "console" ? "Run here" : "Your own"}
        </span>
        <span className="inst-state">{state}</span>
      </div>
      <div className="use-detail">
        {conn.address ? (
          <code className="use-addr">{conn.address}</code>
        ) : (
          <span>console-only</span>
        )}
        {kind === "neo4j" ? (
          <span>
            login <b>{conn.user}</b>
            {" · "}
            {conn.source === "console" ? (
              shown !== null ? (
                <>
                  password <code className="use-secret">{shown}</code>{" "}
                  <button type="button" className="linkbtn" onClick={() => setShown(null)}>
                    hide
                  </button>
                </>
              ) : (
                <>
                  password ••••{" "}
                  <button
                    type="button"
                    className="linkbtn"
                    title="for Neo4j Browser"
                    onClick={reveal}
                  >
                    show
                  </button>
                </>
              )
            ) : conn.password_set ? (
              "password saved"
            ) : (
              "no password"
            )}
            {conn.database && conn.database !== "neo4j" && <> · database <b>{conn.database}</b></>}
          </span>
        ) : (
          <span>no login</span>
        )}
      </div>
      {conn.unencrypted && <div className="field-hint warn">{UNENCRYPTED[kind]}</div>}
      {checked && !conn.ok && conn.error && <div className="field-hint warn">{conn.error}</div>}
      {err && <div className="field-hint warn">{err}</div>}
    </div>
  );
}

type DbMode = "here" | "own";

const UNENCRYPTED: Record<InstanceKind, string> = {
  neo4j: "not encrypted · use neo4j+s:// or bolt+s:// if the server supports it",
  mork: "not encrypted · use https:// if the server supports it",
};

/** Same rule as the server: another computer, and an address that doesn't encrypt. */
function isUnencrypted(kind: InstanceKind, address: string): boolean {
  const a = address.trim();
  if (!a) return false;
  const withScheme = a.includes("://") ? a : `${kind === "neo4j" ? "bolt" : "http"}://${a}`;
  let url: URL;
  try {
    url = new URL(withScheme.replace(/^(bolt|neo4j)(\+s|\+ssc)?:/, "http:"));
  } catch {
    return false;
  }
  const scheme = withScheme.split("://")[0].toLowerCase();
  const host = url.hostname.replace(/^\[|\]$/g, "").toLowerCase();
  const local = ["localhost", "127.0.0.1", "::1", "host.docker.internal"].includes(host) || host.endsWith(".localhost");
  return !local && !["bolt+s", "bolt+ssc", "neo4j+s", "neo4j+ssc", "https"].includes(scheme);
}

/** One species' Neo4j or MORK in "Species databases". The switch picks how the
 * species gets it: "Run here" (the console runs it) or "Use existing" (a database
 * you run). The switch only changes the view; nothing changes until you set up or
 * connect. */
function DbCell({
  kind,
  info,
  conn,
  species,
  docker,
  blocked,
  onAction,
  onConnect,
  onDisconnect,
}: {
  kind: InstanceKind;
  info?: InstanceInfo;
  conn?: ConnInfo;
  species: string;
  docker: boolean;
  blocked?: string | null; // why Run here can't set up or start it yet
  onAction: (action: InstanceAction) => void;
  onConnect: (body: ExistingDatabase) => Promise<void>;
  onDisconnect: () => Promise<void>;
}) {
  const usingOwn = conn?.source === "existing";
  const initial: DbMode = usingOwn || !docker || !info ? "own" : "here";
  const [mode, setMode] = useState<DbMode>(initial);
  useEffect(() => setMode(initial), [initial]);

  return (
    <div className="inst-cell">
      {docker && info && (
        <span className="group-actions inst-mode">
          <button
            type="button"
            className={`linkbtn ${mode === "here" ? "seg-on" : ""}`}
            onClick={() => setMode("here")}
          >
            Run here
          </button>
          <button
            type="button"
            className={`linkbtn ${mode === "own" ? "seg-on" : ""}`}
            onClick={() => setMode("own")}
          >
            Use existing
          </button>
        </span>
      )}
      {mode === "here" && info ? (
        <RunHere info={info} what={`${species}’s ${LABEL[kind]}`} blocked={blocked} onAction={onAction} />
      ) : (
        <UseExisting
          kind={kind}
          own={usingOwn ? conn : undefined}
          info={info}
          onAction={onAction}
          onConnect={onConnect}
          onDisconnect={onDisconnect}
        />
      )}
    </div>
  );
}

const LABEL: Record<InstanceKind, string> = { neo4j: "Neo4j", mork: "MORK" };

const ON_THIS_COMPUTER = ["localhost", "127.0.0.1", "[::1]", "::1"].includes(window.location.hostname);

/** The instance the console runs: its state, and set up / start / stop / remove. */
function RunHere({
  info,
  what,
  blocked,
  onAction,
}: {
  info: InstanceInfo;
  what: string; // e.g. "hsa’s Neo4j"
  blocked?: string | null;
  onAction: (action: InstanceAction) => void;
}) {
  const working = isWorking(info);
  const failed = info.progress?.state === "error";
  return (
    <>
      <div className="inst-line">
        <span className={`src-dot ${info.running ? "ok" : "missing"}`} />
        <span className="inst-state">
          {working
            ? info.progress!.message
            : info.running
              ? info.connected
                ? "running · in use"
                : "running · not in use"
              : info.exists
                ? "stopped"
                : "not set up"}
        </span>
      </div>
      {!working && (
        // One action row, same order everywhere: the main step, open, stop, remove.
        <div className="inst-actions">
          {info.running && !info.connected && (
            // Running, but this species uses another database (or none) yet.
            <button type="button" className="inst-btn" onClick={() => onAction("setup")}>
              Use this one
            </button>
          )}
          {!info.running && (
            <button
              type="button"
              className="inst-btn"
              disabled={!!blocked}
              title={blocked ?? undefined}
              onClick={() => onAction("setup")}
            >
              {info.exists ? "Start" : "Set up"}
            </button>
          )}
          {/* Its ports are on the console's computer only, so the link works there. */}
          {info.running && info.name.includes("neo4j") && ON_THIS_COMPUTER && (
            <a className="src-link" href={info.open_url} target="_blank" rel="noreferrer">
              browser ↗
            </a>
          )}
          {info.running && (
            <button
              type="button"
              className="linkbtn"
              title="start restores it"
              onClick={() => onAction("stop")}
            >
              stop
            </button>
          )}
          {info.exists && (
            <button
              type="button"
              className="linkbtn danger"
              title="data and password kept"
              onClick={() => {
                if (
                  window.confirm(
                    `Remove ${what}? Its data and password are kept.`,
                  )
                )
                  onAction("remove");
              }}
            >
              remove
            </button>
          )}
        </div>
      )}
      {failed && <div className="field-hint warn">{info.progress!.message}</div>}
      {blocked && !info.running && !working && <div className="field-hint warn">{blocked}</div>}
    </>
  );
}

/** A database you already run, on this machine or a server: its address and, for
 * Neo4j, its own login. The server checks them before saving anything. */
function UseExisting({
  kind,
  own,
  info,
  onAction,
  onConnect,
  onDisconnect,
}: {
  kind: InstanceKind;
  own?: ConnInfo; // this species' own database, if it uses one
  info?: InstanceInfo;
  onAction: (action: InstanceAction) => void;
  onConnect: (body: ExistingDatabase) => Promise<void>;
  onDisconnect: () => Promise<void>;
}) {
  const neo4j = kind === "neo4j";
  const savedAddress = own?.address ?? "";
  const savedUser = own?.user ?? "";
  const savedDb = own?.database ?? "";
  const [address, setAddress] = useState(savedAddress);
  const [user, setUser] = useState(savedUser);
  const [password, setPassword] = useState("");
  const [database, setDatabase] = useState(savedDb);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    setAddress(savedAddress);
    setUser(savedUser);
    setDatabase(savedDb);
    setPassword("");
  }, [savedAddress, savedUser, savedDb]);

  const addr = address.trim();
  // A blank password keeps the saved one, for the same address only.
  const keepsPassword = !!own?.password_set && addr === savedAddress;
  const changed =
    !savedAddress ||
    addr !== savedAddress ||
    (neo4j && (user.trim() !== savedUser || database.trim() !== savedDb || password !== ""));
  const ready =
    !!addr && changed &&
    (!neo4j || (!!user.trim() && !!database.trim() && (!!password || keepsPassword)));

  async function connect() {
    setBusy(true);
    setError(null);
    try {
      await onConnect(
        neo4j ? { address: addr, user: user.trim(), password, database: database.trim() } : { address: addr },
      );
      setPassword("");
    } catch (e) {
      setError(errText(e));
    } finally {
      setBusy(false);
    }
  }

  async function disconnect() {
    setBusy(true);
    setError(null);
    try {
      await onDisconnect();
    } catch (e) {
      setError(errText(e));
    } finally {
      setBusy(false);
    }
  }

  const state = busy
    ? "checking…"
    : savedAddress
      ? own?.ok === false
        ? "not working"
        : "in use"
      : info?.connected
        ? "not in use · run-here one is"
        : "not connected";
  const dot = busy || !savedAddress ? "missing" : own?.ok === false ? "bad" : own?.ok ? "ok" : "missing";

  return (
    <>
      <div className="inst-line">
        <span className={`src-dot ${dot}`} />
        <span className="inst-state">{state}</span>
      </div>
      <form
        className="inst-form"
        onSubmit={(e) => {
          e.preventDefault();
          if (ready && !busy) connect();
        }}
      >
        <label className="mini-field">
          <span>Address</span>
          <input
            type="text"
            value={address}
            onChange={(e) => setAddress(e.target.value)}
            placeholder={neo4j ? "localhost:7687" : "localhost:8027"}
          />
        </label>
        {isUnencrypted(kind, address) && <span className="field-hint warn">{UNENCRYPTED[kind]}</span>}
        {neo4j ? (
          <div className="inst-login">
            <label className="mini-field">
              <span>Username</span>
              <input type="text" value={user} onChange={(e) => setUser(e.target.value)} placeholder="neo4j" autoComplete="off" />
            </label>
            <label className="mini-field">
              <span>Password</span>
              <input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder={keepsPassword ? "saved" : ""}
                autoComplete="new-password"
              />
            </label>
            <label className="mini-field">
              <span>Database</span>
              <input type="text" value={database} onChange={(e) => setDatabase(e.target.value)} placeholder="neo4j" />
            </label>
          </div>
        ) : (
          <span className="field-hint">no login · address only</span>
        )}
        <div className="inst-actions">
          <button type="submit" className="inst-btn" disabled={!ready || busy}>
            {busy ? "Checking…" : savedAddress ? "Save" : "Connect"}
          </button>
          {savedAddress && (
            <button
              type="button"
              className="linkbtn"
              disabled={busy}
              title="stop using it · the database is untouched"
              onClick={disconnect}
            >
              disconnect
            </button>
          )}
        </div>
      </form>
      {savedAddress && info?.running && (
        // The console's own instance keeps running after you switch away from it.
        <div className="inst-actions inst-note">
          <span>run-here one still running, unused</span>
          <button type="button" className="linkbtn" onClick={() => onAction("stop")}>
            stop
          </button>
        </div>
      )}
      {error && <div className="field-hint warn">{error}</div>}
    </>
  );
}

function errText(e: unknown): string {
  return e instanceof ApiError ? String(e.detail) : e instanceof Error ? e.message : String(e);
}

/** A folder path you can type, or choose in the computer's own Finder window.
 * Browsers never tell a page where a chosen folder is (only its name and what's in
 * it), so the console then finds that folder among the ones it can open. */
function FolderField({
  label,
  value,
  onChange,
  placeholder,
  hint,
  kind,
  species,
}: {
  label: string;
  value: string;
  onChange: (path: string) => void;
  placeholder: string;
  hint: React.ReactNode;
  kind?: FolderKind;
  species?: string; // kind "source": whose data
}) {
  const [finding, setFinding] = useState<string | null>(null); // the chosen folder's name
  const [error, setError] = useState<string | null>(null);
  const [choices, setChoices] = useState<string[] | null>(null); // identical copies

  async function browse() {
    setError(null);
    setChoices(null);
    let chosen: ChosenFolder | null;
    try {
      chosen = await chooseInFinder(kind);
    } catch (e) {
      setError(errText(e));
      return;
    }
    if (!chosen) return; // cancelled
    setFinding(chosen.name);
    try {
      const { matches } = await api.locateFolder({ ...chosen, kind, species });
      if (matches.length === 1) onChange(matches[0]);
      else setChoices(matches);
    } catch (e) {
      setError(errText(e));
    } finally {
      setFinding(null);
    }
  }

  return (
    <div className="field">
      <span className="field-label">{label}</span>
      <div className="folder-row">
        <input
          type="text"
          value={value}
          onChange={(e) => onChange(e.target.value)}
          placeholder={placeholder}
          aria-label={label}
        />
        <button type="button" onClick={browse} disabled={!!finding}>
          {finding ? "Finding…" : "Browse…"}
        </button>
      </div>
      {finding && <span className="field-hint">locating “{finding}”…</span>}
      {choices && (
        <div className="folder-choices">
          <span className="field-hint">several copies · pick one</span>
          {choices.map((path) => (
            <button
              type="button"
              key={path}
              className="linkbtn"
              onClick={() => {
                onChange(path);
                setChoices(null);
              }}
            >
              {path}
            </button>
          ))}
        </div>
      )}
      {error && <span className="field-hint warn">{error}</span>}
      {hint}
    </div>
  );
}

type DirHandle = {
  name: string;
  entries(): AsyncIterable<[string, { kind: "file" | "directory"; getFile?: () => Promise<File> }]>;
};
type DirPicker = (options?: { id?: string; mode?: "read"; startIn?: string }) => Promise<DirHandle>;

const SAMPLE = 200; // entries described to the server; enough to tell folders apart

/** Open the computer's own Finder window to choose a folder. Returns its name and
 * what's in it (no path: browsers don't give one), or null if cancelled. */
async function chooseInFinder(kind?: FolderKind): Promise<ChosenFolder | null> {
  const picker = (window as unknown as { showDirectoryPicker?: DirPicker }).showDirectoryPicker;
  if (picker) {
    let dir: DirHandle;
    try {
      // id: each kind of field reopens where you last chose a folder for it.
      dir = await picker({ id: `kg-console-${kind ?? "data"}`, mode: "read", startIn: "documents" });
    } catch (e) {
      if (e instanceof DOMException && e.name === "AbortError") return null;
      throw e;
    }
    const entries: ChosenFolder["entries"] = [];
    let count = 0;
    for await (const [name, handle] of dir.entries()) {
      count += 1;
      if (entries.length >= SAMPLE) continue;
      if (handle.kind === "directory") entries.push({ name, dir: true });
      else entries.push({ name, dir: false, size: (await handle.getFile!()).size });
    }
    return { name: dir.name, entries, count };
  }
  // Browsers without that window (Safari, Firefox): the folder upload picker, which
  // also opens Finder. Nothing is uploaded; only names and sizes are read.
  return new Promise((resolve) => {
    const input = document.createElement("input");
    input.type = "file";
    input.webkitdirectory = true;
    input.addEventListener("cancel", () => resolve(null));
    input.addEventListener("change", () => {
      const files = Array.from(input.files ?? []);
      if (!files.length) return resolve(null);
      const name = files[0].webkitRelativePath.split("/")[0];
      const seen = new Map<string, ChosenFolder["entries"][number]>();
      for (const f of files) {
        const parts = f.webkitRelativePath.split("/");
        if (parts.length === 2) seen.set(parts[1], { name: parts[1], dir: false, size: f.size });
        else if (parts.length > 2) seen.set(parts[1], { name: parts[1], dir: true });
      }
      // This picker skips empty folders, so the full count isn't known.
      resolve({ name, entries: Array.from(seen.values()).slice(0, SAMPLE), count: null });
    });
    input.click();
  });
}

/** Your read-only source folder for a species, and where downloads go. */
function LocationHint({ loc, typed }: { loc: SpeciesDataLocation; typed: string }) {
  if (typed.trim() !== (loc.override ?? "")) {
    return <span className="field-hint">unsaved</span>;
  }
  const downloads = loc.path ?? "set the console data folder";
  if (loc.readonly) {
    const check = loc.readonly_check;
    if (!loc.readonly_exists) {
      return <span className="field-hint warn">folder not found · choose again or clear</span>;
    }
    if (check?.problem) {
      return <span className="field-hint warn">{check.problem}</span>;
    }
    return (
      <span className="field-hint ok">
        {check?.found.length ?? 0}/{check?.total ?? 0} sources · read-only · missing → {downloads}
      </span>
    );
  }
  return <span className="field-hint">downloads → {downloads}</span>;
}

/** Whether a usable dbSNP cache is in your read-only folder or the console's own. */
function DbsnpHint({ info, typed }: { info: DataLocationsInfo["dbsnp"]; typed: string }) {
  if (typed.trim() !== (info.override ?? "")) {
    return <span className="field-hint">unsaved</span>;
  }
  const readyIn = (v: Record<"common" | "full", DbsnpVariantInfo> | null) =>
    v ? (["common", "full"] as const).filter((k) => v[k].present) : [];
  const yours = readyIn(info.readonly_variants);
  const console_ = readyIn(info.variants);
  if (info.override) {
    if (!yours.length && info.readonly_problem) {
      return <span className="field-hint warn">{info.readonly_problem}</span>;
    }
    return yours.length ? (
      <span className="field-hint ok">
        {yours.join(" + ")} · read-only · other variants → {info.path ?? "data folder (not set)"}
      </span>
    ) : (
      <span className="field-hint warn">
        no cache here · expects common/ or full/ with dbsnp_mapping.db
      </span>
    );
  }
  const broken = (["common", "full"] as const).find((k) => info.variants[k].problem);
  if (!console_.length && broken) {
    return (
      <span className="field-hint warn">
        {broken} cache unusable: {info.variants[broken].problem} · prepare again in New Build
      </span>
    );
  }
  return console_.length ? (
    <span className="field-hint ok">
      {console_.join(" + ")} · {info.path ?? "data folder (not set)"}
    </span>
  ) : (
    <span className="field-hint">
      none yet · prepared from New Build when needed
    </span>
  );
}

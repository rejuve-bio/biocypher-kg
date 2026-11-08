import { lazy, Suspense, useEffect, useState } from "react";
import { NavLink, Route, Routes, useLocation, useNavigate } from "react-router-dom";
import BuildWizard from "./pages/BuildWizard";
import BuildHistory from "./pages/BuildHistory";
import BuildDetail from "./pages/BuildDetail";
import { HintsProvider, typing } from "./components/Hints";
import { api, ApiError, SIGN_IN_EVENT } from "./api/client";

// Lazy so their heavy libraries (CodeMirror, Cytoscape) ship in their own chunks.
const ConfigEditor = lazy(() => import("./pages/ConfigEditor"));
const ExploreGraph = lazy(() => import("./pages/ExploreGraph"));
const Settings = lazy(() => import("./pages/Settings"));

type Theme = "dark" | "light";

// The sections, in tab order (Q and E step through them).
const SECTIONS = [
  { to: "/", label: "New Build", end: true },
  { to: "/history", label: "History" },
  { to: "/explore", label: "Explore" },
  { to: "/config", label: "Config" },
  { to: "/settings", label: "Settings" },
];

function sectionIndex(pathname: string): number {
  if (pathname.startsWith("/builds/")) return 1; // a build belongs to History
  const i = SECTIONS.findIndex((s) => (s.end ? pathname === s.to : pathname.startsWith(s.to)));
  return i < 0 ? 0 : i;
}

type Auth = { required: boolean; signed_in: boolean };

export default function App() {
  // On a server the console asks for a password first; on your own computer it doesn't.
  const [auth, setAuth] = useState<Auth | null>(null);
  useEffect(() => {
    api.session().then(setAuth).catch(() => setAuth({ required: false, signed_in: true }));
    const signedOut = () => setAuth((a) => (a ? { ...a, signed_in: false } : a));
    window.addEventListener(SIGN_IN_EVENT, signedOut);
    return () => window.removeEventListener(SIGN_IN_EVENT, signedOut);
  }, []);
  const [theme, setTheme] = useState<Theme>(() => {
    try {
      return (localStorage.getItem("kg-theme") as Theme) || "dark";
    } catch {
      return "dark";
    }
  });
  const location = useLocation();
  const navigate = useNavigate();

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    try {
      localStorage.setItem("kg-theme", theme);
    } catch {
      /* private mode: the theme just isn't remembered */
    }
  }, [theme]);

  const current = sectionIndex(location.pathname);

  // On narrow screens the tabs scroll sideways: keep the current one in view.
  useEffect(() => {
    const center = () => {
      const tab = document.querySelector<HTMLElement>(".tabs a.active");
      const strip = tab?.parentElement;
      if (tab && strip && strip.scrollWidth > strip.clientWidth) {
        strip.scrollLeft = tab.offsetLeft - (strip.clientWidth - tab.offsetWidth) / 2;
      }
    };
    // Now, and again once the page has settled (layout can still shift on load).
    const ids = [0, 300].map((ms) => window.setTimeout(center, ms));
    return () => ids.forEach((id) => window.clearTimeout(id));
  }, [location.pathname]);
  const step = (by: number) =>
    navigate(SECTIONS[(current + by + SECTIONS.length) % SECTIONS.length].to);

  // Q / E: previous / next section (not while typing in a field).
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.metaKey || e.ctrlKey || e.altKey || typing(e)) return;
      const k = e.key.toLowerCase();
      if (k === "q") step(-1);
      else if (k === "e") step(1);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  });

  if (auth === null) return <div className="signin" />;
  if (auth.required && !auth.signed_in) {
    return <SignIn onSignedIn={() => setAuth({ required: true, signed_in: true })} />;
  }

  return (
    <HintsProvider global={[{ keys: ["Q", "E"], label: "Switch section" }]}>
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <span className="brand-mark" aria-hidden="true">kg</span>
          <span className="brand-text">BioCypher KG Console</span>
        </div>
        <nav className="tabs" aria-label="Sections">
          <button type="button" className="keycap" title="Previous section (Q)" onClick={() => step(-1)}>
            Q
          </button>
          {SECTIONS.map((s) => (
            <NavLink key={s.to} to={s.to} end={s.end}>
              {s.label}
            </NavLink>
          ))}
          <button type="button" className="keycap" title="Next section (E)" onClick={() => step(1)}>
            E
          </button>
        </nav>
        <div className="topbar-end">
          <button
            className="theme-toggle"
            title={`${theme === "dark" ? "light" : "dark"} mode`}
            aria-label={`${theme === "dark" ? "light" : "dark"} mode`}
            onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
          >
            <span className="theme-glyph" />
          </button>
          {auth.required && (
            <button
              type="button"
              className="linkbtn"
              onClick={() => api.logout().finally(() => setAuth({ required: true, signed_in: false }))}
            >
              sign out
            </button>
          )}
        </div>
      </header>
      <main className="content">
        <Routes>
          <Route path="/" element={<BuildWizard />} />
          <Route path="/history" element={<BuildHistory />} />
          <Route
            path="/explore"
            element={
              <Suspense fallback={<div className="muted">Loading explorer…</div>}>
                <ExploreGraph />
              </Suspense>
            }
          />
          <Route
            path="/config"
            element={
              <Suspense fallback={<div className="muted">Loading editor…</div>}>
                <ConfigEditor />
              </Suspense>
            }
          />
          <Route
            path="/settings"
            element={
              <Suspense fallback={<div className="muted">Loading settings…</div>}>
                <Settings />
              </Suspense>
            }
          />
          <Route path="/builds/:id" element={<BuildDetail />} />
        </Routes>
      </main>
    </div>
    </HintsProvider>
  );
}

/** The password screen, when the console is on a server (up.sh --host). */
function SignIn({ onSignedIn }: { onSignedIn: () => void }) {
  const [password, setPassword] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  async function submit(e: React.FormEvent) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      await api.login(password);
      onSignedIn();
    } catch (err) {
      setError(err instanceof ApiError && err.status === 401 ? "wrong password" : String(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="signin">
      <form className="card signin-card" onSubmit={submit}>
        <div className="brand">
          <span className="brand-mark" aria-hidden="true">kg</span>
          <span className="brand-text">BioCypher KG Console</span>
        </div>
        <label className="field">
          Password
          <input
            type="password"
            autoFocus
            autoComplete="current-password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
          />
        </label>
        {error && <span className="field-hint warn">{error}</span>}
        <button type="submit" className="primary" disabled={!password || busy}>
          {busy ? "Signing in…" : "Sign in"}
        </button>
        <span className="field-hint">on the server: cat docker/.console-password</span>
      </form>
    </div>
  );
}

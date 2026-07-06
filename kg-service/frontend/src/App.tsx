import { useEffect, useState } from "react";
import { NavLink, Route, Routes } from "react-router-dom";
import BuildWizard from "./pages/BuildWizard";
import BuildHistory from "./pages/BuildHistory";
import BuildDetail from "./pages/BuildDetail";

type Theme = "dark" | "light";

export default function App() {
  const [theme, setTheme] = useState<Theme>(
    () => (localStorage.getItem("kg-theme") as Theme) || "dark",
  );

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem("kg-theme", theme);
  }, [theme]);

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <span className="brand-mark">🧬</span>
          <span className="brand-text">BioCypher KG Console</span>
        </div>
        <nav>
          <NavLink to="/" end>
            New Build
          </NavLink>
          <NavLink to="/history">History</NavLink>
          <button
            className="theme-toggle"
            title={`Switch to ${theme === "dark" ? "light" : "dark"} mode`}
            onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
          >
            {theme === "dark" ? "☀️" : "🌙"}
          </button>
        </nav>
      </header>
      <main className="content">
        <Routes>
          <Route path="/" element={<BuildWizard />} />
          <Route path="/history" element={<BuildHistory />} />
          <Route path="/builds/:id" element={<BuildDetail />} />
        </Routes>
      </main>
    </div>
  );
}

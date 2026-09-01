import { useState } from "react";
import { api, type PatternResult } from "../api/client";

const EXAMPLES: { label: string; pattern: string; template: string }[] = [
  { label: "All edges", pattern: "($rel ($st $sx) ($tt $tx))", template: "($rel $st $tt)" },
  { label: "Genes → transcripts", pattern: "(transcribes_to (gene $g) (transcript $t))", template: "($g $t)" },
  { label: "A gene's properties", pattern: "($p (gene $id) $v)", template: "($id $p $v)" },
];

export default function PatternPanel() {
  const [pattern, setPattern] = useState(EXAMPLES[0].pattern);
  const [template, setTemplate] = useState(EXAMPLES[0].template);
  const [limit, setLimit] = useState(200);
  const [result, setResult] = useState<PatternResult | null>(null);
  const [loading, setLoading] = useState(false);

  const run = () => {
    setLoading(true);
    setResult(null);
    api
      .morkQuery(pattern, template.trim() || null, limit)
      .then(setResult)
      .catch((e) => setResult({ reachable: false, error: e instanceof Error ? e.message : String(e) }))
      .finally(() => setLoading(false));
  };

  const columns = result?.parsed && result.parsed.every((r) => r.length > 1);

  return (
    <div>
      <div className="muted" style={{ fontSize: 12, lineHeight: 1.55, marginBottom: 10 }}>
        Run a read-only MeTTa pattern against the live atomspace. Use <span className="mono">$name</span> for
        variables in the pattern; the template projects each match. Read-only: this never modifies the space.
      </div>

      <div className="row" style={{ gap: 8, flexWrap: "wrap", marginBottom: 10 }}>
        {EXAMPLES.map((ex) => (
          <button
            key={ex.label}
            className="secondary btn-sm"
            onClick={() => {
              setPattern(ex.pattern);
              setTemplate(ex.template);
            }}
          >
            {ex.label}
          </button>
        ))}
      </div>

      <label className="muted" style={{ fontSize: 12 }}>Pattern</label>
      <input className="input mono" value={pattern} onChange={(e) => setPattern(e.target.value)} style={{ width: "100%", marginBottom: 8 }} />
      <label className="muted" style={{ fontSize: 12 }}>Template (optional, defaults to the pattern)</label>
      <input className="input mono" value={template} onChange={(e) => setTemplate(e.target.value)} style={{ width: "100%", marginBottom: 10 }} />

      <div className="row" style={{ gap: 10, alignItems: "center" }}>
        <button className="suggested btn-sm" onClick={run} disabled={loading}>
          {loading ? "Running…" : "Run"}
        </button>
        <label className="muted" style={{ fontSize: 12 }}>Limit</label>
        <input
          className="input mono"
          type="number"
          min={1}
          max={2000}
          value={limit}
          onChange={(e) => setLimit(Math.max(1, Math.min(2000, Number(e.target.value) || 1)))}
          style={{ width: 90 }}
        />
      </div>

      {result && (
        <div style={{ marginTop: 12 }}>
          {result.error ? (
            <div className="alert err">{result.error}</div>
          ) : (
            <>
              <div className="muted" style={{ fontSize: 12, marginBottom: 6 }}>
                {result.count ?? 0} rows{result.truncated ? " (capped at limit)" : ""}
              </div>
              <div className="query-results">
                {columns ? (
                  <table className="query-table">
                    <tbody>
                      {result.parsed!.map((row, i) => (
                        <tr key={i}>
                          {row.map((cell, j) => (
                            <td key={j} className="mono">{cell}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                ) : (
                  (result.rows || []).map((r, i) => (
                    <div key={i} className="mono" style={{ fontSize: 12, padding: "2px 0" }}>{r}</div>
                  ))
                )}
                {result.count === 0 && <div className="muted" style={{ padding: 8 }}>No matches.</div>}
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}

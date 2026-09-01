import { useEffect, useState } from "react";
import { api, type RawAtoms } from "../api/client";

type Node = string | Node[];

// Minimal S-expression parser: turns "(a (b c) d)" into nested arrays.
function parseSexpr(text: string): Node {
  let i = 0;
  function parse(): Node {
    skipWs();
    if (text[i] === "(") {
      i++; // consume (
      const list: Node[] = [];
      while (i < text.length && text[i] !== ")") {
        list.push(parse());
        skipWs();
      }
      i++; // consume )
      return list;
    }
    // atom token
    let start = i;
    while (i < text.length && !/[\s()]/.test(text[i])) i++;
    return text.slice(start, i);
  }
  function skipWs() {
    while (i < text.length && /\s/.test(text[i])) i++;
  }
  return parse();
}

function hasNested(node: Node): boolean {
  return Array.isArray(node) && node.some((c) => Array.isArray(c));
}

function Sexpr({ node, depth = 0 }: { node: Node; depth?: number }) {
  const [open, setOpen] = useState(depth < 1);
  if (!Array.isArray(node)) {
    return <span className="mono sx-atom">{node}</span>;
  }
  if (!hasNested(node)) {
    // flat list: render inline
    return (
      <span className="mono">
        (<span className="sx-head">{String(node[0] ?? "")}</span>
        {node.slice(1).map((c, i) => (
          <span key={i}> {String(c)}</span>
        ))}
        )
      </span>
    );
  }
  const head = String(node[0] ?? "");
  return (
    <div className="sx-node">
      <button className="sx-toggle mono" onClick={() => setOpen((o) => !o)}>
        <span className="sx-caret">{open ? "▾" : "▸"}</span>
        <span className="sx-head">{head}</span>
        {!open && <span className="muted"> …</span>}
      </button>
      {open && (
        <div className="sx-children">
          {node.slice(1).map((c, i) => (
            <div key={i} className="sx-child">
              <Sexpr node={c} depth={depth + 1} />
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export default function AtomTree() {
  const [limit, setLimit] = useState(100);
  const [data, setData] = useState<RawAtoms | null>(null);
  const [loading, setLoading] = useState(false);

  const load = (n: number) => {
    setLoading(true);
    api
      .morkAtoms(n)
      .then(setData)
      .catch((e) => setData({ reachable: false, error: e instanceof Error ? e.message : String(e) }))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    load(limit);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <div>
      <div className="row" style={{ justifyContent: "space-between", marginBottom: 8 }}>
        <span className="muted" style={{ fontSize: 12 }}>
          A live sample of raw MeTTa atoms. Nested expressions collapse; click to expand.
        </span>
        <button className="secondary btn-sm" disabled={loading} onClick={() => { const n = limit + 100; setLimit(n); load(n); }}>
          {loading ? "Loading…" : "Load more"}
        </button>
      </div>

      {data?.error && <div className="alert err">{data.error}</div>}

      <div className="atom-tree">
        {(data?.atoms || []).map((a, i) => (
          <div key={i} className="atom-row">
            <Sexpr node={parseSexpr(a)} />
          </div>
        ))}
        {data && (data.atoms?.length ?? 0) === 0 && !data.error && (
          <div className="muted" style={{ padding: 8 }}>No atoms.</div>
        )}
      </div>
      {data?.truncated && (
        <div className="muted" style={{ fontSize: 12, marginTop: 6 }}>Showing a sample; more atoms exist.</div>
      )}
    </div>
  );
}

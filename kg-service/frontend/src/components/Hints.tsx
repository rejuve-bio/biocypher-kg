import { createContext, useContext, useEffect, useRef, useState } from "react";
import type { ReactNode } from "react";

/** A key hint in the bar along the bottom: the keys, what they do, and (optionally)
 * the same action on click. */
export interface Hint {
  keys: string[];
  label: string;
  onClick?: () => void;
  disabled?: boolean;
}

export interface Status {
  text: string;
  tone?: "ok" | "err";
}

interface HintsApi {
  setPage: (hints: Hint[]) => void;
  setStatus: (status: Status | null) => void;
}

const Ctx = createContext<HintsApi>({ setPage: () => {}, setStatus: () => {} });

export const isMac = typeof navigator !== "undefined" && /Mac|iPhone|iPad/.test(navigator.platform);
export const MOD = isMac ? "⌘" : "Ctrl";

/** True while the person is typing (keys then belong to the field, not shortcuts). */
export function typing(e: KeyboardEvent): boolean {
  const t = e.target as HTMLElement | null;
  if (!t) return false;
  const tag = t.tagName;
  return tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT" || t.isContentEditable;
}

export function HintsProvider({
  global,
  children,
}: {
  global: Hint[];
  children: ReactNode;
}) {
  const [page, setPage] = useState<Hint[]>([]);
  const [status, setStatus] = useState<Status | null>(null);
  return (
    <Ctx.Provider value={{ setPage, setStatus }}>
      {children}
      <footer className="hintbar" aria-label="Keyboard shortcuts">
        {[...global, ...page].map((h) => (
          <HintButton key={`${h.keys.join("+")}:${h.label}`} hint={h} />
        ))}
        <span className="hint-sep" />
        {status && (
          <span className={`hint-status ${status.tone ?? ""}`} role="status">
            {status.text}
          </span>
        )}
      </footer>
    </Ctx.Provider>
  );
}

function HintButton({ hint }: { hint: Hint }) {
  const content = (
    <>
      {hint.keys.length > 0 && (
        <span className="keys">
          {hint.keys.map((k) => (
            <span key={k} className="keycap">
              {k}
            </span>
          ))}
        </span>
      )}
      {hint.label}
    </>
  );
  return hint.onClick ? (
    <button type="button" className="hint" onClick={hint.onClick} disabled={hint.disabled}>
      {content}
    </button>
  ) : (
    <span className="hint">{content}</span>
  );
}

/** Show this page's hints in the bottom bar while it's open. Clicks always call
 * the latest handlers, so pages can pass inline functions. */
export function usePageHints(hints: Hint[]) {
  const { setPage } = useContext(Ctx);
  const latest = useRef(hints);
  latest.current = hints;
  const signature = hints.map((h) => `${h.keys.join("+")}:${h.label}:${h.disabled ? 1 : 0}:${h.onClick ? 1 : 0}`).join("|");
  useEffect(() => {
    setPage(
      latest.current.map((h, i) => ({
        ...h,
        onClick: h.onClick ? () => latest.current[i]?.onClick?.() : undefined,
      })),
    );
  }, [signature, setPage]);
  useEffect(() => () => setPage([]), [setPage]);
}

/** A short status in the bottom bar (e.g. "Settings saved."), cleared after a while. */
export function useStatus() {
  const { setStatus } = useContext(Ctx);
  const timer = useRef<number | undefined>(undefined);
  useEffect(() => () => {
    window.clearTimeout(timer.current);
    setStatus(null);
  }, [setStatus]);
  return (status: Status | null, ms = 6000) => {
    window.clearTimeout(timer.current);
    setStatus(status);
    if (status) timer.current = window.setTimeout(() => setStatus(null), ms);
  };
}

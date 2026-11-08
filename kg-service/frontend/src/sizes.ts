import type { KnownSize } from "./api/client";

/** How a build's size is limited: rows per adapter (0 = none), or a percent of each
 * adapter's known size. The server (backend/core/console/sizes.py) applies the same
 * rules; this file only previews them. */
export type SizeUnit = "rows" | "percent";

export const ROW_CHOICES = [0, 100, 1000, 10000, 100000];
export const PERCENT_CHOICES = [1, 5, 10, 25, 50];

/** 12345 -> "12.3k", for tight spaces. */
export function compact(n: number): string {
  if (n >= 1e9) return `${(n / 1e9).toFixed(n >= 1e10 ? 0 : 1)}B`;
  if (n >= 1e6) return `${(n / 1e6).toFixed(n >= 1e7 ? 0 : 1)}M`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(n >= 1e4 ? 0 : 1)}k`;
  return String(n);
}

/** A percent of a known count, at least one row (as sizes.py does). */
export function percentOf(n: number | null, percent: number): number {
  if (!n) return 0;
  return Math.max(1, Math.ceil((n * percent) / 100));
}

function capped(n: number | null, unit: SizeUnit, limit: number, percent: number): number {
  if (n == null) return 0;
  if (unit === "percent") return percentOf(n, percent);
  return limit > 0 ? Math.min(n, limit) : n;
}

export interface SizePreview {
  nodes: number;
  edges: number;
  known: number; // adapters with a known size
  unknown: string[]; // adapters whose size isn't known yet
  dates: string[]; // build dates the known sizes come from (YYYY-MM-DD)
}

/** What a build of these adapters will write, from their known sizes. */
export function preview(
  adapters: string[],
  known: Record<string, KnownSize>,
  unit: SizeUnit,
  limit: number,
  percent: number,
): SizePreview {
  let nodes = 0;
  let edges = 0;
  let count = 0;
  const unknown: string[] = [];
  const dates = new Set<string>();
  for (const name of adapters) {
    const size = known[name];
    if (!size) {
      unknown.push(name);
      continue;
    }
    count += 1;
    if (size.built_at) dates.add(size.built_at.slice(0, 10));
    nodes += capped(size.nodes, unit, limit, percent);
    edges += capped(size.edges, unit, limit, percent);
  }
  return { nodes, edges, known: count, unknown, dates: [...dates].sort() };
}

/** A source's full size: the sum of its adapters', once every one is known. */
export function sourceSize(
  adapters: string[],
  known: Record<string, KnownSize>,
): { nodes: number; edges: number } | null {
  if (!adapters.length || adapters.some((a) => !known[a])) return null;
  const full = preview(adapters, known, "rows", 0, 0);
  return { nodes: full.nodes, edges: full.edges };
}

/** "Nov 5" or "Oct 30 to Nov 5", for where known sizes come from. */
export function datesText(dates: string[]): string {
  const fmt = (d: string) =>
    new Date(`${d}T00:00:00`).toLocaleDateString(undefined, { month: "short", day: "numeric" });
  if (!dates.length) return "";
  return dates.length === 1 ? fmt(dates[0]) : `${fmt(dates[0])} to ${fmt(dates[dates.length - 1])}`;
}

/** A build's size setting, from its params: "full", "1,000 rows/adapter" or
 * "10% of full". Shared by the build page and History. */
export function sizeSetting(params: Record<string, unknown> | null | undefined): string {
  const percent = Number(params?.percent) || 0;
  const limit = Number(params?.limit) || 0;
  if (percent) return `${percent}% of full`;
  if (limit) return `${limit.toLocaleString()} rows/adapter`;
  return "full";
}

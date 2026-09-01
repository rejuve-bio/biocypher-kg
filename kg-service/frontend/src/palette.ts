// Curated categorical palette: vivid, dark- and light-legible, ordered so
// consecutive indices land far apart in hue. Shared by the schema map and the
// instance neighborhood graph so a type keeps a stable, distinct color.
export const PALETTE = [
  "#ef4444", // red
  "#3b82f6", // blue
  "#22c55e", // green
  "#f59e0b", // amber
  "#a855f7", // purple
  "#14b8a6", // teal
  "#ec4899", // pink
  "#84cc16", // lime
  "#6366f1", // indigo
  "#f97316", // orange
  "#06b6d4", // cyan
  "#d946ef", // fuchsia
  "#eab308", // yellow
  "#0ea5e9", // sky
  "#e11d48", // rose
  "#8b5cf6", // violet
  "#10b981", // emerald
  "#fb923c", // light orange
  "#a3e635", // yellow-green
  "#f472b6", // light pink
  "#7c3aed", // deep violet
  "#2dd4bf", // turquoise
  "#facc15", // gold
  "#60a5fa", // light blue
];

export function paletteColor(i: number): string {
  return PALETTE[((i % PALETTE.length) + PALETTE.length) % PALETTE.length];
}

// A color assigner that hands out palette colors in first-seen order and keeps
// each type's color stable for the life of the map.
export function makeColorMap() {
  const map = new Map<string, string>();
  return (type: string): string => {
    let c = map.get(type);
    if (!c) {
      c = paletteColor(map.size);
      map.set(type, c);
    }
    return c;
  };
}

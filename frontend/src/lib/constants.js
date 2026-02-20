export const API_BASE = "/api";

export const NYC_CENTER = {
  longitude: -73.98,
  latitude: 40.74,
  zoom: 11,
  pitch: 45,
  bearing: -17,
};

export const MAP_STYLE =
  "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json";

export const LAYER_TYPES = {
  HEATMAP: "heatmap",
  ZONES: "zones",
  FLOWS: "flows",
};

export const METRIC_OPTIONS = [
  { key: "total_trips", label: "Trip Volume", format: (v) => v?.toLocaleString() ?? "—" },
  { key: "avg_fare", label: "Avg Fare", format: (v) => v ? `$${v.toFixed(2)}` : "—" },
  { key: "avg_distance", label: "Avg Distance", format: (v) => v ? `${v.toFixed(1)} mi` : "—" },
  { key: "avg_speed_mph", label: "Avg Speed", format: (v) => v ? `${v.toFixed(1)} mph` : "—" },
  { key: "avg_tip_pct", label: "Avg Tip %", format: (v) => v ? `${v.toFixed(1)}%` : "—" },
  { key: "avg_duration_min", label: "Avg Duration", format: (v) => v ? `${v.toFixed(0)} min` : "—" },
];

export const CLUSTER_COLORS = [
  [59, 130, 246],   // blue
  [6, 182, 212],    // cyan
  [139, 92, 246],   // purple
  [249, 115, 22],   // orange
  [34, 197, 94],    // green
  [239, 68, 68],    // red
  [236, 72, 153],   // pink
  [234, 179, 8],    // yellow
];

export const SEVERITY_COLORS = {
  critical: "#ef4444",
  high: "#f97316",
  medium: "#eab308",
  normal: "#6b7280",
};

export const BOROUGHS = [
  "Manhattan",
  "Brooklyn",
  "Queens",
  "Bronx",
  "Staten Island",
  "EWR",
];

export function colorScale(value, min, max) {
  if (value == null || max === min) return [100, 100, 120];
  const t = Math.min(1, Math.max(0, (value - min) / (max - min)));
  // Blue → Cyan → Yellow → Orange → Red
  if (t < 0.25) {
    const s = t / 0.25;
    return [30, Math.round(60 + 140 * s), Math.round(200 + 55 * s)];
  }
  if (t < 0.5) {
    const s = (t - 0.25) / 0.25;
    return [Math.round(30 + 200 * s), 200, Math.round(255 - 55 * s)];
  }
  if (t < 0.75) {
    const s = (t - 0.5) / 0.25;
    return [230, Math.round(200 - 80 * s), Math.round(200 - 200 * s)];
  }
  const s = (t - 0.75) / 0.25;
  return [Math.round(230 + 25 * s), Math.round(120 - 80 * s), 0];
}

import { Layers, Map, GitBranch, PanelRightOpen, PanelRightClose } from "lucide-react";
import { LAYER_TYPES, METRIC_OPTIONS, BOROUGHS } from "../lib/constants";

const LAYER_BUTTONS = [
  { key: LAYER_TYPES.ZONES, label: "Zones", icon: Map },
  { key: LAYER_TYPES.HEATMAP, label: "Heatmap", icon: Layers },
  { key: LAYER_TYPES.FLOWS, label: "Flows", icon: GitBranch },
];

export default function TopBar({
  activeLayer,
  setActiveLayer,
  selectedMetric,
  setSelectedMetric,
  selectedBorough,
  setSelectedBorough,
  sidebarOpen,
  setSidebarOpen,
}) {
  return (
    <header className="glass z-50 flex items-center gap-3 px-4 py-2 border-b border-white/5">
      <div className="flex items-center gap-2 mr-4">
        <div className="w-2 h-2 rounded-full bg-accent-cyan animate-pulse" />
        <h1 className="text-sm font-semibold tracking-wide text-white/90">
          NYC Urban Mobility
        </h1>
      </div>

      {/* Layer toggle */}
      <div className="flex items-center bg-dark-800 rounded-lg p-0.5 gap-0.5">
        {LAYER_BUTTONS.map(({ key, label, icon: Icon }) => (
          <button
            key={key}
            onClick={() => setActiveLayer(key)}
            className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-medium transition-all ${
              activeLayer === key
                ? "bg-accent-blue/20 text-accent-blue"
                : "text-white/50 hover:text-white/80 hover:bg-dark-700"
            }`}
          >
            <Icon size={13} />
            {label}
          </button>
        ))}
      </div>

      {/* Metric selector (for zone layer) */}
      {activeLayer === LAYER_TYPES.ZONES && (
        <select
          value={selectedMetric}
          onChange={(e) => setSelectedMetric(e.target.value)}
          className="bg-dark-800 text-white/80 text-xs px-3 py-1.5 rounded-lg border border-white/5 outline-none cursor-pointer"
        >
          {METRIC_OPTIONS.map((m) => (
            <option key={m.key} value={m.key}>
              {m.label}
            </option>
          ))}
        </select>
      )}

      {/* Borough filter */}
      <select
        value={selectedBorough || ""}
        onChange={(e) => setSelectedBorough(e.target.value || null)}
        className="bg-dark-800 text-white/80 text-xs px-3 py-1.5 rounded-lg border border-white/5 outline-none cursor-pointer"
      >
        <option value="">All Boroughs</option>
        {BOROUGHS.map((b) => (
          <option key={b} value={b}>
            {b}
          </option>
        ))}
      </select>

      <div className="flex-1" />

      <button
        onClick={() => setSidebarOpen((p) => !p)}
        className="text-white/50 hover:text-white/90 transition-colors p-1"
        title={sidebarOpen ? "Close panel" : "Open panel"}
      >
        {sidebarOpen ? <PanelRightClose size={18} /> : <PanelRightOpen size={18} />}
      </button>
    </header>
  );
}

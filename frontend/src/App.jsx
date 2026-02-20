import { useState, useEffect, useMemo, useCallback } from "react";
import MapView from "./components/MapView";
import Sidebar from "./components/Sidebar";
import TopBar from "./components/TopBar";
import BottomBar from "./components/BottomBar";
import { useApi } from "./hooks/useApi";
import { LAYER_TYPES } from "./lib/constants";

export default function App() {
  const [activeLayer, setActiveLayer] = useState(LAYER_TYPES.ZONES);
  const [selectedMetric, setSelectedMetric] = useState("total_trips");
  const [selectedZone, setSelectedZone] = useState(null);
  const [selectedBorough, setSelectedBorough] = useState(null);
  const [sidebarOpen, setSidebarOpen] = useState(true);

  const { data: overview } = useApi("/stats/overview");
  const { data: zoneStats } = useApi("/zones/stats", { limit: 265 });
  const { data: odFlows } = useApi("/flows", { limit: 300 });
  const { data: clusters } = useApi("/clusters");
  const { data: anomalies } = useApi("/anomalies", { limit: 500 });
  const { data: zonesGeo } = useApi("/zones");

  const zoneStatsMap = useMemo(() => {
    if (!zoneStats) return {};
    const map = {};
    for (const z of zoneStats) {
      map[z.zone_id] = z;
    }
    return map;
  }, [zoneStats]);

  const clusterMap = useMemo(() => {
    if (!clusters) return {};
    const map = {};
    for (const c of clusters) {
      map[c.zone_id] = c;
    }
    return map;
  }, [clusters]);

  const handleZoneClick = useCallback((zoneId) => {
    setSelectedZone((prev) => (prev === zoneId ? null : zoneId));
    setSidebarOpen(true);
  }, []);

  return (
    <div className="w-full h-full flex flex-col bg-dark-900">
      <TopBar
        activeLayer={activeLayer}
        setActiveLayer={setActiveLayer}
        selectedMetric={selectedMetric}
        setSelectedMetric={setSelectedMetric}
        selectedBorough={selectedBorough}
        setSelectedBorough={setSelectedBorough}
        sidebarOpen={sidebarOpen}
        setSidebarOpen={setSidebarOpen}
      />

      <div className="flex-1 flex relative overflow-hidden">
        <div className="flex-1 relative">
          <MapView
            activeLayer={activeLayer}
            selectedMetric={selectedMetric}
            selectedBorough={selectedBorough}
            zoneStats={zoneStats}
            zoneStatsMap={zoneStatsMap}
            odFlows={odFlows}
            clusters={clusters}
            clusterMap={clusterMap}
            zonesGeo={zonesGeo}
            selectedZone={selectedZone}
            onZoneClick={handleZoneClick}
          />
        </div>

        {sidebarOpen && (
          <Sidebar
            selectedZone={selectedZone}
            zoneStatsMap={zoneStatsMap}
            clusterMap={clusterMap}
            anomalies={anomalies}
            zoneStats={zoneStats}
            onZoneClick={handleZoneClick}
            onClose={() => setSidebarOpen(false)}
          />
        )}
      </div>

      <BottomBar overview={overview} />
    </div>
  );
}

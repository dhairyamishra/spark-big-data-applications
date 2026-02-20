import { useMemo } from "react";
import { X, TrendingUp, AlertTriangle, BarChart3 } from "lucide-react";
import DemandChart from "./DemandChart";
import AnomalyFeed from "./AnomalyFeed";
import { CLUSTER_COLORS } from "../lib/constants";
import { useApi } from "../hooks/useApi";

function StatCard({ label, value, sub }) {
  return (
    <div className="bg-dark-800 rounded-lg px-3 py-2">
      <div className="text-[10px] text-white/40 uppercase tracking-wider">{label}</div>
      <div className="text-sm font-semibold text-white/90 mt-0.5">{value}</div>
      {sub && <div className="text-[10px] text-white/40 mt-0.5">{sub}</div>}
    </div>
  );
}

function ZoneDetail({ zone, cluster, anomalies }) {
  const clusterColor = cluster?.cluster_id != null
    ? CLUSTER_COLORS[cluster.cluster_id % CLUSTER_COLORS.length]
    : [100, 100, 120];

  const zoneAnomalies = useMemo(() => {
    if (!anomalies) return [];
    return anomalies
      .filter((a) => a.pu_location_id === zone.zone_id)
      .slice(0, 10);
  }, [anomalies, zone.zone_id]);

  const { data: demandData } = useApi("/timeseries", {
    zone_id: zone.zone_id,
    limit: 730,
  });

  const { data: predData } = useApi("/predictions", {
    zone_id: zone.zone_id,
    limit: 500,
  });

  return (
    <div className="space-y-3">
      {/* Zone header */}
      <div>
        <h3 className="text-sm font-semibold text-white/90">
          {zone.zone_name || `Zone ${zone.zone_id}`}
        </h3>
        <div className="flex items-center gap-2 mt-1">
          <span className="text-xs text-white/50">{zone.borough}</span>
          {cluster?.cluster_id != null && (
            <span
              className="text-[10px] px-1.5 py-0.5 rounded-full font-medium"
              style={{
                backgroundColor: `rgba(${clusterColor.join(",")}, 0.2)`,
                color: `rgb(${clusterColor.join(",")})`,
              }}
            >
              Cluster {cluster.cluster_id}
            </span>
          )}
        </div>
      </div>

      {/* Stats grid */}
      <div className="grid grid-cols-2 gap-1.5">
        <StatCard
          label="Total Trips"
          value={zone.total_trips?.toLocaleString() ?? "—"}
        />
        <StatCard
          label="Avg Fare"
          value={zone.avg_fare ? `$${zone.avg_fare.toFixed(2)}` : "—"}
        />
        <StatCard
          label="Avg Distance"
          value={zone.avg_distance ? `${zone.avg_distance.toFixed(1)} mi` : "—"}
        />
        <StatCard
          label="Avg Duration"
          value={zone.avg_duration_min ? `${zone.avg_duration_min.toFixed(0)} min` : "—"}
        />
        <StatCard
          label="Avg Speed"
          value={zone.avg_speed_mph ? `${zone.avg_speed_mph.toFixed(1)} mph` : "—"}
        />
        <StatCard
          label="Peak Hour"
          value={zone.peak_hour != null ? `${zone.peak_hour}:00` : "—"}
        />
        <StatCard
          label="Revenue"
          value={
            zone.total_revenue
              ? `$${(zone.total_revenue / 1e6).toFixed(1)}M`
              : "—"
          }
        />
        <StatCard
          label="Avg Tip"
          value={zone.avg_tip_pct ? `${zone.avg_tip_pct.toFixed(1)}%` : "—"}
        />
      </div>

      {/* Demand chart */}
      <div>
        <div className="flex items-center gap-1.5 mb-2">
          <TrendingUp size={12} className="text-accent-cyan" />
          <span className="text-[11px] font-medium text-white/70">Daily Demand</span>
        </div>
        <DemandChart data={demandData} predictions={predData} />
      </div>

      {/* Anomalies */}
      {zoneAnomalies.length > 0 && (
        <div>
          <div className="flex items-center gap-1.5 mb-2">
            <AlertTriangle size={12} className="text-accent-orange" />
            <span className="text-[11px] font-medium text-white/70">
              Anomalies ({zoneAnomalies.length})
            </span>
          </div>
          <AnomalyFeed anomalies={zoneAnomalies} compact />
        </div>
      )}
    </div>
  );
}

function TopZones({ zoneStats, onZoneClick }) {
  const top10 = useMemo(() => {
    if (!zoneStats) return [];
    return [...zoneStats]
      .sort((a, b) => (b.total_trips || 0) - (a.total_trips || 0))
      .slice(0, 10);
  }, [zoneStats]);

  return (
    <div>
      <div className="flex items-center gap-1.5 mb-2">
        <BarChart3 size={12} className="text-accent-purple" />
        <span className="text-[11px] font-medium text-white/70">Top Zones by Volume</span>
      </div>
      <div className="space-y-1">
        {top10.map((z, i) => (
          <button
            key={z.zone_id}
            onClick={() => onZoneClick(z.zone_id)}
            className="w-full flex items-center gap-2 px-2 py-1.5 rounded-md text-xs hover:bg-dark-700 transition-colors text-left"
          >
            <span className="text-white/30 w-4 text-right">{i + 1}</span>
            <span className="text-white/80 flex-1 truncate">
              {z.zone_name || `Zone ${z.zone_id}`}
            </span>
            <span className="text-white/50 text-[10px]">
              {z.total_trips?.toLocaleString()}
            </span>
          </button>
        ))}
      </div>
    </div>
  );
}

export default function Sidebar({
  selectedZone,
  zoneStatsMap,
  clusterMap,
  anomalies,
  zoneStats,
  onZoneClick,
  onClose,
}) {
  const zone = selectedZone ? zoneStatsMap[selectedZone] : null;
  const cluster = selectedZone ? clusterMap[selectedZone] : null;

  return (
    <aside className="glass w-[340px] min-w-[340px] h-full overflow-y-auto border-l border-white/5">
      <div className="flex items-center justify-between px-4 py-2.5 border-b border-white/5">
        <span className="text-xs font-medium text-white/70">
          {zone ? "Zone Detail" : "Analytics"}
        </span>
        <button
          onClick={onClose}
          className="text-white/30 hover:text-white/70 transition-colors"
        >
          <X size={14} />
        </button>
      </div>

      <div className="p-3 space-y-4">
        {zone ? (
          <ZoneDetail zone={zone} cluster={cluster} anomalies={anomalies} />
        ) : (
          <>
            <TopZones zoneStats={zoneStats} onZoneClick={onZoneClick} />
            <div>
              <div className="flex items-center gap-1.5 mb-2">
                <AlertTriangle size={12} className="text-accent-orange" />
                <span className="text-[11px] font-medium text-white/70">
                  Recent Anomalies
                </span>
              </div>
              <AnomalyFeed anomalies={anomalies?.slice(0, 15)} />
            </div>
          </>
        )}
      </div>
    </aside>
  );
}

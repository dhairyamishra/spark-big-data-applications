import { useMemo } from "react";
import { useApi } from "../hooks/useApi";
import { CLUSTER_COLORS } from "../lib/constants";

function ClusterCard({ profile }) {
  const color = CLUSTER_COLORS[profile.cluster_id % CLUSTER_COLORS.length];
  const rgbStr = `rgb(${color.join(",")})`;
  const bgStr = `rgba(${color.join(",")}, 0.1)`;

  return (
    <div
      className="rounded-lg p-3 border"
      style={{ borderColor: `rgba(${color.join(",")}, 0.2)`, backgroundColor: bgStr }}
    >
      <div className="flex items-center justify-between mb-2">
        <span className="text-xs font-semibold" style={{ color: rgbStr }}>
          Cluster {profile.cluster_id}
        </span>
        <span className="text-[10px] text-white/50">
          {profile.zone_count} zones
        </span>
      </div>
      <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-[10px]">
        <div>
          <span className="text-white/40">Avg Trips</span>
          <span className="text-white/80 ml-1">
            {Math.round(profile.avg_trips).toLocaleString()}
          </span>
        </div>
        <div>
          <span className="text-white/40">Avg Fare</span>
          <span className="text-white/80 ml-1">
            ${profile.avg_fare?.toFixed(2)}
          </span>
        </div>
        <div>
          <span className="text-white/40">Avg Dist</span>
          <span className="text-white/80 ml-1">
            {profile.avg_distance?.toFixed(1)} mi
          </span>
        </div>
        <div>
          <span className="text-white/40">Avg Tip</span>
          <span className="text-white/80 ml-1">
            {profile.avg_tip_pct?.toFixed(1)}%
          </span>
        </div>
      </div>
      <div className="mt-2 text-[9px] text-white/30 truncate">
        {profile.boroughs?.join(", ")}
      </div>
    </div>
  );
}

export default function ClusterView() {
  const { data: profiles, loading } = useApi("/clusters/profiles");

  if (loading) {
    return (
      <div className="text-white/30 text-xs text-center py-4">
        Loading clusters...
      </div>
    );
  }

  if (!profiles || !profiles.length) {
    return (
      <div className="text-white/30 text-xs text-center py-4">
        No cluster data available
      </div>
    );
  }

  return (
    <div className="space-y-2">
      {profiles.map((p) => (
        <ClusterCard key={p.cluster_id} profile={p} />
      ))}
    </div>
  );
}

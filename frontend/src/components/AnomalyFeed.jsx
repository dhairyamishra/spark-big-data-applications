import { AlertTriangle, TrendingUp, TrendingDown } from "lucide-react";
import { SEVERITY_COLORS } from "../lib/constants";

function AnomalyItem({ anomaly, compact }) {
  const color = SEVERITY_COLORS[anomaly.severity] || SEVERITY_COLORS.normal;
  const isSurge = anomaly.direction === "surge";

  return (
    <div className="flex items-start gap-2 px-2 py-1.5 rounded-md hover:bg-dark-700/50 transition-colors">
      <div className="mt-0.5">
        {isSurge ? (
          <TrendingUp size={12} style={{ color }} />
        ) : (
          <TrendingDown size={12} style={{ color }} />
        )}
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-1.5">
          <span
            className="text-[9px] uppercase font-bold px-1 py-0.5 rounded"
            style={{
              backgroundColor: `${color}20`,
              color,
            }}
          >
            {anomaly.severity}
          </span>
          <span className="text-[10px] text-white/50 truncate">
            Zone {anomaly.pu_location_id}
          </span>
        </div>
        {!compact && (
          <div className="text-[10px] text-white/40 mt-0.5">
            {anomaly.datetime_hour?.slice(0, 16)} | z={anomaly.z_score?.toFixed(1)} |{" "}
            {anomaly.trip_count} trips ({isSurge ? "+" : ""}
            {((anomaly.trip_count - (anomaly.baseline_mean || 0)) /
              Math.max(anomaly.baseline_mean || 1, 1) *
              100
            ).toFixed(0)}
            % vs baseline)
          </div>
        )}
      </div>
    </div>
  );
}

export default function AnomalyFeed({ anomalies, compact = false }) {
  if (!anomalies || !anomalies.length) {
    return (
      <div className="text-white/30 text-xs text-center py-3">
        No anomalies detected
      </div>
    );
  }

  return (
    <div className="space-y-0.5 max-h-60 overflow-y-auto">
      {anomalies.map((a, i) => (
        <AnomalyItem key={`${a.pu_location_id}-${a.datetime_hour}-${i}`} anomaly={a} compact={compact} />
      ))}
    </div>
  );
}

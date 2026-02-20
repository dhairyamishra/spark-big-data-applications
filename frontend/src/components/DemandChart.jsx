import { useMemo } from "react";
import {
  ResponsiveContainer,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
} from "recharts";

export default function DemandChart({ data, predictions }) {
  const chartData = useMemo(() => {
    if (!data || !data.length) return [];

    // Merge demand data with predictions if available
    const predMap = {};
    if (predictions) {
      for (const p of predictions) {
        const key = p.datetime_hour?.slice(0, 10);
        if (key) {
          if (!predMap[key]) predMap[key] = { predicted: 0, count: 0 };
          predMap[key].predicted += p.predicted_demand || 0;
          predMap[key].count += 1;
        }
      }
    }

    return data.slice(-180).map((d) => {
      const date = d.pickup_date;
      const pred = predMap[date];
      return {
        date: date?.slice(5) || "",
        trips: d.trip_count || 0,
        predicted: pred ? Math.round(pred.predicted / pred.count) : null,
      };
    });
  }, [data, predictions]);

  if (!chartData.length) {
    return (
      <div className="h-32 flex items-center justify-center text-white/30 text-xs">
        No demand data available
      </div>
    );
  }

  return (
    <div className="h-36">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={chartData} margin={{ top: 4, right: 4, bottom: 0, left: -20 }}>
          <defs>
            <linearGradient id="demandGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.3} />
              <stop offset="95%" stopColor="#3b82f6" stopOpacity={0} />
            </linearGradient>
            <linearGradient id="predGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#06b6d4" stopOpacity={0.2} />
              <stop offset="95%" stopColor="#06b6d4" stopOpacity={0} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" />
          <XAxis
            dataKey="date"
            tick={{ fontSize: 9, fill: "rgba(255,255,255,0.3)" }}
            tickLine={false}
            axisLine={false}
            interval="preserveStartEnd"
          />
          <YAxis
            tick={{ fontSize: 9, fill: "rgba(255,255,255,0.3)" }}
            tickLine={false}
            axisLine={false}
            tickFormatter={(v) => (v >= 1000 ? `${(v / 1000).toFixed(0)}k` : v)}
          />
          <Tooltip
            contentStyle={{
              backgroundColor: "rgba(18,18,26,0.95)",
              border: "1px solid rgba(255,255,255,0.08)",
              borderRadius: 8,
              fontSize: 11,
              color: "rgba(255,255,255,0.8)",
            }}
            labelStyle={{ color: "rgba(255,255,255,0.5)" }}
          />
          <Area
            type="monotone"
            dataKey="trips"
            stroke="#3b82f6"
            fill="url(#demandGrad)"
            strokeWidth={1.5}
            dot={false}
            name="Actual"
          />
          <Area
            type="monotone"
            dataKey="predicted"
            stroke="#06b6d4"
            fill="url(#predGrad)"
            strokeWidth={1.5}
            strokeDasharray="4 3"
            dot={false}
            name="Predicted"
            connectNulls={false}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}

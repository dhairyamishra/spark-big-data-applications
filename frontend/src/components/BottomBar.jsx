import { Database, MapPin, DollarSign, Clock } from "lucide-react";

function Stat({ icon: Icon, label, value }) {
  return (
    <div className="flex items-center gap-2">
      <Icon size={13} className="text-accent-cyan/70" />
      <span className="text-white/40 text-[11px]">{label}</span>
      <span className="text-white/80 text-xs font-medium">{value}</span>
    </div>
  );
}

export default function BottomBar({ overview }) {
  if (!overview) return null;

  return (
    <footer className="glass z-50 flex items-center gap-6 px-4 py-1.5 border-t border-white/5 text-xs">
      <Stat
        icon={Database}
        label="Total Trips"
        value={overview.total_trips?.toLocaleString() ?? "—"}
      />
      <Stat
        icon={MapPin}
        label="Zones"
        value={overview.total_zones ?? "—"}
      />
      <Stat
        icon={DollarSign}
        label="Revenue"
        value={
          overview.total_revenue
            ? `$${(overview.total_revenue / 1e9).toFixed(2)}B`
            : "—"
        }
      />
      <Stat
        icon={Clock}
        label="Period"
        value={
          overview.data_range
            ? `${overview.data_range.start} — ${overview.data_range.end}`
            : "—"
        }
      />
      <div className="flex-1" />
      <span className="text-white/25 text-[10px]">
        Powered by Apache Spark + Deck.gl
      </span>
    </footer>
  );
}

import { useMemo, useState } from "react";
import { Map } from "react-map-gl/maplibre";
import DeckGL from "@deck.gl/react";
import { GeoJsonLayer, ArcLayer } from "@deck.gl/layers";
import { HexagonLayer } from "@deck.gl/aggregation-layers";
import {
  NYC_CENTER,
  MAP_STYLE,
  LAYER_TYPES,
  colorScale,
} from "../lib/constants";

export default function MapView({
  activeLayer,
  selectedMetric,
  selectedBorough,
  zoneStats,
  zoneStatsMap,
  odFlows,
  clusters,
  clusterMap,
  zonesGeo,
  selectedZone,
  onZoneClick,
}) {
  const [viewState, setViewState] = useState(NYC_CENTER);
  const [hoverInfo, setHoverInfo] = useState(null);

  // Compute metric range for color scaling
  const metricRange = useMemo(() => {
    if (!zoneStats || !zoneStats.length) return { min: 0, max: 1 };
    const vals = zoneStats
      .map((z) => z[selectedMetric])
      .filter((v) => v != null && v > 0);
    return {
      min: Math.min(...vals),
      max: Math.max(...vals),
    };
  }, [zoneStats, selectedMetric]);

  // ── Zone choropleth layer ──
  const zoneLayer = useMemo(() => {
    if (activeLayer !== LAYER_TYPES.ZONES || !zonesGeo) return null;

    let features = zonesGeo?.features || [];
    if (selectedBorough) {
      features = features.filter(
        (f) =>
          (f.properties?.Borough || "").toLowerCase() ===
          selectedBorough.toLowerCase()
      );
    }

    const geoData = { type: "FeatureCollection", features };

    return new GeoJsonLayer({
      id: "zone-layer",
      data: geoData,
      pickable: true,
      stroked: true,
      filled: true,
      extruded: false,
      lineWidthMinPixels: 1,
      getLineColor: (f) => {
        const zid =
          f.properties?.LocationID || f.properties?.location_id;
        return selectedZone === zid
          ? [255, 255, 255, 200]
          : [255, 255, 255, 30];
      },
      getLineWidth: (f) => {
        const zid =
          f.properties?.LocationID || f.properties?.location_id;
        return selectedZone === zid ? 3 : 1;
      },
      getFillColor: (f) => {
        const zid =
          f.properties?.LocationID || f.properties?.location_id;
        const stats = zoneStatsMap[zid];
        if (!stats) return [40, 40, 60, 100];
        const val = stats[selectedMetric];
        const rgb = colorScale(val, metricRange.min, metricRange.max);
        return [...rgb, selectedZone === zid ? 220 : 160];
      },
      onClick: (info) => {
        if (info.object) {
          const zid =
            info.object.properties?.LocationID ||
            info.object.properties?.location_id;
          if (zid) onZoneClick(zid);
        }
      },
      onHover: (info) => setHoverInfo(info.object ? info : null),
      updateTriggers: {
        getFillColor: [selectedMetric, metricRange, selectedZone],
        getLineColor: [selectedZone],
        getLineWidth: [selectedZone],
      },
    });
  }, [
    activeLayer,
    zonesGeo,
    zoneStatsMap,
    selectedMetric,
    metricRange,
    selectedZone,
    selectedBorough,
    onZoneClick,
  ]);

  // ── Heatmap layer ──
  const heatmapLayer = useMemo(() => {
    if (activeLayer !== LAYER_TYPES.HEATMAP || !zonesGeo) return null;

    const points = [];
    const features = zonesGeo?.features || [];
    for (const f of features) {
      const zid = f.properties?.LocationID || f.properties?.location_id;
      const stats = zoneStatsMap[zid];
      if (!stats) continue;
      if (
        selectedBorough &&
        (stats.borough || "").toLowerCase() !== selectedBorough.toLowerCase()
      )
        continue;
      const lng = f.properties?.centroid_lng;
      const lat = f.properties?.centroid_lat;
      if (lng && lat) {
        points.push({
          position: [lng, lat],
          weight: stats[selectedMetric] || 0,
        });
      }
    }

    return new HexagonLayer({
      id: "heatmap-layer",
      data: points,
      getPosition: (d) => d.position,
      getElevationWeight: (d) => d.weight,
      getColorWeight: (d) => d.weight,
      elevationScale: 50,
      extruded: true,
      radius: 400,
      coverage: 0.88,
      upperPercentile: 95,
      colorRange: [
        [30, 60, 200],
        [30, 200, 255],
        [100, 230, 100],
        [230, 200, 0],
        [230, 120, 0],
        [255, 40, 40],
      ],
      material: {
        ambient: 0.6,
        diffuse: 0.6,
        shininess: 40,
      },
    });
  }, [activeLayer, zonesGeo, zoneStatsMap, selectedMetric, selectedBorough]);

  // ── Flow arc layer ──
  const flowLayer = useMemo(() => {
    if (activeLayer !== LAYER_TYPES.FLOWS || !odFlows || !zonesGeo)
      return null;

    // Build centroid lookup from GeoJSON
    const centroids = {};
    for (const f of zonesGeo?.features || []) {
      const zid = f.properties?.LocationID || f.properties?.location_id;
      const lng = f.properties?.centroid_lng;
      const lat = f.properties?.centroid_lat;
      if (zid && lng && lat) centroids[zid] = [lng, lat];
    }

    const arcs = odFlows
      .filter((f) => {
        const from = centroids[f.pu_location_id];
        const to = centroids[f.do_location_id];
        if (!from || !to) return false;
        if (selectedBorough) {
          const bLower = selectedBorough.toLowerCase();
          return (
            (f.pu_borough || "").toLowerCase() === bLower ||
            (f.do_borough || "").toLowerCase() === bLower
          );
        }
        return true;
      })
      .map((f) => ({
        ...f,
        from: centroids[f.pu_location_id],
        to: centroids[f.do_location_id],
      }));

    const maxTrips = Math.max(...arcs.map((a) => a.trip_count || 1));

    return new ArcLayer({
      id: "flow-layer",
      data: arcs,
      pickable: true,
      getSourcePosition: (d) => d.from,
      getTargetPosition: (d) => d.to,
      getSourceColor: [59, 130, 246, 180],
      getTargetColor: [6, 182, 212, 180],
      getWidth: (d) => 1 + (d.trip_count / maxTrips) * 6,
      greatCircle: false,
      onHover: (info) =>
        setHoverInfo(info.object ? info : null),
    });
  }, [activeLayer, odFlows, zonesGeo, selectedBorough]);

  // Collect active layers
  const layers = useMemo(
    () => [zoneLayer, heatmapLayer, flowLayer].filter(Boolean),
    [zoneLayer, heatmapLayer, flowLayer]
  );

  // Tooltip
  const tooltip = useMemo(() => {
    if (!hoverInfo || !hoverInfo.object) return null;
    const { x, y, object } = hoverInfo;

    // Zone hover
    if (object.properties) {
      const zid =
        object.properties.LocationID || object.properties.location_id;
      const stats = zoneStatsMap[zid];
      const name = stats?.zone_name || object.properties.Zone || `Zone ${zid}`;
      const borough = stats?.borough || object.properties.Borough || "";
      return (
        <div
          className="glass rounded-lg px-3 py-2 pointer-events-none text-xs"
          style={{ position: "absolute", left: x + 12, top: y - 12, zIndex: 100 }}
        >
          <div className="font-semibold text-white/90">{name}</div>
          <div className="text-white/50">{borough}</div>
          {stats && (
            <div className="mt-1 space-y-0.5 text-white/70">
              <div>Trips: {stats.total_trips?.toLocaleString()}</div>
              <div>Avg Fare: ${stats.avg_fare?.toFixed(2)}</div>
              <div>Avg Distance: {stats.avg_distance?.toFixed(1)} mi</div>
            </div>
          )}
        </div>
      );
    }

    // Arc hover
    if (object.pu_borough) {
      return (
        <div
          className="glass rounded-lg px-3 py-2 pointer-events-none text-xs"
          style={{ position: "absolute", left: x + 12, top: y - 12, zIndex: 100 }}
        >
          <div className="font-semibold text-white/90">
            {object.pu_zone || object.pu_location_id} → {object.do_zone || object.do_location_id}
          </div>
          <div className="text-white/50">
            {object.pu_borough} → {object.do_borough}
          </div>
          <div className="mt-1 text-white/70">
            Trips: {object.trip_count?.toLocaleString()}
          </div>
        </div>
      );
    }

    return null;
  }, [hoverInfo, zoneStatsMap]);

  return (
    <div className="w-full h-full relative">
      <DeckGL
        viewState={viewState}
        onViewStateChange={({ viewState: vs }) => setViewState(vs)}
        controller={true}
        layers={layers}
        getCursor={({ isHovering }) => (isHovering ? "pointer" : "grab")}
      >
        <Map mapStyle={MAP_STYLE} />
      </DeckGL>
      {tooltip}
    </div>
  );
}

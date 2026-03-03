const {MapboxOverlay} = deck;
const h3lib = window.h3 || window.h3js || null;

const API_BASE = window.API_BASE_URL || "http://localhost:8000/api/v1";
const statusEl = document.getElementById("status");
const toggleH3 = document.getElementById("toggle-h3");
const toggleSettlements = document.getElementById("toggle-settlements");
const toggleSubscribers = document.getElementById("toggle-subscribers");
const toggleLabels = document.getElementById("toggle-labels");
const h3ModeInputs = Array.from(document.querySelectorAll('input[name="h3-mode"]'));

const map = new maplibregl.Map({
  container: "map",
  style: "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
  center: [37.6176, 55.7558],
  zoom: 9,
});

const overlay = new MapboxOverlay({
  layers: [],
  getTooltip: ({object, layer}) => {
    if (!object) return null;
    const p = object.properties || object;
    if (p.cluster) {
      const active = p.active_cnt || p.point_count || 0;
      const sysblock = p.sysblock_cnt || 0;
      const paySum = Math.round(p.payments_sum_m || 0);
      const chgSum = Math.round(p.charges_sum_m || 0);
      return `A: ${active}\nS: ${sysblock}\nP: ${paySum}\nC: ${chgSum}`;
    }
    if (layer && layer.id === "h3-layer") {
      const active = p.active_cnt || 0;
      const paySum = Math.round(p.payments_sum_m || 0);
      const chgSum = Math.round(p.charges_sum_m || 0);
      return `A: ${active}\nP: ${paySum}\nC: ${chgSum}`;
    }
    if (layer && layer.id === "settlements-layer") {
      const active = p.active_cnt || 0;
      const paySum = Math.round(p.payments_sum_m || 0);
      const chgSum = Math.round(p.charges_sum_m || 0);
      return `A: ${active}\nP: ${paySum}\nC: ${chgSum}`;
    }
    return null;
  },
});
map.addControl(overlay);

let debounceTimer = null;
let lastFetch = 0;

function getBBox() {
  const b = map.getBounds();
  return [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()];
}

function bboxToParam(bbox) {
  return bbox.map((v) => v.toFixed(6)).join(",");
}

function pickH3Res(zoom) {
  if (zoom <= 7) return 6;
  if (zoom <= 9) return 7;
  if (zoom <= 11) return 8;
  if (zoom <= 13) return 9;
  if (zoom <= 15) return 10;
  return 11;
}

function getH3Res(zoom) {
  const selected = h3ModeInputs.find((el) => el.checked);
  if (!selected || selected.value === "auto") {
    return pickH3Res(zoom);
  }
  const parsed = parseInt(selected.value, 10);
  return Number.isFinite(parsed) ? parsed : pickH3Res(zoom);
}

function h3Boundary(h3Index) {
  if (!h3lib) return null;
  if (typeof h3lib.cellToBoundary === "function") {
    return h3lib.cellToBoundary(h3Index, true);
  }
  if (typeof h3lib.h3ToGeoBoundary === "function") {
    return h3lib.h3ToGeoBoundary(h3Index, true);
  }
  return null;
}

function buildH3GeoJson(items) {
  if (!h3lib) return {type: "FeatureCollection", features: []};
  const features = [];
  for (const d of items || []) {
    const boundary = h3Boundary(d.h3_index);
    if (!boundary) continue;
    // boundary with geoJson=true should already be [lng, lat]
    const coords = boundary.map(([lng, lat]) => [lng, lat]);
    if (coords.length && (coords[0][0] !== coords[coords.length - 1][0] || coords[0][1] !== coords[coords.length - 1][1])) {
      coords.push(coords[0]);
    }
    features.push({
      type: "Feature",
      geometry: {type: "Polygon", coordinates: [coords]},
      properties: {
        h3_index: d.h3_index,
        active_cnt: d.active_cnt || 0,
        charges_sum_m: d.charges_sum_m || 0,
        payments_sum_m: d.payments_sum_m || 0,
      },
    });
  }
  return {type: "FeatureCollection", features};
}
async function fetchLayer(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return await res.json();
}

function setStatus(text) {
  statusEl.textContent = text;
}

const CLUSTER_RADIUS_LOW_ZOOM = 80;
const CLUSTER_RADIUS_HIGH_ZOOM = 120;
const SETTLEMENT_ICON = {
  url: "data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='64' height='64' viewBox='0 0 64 64'><path fill='%23252a2f' d='M8 30L32 10l24 20v26a4 4 0 0 1-4 4H12a4 4 0 0 1-4-4V30z'/><path fill='%23ffffff' d='M20 28h24v24H20z'/><path fill='%23252a2f' d='M28 40h8v12h-8z'/></svg>",
  width: 64,
  height: 64,
  anchorY: 64,
  mask: false,
};

function settlementIconSize(zoom) {
  if (zoom <= 10) return 18;
  if (zoom <= 14) return 22;
  return 26;
}

function settlementsLayer(features, zoom) {
  return new deck.IconLayer({
    id: "settlements-layer",
    data: features || [],
    getPosition: (d) => d.geometry.coordinates,
    getIcon: () => SETTLEMENT_ICON,
    getSize: settlementIconSize(zoom),
    sizeUnits: "pixels",
    sizeMinPixels: 14,
    sizeMaxPixels: 36,
    billboard: true,
    alphaCutoff: 0.05,
    pickable: true,
  });
}

async function updateLayers() {
  const now = Date.now();
  if (now - lastFetch < 300) return;
  lastFetch = now;

  const zoom = map.getZoom();
  const bbox = getBBox();
  const bboxParam = bboxToParam(bbox);
  const h3Res = getH3Res(zoom);
  setStatus(`зум: ${zoom.toFixed(1)} | h3: ${h3Res} | bbox: ${bboxParam}`);

  try {
    const layers = [];
    const needSubscribers = (toggleSubscribers && toggleSubscribers.checked) || (toggleLabels && toggleLabels.checked);
    let subscribers = null;
    if (needSubscribers) {
      const subUrl = `${API_BASE}/layers/subscribers?month=current&bbox=${bboxParam}&limit=20000`;
      subscribers = await fetchLayer(subUrl);
    }

    if (toggleH3 && toggleH3.checked) {
      const h3Res = getH3Res(zoom);
      const h3Url = `${API_BASE}/layers/h3?month=current&bbox=${bboxParam}&h3_res=${h3Res}`;
      const h3Data = await fetchLayer(h3Url);
      const h3Geo = buildH3GeoJson(h3Data.data || []);
      layers.push(
        new deck.GeoJsonLayer({
          id: "h3-layer",
          data: h3Geo,
          stroked: true,
          filled: true,
          getFillColor: (f) => {
            const v = f.properties.payments_sum_m || 0;
            const c = Math.min(255, Math.round(v / 1000));
            return [255, 140 - Math.min(120, c), 0, 30];
          },
          getLineColor: [255, 255, 255, 40],
          lineWidthMinPixels: 2,
          lineWidthMaxPixels: 4,
          pickable: true,
        })
      );
      if (!h3lib) {
        setStatus("h3-js не загружен: слой H3 не построен");
      }
    }

    if (zoom <= 10) {
      if (toggleSettlements && toggleSettlements.checked) {
        const settlementsUrl = `${API_BASE}/layers/settlements?month=current&bbox=${bboxParam}`;
        const settlements = await fetchLayer(settlementsUrl);
        layers.push(settlementsLayer(settlements.features || [], zoom));
      }
    } else if (zoom <= 14) {
      if (toggleSettlements && toggleSettlements.checked) {
        const settlementsUrl = `${API_BASE}/layers/settlements?month=current&bbox=${bboxParam}`;
        const settlements = await fetchLayer(settlementsUrl);
        layers.push(settlementsLayer(settlements.features || [], zoom));
      }
    } else {
      if (toggleSettlements && toggleSettlements.checked) {
        const settlementsUrl = `${API_BASE}/layers/settlements?month=current&bbox=${bboxParam}`;
        const settlements = await fetchLayer(settlementsUrl);
        layers.push(settlementsLayer(settlements.features || [], zoom));
      }
    }

    if (needSubscribers && subscribers) {
      const points = (subscribers.features || []).map((f) => ({
        type: "Feature",
        geometry: f.geometry,
        properties: f.properties || {},
      }));

      const cluster = new Supercluster({
        radius: zoom <= 10 ? CLUSTER_RADIUS_LOW_ZOOM : CLUSTER_RADIUS_HIGH_ZOOM,
        maxZoom: 18,
        map: (props) => ({
          active_cnt: props.active_cnt || 0,
          sysblock_cnt: props.is_sysblock ? 1 : 0,
          payments_sum_m: props.payments_sum_m || 0,
          charges_sum_m: props.charges_sum_m || 0,
        }),
        reduce: (accum, props) => {
          accum.active_cnt += props.active_cnt || 0;
          accum.sysblock_cnt += props.sysblock_cnt || 0;
          accum.payments_sum_m += props.payments_sum_m || 0;
          accum.charges_sum_m += props.charges_sum_m || 0;
        },
      });
      cluster.load(points);
      const z = Math.round(zoom);
      const clusters = cluster.getClusters(bbox, z);

      if (toggleSubscribers && toggleSubscribers.checked) {
        layers.push(
          new deck.ScatterplotLayer({
            id: "subscribers-layer",
            data: clusters,
            getPosition: (d) => d.geometry.coordinates,
            getRadius: (d) => (d.properties.cluster ? 18 + d.properties.point_count : 5),
            radiusMinPixels: 2,
            radiusMaxPixels: 30,
            getFillColor: (d) => (d.properties.cluster ? [30, 144, 255, 200] : [0, 0, 0, 180]),
            pickable: true,
          })
        );
      }

      if (toggleLabels && toggleLabels.checked) {
        const clusterLabels = clusters.filter((d) => d.properties && d.properties.cluster);
        layers.push(
          new deck.TextLayer({
            id: "cluster-labels",
            data: clusterLabels,
            getPosition: (d) => d.geometry.coordinates,
            getText: (d) => {
              const p = d.properties || {};
              const active = p.active_cnt || p.point_count || 0;
              const paySum = p.payments_sum_m || 0;
              return `${active}/${Math.round(paySum)}`;
            },
            getSize: 12,
            sizeUnits: "pixels",
            sizeMinPixels: 10,
            sizeMaxPixels: 16,
            getTextAnchor: "middle",
            getAlignmentBaseline: "center",
            getColor: [0, 0, 0, 230],
            background: false,
            billboard: true,
            pickable: false,
          })
        );
      }
    }

    overlay.setProps({layers});
  } catch (err) {
    setStatus(`ошибка: ${err.message}`);
  }
}

function scheduleUpdate() {
  if (debounceTimer) clearTimeout(debounceTimer);
  debounceTimer = setTimeout(updateLayers, 350);
}

map.on("load", () => {
  updateLayers();
  map.on("moveend", scheduleUpdate);
  map.on("zoomend", scheduleUpdate);
});

[toggleH3, toggleSettlements, toggleSubscribers, toggleLabels].forEach((el) => {
  if (!el) return;
  el.addEventListener("change", scheduleUpdate);
});

h3ModeInputs.forEach((el) => {
  el.addEventListener("change", scheduleUpdate);
});

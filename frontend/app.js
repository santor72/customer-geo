const {MapboxOverlay} = deck;

const API_BASE = window.API_BASE_URL || "http://localhost:8000/api/v1";
const statusEl = document.getElementById("status");

const map = new maplibregl.Map({
  container: "map",
  style: "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
  center: [37.6176, 55.7558],
  zoom: 9,
});

const overlay = new MapboxOverlay({layers: []});
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
  if (zoom <= 8) return 6;
  if (zoom <= 10) return 7;
  if (zoom <= 12) return 8;
  return 9;
}

async function fetchLayer(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return await res.json();
}

function setStatus(text) {
  statusEl.textContent = text;
}

async function updateLayers() {
  const now = Date.now();
  if (now - lastFetch < 300) return;
  lastFetch = now;

  const zoom = map.getZoom();
  const bbox = getBBox();
  const bboxParam = bboxToParam(bbox);
  setStatus(`зум: ${zoom.toFixed(1)} | bbox: ${bboxParam}`);

  try {
    const layers = [];
    const subUrl = `${API_BASE}/layers/subscribers?month=current&bbox=${bboxParam}&limit=20000`;
    const subscribers = await fetchLayer(subUrl);

    if (zoom <= 10) {
      const h3Res = pickH3Res(zoom);
      const h3Url = `${API_BASE}/layers/h3?month=current&bbox=${bboxParam}&h3_res=${h3Res}`;
      const settlementsUrl = `${API_BASE}/layers/settlements?month=current&bbox=${bboxParam}`;

      const [h3Data, settlements] = await Promise.all([
        fetchLayer(h3Url),
        fetchLayer(settlementsUrl),
      ]);

      layers.push(
        new deck.GeoJsonLayer({
          id: "h3-layer",
          data: h3Data,
          stroked: false,
          filled: true,
          getFillColor: (f) => {
            const v = f.properties.payments_sum_m || 0;
            const c = Math.min(255, Math.round(v / 1000));
            return [255, 140 - Math.min(120, c), 0, 160];
          },
          pickable: true,
        })
      );

      layers.push(
        new deck.ScatterplotLayer({
          id: "settlements-layer",
          data: settlements.features || [],
          getPosition: (d) => d.geometry.coordinates,
          getRadius: 80,
          radiusMinPixels: 2,
          radiusMaxPixels: 10,
          getFillColor: [20, 20, 20, 180],
          pickable: true,
        })
      );
    } else if (zoom <= 14) {
      const settlementsUrl = `${API_BASE}/layers/settlements?month=current&bbox=${bboxParam}`;
      const settlements = await fetchLayer(settlementsUrl);
      layers.push(
        new deck.ScatterplotLayer({
          id: "settlements-layer",
          data: settlements.features || [],
          getPosition: (d) => d.geometry.coordinates,
          getRadius: 120,
          radiusMinPixels: 2,
          radiusMaxPixels: 12,
          getFillColor: [20, 20, 20, 200],
          pickable: true,
        })
      );
    } else {
      const settlementsUrl = `${API_BASE}/layers/settlements?month=current&bbox=${bboxParam}`;
      const settlements = await fetchLayer(settlementsUrl);
      layers.push(
        new deck.ScatterplotLayer({
          id: "settlements-layer",
          data: settlements.features || [],
          getPosition: (d) => d.geometry.coordinates,
          getRadius: 140,
          radiusMinPixels: 2,
          radiusMaxPixels: 12,
          getFillColor: [20, 20, 20, 180],
          pickable: true,
        })
      );
    }

    const points = (subscribers.features || []).map((f) => ({
      type: "Feature",
      geometry: f.geometry,
      properties: f.properties || {},
    }));

    const cluster = new Supercluster({
      radius: zoom <= 10 ? 80 : 60,
      maxZoom: 18,
    });
    cluster.load(points);
    const z = Math.round(zoom);
    const clusters = cluster.getClusters(bbox, z);

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

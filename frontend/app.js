const {MapboxOverlay} = deck;
const h3lib = window.h3 || window.h3js || null;

const API_BASE = window.API_BASE_URL || "http://localhost:8000/api/v1";
const statusEl = document.getElementById("status");
const toggleH3 = document.getElementById("toggle-h3");
const toggleSettlements = document.getElementById("toggle-settlements");
const toggleSubscribers = document.getElementById("toggle-subscribers");
const toggleLabels = document.getElementById("toggle-labels");
const resetViewBtn = document.getElementById("reset-view");
const searchToggle = document.getElementById("search-toggle");
const searchPanel = document.getElementById("search-panel");
const searchInput = document.getElementById("search-input");
const searchResults = document.getElementById("search-results");
const geocodeInput = document.getElementById("geocode-input");
const geocodeResults = document.getElementById("geocode-results");

const map = new maplibregl.Map({
  container: "map",
  style: "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
  center: [37.6176, 55.7558],
  zoom: 7,
});

const numberFmt = new Intl.NumberFormat("ru-RU");
const moneyFmt = new Intl.NumberFormat("ru-RU", {maximumFractionDigits: 0});

function formatInt(value) {
  return numberFmt.format(Math.round(value || 0));
}

function formatMoney(value) {
  return moneyFmt.format(Math.round(value || 0));
}

function tooltipHtml({type, title, metrics}) {
  const rows = (metrics || [])
    .map(
      (m) => `
        <div class="tooltip-item">
          <div class="tooltip-label">${m.label}</div>
          <div class="tooltip-value">${m.value}</div>
        </div>
      `
    )
    .join("");
  return `
    <div class="tooltip-card tooltip-${type}">
      <div class="tooltip-title">
        ${title}
        <span class="tooltip-type">${type}</span>
      </div>
      <div class="tooltip-grid">${rows}</div>
    </div>
  `;
}

const overlay = new MapboxOverlay({
  layers: [],
  getTooltip: ({object, layer}) => {
    if (!object) return null;
    const p = object.properties || object;
    if (p.cluster) {
      const active = p.active_cnt || p.point_count || 0;
      const sysblock = p.sysblock_cnt || 0;
      const paySum = p.payments_sum_m || 0;
      const chgSum = p.charges_sum_m || 0;
      return {
        html: tooltipHtml({
          type: "cluster",
          title: `Кластер (${formatInt(p.point_count || 0)})`,
          metrics: [
            {label: "Активные", value: formatInt(active)},
            {label: "Блокировки", value: formatInt(sysblock)},
            {label: "Платежи, ₽", value: formatMoney(paySum)},
            {label: "Начисления, ₽", value: formatMoney(chgSum)},
          ],
        }),
      };
    }
    if (layer && layer.id === "h3-layer") {
      const active = p.active_cnt || 0;
      const paySum = p.payments_sum_m || 0;
      const chgSum = p.charges_sum_m || 0;
      return {
        html: tooltipHtml({
          type: "h3",
          title: `H3 ${p.h3_index || ""}`.trim(),
          metrics: [
            {label: "Активные", value: formatInt(active)},
            {label: "Платежи, ₽", value: formatMoney(paySum)},
            {label: "Начисления, ₽", value: formatMoney(chgSum)},
          ],
        }),
      };
    }
    if (layer && layer.id === "settlements-layer") {
      const active = p.active_cnt || 0;
      const paySum = p.payments_sum_m || 0;
      const chgSum = p.charges_sum_m || 0;
      return {
        html: tooltipHtml({
          type: "settlement",
          title: `Поселок ${p.title || formatInt(p.settlement_id || 0)}`,
          metrics: [
            {label: "Активные", value: formatInt(active)},
            {label: "Платежи, ₽", value: formatMoney(paySum)},
            {label: "Начисления, ₽", value: formatMoney(chgSum)},
          ],
        }),
      };
    }
    if (layer && layer.id === "subscribers-layer" && !p.cluster) {
      return {
        html: tooltipHtml({
          type: "subscriber",
          title: `Абонент ${formatInt(p.account_id || 0)}`,
          metrics: [
            {label: "Активен", value: p.is_active ? "Да" : "Нет"},
            {label: "Блокировка", value: p.is_sysblock ? "Да" : "Нет"},
            {label: "Платежи, ₽", value: formatMoney(p.payments_sum_m || 0)},
            {label: "Начисления, ₽", value: formatMoney(p.charges_sum_m || 0)},
          ],
        }),
      };
    }
    return null;
  },
});
map.addControl(overlay);

const STORAGE_KEY = "customer_geo_layers";

function readLayerPrefs() {
  const raw = window.localStorage.getItem(STORAGE_KEY);
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch (err) {
    return null;
  }
}

function writeLayerPrefs() {
  const prefs = {
    h3: !!(toggleH3 && toggleH3.checked),
    settlements: !!(toggleSettlements && toggleSettlements.checked),
    subscribers: !!(toggleSubscribers && toggleSubscribers.checked),
    labels: !!(toggleLabels && toggleLabels.checked),
  };
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify(prefs));
}

function applyLayerPrefs() {
  const prefs = readLayerPrefs();
  if (!prefs) return;
  if (toggleH3 && typeof prefs.h3 === "boolean") toggleH3.checked = prefs.h3;
  if (toggleSettlements && typeof prefs.settlements === "boolean") toggleSettlements.checked = prefs.settlements;
  if (toggleSubscribers && typeof prefs.subscribers === "boolean") toggleSubscribers.checked = prefs.subscribers;
  if (toggleLabels && typeof prefs.labels === "boolean") toggleLabels.checked = prefs.labels;
}

let debounceTimer = null;
let lastFetch = 0;
let searchTimer = null;
let geocodeTimer = null;
let searchMarker = null;
let searchActiveIndex = -1;
let geocodeActiveIndex = -1;
let initialBounds = null;
let initialCenter = null;
let initialZoom = null;

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
  return pickH3Res(zoom);
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

function setSearchMarker(lng, lat) {
  if (!Number.isFinite(lng) || !Number.isFinite(lat)) return;
  if (!searchMarker) {
    searchMarker = new maplibregl.Marker({color: "#ef4444"});
  }
  searchMarker.setLngLat([lng, lat]).addTo(map);
}

function clearSearchResults() {
  if (!searchResults) return;
  searchResults.innerHTML = "";
  searchActiveIndex = -1;
}

function renderSearchResults(items) {
  clearSearchResults();
  if (!searchResults) return;
  if (!items || !items.length) {
    const empty = document.createElement("div");
    empty.className = "search-item";
    empty.textContent = "Ничего не найдено";
    searchResults.appendChild(empty);
    return;
  }
  for (const item of items) {
    const row = document.createElement("div");
    row.className = "search-item";
    row.dataset.title = item.title || "";
    row.innerHTML = `
      <div class="search-item-title">${item.title || "Без названия"}</div>
      <div class="search-item-sub">ID ${item.settlement_id} • A ${formatInt(item.active_cnt || 0)}</div>
    `;
    row.addEventListener("click", () => {
      if (searchInput) searchInput.value = item.title || "";
      if (Number.isFinite(item.lng) && Number.isFinite(item.lat)) {
        setSearchMarker(item.lng, item.lat);
        map.flyTo({center: [item.lng, item.lat], zoom: Math.max(map.getZoom(), 12)});
      }
      closeSearchPanel();
    });
    searchResults.appendChild(row);
  }
}

function clearGeocodeResults() {
  if (!geocodeResults) return;
  geocodeResults.innerHTML = "";
  geocodeActiveIndex = -1;
}

function clickFirstResult(container) {
  if (!container) return false;
  const first = container.querySelector(".search-item");
  if (first) {
    first.click();
    return true;
  }
  return false;
}

function setActiveResult(container, index) {
  if (!container) return -1;
  const items = Array.from(container.querySelectorAll(".search-item"));
  if (!items.length) return -1;
  const next = Math.max(0, Math.min(index, items.length - 1));
  items.forEach((el, i) => el.classList.toggle("active", i === next));
  const active = items[next];
  if (active && typeof active.scrollIntoView === "function") {
    active.scrollIntoView({block: "nearest"});
  }
  return next;
}

function renderGeocodeResults(items) {
  clearGeocodeResults();
  if (!geocodeResults) return;
  if (!items || !items.length) {
    const empty = document.createElement("div");
    empty.className = "search-item";
    empty.textContent = "Ничего не найдено";
    geocodeResults.appendChild(empty);
    return;
  }
  for (const item of items) {
    const row = document.createElement("div");
    row.className = "search-item";
    row.dataset.value = item.value || "";
    row.dataset.unrestricted = item.unrestricted_value || "";
    row.innerHTML = `
      <div class="search-item-title">${item.value || "Адрес"}</div>
      <div class="search-item-sub">${item.unrestricted_value || ""}</div>
    `;
    row.addEventListener("click", async () => {
      try {
        const q = item.value || item.unrestricted_value;
        if (!q) return;
        if (geocodeInput) geocodeInput.value = q;
        const url = `${API_BASE}/address/geocode?q=${encodeURIComponent(q)}`;
        const data = await fetchLayer(url);
        if (data && data.found && Number.isFinite(data.lng) && Number.isFinite(data.lat)) {
          setSearchMarker(data.lng, data.lat);
          map.flyTo({center: [data.lng, data.lat], zoom: Math.max(map.getZoom(), 14)});
        }
      } catch (err) {
        // ignore
      }
    });
    geocodeResults.appendChild(row);
  }
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
  initialBounds = map.getBounds();
  initialCenter = map.getCenter();
  initialZoom = map.getZoom();
  applyLayerPrefs();
  updateLayers();
  map.on("moveend", scheduleUpdate);
  map.on("zoomend", scheduleUpdate);
});

if (resetViewBtn) {
  resetViewBtn.addEventListener("click", () => {
    if (initialCenter && Number.isFinite(initialZoom)) {
      map.easeTo({center: initialCenter, zoom: initialZoom, duration: 600});
    } else if (initialBounds) {
      map.fitBounds(initialBounds, {padding: 20, duration: 600});
    }
  });
}

[toggleH3, toggleSettlements, toggleSubscribers, toggleLabels].forEach((el) => {
  if (!el) return;
  el.addEventListener("change", () => {
    writeLayerPrefs();
    scheduleUpdate();
  });
});

if (searchToggle && searchPanel) {
  searchToggle.addEventListener("click", () => {
    searchPanel.classList.toggle("open");
    if (searchPanel.classList.contains("open") && searchInput) {
      searchInput.focus();
    }
  });
}

if (searchInput) {
  searchInput.addEventListener("input", () => {
    if (searchTimer) clearTimeout(searchTimer);
    const q = searchInput.value.trim();
    searchTimer = setTimeout(async () => {
      if (!q) {
        clearSearchResults();
        return;
      }
      try {
        const url = `${API_BASE}/settlements/search?month=current&q=${encodeURIComponent(q)}&limit=20`;
        const data = await fetchLayer(url);
        renderSearchResults(data.items || []);
      } catch (err) {
        clearSearchResults();
      }
    }, 250);
  });
  searchInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      const active = searchResults ? searchResults.querySelector(".search-item.active") : null;
      if (active) {
        active.click();
      } else if (!clickFirstResult(searchResults)) {
        closeSearchPanel();
      }
    }
    if (e.key === " " || e.key === "Spacebar") {
      const active = searchResults ? searchResults.querySelector(".search-item.active") : null;
      if (active && searchInput) {
        e.preventDefault();
        searchInput.value = active.dataset.title || "";
      }
    }
    if (e.key === "Escape") {
      e.preventDefault();
      closeSearchPanel();
    }
    if (e.key === "ArrowDown") {
      e.preventDefault();
      searchActiveIndex = setActiveResult(searchResults, searchActiveIndex + 1);
    }
    if (e.key === "ArrowUp") {
      e.preventDefault();
      searchActiveIndex = setActiveResult(searchResults, searchActiveIndex - 1);
    }
  });
}


function closeSearchPanel() {
  if (!searchPanel) return;
  searchPanel.classList.remove("open");
  if (searchInput) searchInput.value = "";
  clearSearchResults();
}

document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") closeSearchPanel();
});

document.addEventListener("click", (e) => {
  if (!searchPanel || !searchPanel.classList.contains("open")) return;
  const target = e.target;
  if (!target) return;
  if (searchPanel.contains(target) || (searchToggle && searchToggle.contains(target))) return;
  closeSearchPanel();
});

if (geocodeInput) {
  geocodeInput.addEventListener("input", () => {
    if (geocodeTimer) clearTimeout(geocodeTimer);
    const q = geocodeInput.value.trim();
    geocodeTimer = setTimeout(async () => {
      if (!q) {
        clearGeocodeResults();
        return;
      }
      try {
        const url = `${API_BASE}/address/suggest?q=${encodeURIComponent(q)}&limit=10`;
        const data = await fetchLayer(url);
        renderGeocodeResults(data.items || []);
      } catch (err) {
        clearGeocodeResults();
      }
    }, 250);
  });
  geocodeInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      const active = geocodeResults ? geocodeResults.querySelector(".search-item.active") : null;
      if (active) {
        active.click();
      } else {
        clickFirstResult(geocodeResults);
      }
    }
    if (e.key === " " || e.key === "Spacebar") {
      const active = geocodeResults ? geocodeResults.querySelector(".search-item.active") : null;
      if (active && geocodeInput) {
        e.preventDefault();
        const value = active.dataset.value || active.dataset.unrestricted || "";
        geocodeInput.value = value;
      }
    }
    if (e.key === "Escape") {
      e.preventDefault();
      clearGeocodeResults();
    }
    if (e.key === "ArrowDown") {
      e.preventDefault();
      geocodeActiveIndex = setActiveResult(geocodeResults, geocodeActiveIndex + 1);
    }
    if (e.key === "ArrowUp") {
      e.preventDefault();
      geocodeActiveIndex = setActiveResult(geocodeResults, geocodeActiveIndex - 1);
    }
  });
}

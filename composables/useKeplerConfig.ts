export const useKeplerConfig = () => {
  const createKeplerConfig = (hexagons: any[], locations: any[], accounts: any[]) => {
    return {
      version: 'v1',
      config: {
        visState: {
          layers: [
            {
              id: 'hexagons',
              type: 'hexagon',
              config: {
                dataId: 'hexagons',
                label: 'H3 Hexagons',
                color: [34, 63, 224],
                columns: {
                  lat: 'lat',
                  lng: 'lng',
              hex: 'hex',
                },
                isVisible: true,
                visConfig: {
                  opacity: 0.8,
                  colorRange: {
                    name: 'Global Warming',
                    type: 'sequential',
                    category: 'Uber',
                    colors: ['#3288bd', '#66c2a5', '#abdda4', '#e6f598', '#fee090', '#fdae61', '#f46d43', '#d73027'],
                  },
                  sizeRange: [0, 500],
                  coverage: 1,
                  elevationScale: 1,
                  colorByFieldName: 'activeCount',
                  colorField: {
                    name: 'activeCount',
                    type: 'integer',
                  },
                  sizeByFieldName: 'activeCount',
                  sizeField: {
                    name: 'activeCount',
                    type: 'integer',
                  },
                },
              },
            },
            {
              id: 'locations',
              type: 'point',
              config: {
                dataId: 'locations',
                label: 'Settlements',
                color: [255, 0, 0],
                columns: {
                  lat: 'latitude',
                  lng: 'longitude',
                  altitude: null,
                },
                isVisible: true,
                visConfig: {
                  radius: 10,
                  fixedRadius: false,
                  opacity: 0.8,
                  outline: true,
                  thickness: 2,
                  colorRange: {
                    name: 'Global Warming',
                    type: 'sequential',
                    category: 'Uber',
                    colors: ['#3288bd', '#66c2a5', '#abdda4', '#e6f598', '#fee090', '#fdae61', '#f46d43', '#d73027'],
                  },
                  radiusRange: [0, 50],
                  colorByFieldName: 'activeCount',
                  colorField: {
                    name: 'activeCount',
                    type: 'integer',
                  },
                },
              },
            },
            {
              id: 'accounts',
              type: 'scatterplot',
              config: {
                dataId: 'accounts',
                label: 'Subscribers',
                color: [72, 150, 176],
                columns: {
                  lat: 'lat',
                  lng: 'lng',
                  altitude: null,
                },
                isVisible: true,
                visConfig: {
                  radius: 4,
                  fixedRadius: false,
                  opacity: 0.6,
                  outline: false,
                  thickness: 0.5,
                  colorRange: {
                    name: 'Viridis',
                    type: 'sequential',
                    category: 'Uber',
                    colors: ['#440154', '#482777', '#3b528b', '#2d708e', '#20908c', '#5ec962', '#fde724'],
                  },
                  radiusRange: [1, 20],
                  colorByFieldName: 'active',
                  colorField: {
                    name: 'active',
                    type: 'boolean',
                  },
                },
              },
            },
          ],
          filters: [],
          interactionConfig: {
            tooltip: {
              enabled: true,
              compareMode: false,
              compareType: 'absolute',
              fieldsToShow: {},
            },
            brush: {
              enabled: false,
            },
          },
          layerBlending: 'normal',
          splitMaps: [],
          animationConfig: {
            currentTime: null,
            speed: 1,
          },
        },
        mapState: {
          bearing: 0,
          dragRotate: false,
          latitude: 55.7558,
          longitude: 37.6173,
          pitch: 0,
          zoom: 4,
          isSplit: false,
        },
        mapStyle: {
          styleType: 'dark',
          topLayerGroups: {},
          visibleLayerGroups: {
            label: true,
            road: true,
            border: false,
            building: true,
            water: true,
            land: true,
            '3d building': false,
          },
          threeDBuildingColor: [9, 76, 128],
          mapStyles: [],
        },
      },
      datasets: [
        {
          version: 'v1',
          id: 'hexagons',
          label: 'H3 Hexagons',
          data: {
            fields: [
              { name: 'hex', type: 'string' },
              { name: 'lat', type: 'real' },
              { name: 'lng', type: 'real' },
              { name: 'activeCount', type: 'integer' },
              { name: 'blockedCount', type: 'integer' },
              { name: 'chargesCount', type: 'integer' },
              { name: 'chargesSum', type: 'real' },
              { name: 'paymentsCount', type: 'integer' },
              { name: 'paymentsSum', type: 'real' },
            ],
            rows: hexagons.map((h) => [
              h.hex,
              h.lat,
              h.lng,
              h.activeCount,
              h.blockedCount,
              h.chargesCount,
              h.chargesSum,
              h.paymentsCount,
              h.paymentsSum,
            ]),
          },
        },
        {
          version: 'v1',
          id: 'locations',
          label: 'Settlements',
          data: {
            fields: [
              { name: 'id', type: 'string' },
              { name: 'title', type: 'string' },
              { name: 'latitude', type: 'real' },
              { name: 'longitude', type: 'real' },
              { name: 'activeCount', type: 'integer' },
              { name: 'blockedCount', type: 'integer' },
              { name: 'chargesCount', type: 'integer' },
              { name: 'chargesSum', type: 'real' },
              { name: 'paymentsCount', type: 'integer' },
              { name: 'paymentsSum', type: 'real' },
            ],
            rows: locations.map((l) => [
              l.id,
              l.title,
              l.latitude,
              l.longitude,
              l.activeCount,
              l.blockedCount,
              l.chargesCount,
              l.chargesSum,
              l.paymentsCount,
              l.paymentsSum,
            ]),
          },
        },
        {
          version: 'v1',
          id: 'accounts',
          label: 'Subscribers',
          data: {
            fields: [
              { name: 'lat', type: 'real' },
              { name: 'lng', type: 'real' },
              { name: 'active', type: 'boolean' },
              { name: 'blocked', type: 'boolean' },
              { name: 'chargesCount', type: 'integer' },
              { name: 'chargesSum', type: 'real' },
              { name: 'paymentsCount', type: 'integer' },
              { name: 'paymentsSum', type: 'real' },
            ],
            rows: accounts.map((a) => [
              a.lat,
              a.lng,
              a.active,
              a.blocked,
              a.chargesCount,
              a.chargesSum,
              a.paymentsCount,
              a.paymentsSum,
            ]),
          },
        },
      ],
    };
  };

  const getKeplerHTML = (config: any) => {
    const configStr = JSON.stringify(config).replace(/'/g, "\\'");
    return `
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Customer Map</title>
  <script src="https://uber.github.io/deck.gl/script/kepler.gl.js"></script>
  <style>
    body { margin: 0; padding: 0; }
    #app { width: 100%; height: 100vh; }
  </style>
</head>
<body>
  <div id="app"></div>
  <script type="text/javascript">
    KeplerGl.createKeplerGlHTML(
      document.getElementById('app'),
      '${configStr}'
    );
  </script>
</body>
</html>
    `;
  };

  return {
    createKeplerConfig,
    getKeplerHTML,
  };
};

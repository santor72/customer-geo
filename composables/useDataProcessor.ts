import { latLngToCell, cellToLatLng, gridDisk } from 'h3-js';
import type { DataSet } from './useDataLoader';

export interface HexagonData {
  hex: string;
  lat: number;
  lng: number;
  activeCount: number;
  blockedCount: number;
  chargesCount: number;
  chargesSum: number;
  paymentsCount: number;
  paymentsSum: number;
}

export interface LocationSummary {
  id: string;
  title: string;
  latitude: number;
  longitude: number;
  activeCount: number;
  blockedCount: number;
  chargesCount: number;
  chargesSum: number;
  paymentsCount: number;
  paymentsSum: number;
}

export interface AccountData {
  account_id: string;
  lat: number;
  lng: number;
  active: boolean;
  blocked: boolean;
  chargesCount: number;
  chargesSum: number;
  paymentsCount: number;
  paymentsSum: number;
}

export const useDataProcessor = () => {
  const processHexagons = (
    data: DataSet,
    accountCoordinates: Map<string, { lat: number; lng: number }>,
    resolution: number = 5
  ): HexagonData[] => {
    const hexagons = new Map<string, HexagonData>();
    const activeSet = new Set(data.active.map((a) => a.account_id));
    const blockedSet = new Set(data.blocked.map((b) => b.account_id));
    const chargesMap = new Map<string, { count: number; sum: number }>();
    const paymentsMap = new Map<string, { count: number; sum: number }>();

    data.charges.forEach((charge) => {
      const current = chargesMap.get(charge.account_id) || { count: 0, sum: 0 };
      current.count += 1;
      current.sum += charge.discount;
      chargesMap.set(charge.account_id, current);
    });

    const validPaymentMethods = [6553998, 103, 104];
    data.payments.forEach((payment) => {
      if (validPaymentMethods.includes(payment.method)) {
        const current = paymentsMap.get(payment.account_id) || { count: 0, sum: 0 };
        current.count += 1;
        current.sum += payment.payment_absolute;
        paymentsMap.set(payment.account_id, current);
      }
    });

    accountCoordinates.forEach((coords, accountId) => {
      const hex = latLngToCell(coords.lat, coords.lng, resolution);

      let hexData = hexagons.get(hex);
      if (!hexData) {
        const [lat, lng] = cellToLatLng(hex);
        hexData = {
          hex,
          lat,
          lng,
          activeCount: 0,
          blockedCount: 0,
          chargesCount: 0,
          chargesSum: 0,
          paymentsCount: 0,
          paymentsSum: 0,
        };
        hexagons.set(hex, hexData);
      }

      if (activeSet.has(accountId)) hexData.activeCount++;
      if (blockedSet.has(accountId)) hexData.blockedCount++;

      const charges = chargesMap.get(accountId);
      if (charges) {
        hexData.chargesCount += charges.count;
        hexData.chargesSum += charges.sum;
      }

      const payments = paymentsMap.get(accountId);
      if (payments) {
        hexData.paymentsCount += payments.count;
        hexData.paymentsSum += payments.sum;
      }
    });

    return Array.from(hexagons.values());
  };

  const processLocationSummaries = (
    data: DataSet,
    accountLocationMap: Map<string, string>
  ): LocationSummary[] => {
    const activeSet = new Set(data.active.map((a) => a.account_id));
    const blockedSet = new Set(data.blocked.map((b) => b.account_id));
    const chargesMap = new Map<string, { count: number; sum: number }>();
    const paymentsMap = new Map<string, { count: number; sum: number }>();

    data.charges.forEach((charge) => {
      const current = chargesMap.get(charge.account_id) || { count: 0, sum: 0 };
      current.count += 1;
      current.sum += charge.discount;
      chargesMap.set(charge.account_id, current);
    });

    const validPaymentMethods = [6553998, 103, 104];
    data.payments.forEach((payment) => {
      if (validPaymentMethods.includes(payment.method)) {
        const current = paymentsMap.get(payment.account_id) || { count: 0, sum: 0 };
        current.count += 1;
        current.sum += payment.payment_absolute;
        paymentsMap.set(payment.account_id, current);
      }
    });

    const locationStats = new Map<string, LocationSummary>();

    accountLocationMap.forEach((locationId, accountId) => {
      const location = data.locations.find((l) => l.id === locationId);
      if (!location) return;

      let stats = locationStats.get(locationId);
      if (!stats) {
        stats = {
          id: location.id,
          title: location.title,
          latitude: location.latitude,
          longitude: location.longitude,
          activeCount: 0,
          blockedCount: 0,
          chargesCount: 0,
          chargesSum: 0,
          paymentsCount: 0,
          paymentsSum: 0,
        };
        locationStats.set(locationId, stats);
      }

      if (activeSet.has(accountId)) stats.activeCount++;
      if (blockedSet.has(accountId)) stats.blockedCount++;

      const charges = chargesMap.get(accountId);
      if (charges) {
        stats.chargesCount += charges.count;
        stats.chargesSum += charges.sum;
      }

      const payments = paymentsMap.get(accountId);
      if (payments) {
        stats.paymentsCount += payments.count;
        stats.paymentsSum += payments.sum;
      }
    });

    return Array.from(locationStats.values());
  };

  return {
    processHexagons,
    processLocationSummaries,
  };
};

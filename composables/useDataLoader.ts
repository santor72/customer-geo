export interface CustomerActive {
  account_id: string;
  recv_mon: string;
}

export interface CustomerBlocked {
  account_id: string;
  recv_mon: string;
}

export interface Location {
  id: string;
  title: string;
  latitude: number;
  longitude: number;
}

export interface Charge {
  account_id: string;
  discount: number;
  discount_date: number;
}

export interface Payment {
  account_id: string;
  payment_absolute: number;
  actual_date: number;
  method: number;
}

export interface DataSet {
  active: CustomerActive[];
  blocked: CustomerBlocked[];
  locations: Location[];
  charges: Charge[];
  payments: Payment[];
}

export const useDataLoader = () => {
  const loadData = async (monthDate: string): Promise<DataSet> => {
    const dataset: DataSet = {
      active: [],
      blocked: [],
      locations: [],
      charges: [],
      payments: [],
    };

    try {
      const active = await fetch(`/json_data/${monthDate}/point_customer_active.${monthDate}.json`);
      if (active.ok) dataset.active = await active.json();
    } catch (e) {
      console.warn(`Could not load active customers for ${monthDate}`);
    }

    try {
      const blocked = await fetch(`/json_data/${monthDate}/point_customer_sysblock.${monthDate}.json`);
      if (blocked.ok) dataset.blocked = await blocked.json();
    } catch (e) {
      console.warn(`Could not load blocked customers for ${monthDate}`);
    }

    try {
      const locations = await fetch(`/json_data/${monthDate}/bx24_location.${monthDate}.json`);
      if (locations.ok) {
        const raw = await locations.json();
        dataset.locations = raw.map((loc: any) => ({
          id: loc.id,
          title: loc.title,
          latitude: loc.ufCrm11_1732803360,
          longitude: loc.ufCrm11_1732783301,
        }));
      }
    } catch (e) {
      console.warn(`Could not load locations for ${monthDate}`);
    }

    try {
      const charges = await fetch(`/json_data/${monthDate}/dtall.${monthDate}.json`);
      if (charges.ok) dataset.charges = await charges.json();
    } catch (e) {
      console.warn(`Could not load charges for ${monthDate}`);
    }

    try {
      const payments = await fetch(`/json_data/${monthDate}/pt.${monthDate}.json`);
      if (payments.ok) dataset.payments = await payments.json();
    } catch (e) {
      console.warn(`Could not load payments for ${monthDate}`);
    }

    return dataset;
  };

  const getAvailableMonths = async (): Promise<string[]> => {
    try {
      const response = await fetch('/json_data');
      const html = await response.text();
      const folders = html.match(/href="([0-9]{4}-[0-9]{2}-[0-9]{2})\//g) || [];
      return folders
        .map((f) => f.replace(/href="([^"]+)\//g, '$1'))
        .sort()
        .reverse();
    } catch (e) {
      console.warn('Could not fetch available months');
      return [];
    }
  };

  return {
    loadData,
    getAvailableMonths,
  };
};

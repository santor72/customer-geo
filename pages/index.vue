<template>
  <div class="space-y-6">
    <div class="bg-white rounded-lg shadow-md p-6">
      <div class="flex flex-col md:flex-row md:items-end md:gap-4 gap-4">
        <div class="flex-1">
          <label class="block text-sm font-semibold text-gray-700 mb-2">Select Month</label>
          <select
            v-model="selectedMonth"
            @change="loadMapData"
            class="w-full px-4 py-3 border-2 border-gray-300 rounded-lg focus:outline-none focus:border-blue-500 transition-colors"
          >
            <option value="">-- Choose a month --</option>
            <option v-for="month in availableMonths" :key="month" :value="month">
              {{ formatDate(month) }}
            </option>
          </select>
        </div>

        <div v-if="loading" class="flex items-center gap-2 text-blue-600">
          <div class="animate-spin h-5 w-5 border-2 border-blue-600 border-t-transparent rounded-full"></div>
          <span>Loading...</span>
        </div>

        <div v-if="error" class="text-red-600 text-sm">{{ error }}</div>
      </div>
    </div>

    <div v-if="selectedMonth && !loading" class="grid grid-cols-1 md:grid-cols-2 gap-4">
      <div class="bg-white rounded-lg shadow-md p-4">
        <h3 class="text-lg font-semibold text-gray-900 mb-3">Active Subscribers</h3>
        <p class="text-3xl font-bold text-green-600">{{ stats.activeCount }}</p>
      </div>

      <div class="bg-white rounded-lg shadow-md p-4">
        <h3 class="text-lg font-semibold text-gray-900 mb-3">Blocked Subscribers</h3>
        <p class="text-3xl font-bold text-red-600">{{ stats.blockedCount }}</p>
      </div>

      <div class="bg-white rounded-lg shadow-md p-4">
        <h3 class="text-lg font-semibold text-gray-900 mb-3">Total Charges</h3>
        <p class="text-3xl font-bold text-orange-600">{{ stats.chargesSum.toFixed(2) }}</p>
        <p class="text-sm text-gray-500 mt-1">{{ stats.chargesCount }} transactions</p>
      </div>

      <div class="bg-white rounded-lg shadow-md p-4">
        <h3 class="text-lg font-semibold text-gray-900 mb-3">Total Payments</h3>
        <p class="text-3xl font-bold text-blue-600">{{ stats.paymentsSum.toFixed(2) }}</p>
        <p class="text-sm text-gray-500 mt-1">{{ stats.paymentsCount }} transactions</p>
      </div>
    </div>

    <div v-if="selectedMonth && !loading" class="bg-white rounded-lg shadow-md p-6 h-screen">
      <h3 class="text-lg font-semibold text-gray-900 mb-4">Interactive Map</h3>
      <div class="h-full">
        <iframe
          v-if="mapHTML"
          :srcDoc="mapHTML"
          class="w-full h-full border-2 border-gray-200 rounded-lg"
          frameborder="0"
          allow="geolocation"
        ></iframe>
        <div v-else class="flex items-center justify-center h-full bg-gray-100 rounded-lg">
          <p class="text-gray-500">Loading map...</p>
        </div>
      </div>
    </div>

    <div v-if="selectedMonth && !loading && locations.length > 0" class="bg-white rounded-lg shadow-md p-6">
      <h3 class="text-lg font-semibold text-gray-900 mb-4">Settlement Statistics</h3>
      <div class="overflow-x-auto">
        <table class="min-w-full divide-y divide-gray-200">
          <thead class="bg-gray-50">
            <tr>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-700 uppercase">Settlement</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-700 uppercase">Active</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-700 uppercase">Blocked</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-700 uppercase">Charges</th>
              <th class="px-6 py-3 text-left text-xs font-medium text-gray-700 uppercase">Payments</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-gray-200">
            <tr v-for="loc in locations" :key="loc.id" class="hover:bg-gray-50">
              <td class="px-6 py-4 text-sm text-gray-900 font-medium">{{ loc.title }}</td>
              <td class="px-6 py-4 text-sm text-gray-600">{{ loc.activeCount }}</td>
              <td class="px-6 py-4 text-sm text-gray-600">{{ loc.blockedCount }}</td>
              <td class="px-6 py-4 text-sm text-gray-600">{{ loc.chargesSum.toFixed(2) }}</td>
              <td class="px-6 py-4 text-sm text-gray-600">{{ loc.paymentsSum.toFixed(2) }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue';
import { useDataLoader } from '~/composables/useDataLoader';
import { useDataProcessor } from '~/composables/useDataProcessor';
import { useKeplerConfig } from '~/composables/useKeplerConfig';

const { loadData, getAvailableMonths } = useDataLoader();
const { processHexagons, processLocationSummaries } = useDataProcessor();
const { createKeplerConfig, getKeplerHTML } = useKeplerConfig();

const selectedMonth = ref('');
const availableMonths = ref<string[]>([]);
const loading = ref(false);
const error = ref('');
const mapHTML = ref('');
const locations = ref<any[]>([]);
const stats = ref({
  activeCount: 0,
  blockedCount: 0,
  chargesSum: 0,
  chargesCount: 0,
  paymentsSum: 0,
  paymentsCount: 0,
});

const formatDate = (dateStr: string) => {
  try {
    const [year, month, day] = dateStr.split('-');
    return new Date(parseInt(year), parseInt(month) - 1, parseInt(day)).toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'long',
      day: 'numeric',
    });
  } catch {
    return dateStr;
  }
};

const loadMapData = async () => {
  if (!selectedMonth.value) return;

  loading.value = true;
  error.value = '';
  mapHTML.value = '';

  try {
    const dataset = await loadData(selectedMonth.value);

    const activeSet = new Set(dataset.active.map((a) => a.account_id));
    const blockedSet = new Set(dataset.blocked.map((b) => b.account_id));
    const chargesMap = new Map<string, { count: number; sum: number }>();
    const paymentsMap = new Map<string, { count: number; sum: number }>();

    dataset.charges.forEach((charge) => {
      const current = chargesMap.get(charge.account_id) || { count: 0, sum: 0 };
      current.count += 1;
      current.sum += charge.discount;
      chargesMap.set(charge.account_id, current);
    });

    const validPaymentMethods = [6553998, 103, 104];
    dataset.payments.forEach((payment) => {
      if (validPaymentMethods.includes(payment.method)) {
        const current = paymentsMap.get(payment.account_id) || { count: 0, sum: 0 };
        current.count += 1;
        current.sum += payment.payment_absolute;
        paymentsMap.set(payment.account_id, current);
      }
    });

    stats.value = {
      activeCount: dataset.active.length,
      blockedCount: dataset.blocked.length,
      chargesSum: Array.from(chargesMap.values()).reduce((sum, c) => sum + c.sum, 0),
      chargesCount: Array.from(chargesMap.values()).reduce((sum, c) => sum + c.count, 0),
      paymentsSum: Array.from(paymentsMap.values()).reduce((sum, c) => sum + c.sum, 0),
      paymentsCount: Array.from(paymentsMap.values()).reduce((sum, c) => sum + c.count, 0),
    };

    locations.value = processLocationSummaries(
      dataset,
      new Map<string, string>()
    );

    const keplerConfig = createKeplerConfig([], locations.value, []);
    mapHTML.value = getKeplerHTML(keplerConfig);
  } catch (err) {
    error.value = `Failed to load data: ${err instanceof Error ? err.message : 'Unknown error'}`;
    console.error(err);
  } finally {
    loading.value = false;
  }
};

onMounted(async () => {
  availableMonths.value = await getAvailableMonths();
  if (availableMonths.value.length > 0) {
    selectedMonth.value = availableMonths.value[0];
    await loadMapData();
  }
});
</script>

<style scoped>
</style>

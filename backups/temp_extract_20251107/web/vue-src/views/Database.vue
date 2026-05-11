<template>
  <div class="space-y-6">
    <!-- Заголовок -->
    <div class="bg-white shadow rounded-lg p-6">
      <h1 class="text-2xl font-bold text-gray-900 mb-2">База данных</h1>
      <p class="text-gray-600">Управление и просмотр данных в базе</p>
    </div>

    <!-- Статистика БД -->
    <div class="grid grid-cols-1 md:grid-cols-4 gap-6">
      <div class="card">
        <div class="flex items-center">
          <div class="flex-shrink-0">
            <div class="w-8 h-8 bg-blue-500 rounded-lg flex items-center justify-center">
              <svg class="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4"></path>
              </svg>
            </div>
          </div>
          <div class="ml-4">
            <p class="text-sm font-medium text-gray-500">Таблиц</p>
            <p class="text-2xl font-semibold text-gray-900">{{ dbStats.tables || 0 }}</p>
          </div>
        </div>
      </div>

      <div class="card">
        <div class="flex items-center">
          <div class="flex-shrink-0">
            <div class="w-8 h-8 bg-green-500 rounded-lg flex items-center justify-center">
              <svg class="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"></path>
              </svg>
            </div>
          </div>
          <div class="ml-4">
            <p class="text-sm font-medium text-gray-500">Записей</p>
            <p class="text-2xl font-semibold text-gray-900">{{ dbStats.records || 0 }}</p>
          </div>
        </div>
      </div>

      <div class="card">
        <div class="flex items-center">
          <div class="flex-shrink-0">
            <div class="w-8 h-8 bg-purple-500 rounded-lg flex items-center justify-center">
              <svg class="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4"></path>
              </svg>
            </div>
          </div>
          <div class="ml-4">
            <p class="text-sm font-medium text-gray-500">Размер</p>
            <p class="text-2xl font-semibold text-gray-900">{{ formatSize(dbStats.size) }}</p>
          </div>
        </div>
      </div>

      <div class="card">
        <div class="flex items-center">
          <div class="flex-shrink-0">
            <div class="w-8 h-8 bg-yellow-500 rounded-lg flex items-center justify-center">
              <svg class="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"></path>
              </svg>
            </div>
          </div>
          <div class="ml-4">
            <p class="text-sm font-medium text-gray-500">Обновлено</p>
            <p class="text-2xl font-semibold text-gray-900">{{ formatTime(dbStats.lastUpdate) }}</p>
          </div>
        </div>
      </div>
    </div>

    <!-- Список таблиц -->
    <div class="card">
      <div class="flex justify-between items-center mb-4">
        <h2 class="text-xl font-semibold text-gray-900">Таблицы базы данных</h2>
        <button 
          @click="refreshDbStats"
          class="btn-secondary"
          :disabled="loading"
        >
          <svg v-if="loading" class="animate-spin -ml-1 mr-2 h-4 w-4 text-gray-800" fill="none" viewBox="0 0 24 24">
            <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
            <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
          </svg>
          {{ loading ? 'Обновление...' : 'Обновить' }}
        </button>
      </div>

      <div class="table-container">
        <table class="table">
          <thead>
            <tr>
              <th>Таблица</th>
              <th>Количество записей</th>
              <th>Размер</th>
              <th>Последнее обновление</th>
              <th>Действия</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="table in tables" :key="table.name" class="hover:bg-gray-50">
              <td class="font-medium text-gray-900">
                <div>
                  <div class="font-semibold">{{ table.displayName || table.name }}</div>
                  <div class="text-sm text-gray-500">{{ table.name }}</div>
                </div>
              </td>
              <td class="text-gray-900">{{ table.records }}</td>
              <td class="text-gray-900">{{ formatSize(table.size) }}</td>
              <td class="text-gray-500">{{ formatTime(table.lastUpdate) }}</td>
              <td>
                <div class="flex space-x-2">
                  <button 
                    @click="viewTable(table.name)"
                    class="text-primary-600 hover:text-primary-800 text-sm font-medium"
                  >
                    Просмотр
                  </button>
                  <button 
                    @click="exportTable(table.name)"
                    class="text-green-600 hover:text-green-800 text-sm font-medium"
                  >
                    Экспорт
                  </button>
                </div>
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <!-- Пустое состояние -->
      <div v-if="tables.length === 0 && !loading" class="text-center py-12">
        <svg class="mx-auto h-12 w-12 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4"></path>
        </svg>
        <h3 class="mt-2 text-sm font-medium text-gray-900">Таблицы не найдены</h3>
        <p class="mt-1 text-sm text-gray-500">Попробуйте обновить данные</p>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import axios from 'axios'

const dbStats = ref({
  tables: 0,
  records: 0,
  size: 0,
  lastUpdate: new Date()
})

const tables = ref([])
const loading = ref(false)

const refreshDbStats = async () => {
  loading.value = true
  try {
    console.log('Обновление статистики БД...')
    const response = await axios.get('/api/vue/database')
    dbStats.value = response.data.stats
    tables.value = response.data.tables
  } catch (error) {
    console.error('Ошибка при получении статистики БД:', error)
  } finally {
    loading.value = false
  }
}

const formatSize = (bytes) => {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
}

const formatTime = (timestamp) => {
  if (!timestamp) return 'Неизвестно'
  return new Date(timestamp).toLocaleString('ru-RU')
}

const viewTable = (tableName) => {
  console.log('Просмотр таблицы:', tableName)
  // Здесь будет переход к просмотру таблицы
}

const exportTable = (tableName) => {
  console.log('Экспорт таблицы:', tableName)
  // Здесь будет экспорт таблицы
}

onMounted(() => {
  refreshDbStats()
})
</script>

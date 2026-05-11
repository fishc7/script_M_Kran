<template>
  <div class="space-y-6">
    <!-- Заголовок -->
    <div class="bg-white shadow rounded-lg p-6">
      <h1 class="text-2xl font-bold text-gray-900 mb-2">Системные логи</h1>
      <p class="text-gray-600">Просмотр и анализ системных логов</p>
    </div>

    <!-- Фильтры -->
    <div class="card">
      <div class="flex flex-wrap gap-4 items-center">
        <div class="flex-1 min-w-0">
          <label class="block text-sm font-medium text-gray-700 mb-1">Поиск</label>
          <input 
            v-model="searchQuery" 
            type="text" 
            placeholder="Поиск в логах..."
            class="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent"
          >
        </div>
        
        <div>
          <label class="block text-sm font-medium text-gray-700 mb-1">Уровень</label>
          <select 
            v-model="selectedLevel"
            class="px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent"
          >
            <option value="">Все уровни</option>
            <option value="INFO">INFO</option>
            <option value="WARNING">WARNING</option>
            <option value="ERROR">ERROR</option>
            <option value="DEBUG">DEBUG</option>
          </select>
        </div>

        <div>
          <label class="block text-sm font-medium text-gray-700 mb-1">Количество</label>
          <select 
            v-model="logLimit"
            class="px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-transparent"
          >
            <option value="50">50</option>
            <option value="100">100</option>
            <option value="200">200</option>
            <option value="500">500</option>
          </select>
        </div>

        <div class="flex items-end">
          <button 
            @click="loadLogs"
            class="btn-primary"
            :disabled="loading"
          >
            <svg v-if="loading" class="animate-spin -ml-1 mr-2 h-4 w-4 text-white" fill="none" viewBox="0 0 24 24">
              <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
              <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
            </svg>
            {{ loading ? 'Загрузка...' : 'Обновить' }}
          </button>
        </div>
      </div>
    </div>

    <!-- Таблица логов -->
    <div class="card">
      <div class="table-container">
        <table class="table">
          <thead>
            <tr>
              <th>Время</th>
              <th>Уровень</th>
              <th>Модуль</th>
              <th>Сообщение</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="log in filteredLogs" :key="log.id" class="hover:bg-gray-50">
              <td class="text-sm text-gray-500">{{ formatTime(log.timestamp) }}</td>
              <td>
                <span 
                  :class="getLevelClass(log.level)"
                  class="px-2 py-1 text-xs font-medium rounded-full"
                >
                  {{ log.level }}
                </span>
              </td>
              <td class="text-sm text-gray-900">{{ log.module }}</td>
              <td class="text-sm text-gray-900 max-w-md truncate">{{ log.message }}</td>
            </tr>
          </tbody>
        </table>
      </div>

      <!-- Пустое состояние -->
      <div v-if="logs.length === 0 && !loading" class="text-center py-12">
        <svg class="mx-auto h-12 w-12 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"></path>
        </svg>
        <h3 class="mt-2 text-sm font-medium text-gray-900">Логи не найдены</h3>
        <p class="mt-1 text-sm text-gray-500">Попробуйте изменить фильтры или обновить данные</p>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import axios from 'axios'

const logs = ref([])
const loading = ref(false)
const searchQuery = ref('')
const selectedLevel = ref('')
const logLimit = ref('100')

const filteredLogs = computed(() => {
  let filtered = logs.value

  if (searchQuery.value) {
    const query = searchQuery.value.toLowerCase()
    filtered = filtered.filter(log => 
      log.message.toLowerCase().includes(query) ||
      log.module.toLowerCase().includes(query)
    )
  }

  if (selectedLevel.value) {
    filtered = filtered.filter(log => log.level === selectedLevel.value)
  }

  return filtered
})

const loadLogs = async () => {
  loading.value = true
  try {
    console.log('Загрузка логов...')
    const response = await axios.get(`/api/vue/logs?limit=${logLimit.value}`)
    logs.value = response.data
  } catch (error) {
    console.error('Ошибка при загрузке логов:', error)
  } finally {
    loading.value = false
  }
}

const formatTime = (timestamp) => {
  return new Date(timestamp).toLocaleString('ru-RU')
}

const getLevelClass = (level) => {
  switch (level) {
    case 'ERROR':
      return 'bg-red-100 text-red-800'
    case 'WARNING':
      return 'bg-yellow-100 text-yellow-800'
    case 'INFO':
      return 'bg-blue-100 text-blue-800'
    case 'DEBUG':
      return 'bg-gray-100 text-gray-800'
    default:
      return 'bg-gray-100 text-gray-800'
  }
}

onMounted(() => {
  loadLogs()
})
</script>

/**
 * Серверная фильтрация таблицы по столбцам
 * Отправляет запросы на сервер для фильтрации данных
 */

class ServerColumnFilters {
    constructor() {
        this.form = document.getElementById('columnFiltersForm');
        this.filterInputs = document.querySelectorAll('.column-filter');
        this.clearButtons = document.querySelectorAll('.clear-column-filter');
        this.clearAllButton = document.getElementById('clearAllFilters');
        this.submitButton = this.form.querySelector('button[type="submit"]');
        this.debounceTimer = null;
        
        this.init();
    }
    
    init() {
        this.bindEvents();
        this.updateStats();
    }
    
    bindEvents() {
        // Обработчики для полей фильтрации - автоматическая фильтрация при вводе
        this.filterInputs.forEach(input => {
            // Основной обработчик для автоматической фильтрации
            input.addEventListener('input', (e) => {
                this.handleFilterChange(e);
            });
            
            // Дополнительный обработчик для изменения значения
            input.addEventListener('change', (e) => {
                this.handleFilterChange(e);
            });
            
            // Обработчик для вставки текста
            input.addEventListener('paste', (e) => {
                setTimeout(() => {
                    this.handleFilterChange(e);
                }, 10);
            });
        });
        
        // Обработчики для кнопок очистки отдельных фильтров
        this.clearButtons.forEach(button => {
            button.addEventListener('click', (e) => {
                const column = e.target.closest('button').dataset.column;
                this.clearColumnFilter(column);
            });
        });
        
        // Обработчик для кнопки очистки всех фильтров
        if (this.clearAllButton) {
            this.clearAllButton.addEventListener('click', () => {
                this.clearAllFilters();
            });
        }
        
        // Обработчики для пагинации (AJAX)
        this.bindPaginationEvents();
    }
    
    bindPaginationEvents() {
        // Делегирование событий для пагинации
        document.addEventListener('click', (e) => {
            if (e.target.closest('.pagination .page-link')) {
                e.preventDefault();
                const link = e.target.closest('.page-link');
                const href = link.getAttribute('href');
                if (href) {
                    this.loadPage(href);
                }
            }
        });
    }
    
    loadPage(url) {
        // Показываем индикатор загрузки
        this.showLoadingIndicator();
        
        fetch(url)
            .then(response => response.text())
            .then(html => {
                // Обновляем только содержимое таблицы
                this.updateTableContent(html);
                // Обновляем URL без перезагрузки
                window.history.pushState({}, '', url);
                // Скрываем индикатор загрузки
                this.hideLoadingIndicator();
            })
            .catch(error => {
                console.error('Ошибка загрузки страницы:', error);
                this.hideLoadingIndicator();
            });
    }
    
    handleFilterChange(event) {
        const input = event.target;
        const column = input.name.replace('filter_', '');
        
        console.log('Фильтрация изменена:', column, '=', input.value);
        
        // Автоматически отправляем запрос при вводе (с небольшой задержкой)
        clearTimeout(this.debounceTimer);
        this.debounceTimer = setTimeout(() => {
            console.log('Отправляем фильтры...');
            this.submitFilters();
        }, 200); // Задержка 200мс для оптимизации запросов
    }
    
    submitFilters() {
        // Собираем все значения фильтров
        const formData = new FormData(this.form);
        const params = new URLSearchParams();
        
        // Добавляем все параметры формы
        for (let [key, value] of formData.entries()) {
            if (value.trim()) { // Добавляем только непустые значения
                params.append(key, value.trim());
            }
        }
        
        // Сбрасываем на первую страницу только если есть новые фильтры
        const currentUrl = new URL(window.location);
        const currentParams = new URLSearchParams(currentUrl.search);
        const hasNewFilters = Array.from(params.entries()).some(([key, value]) => {
            return currentParams.get(key) !== value;
        });
        
        if (hasNewFilters) {
            params.set('page', '1');
        } else {
            // Сохраняем текущую страницу
            const currentPage = currentParams.get('page');
            if (currentPage) {
                params.set('page', currentPage);
            }
        }
        
        // Используем AJAX для фильтрации без перезагрузки страницы
        this.performAjaxFilter(params);
    }
    
    performAjaxFilter(params) {
        const currentUrl = new URL(window.location);
        const url = `${currentUrl.pathname}?${params.toString()}`;
        
        console.log('AJAX запрос:', url);
        console.log('Параметры фильтрации:', params.toString());
        
        // Показываем индикатор загрузки
        this.showLoadingIndicator();
        
        fetch(url)
            .then(response => {
                console.log('Ответ получен:', response.status);
                return response.text();
            })
            .then(html => {
                console.log('HTML получен, обновляем таблицу...');
                
                // Проверяем количество записей в ответе
                const tempDiv = document.createElement('div');
                tempDiv.innerHTML = html;
                const rows = tempDiv.querySelectorAll('#dataTable tbody tr');
                console.log('Количество записей в ответе:', rows.length);
                
                // Обновляем только содержимое таблицы
                this.updateTableContent(html);
                // Обновляем URL без перезагрузки
                window.history.pushState({}, '', url);
                // Скрываем индикатор загрузки
                this.hideLoadingIndicator();
            })
            .catch(error => {
                console.error('Ошибка фильтрации:', error);
                this.hideLoadingIndicator();
            });
    }
    
    updateTableContent(html) {
        // Создаем временный элемент для парсинга HTML
        const tempDiv = document.createElement('div');
        tempDiv.innerHTML = html;
        
        // Находим новую таблицу
        const newTable = tempDiv.querySelector('#dataTable');
        const currentTable = document.querySelector('#dataTable');
        
        if (newTable && currentTable) {
            // Обновляем содержимое таблицы
            currentTable.innerHTML = newTable.innerHTML;
        }
        
        // Обновляем статистику
        const newStats = tempDiv.querySelector('#filteredCount');
        const currentStats = document.querySelector('#filteredCount');
        
        if (newStats && currentStats) {
            currentStats.textContent = newStats.textContent;
        }
        
        // Обновляем пагинацию
        const newPagination = tempDiv.querySelector('.pagination');
        const currentPagination = document.querySelector('.pagination');
        
        if (newPagination && currentPagination) {
            currentPagination.innerHTML = newPagination.innerHTML;
        }
    }
    
    showLoadingIndicator() {
        // Показываем простой индикатор загрузки
        const table = document.querySelector('#dataTable');
        if (table) {
            table.style.opacity = '0.6';
        }
    }
    
    hideLoadingIndicator() {
        // Скрываем индикатор загрузки
        const table = document.querySelector('#dataTable');
        if (table) {
            table.style.opacity = '1';
        }
    }
    
    clearColumnFilter(column) {
        const input = document.querySelector(`input[name="filter_${column}"]`);
        if (input) {
            input.value = '';
            this.submitFilters();
        }
    }
    
    clearAllFilters() {
        // Очищаем все поля ввода
        this.filterInputs.forEach(input => {
            input.value = '';
        });
        
        // Отправляем запрос без фильтров
        this.submitFilters();
    }
    

    
    updateStats() {
        const statsElement = document.getElementById('filteredCount');
        if (statsElement) {
            // Статистика обновляется сервером
            console.log('Статистика фильтрации обновлена');
        }
    }
}

// Инициализация при загрузке страницы
document.addEventListener('DOMContentLoaded', function() {
    // Проверяем, есть ли форма фильтрации на странице
    const form = document.getElementById('columnFiltersForm');
    if (form) {
        window.serverColumnFilters = new ServerColumnFilters();
        console.log('Серверная фильтрация по столбцам инициализирована');
    }
});

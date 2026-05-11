/**
 * Универсальный модуль фильтрации таблиц
 * Автоматически добавляет функциональность поиска в реальном времени ко всем таблицам
 */

// Переменная для хранения оригинальных данных таблицы
let originalTableData = [];

/**
 * Инициализация фильтрации для всех таблиц на странице
 */
function initializeTableFilters() {
    console.log('🔍 Инициализация фильтрации таблиц...');
    
    // Проверяем, есть ли на странице специальные поля поиска
    const hasCustomSearch = document.getElementById('search-input') || 
                           document.querySelector('input[id*="search"]') ||
                           document.querySelector('.search-input') ||
                           document.querySelector('input[name="search"]') ||
                           document.querySelector('.search-box') ||
                           document.querySelector('form[id="searchForm"]');
    
    if (hasCustomSearch) {
        console.log('🔍 Обнаружены специальные поля поиска, пропускаем автоматическую инициализацию');
        return;
    }
    
    // Находим все таблицы на странице
    const tables = document.querySelectorAll('table');
    console.log(`📊 Найдено таблиц: ${tables.length}`);
    
    tables.forEach((table, tableIndex) => {
        const tableId = table.id || `table-${tableIndex}`;
        console.log(`📊 Обрабатываем таблицу: ${tableId}`);
        
        // Проверяем, есть ли уже поле поиска для этой таблицы
        const existingFilter = document.getElementById(`${tableId}Filter`) || 
                              document.getElementById('tableFilter') ||
                              document.querySelector(`input[id*="Filter"]`);
        
        if (existingFilter) {
            console.log(`✅ Поле поиска уже существует для таблицы ${tableId}`);
            // Если поле поиска уже существует, просто инициализируем логику фильтрации
            initializeFilterForTable(table, tableId);
        } else {
            console.log(`🆕 Создаем поле поиска для таблицы ${tableId}`);
            // Создаем поле поиска только если его нет
            createSearchField(table, tableId);
            
            // Инициализируем фильтрацию
            initializeFilterForTable(table, tableId);
        }
    });
}

/**
 * Создает поле поиска для таблицы
 */
function createSearchField(table, tableId) {
    // Дополнительная проверка на наличие специальных полей поиска
    const hasCustomSearch = document.getElementById('search-input') || 
                           document.querySelector('input[id*="search"]') ||
                           document.querySelector('.search-input') ||
                           document.querySelector('input[name="search"]') ||
                           document.querySelector('.search-box') ||
                           document.querySelector('form[id="searchForm"]');
    
    if (hasCustomSearch) {
        console.log(`🔍 Обнаружены специальные поля поиска, пропускаем создание поля для таблицы ${tableId}`);
        return;
    }
    
    // Ищем контейнер таблицы
    let container = table.closest('.card-body') || table.closest('.table-responsive') || table.parentElement;
    
    // Создаем элементы поиска
    const searchContainer = document.createElement('div');
    searchContainer.className = 'row mb-3';
    searchContainer.innerHTML = `
        <div class="col-md-6">
            <div class="input-group">
                <span class="input-group-text">
                    <i class="fas fa-search"></i>
                </span>
                <input type="text" class="form-control" id="${tableId}Filter" placeholder="Поиск в таблице..." autocomplete="off">
                <button class="btn btn-outline-secondary" type="button" id="${tableId}ClearBtn">
                    <i class="fas fa-times"></i>
                </button>
            </div>
        </div>
        <div class="col-md-6 text-end">
            <div class="d-flex align-items-center justify-content-end">
                <span class="text-muted me-3">
                    Показано: <span id="${tableId}FilteredCount">0</span> записей
                </span>
            </div>
        </div>
    `;
    
    // Создаем сообщение об отсутствии результатов
    const noResultsDiv = document.createElement('div');
    noResultsDiv.id = `${tableId}NoResults`;
    noResultsDiv.className = 'alert alert-info text-center';
    noResultsDiv.style.display = 'none';
    noResultsDiv.innerHTML = `
        <i class="fas fa-search fa-2x text-muted mb-2"></i>
        <h5>Результаты не найдены</h5>
        <p class="text-muted">Попробуйте изменить поисковый запрос</p>
    `;
    
    // Вставляем элементы перед таблицей
    container.insertBefore(searchContainer, table);
    container.insertBefore(noResultsDiv, table);
    
    console.log(`✅ Создано поле поиска для таблицы ${tableId}`);
}

/**
 * Инициализирует фильтрацию для конкретной таблицы
 */
function initializeFilterForTable(table, tableId) {
    // Сохраняем оригинальные данные таблицы
    const rows = table.querySelectorAll('tbody tr');
    originalTableData[tableId] = Array.from(rows).map(row => {
        const cells = row.querySelectorAll('td');
        return {
            element: row,
            text: Array.from(cells).map(cell => cell.textContent || cell.innerText).join(' ').toLowerCase()
        };
    });
    
    // Добавляем обработчик события для поля поиска
    // Проверяем разные возможные ID для поля поиска
    const filterInput = document.getElementById(`${tableId}Filter`) || 
                       document.getElementById('tableFilter') ||
                       document.querySelector(`input[id*="Filter"]`);
    
    if (filterInput) {
        // Удаляем существующие обработчики, чтобы избежать дублирования
        const newFilterInput = filterInput.cloneNode(true);
        filterInput.parentNode.replaceChild(newFilterInput, filterInput);
        
        newFilterInput.addEventListener('input', function() {
            filterTable(table, tableId, this.value);
        });
        
        // Добавляем обработчик для клавиши Escape
        newFilterInput.addEventListener('keydown', function(e) {
            if (e.key === 'Escape') {
                clearTableFilter(tableId);
            }
        });
        
        // Добавляем обработчик для кнопки очистки
        const clearBtn = document.getElementById(`${tableId}ClearBtn`) || 
                        document.getElementById('clearFilterBtn') ||
                        document.querySelector('button[onclick*="clearFilter"]') ||
                        document.querySelector('button[onclick*="clearTableFilter"]') ||
                        document.querySelector('button[id*="clear"]') ||
                        document.querySelector('button:has(.fa-times)');
        
        if (clearBtn) {
            // Удаляем существующий onclick
            clearBtn.removeAttribute('onclick');
            
            // Удаляем существующие обработчики
            const newClearBtn = clearBtn.cloneNode(true);
            clearBtn.parentNode.replaceChild(newClearBtn, clearBtn);
            
            newClearBtn.addEventListener('click', function() {
                clearTableFilter(tableId);
            });
            
            console.log(`✅ Обработчик кнопки очистки добавлен для таблицы ${tableId}`);
        } else {
            console.log(`⚠️ Кнопка очистки не найдена для таблицы ${tableId}`);
        }
        
        // Обновляем начальную статистику
        updateTableFilterStats(tableId, rows.length, rows.length);
    }
}

/**
 * Фильтрует таблицу по поисковому запросу
 */
function filterTable(table, tableId, searchTerm) {
    const searchLower = searchTerm.toLowerCase();
    const rows = table.querySelectorAll('tbody tr');
    // Проверяем разные возможные ID для элемента "нет результатов"
    const noResults = document.getElementById(`${tableId}NoResults`) || 
                     document.getElementById('noResults') ||
                     document.querySelector(`div[id*="NoResults"]`);
    let visibleCount = 0;
    
    rows.forEach((row, index) => {
        const rowData = originalTableData[tableId][index];
        const cells = row.querySelectorAll('td');
        
        if (searchTerm === '') {
            // Показываем все строки и убираем подсветку
            row.style.display = '';
            cells.forEach(cell => {
                // Убираем подсветку, если она есть
                if (cell.innerHTML.includes('<span class="highlight">')) {
                    cell.innerHTML = cell.innerHTML.replace(/<span class="highlight">(.*?)<\/span>/g, '$1');
                }
            });
            visibleCount++;
        } else {
            // Проверяем, содержит ли строка поисковый термин
            if (rowData && rowData.text && rowData.text.includes(searchLower)) {
                row.style.display = '';
                visibleCount++;
                
                // НЕ подсвечиваем найденный текст (убираем подсветку)
                cells.forEach(cell => {
                    // Убираем подсветку, если она есть
                    if (cell.innerHTML.includes('<span class="highlight">')) {
                        cell.innerHTML = cell.innerHTML.replace(/<span class="highlight">(.*?)<\/span>/g, '$1');
                    }
                });
            } else {
                row.style.display = 'none';
            }
        }
    });
    
    // Обновляем статистику
    updateTableFilterStats(tableId, visibleCount, rows.length);
    
    // Показываем/скрываем сообщение об отсутствии результатов
    if (visibleCount === 0 && searchTerm !== '') {
        if (noResults) noResults.style.display = 'block';
    } else {
        if (noResults) noResults.style.display = 'none';
    }
}

/**
 * Подсвечивает найденный текст
 */
function highlightText(text, searchTerm) {
    if (!searchTerm) return text;
    
    const regex = new RegExp(`(${escapeRegExp(searchTerm)})`, 'gi');
    return text.replace(regex, '<span class="highlight">$1</span>');
}

/**
 * Экранирует специальные символы для регулярных выражений
 */
function escapeRegExp(string) {
    return string.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Обновляет статистику фильтрации
 */
function updateTableFilterStats(tableId, visible, total) {
    // Проверяем разные возможные ID для элемента статистики
    const statsElement = document.getElementById(`${tableId}FilteredCount`) || 
                        document.getElementById('filteredCount') ||
                        document.querySelector(`span[id*="FilteredCount"]`);
    
    if (statsElement) {
        statsElement.textContent = visible;
    }
}

/**
 * Очищает фильтр для конкретной таблицы
 */
function clearTableFilter(tableId) {
    // Проверяем разные возможные ID для поля поиска
    const filterInput = document.getElementById(`${tableId}Filter`) || 
                       document.getElementById('tableFilter') ||
                       document.querySelector(`input[id*="Filter"]`);
    
    if (filterInput) {
        filterInput.value = '';
        
        // Находим таблицу по ID или по ближайшей таблице к полю поиска
        let table = document.getElementById(tableId);
        if (!table) {
            // Ищем таблицу рядом с полем поиска
            table = filterInput.closest('.card-body')?.querySelector('table') ||
                   filterInput.closest('.table-responsive')?.querySelector('table') ||
                   document.querySelector('table');
        }
        
        if (table) {
            filterTable(table, tableId, '');
            console.log(`✅ Фильтр очищен для таблицы ${tableId}`);
        } else {
            console.log(`⚠️ Таблица не найдена для очистки фильтра ${tableId}`);
        }
        
        filterInput.focus();
    } else {
        console.log(`⚠️ Поле поиска не найдено для очистки фильтра ${tableId}`);
    }
}

/**
 * Обновляет данные фильтрации при изменении содержимого таблицы
 */
function updateTableFilterData(tableId) {
    const table = document.getElementById(tableId) || document.querySelector(`table[id="${tableId}"]`);
    if (table) {
        initializeFilterForTable(table, tableId);
    }
}

// Инициализация при загрузке страницы
document.addEventListener('DOMContentLoaded', function() {
    initializeTableFilters();
});

// Экспорт функций для использования в других скриптах
window.TableFilter = {
    initialize: initializeTableFilters,
    clear: clearTableFilter,
    update: updateTableFilterData
};

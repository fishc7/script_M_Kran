// Обработчики для фильтрации таблиц
console.log('📋 Загрузка table_filters.js...');

// Глобальные переменные для таймеров
let filterTimeouts = {};

// Функция для применения фильтров по столбцам
function applyColumnFilters(activeFilterColumn = null) {
    console.log('🚀 Вызов applyColumnFilters с параметром:', activeFilterColumn);
    
    const form = document.getElementById('columnFiltersForm');
    if (!form) {
        console.error('❌ Форма columnFiltersForm не найдена!');
        return;
    }
    
    const formData = new FormData(form);
    const params = new URLSearchParams();
    
    // Добавляем текущие параметры
    const currentUrl = new URL(window.location);
    const currentColumns = currentUrl.searchParams.get('columns') || '';
    const currentSearch = currentUrl.searchParams.get('search') || '';
    
    console.log('Текущие параметры из URL:');
    console.log('  columns:', currentColumns);
    console.log('  search:', currentSearch);
    
    params.set('columns', currentColumns);
    params.set('search', currentSearch);
    
    // Добавляем фильтры по столбцам (исключаем скрытое поле columns)
    let hasFilters = false;
    console.log('Проверяем форму фильтров:');
    for (let [key, value] of formData.entries()) {
        console.log(`  ${key}: '${value}'`);
        if (key !== 'columns' && value.trim() !== '') {
            // Правильно кодируем параметры для URL
            const encodedKey = encodeURIComponent(key);
            const encodedValue = encodeURIComponent(value);
            params.set(encodedKey, encodedValue);
            hasFilters = true;
            console.log(`Добавлен фильтр: ${encodedKey} = ${encodedValue}`);
        }
    }
    
    if (!hasFilters) {
        console.log('Нет активных фильтров');
    }
    
    // Добавляем информацию об активном фильтре
    if (activeFilterColumn) {
        params.set('activeFilter', encodeURIComponent(activeFilterColumn));
        console.log(`Установлен активный фильтр: ${activeFilterColumn}`);
    }
    
    // Временно используем обычную перезагрузку для надежности
    const requestUrl = `${window.location.pathname}?${params.toString()}`;
    console.log('Переход на:', requestUrl);
    window.location.href = requestUrl;
}

// Функция для очистки фильтра конкретного столбца
function clearColumnFilter(column) {
    const input = document.querySelector(`input[name="filter_${column}"]`);
    if (input) {
        input.value = '';
        applyColumnFilters(column);
    }
}

// Функция для очистки всех фильтров
function clearAllColumnFilters() {
    const inputs = document.querySelectorAll('.column-filter');
    inputs.forEach(input => {
        input.value = '';
    });
    applyColumnFilters();
}

// Функция для инициализации фильтров
function initializeColumnFilters() {
    console.log('🔍 Инициализация фильтров по столбцам...');
    
    // Обработчики для полей фильтрации по столбцам
    const filterInputs = document.querySelectorAll('.column-filter');
    console.log(`Найдено полей фильтрации: ${filterInputs.length}`);
    
    filterInputs.forEach((input, index) => {
        console.log(`Настраиваем поле ${index + 1}: ${input.name}`);
        
        // Очищаем предыдущий таймер для этого поля
        if (filterTimeouts[input.name]) {
            clearTimeout(filterTimeouts[input.name]);
        }
        
        // Обработчик ввода
        input.addEventListener('input', function() {
            console.log(`Ввод в поле ${this.name}: "${this.value}"`);
            
            // Показываем индикатор загрузки
            const loadingIndicator = document.getElementById('filterLoading');
            if (loadingIndicator) {
                loadingIndicator.style.display = 'block';
            }
            
            // Очищаем предыдущий таймер
            if (filterTimeouts[this.name]) {
                clearTimeout(filterTimeouts[this.name]);
            }
            
            // Устанавливаем новый таймер с меньшей задержкой для более быстрого отклика
            filterTimeouts[this.name] = setTimeout(() => {
                const columnName = this.name.replace('filter_', '');
                console.log(`Применяем фильтр для столбца: ${columnName}`);
                applyColumnFilters(columnName);
                
                // Скрываем индикатор загрузки
                if (loadingIndicator) {
                    loadingIndicator.style.display = 'none';
                }
            }, 300); // Уменьшаем задержку до 300мс для более быстрого отклика
        });
        
        // Обработчик нажатия клавиш для отладки
        input.addEventListener('keydown', function(e) {
            console.log(`Клавиша нажата в поле ${this.name}: ${e.key}`);
        });
        
        // Сохраняем фокус при клике
        input.addEventListener('focus', function() {
            const columnName = this.name.replace('filter_', '');
            sessionStorage.setItem('activeFilter', columnName);
            console.log(`Фокус на поле: ${columnName}`);
        });
    });
    
    // Обработчики для кнопок очистки отдельных фильтров
    const clearButtons = document.querySelectorAll('.clear-column-filter');
    console.log(`Найдено кнопок очистки: ${clearButtons.length}`);
    clearButtons.forEach(button => {
        button.addEventListener('click', function() {
            const column = this.getAttribute('data-column');
            clearColumnFilter(column);
        });
    });
    
    // Обработчик для кнопки очистки всех фильтров
    const clearAllButton = document.getElementById('clearAllFilters');
    if (clearAllButton) {
        clearAllButton.addEventListener('click', function() {
            clearAllColumnFilters();
        });
    }
    
    console.log('✅ Инициализация фильтров завершена');
}

// Функция для восстановления фокуса
function restoreFocus() {
    // Получаем информацию об активном фильтре из URL или sessionStorage
    const urlParams = new URLSearchParams(window.location.search);
    const activeFilterFromUrl = urlParams.get('activeFilter');
    const activeFilterFromStorage = sessionStorage.getItem('activeFilter');
    
    const activeFilterColumn = activeFilterFromUrl || activeFilterFromStorage;
    
    if (activeFilterColumn) {
        console.log('Восстанавливаем фокус на фильтре:', activeFilterColumn);
        
        // Находим поле фильтра
        const filterInput = document.querySelector(`input[name="filter_${activeFilterColumn}"]`);
        if (filterInput) {
            setTimeout(() => {
                filterInput.focus();
                // Устанавливаем курсор в конец текста
                const length = filterInput.value.length;
                filterInput.setSelectionRange(length, length);
                console.log('Фокус восстановлен на поле:', activeFilterColumn);
            }, 100);
        }
    }
}

// Экспортируем функции в глобальную область видимости
window.applyColumnFilters = applyColumnFilters;
window.clearColumnFilter = clearColumnFilter;
window.clearAllColumnFilters = clearAllColumnFilters;
window.initializeColumnFilters = initializeColumnFilters;
window.restoreFocus = restoreFocus;

console.log('✅ table_filters.js загружен');

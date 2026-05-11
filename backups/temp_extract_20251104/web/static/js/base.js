console.log('=== BASE.JS ЗАГРУЖЕН ===');

document.addEventListener('DOMContentLoaded', function() {
    console.log('DOM loaded, setting up base functionality...');
    setupSidebar();
    setupMobileMenu();
    setupLoading();
    
    // Инициализация переменных для селектора столбцов
    initializeColumnSelector();
    
    // Автоматическое изменение размера столбцов таблиц
    setupTableColumnSizing();
});

// Функция для автоматического изменения размера столбцов
function setupTableColumnSizing() {
    console.log('Настройка автоматического изменения размера столбцов...');
    
    const tables = document.querySelectorAll('.data-table, .table');
    tables.forEach(table => {
        optimizeTableColumns(table);
    });
}

// Оптимизация столбцов таблицы
function optimizeTableColumns(table) {
    const headers = table.querySelectorAll('thead th');
    const tbody = table.querySelector('tbody');
    
    if (!tbody || headers.length === 0) return;
    
    const rows = tbody.querySelectorAll('tr');
    if (rows.length === 0) return;
    
    headers.forEach((header, columnIndex) => {
        const columnName = header.textContent.trim().toLowerCase();
        const columnData = [];
        
        // Собираем данные столбца
        rows.forEach(row => {
            const cell = row.querySelectorAll('td')[columnIndex];
            if (cell) {
                columnData.push(cell.textContent.trim());
            }
        });
        
        // Определяем тип столбца и применяем соответствующие стили
        const columnType = determineColumnType(columnName, columnData);
        applyColumnStyles(header, columnType);
        
        // Применяем стили к ячейкам столбца
        rows.forEach(row => {
            const cell = row.querySelectorAll('td')[columnIndex];
            if (cell) {
                applyColumnStyles(cell, columnType);
            }
        });
    });
}

// Определение типа столбца
function determineColumnType(columnName, columnData) {
    // Проверяем по названию столбца
    if (columnName.includes('id') || columnName.includes('номер') || columnName.includes('код')) {
        return 'narrow';
    }
    
    if (columnName.includes('дата') || columnName.includes('date')) {
        return 'date';
    }
    
    if (columnName.includes('описание') || columnName.includes('название') || 
        columnName.includes('комментарий') || columnName.includes('примечание')) {
        return 'wide';
    }
    
    // Проверяем по содержимому
    const sampleData = columnData.slice(0, 10); // Берем первые 10 записей для анализа
    
    // Проверяем, являются ли данные числами
    const isNumeric = sampleData.every(item => {
        if (!item) return true;
        return /^\d+(\.\d+)?$/.test(item.replace(/[,\s]/g, ''));
    });
    
    if (isNumeric && sampleData.length > 0) {
        return 'number';
    }
    
    // Проверяем, являются ли данные датами
    const isDate = sampleData.every(item => {
        if (!item) return true;
        return /^\d{1,2}[.\-\/]\d{1,2}[.\-\/]\d{2,4}$/.test(item) ||
               /^\d{4}[.\-\/]\d{1,2}[.\-\/]\d{1,2}$/.test(item);
    });
    
    if (isDate && sampleData.length > 0) {
        return 'date';
    }
    
    // Проверяем длину содержимого
    const maxLength = Math.max(...sampleData.map(item => item ? item.length : 0));
    
    if (maxLength > 50) {
        return 'wide';
    } else if (maxLength < 10) {
        return 'narrow';
    }
    
    return 'default';
}

// Применение стилей к столбцу
function applyColumnStyles(element, columnType) {
    // Удаляем предыдущие классы типов столбцов
    element.classList.remove('narrow-column', 'wide-column', 'date-column', 'number-column');
    
    // Добавляем соответствующий класс
    switch (columnType) {
        case 'narrow':
            element.classList.add('narrow-column');
            break;
        case 'wide':
            element.classList.add('wide-column');
            break;
        case 'date':
            element.classList.add('date-column');
            break;
        case 'number':
            element.classList.add('number-column');
            break;
    }
}

// Инициализация селектора столбцов
function initializeColumnSelector() {
    console.log('Инициализация селектора столбцов...');
    
    // Проверяем, есть ли модальное окно селектора столбцов на странице
    const modal = document.getElementById('columnSelectorModal');
    if (modal) {
        console.log('Модальное окно columnSelectorModal найдено, инициализируем...');
        
        // Инициализируем переменные, если они не определены
        if (!window.selectedColumns) {
            // Получаем текущие выбранные столбцы из URL или используем все столбцы
            const urlParams = new URLSearchParams(window.location.search);
            const columnsParam = urlParams.get('columns');
            
            if (columnsParam) {
                window.selectedColumns = new Set(columnsParam.split(','));
            } else {
                // Если параметр columns не указан, выбираем все столбцы
                const columnItems = document.querySelectorAll('.column-item');
                window.selectedColumns = new Set(Array.from(columnItems).map(item => item.getAttribute('data-column')));
            }
        }
        
        if (!window.allColumns) {
            const columnItems = document.querySelectorAll('.column-item');
            window.allColumns = Array.from(columnItems).map(item => item.getAttribute('data-column'));
        }
        
        console.log('Переменные инициализированы:');
        console.log('selectedColumns:', window.selectedColumns);
        console.log('allColumns:', window.allColumns);
        
        // Обновляем отображение
        updateColumnSelection();
    } else {
        console.log('Модальное окно columnSelectorModal не найдено на этой странице');
    }
}

// Настройка боковой панели
function setupSidebar() {
    const sidebar = document.querySelector('.sidebar');
    const mainContent = document.getElementById('mainContent');
    const toggleBtn = document.querySelector('.sidebar-toggle');
    
    if (toggleBtn) {
        toggleBtn.addEventListener('click', function() {
            toggleSidebar();
        });
    }
    
    // Сохраняем состояние в localStorage
    const sidebarState = localStorage.getItem('sidebarCollapsed');
    if (sidebarState === 'true') {
        collapseSidebar();
    }
}

// Переключение боковой панели
function toggleSidebar() {
    const sidebar = document.querySelector('.sidebar');
    const mainContent = document.getElementById('mainContent');
    
    if (sidebar.classList.contains('collapsed')) {
        expandSidebar();
    } else {
        collapseSidebar();
    }
}

// Сворачивание боковой панели
function collapseSidebar() {
    const sidebar = document.querySelector('.sidebar');
    const mainContent = document.getElementById('mainContent');
    
    if (sidebar && mainContent) {
        sidebar.classList.add('collapsed');
        mainContent.style.marginLeft = 'var(--sidebar-collapsed-width)';
        localStorage.setItem('sidebarCollapsed', 'true');
    }
}

// Разворачивание боковой панели
function expandSidebar() {
    const sidebar = document.querySelector('.sidebar');
    const mainContent = document.getElementById('mainContent');
    
    if (sidebar && mainContent) {
        sidebar.classList.remove('collapsed');
        mainContent.style.marginLeft = 'var(--sidebar-width)';
        localStorage.setItem('sidebarCollapsed', 'false');
    }
}

// Настройка мобильного меню
function setupMobileMenu() {
    const mobileToggle = document.querySelector('.mobile-menu-toggle');
    const sidebar = document.querySelector('.sidebar');
    
    if (mobileToggle && sidebar) {
        mobileToggle.addEventListener('click', function() {
            sidebar.classList.toggle('mobile-open');
        });
        
        // Закрытие при клике вне панели
        document.addEventListener('click', function(e) {
            if (!sidebar.contains(e.target) && !mobileToggle.contains(e.target)) {
                sidebar.classList.remove('mobile-open');
            }
        });
    }
}

// Настройка индикатора загрузки
function setupLoading() {
    const loading = document.getElementById('loading');
    
    if (loading) {
        // Скрываем загрузку после загрузки страницы
        window.addEventListener('load', function() {
            loading.style.display = 'none';
        });
        
        // Показываем загрузку при переходе между страницами
        document.addEventListener('click', function(e) {
            if (e.target.tagName === 'A' && e.target.href && !e.target.href.includes('#')) {
                loading.style.display = 'flex';
            }
        });
    }
}

// Глобальные функции для показа/скрытия индикатора загрузки
function showLoading() {
    const loading = document.getElementById('loading');
    if (loading) {
        loading.style.display = 'flex';
    }
}

function hideLoading() {
    const loading = document.getElementById('loading');
    if (loading) {
        loading.style.display = 'none';
    }
}

// Функция для показа уведомлений
function showNotification(message, type = 'info') {
    // Создаем элемент уведомления
    const notification = document.createElement('div');
    notification.className = `alert alert-${type === 'error' ? 'danger' : type} alert-dismissible fade show position-fixed`;
    notification.style.cssText = 'top: 20px; right: 20px; z-index: 9999; min-width: 300px;';
    notification.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;
    
    // Добавляем уведомление на страницу
    document.body.appendChild(notification);
    
    // Автоматически удаляем через 5 секунд
    setTimeout(() => {
        if (notification.parentNode) {
            notification.remove();
        }
    }, 5000);
}

// Глобальные функции для использования в других скриптах
window.toggleSidebar = toggleSidebar;
window.collapseSidebar = collapseSidebar;
window.expandSidebar = expandSidebar;
window.showLoading = showLoading;
window.hideLoading = hideLoading;
window.showNotification = showNotification;

// Подтверждение удаления
function confirmDelete(message) {
    return confirm(message || 'Вы уверены, что хотите удалить этот элемент?');
}

// Универсальная функция для открытия селектора столбцов
function openColumnSelector() {
    console.log('openColumnSelector вызвана из base.js');
    
    // Проверяем, существует ли модальное окно
    const modal = document.getElementById('columnSelectorModal');
    if (!modal) {
        console.error('Модальное окно columnSelectorModal не найдено');
        alert('Ошибка: модальное окно не найдено');
        return;
    }
    
    // Проверяем, доступен ли Bootstrap
    if (typeof bootstrap === 'undefined') {
        console.error('Bootstrap не загружен');
        alert('Ошибка: Bootstrap не загружен');
        return;
    }
    
    // Проверяем, доступен ли bootstrap.Modal
    if (typeof bootstrap.Modal === 'undefined') {
        console.error('bootstrap.Modal не доступен');
        console.log('Доступные свойства bootstrap:', Object.keys(bootstrap));
        alert('Ошибка: bootstrap.Modal не доступен');
        return;
    }
    
    try {
        // Проверяем, есть ли уже экземпляр модального окна
        let modalInstance = bootstrap.Modal.getInstance(modal);
        if (!modalInstance) {
            console.log('Создаем новый экземпляр модального окна');
            modalInstance = new bootstrap.Modal(modal);
        }
        console.log('Показываем модальное окно');
        modalInstance.show();
        console.log('Модальное окно открыто успешно');
    } catch (error) {
        console.error('Ошибка при открытии модального окна:', error);
        console.error('Тип ошибки:', error.constructor.name);
        console.error('Сообщение ошибки:', error.message);
        console.error('Стек вызовов:', error.stack);
        alert('Ошибка при открытии модального окна: ' + error.message);
    }
}

// Делаем функцию глобально доступной
window.openColumnSelector = openColumnSelector;

// Универсальные функции для работы с селектором столбцов
function toggleColumn(columnName) {
    console.log('=== toggleColumn вызвана ===');
    console.log('Столбец:', columnName);
    console.log('Тип столбца:', typeof columnName);
    
    const columnItem = document.querySelector(`[data-column="${columnName}"]`);
    console.log('Поиск элемента с data-column="' + columnName + '"');
    
    if (!columnItem) {
        console.error('❌ Элемент столбца не найден:', columnName);
        console.log('Доступные элементы:');
        document.querySelectorAll('.column-item').forEach(item => {
            console.log('  - data-column="' + item.getAttribute('data-column') + '"');
        });
        return;
    }
    
    console.log('✅ Найден элемент столбца:', columnItem);
    
    // Получаем текущие выбранные столбцы из глобальной переменной или создаем новый Set
    if (!window.selectedColumns) {
        window.selectedColumns = new Set();
    }
    
    console.log('Текущее состояние selectedColumns:', window.selectedColumns);
    console.log('Размер selectedColumns:', window.selectedColumns.size);
    
    if (window.selectedColumns.has(columnName)) {
        console.log('🗑️ Удаляем столбец из выбора:', columnName);
        window.selectedColumns.delete(columnName);
        columnItem.classList.remove('selected');
        
        // Принудительно убираем стили через несколько способов
        columnItem.style.removeProperty('background-color');
        columnItem.style.removeProperty('border-color');
        columnItem.style.removeProperty('color');
        columnItem.style.setProperty('background-color', '#f8f9fa', 'important');
        columnItem.style.setProperty('border-color', '#dee2e6', 'important');
        columnItem.style.setProperty('color', '#495057', 'important');
        
        // Дополнительно применяем стили через CSS классы
        columnItem.style.cssText += 'background-color: #f8f9fa !important; border-color: #dee2e6 !important; color: #495057 !important;';
        
        console.log('✅ Стили убраны');
        
        // Убираем иконку галочки
        const checkIcon = columnItem.querySelector('.fa-check');
        if (checkIcon) {
            checkIcon.remove();
            console.log('✅ Иконка галочки убрана');
        }
        console.log('✅ Столбец удален из выбора:', columnName);
    } else {
        console.log('➕ Добавляем столбец в выбор:', columnName);
        window.selectedColumns.add(columnName);
        columnItem.classList.add('selected');
        
        // Принудительно добавляем стили через несколько способов
        columnItem.style.removeProperty('background-color');
        columnItem.style.removeProperty('border-color');
        columnItem.style.removeProperty('color');
        columnItem.style.setProperty('background-color', '#d1ecf1', 'important');
        columnItem.style.setProperty('border-color', '#17a2b8', 'important');
        columnItem.style.setProperty('color', '#0c5460', 'important');
        
        // Дополнительно применяем стили через CSS классы
        columnItem.style.cssText += 'background-color: #d1ecf1 !important; border-color: #17a2b8 !important; color: #0c5460 !important;';
        
        console.log('✅ Стили применены');
        
        // Добавляем иконку галочки
        if (!columnItem.querySelector('.fa-check')) {
            const checkIcon = document.createElement('i');
            checkIcon.className = 'fas fa-check text-success ms-2';
            checkIcon.style.setProperty('color', '#28a745', 'important');
            checkIcon.style.setProperty('font-weight', 'bold', 'important');
            columnItem.appendChild(checkIcon);
            console.log('✅ Иконка галочки добавлена');
        }
        console.log('✅ Столбец добавлен в выбор:', columnName);
    }
    
    console.log('Обновленное состояние selectedColumns:', window.selectedColumns);
    updateSelectedCount();
}

function selectAllColumns() {
    if (!window.selectedColumns) {
        window.selectedColumns = new Set();
    }
    if (!window.allColumns) {
        // Если allColumns не определен, получаем из DOM
        const columnItems = document.querySelectorAll('.column-item');
        window.allColumns = Array.from(columnItems).map(item => item.getAttribute('data-column'));
    }
    
    window.selectedColumns.clear();
    window.allColumns.forEach(column => {
        window.selectedColumns.add(column);
    });
    updateColumnSelection();
}

function deselectAllColumns() {
    if (!window.selectedColumns) {
        window.selectedColumns = new Set();
    }
    window.selectedColumns.clear();
    updateColumnSelection();
}

function updateColumnSelection() {
    console.log('updateColumnSelection вызвана');
    console.log('selectedColumns:', window.selectedColumns);
    
    // Обновляем визуальное состояние всех столбцов
    const columnItems = document.querySelectorAll('.column-item');
    console.log('Найдено элементов .column-item:', columnItems.length);
    
    columnItems.forEach(item => {
        const columnName = item.getAttribute('data-column');
        console.log('Обрабатываем столбец:', columnName);
        
        if (window.selectedColumns && window.selectedColumns.has(columnName)) {
            item.classList.add('selected');
            
            // Принудительно добавляем стили через несколько способов
            item.style.removeProperty('background-color');
            item.style.removeProperty('border-color');
            item.style.removeProperty('color');
            item.style.setProperty('background-color', '#d1ecf1', 'important');
            item.style.setProperty('border-color', '#17a2b8', 'important');
            item.style.setProperty('color', '#0c5460', 'important');
            
            // Дополнительно применяем стили через CSS классы
            item.style.cssText += 'background-color: #d1ecf1 !important; border-color: #17a2b8 !important; color: #0c5460 !important;';
            
            // Добавляем иконку галочки если её нет
            if (!item.querySelector('.fa-check')) {
                const checkIcon = document.createElement('i');
                checkIcon.className = 'fas fa-check text-success ms-2';
                checkIcon.style.setProperty('color', '#28a745', 'important');
                checkIcon.style.setProperty('font-weight', 'bold', 'important');
                item.appendChild(checkIcon);
            }
        } else {
            item.classList.remove('selected');
            
            // Принудительно убираем стили через несколько способов
            item.style.removeProperty('background-color');
            item.style.removeProperty('border-color');
            item.style.removeProperty('color');
            item.style.setProperty('background-color', '#f8f9fa', 'important');
            item.style.setProperty('border-color', '#dee2e6', 'important');
            item.style.setProperty('color', '#495057', 'important');
            
            // Дополнительно применяем стили через CSS классы
            item.style.cssText += 'background-color: #f8f9fa !important; border-color: #dee2e6 !important; color: #495057 !important;';
            
            // Убираем иконку галочки
            const checkIcon = item.querySelector('.fa-check');
            if (checkIcon) {
                checkIcon.remove();
            }
        }
    });
    updateSelectedCount();
}

function updateSelectedCount() {
    const countElement = document.getElementById('selectedCount');
    if (countElement && window.selectedColumns) {
        countElement.textContent = window.selectedColumns.size;
    }
}

function applyColumnSelection() {
    console.log('applyColumnSelection вызвана');
    console.log('Количество выбранных столбцов:', window.selectedColumns ? window.selectedColumns.size : 0);
    
    if (!window.selectedColumns || window.selectedColumns.size === 0) {
        alert('Выберите хотя бы один столбец для отображения!');
        return;
    }
    
    const columnsParam = Array.from(window.selectedColumns).join(',');
    console.log('Параметр columns:', columnsParam);
    
    const currentUrl = new URL(window.location);
    currentUrl.searchParams.set('columns', columnsParam);
    currentUrl.searchParams.set('page', '1'); // Сбрасываем на первую страницу
    
    console.log('Новый URL:', currentUrl.toString());
    window.location.href = currentUrl.toString();
}

// Делаем все функции глобально доступными
window.toggleColumn = toggleColumn;
window.selectAllColumns = selectAllColumns;
window.deselectAllColumns = deselectAllColumns;
window.updateColumnSelection = updateColumnSelection;
window.updateSelectedCount = updateSelectedCount;
window.applyColumnSelection = applyColumnSelection;

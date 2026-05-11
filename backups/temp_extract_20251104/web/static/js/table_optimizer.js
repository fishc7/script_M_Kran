/**
 * Оптимизатор таблиц для автоматического изменения размера столбцов
 * Анализирует содержимое таблиц и применяет оптимальные стили
 */

class TableOptimizer {
    constructor() {
        this.initialized = false;
        this.init();
    }
    
    init() {
        if (this.initialized) return;
        
        console.log('Инициализация TableOptimizer...');
        
        // Ждем загрузки DOM
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', () => this.setupTables());
        } else {
            this.setupTables();
        }
        
        // Наблюдаем за изменениями в DOM для динамически загружаемых таблиц
        this.setupMutationObserver();
        
        this.initialized = true;
    }
    
    setupTables() {
        console.log('Настройка таблиц...');
        
        // Находим все таблицы на странице
        const tables = document.querySelectorAll('.data-table, .table');
        tables.forEach(table => {
            this.optimizeTable(table);
        });
        
        // Применяем стили к контейнерам таблиц
        this.setupTableContainers();
    }
    
    setupTableContainers() {
        const containers = document.querySelectorAll('.table-container');
        containers.forEach(container => {
            // Добавляем стили для горизонтальной прокрутки
            container.style.overflowX = 'auto';
            container.style.borderRadius = '15px';
            container.style.boxShadow = '0 10px 30px rgba(0,0,0,0.1)';
        });
    }
    
    optimizeTable(table) {
        console.log('Оптимизация таблицы:', table);
        
        const headers = table.querySelectorAll('thead th');
        const tbody = table.querySelector('tbody');
        
        if (!tbody || headers.length === 0) return;
        
        const rows = tbody.querySelectorAll('tr');
        if (rows.length === 0) return;
        
        // Анализируем каждый столбец
        headers.forEach((header, columnIndex) => {
            const columnName = header.textContent.trim().toLowerCase();
            const columnData = this.getColumnData(rows, columnIndex);
            
            // Определяем тип столбца
            const columnType = this.determineColumnType(columnName, columnData);
            
            // Применяем стили
            this.applyColumnStyles(header, columnType);
            
            // Применяем стили к ячейкам
            rows.forEach(row => {
                const cell = row.querySelectorAll('td')[columnIndex];
                if (cell) {
                    this.applyColumnStyles(cell, columnType);
                }
            });
        });
        
        // Добавляем класс для таблицы
        table.classList.add('optimized-table');
    }
    
    getColumnData(rows, columnIndex) {
        const columnData = [];
        
        rows.forEach(row => {
            const cell = row.querySelectorAll('td')[columnIndex];
            if (cell) {
                columnData.push(cell.textContent.trim());
            }
        });
        
        return columnData;
    }
    
    determineColumnType(columnName, columnData) {
        // Анализ по названию столбца
        if (this.isNarrowColumn(columnName)) {
            return 'narrow';
        }
        
        if (this.isDateColumn(columnName)) {
            return 'date';
        }
        
        if (this.isWideColumn(columnName)) {
            return 'wide';
        }
        
        // Анализ по содержимому
        const sampleData = columnData.slice(0, 20); // Берем первые 20 записей
        
        if (sampleData.length === 0) return 'default';
        
        // Проверяем числовые данные
        if (this.isNumericData(sampleData)) {
            return 'number';
        }
        
        // Проверяем даты
        if (this.isDateData(sampleData)) {
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
    
    isNarrowColumn(columnName) {
        const narrowKeywords = ['id', 'номер', 'код', '№', 'номер_стыка', 'лист'];
        return narrowKeywords.some(keyword => columnName.includes(keyword));
    }
    
    isDateColumn(columnName) {
        const dateKeywords = ['дата', 'date', 'время', 'time'];
        return dateKeywords.some(keyword => columnName.includes(keyword));
    }
    
    isWideColumn(columnName) {
        const wideKeywords = ['описание', 'название', 'комментарий', 'примечание', 'размер_выборки'];
        return wideKeywords.some(keyword => columnName.includes(keyword));
    }
    
    isNumericData(sampleData) {
        return sampleData.every(item => {
            if (!item) return true;
            // Удаляем пробелы и запятые, проверяем на число
            const cleanItem = item.replace(/[,\s]/g, '');
            return /^\d+(\.\d+)?$/.test(cleanItem);
        });
    }
    
    isDateData(sampleData) {
        return sampleData.every(item => {
            if (!item) return true;
            // Проверяем различные форматы дат
            return /^\d{1,2}[.\-\/]\d{1,2}[.\-\/]\d{2,4}$/.test(item) ||
                   /^\d{4}[.\-\/]\d{1,2}[.\-\/]\d{1,2}$/.test(item) ||
                   /^\d{2}[.\-\/]\d{2}[.\-\/]\d{4}$/.test(item);
        });
    }
    
    applyColumnStyles(element, columnType) {
        // Удаляем предыдущие классы
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
    
    setupMutationObserver() {
        // Наблюдаем за изменениями в DOM
        const observer = new MutationObserver((mutations) => {
            mutations.forEach((mutation) => {
                if (mutation.type === 'childList') {
                    mutation.addedNodes.forEach((node) => {
                        if (node.nodeType === Node.ELEMENT_NODE) {
                            // Проверяем, есть ли новые таблицы
                            const tables = node.querySelectorAll ? node.querySelectorAll('.data-table, .table') : [];
                            tables.forEach(table => this.optimizeTable(table));
                            
                            // Если сам узел является таблицей
                            if (node.classList && (node.classList.contains('data-table') || node.classList.contains('table'))) {
                                this.optimizeTable(node);
                            }
                        }
                    });
                }
            });
        });
        
        // Начинаем наблюдение
        observer.observe(document.body, {
            childList: true,
            subtree: true
        });
    }
    
    // Публичный метод для ручной оптимизации таблицы
    optimizeTableById(tableId) {
        const table = document.getElementById(tableId);
        if (table) {
            this.optimizeTable(table);
        }
    }
    
    // Публичный метод для оптимизации всех таблиц на странице
    optimizeAllTables() {
        const tables = document.querySelectorAll('.data-table, .table');
        tables.forEach(table => this.optimizeTable(table));
    }
}

// Создаем глобальный экземпляр
window.tableOptimizer = new TableOptimizer();

// Экспортируем для использования в других модулях
if (typeof module !== 'undefined' && module.exports) {
    module.exports = TableOptimizer;
}


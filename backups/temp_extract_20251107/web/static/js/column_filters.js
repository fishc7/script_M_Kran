/**
 * Фильтрация таблицы по столбцам
 * Позволяет фильтровать данные по каждому столбцу отдельно
 */

class ColumnTableFilter {
    constructor() {
        this.filters = {};
        this.originalData = [];
        this.filteredData = [];
        this.init();
    }
    
    init() {
        this.loadOriginalData();
        this.bindEvents();
        this.updateStats();
    }
    
    loadOriginalData() {
        const table = document.getElementById('dataTable');
        if (!table) return;
        
        const tbody = table.querySelector('tbody');
        if (!tbody) return;
        
        const rows = tbody.querySelectorAll('tr');
        this.originalData = [];
        
        rows.forEach((row, rowIndex) => {
            const cells = row.querySelectorAll('td');
            const rowData = {};
            
            cells.forEach((cell, cellIndex) => {
                const columnName = this.getColumnName(cellIndex);
                if (columnName) {
                    rowData[columnName] = cell.textContent.trim();
                }
            });
            
            this.originalData.push({
                element: row,
                data: rowData,
                index: rowIndex
            });
        });
        
        this.filteredData = [...this.originalData];
        console.log('Загружено записей:', this.originalData.length);
    }
    
    getColumnName(cellIndex) {
        const table = document.getElementById('dataTable');
        if (!table) return null;
        
        const header = table.querySelector('thead tr');
        if (!header) return null;
        
        const headers = header.querySelectorAll('th');
        if (headers[cellIndex]) {
            return headers[cellIndex].textContent.trim();
        }
        
        return null;
    }
    
    bindEvents() {
        // Обработчики для полей фильтрации по столбцам
        const filterInputs = document.querySelectorAll('.column-filter');
        filterInputs.forEach(input => {
            input.addEventListener('input', (e) => {
                const column = e.target.dataset.column;
                const value = e.target.value.toLowerCase();
                this.setFilter(column, value);
            });
        });
        
        // Обработчики для кнопок очистки отдельных фильтров
        const clearButtons = document.querySelectorAll('.clear-column-filter');
        clearButtons.forEach(button => {
            button.addEventListener('click', (e) => {
                const column = e.target.closest('button').dataset.column;
                this.clearColumnFilter(column);
            });
        });
        
        // Обработчик для кнопки очистки всех фильтров
        const clearAllButton = document.getElementById('clearAllFilters');
        if (clearAllButton) {
            clearAllButton.addEventListener('click', () => {
                this.clearAllFilters();
            });
        }
    }
    
    setFilter(column, value) {
        if (value) {
            this.filters[column] = value;
        } else {
            delete this.filters[column];
        }
        
        this.applyFilters();
    }
    
    clearColumnFilter(column) {
        delete this.filters[column];
        
        // Очищаем поле ввода
        const input = document.querySelector(`[data-column="${column}"]`);
        if (input) {
            input.value = '';
        }
        
        this.applyFilters();
    }
    
    clearAllFilters() {
        this.filters = {};
        
        // Очищаем все поля ввода
        const inputs = document.querySelectorAll('.column-filter');
        inputs.forEach(input => {
            input.value = '';
        });
        
        this.applyFilters();
    }
    
    applyFilters() {
        if (Object.keys(this.filters).length === 0) {
            // Если нет фильтров, показываем все данные
            this.showAllRows();
        } else {
            // Применяем фильтры
            this.filteredData = this.originalData.filter(row => {
                return Object.entries(this.filters).every(([column, filterValue]) => {
                    const cellValue = row.data[column] || '';
                    return cellValue.toLowerCase().includes(filterValue);
                });
            });
            
            this.updateTable();
        }
        
        this.updateStats();
    }
    
    showAllRows() {
        this.originalData.forEach(row => {
            row.element.style.display = '';
        });
    }
    
    updateTable() {
        // Скрываем все строки
        this.originalData.forEach(row => {
            row.element.style.display = 'none';
        });
        
        // Показываем только отфильтрованные строки
        this.filteredData.forEach(row => {
            row.element.style.display = '';
        });
    }
    
    updateStats() {
        const statsElement = document.getElementById('filteredCount');
        if (statsElement) {
            const visibleCount = this.filteredData.length;
            const totalCount = this.originalData.length;
            
            if (Object.keys(this.filters).length > 0) {
                statsElement.textContent = `${visibleCount} из ${totalCount}`;
            } else {
                statsElement.textContent = totalCount;
            }
        }
    }
}

// Инициализация при загрузке страницы
document.addEventListener('DOMContentLoaded', function() {
    // Проверяем, есть ли таблица на странице
    const table = document.getElementById('dataTable');
    if (table) {
        window.columnFilter = new ColumnTableFilter();
        console.log('Фильтрация по столбцам инициализирована');
    }
});


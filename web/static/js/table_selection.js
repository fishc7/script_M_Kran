/**
 * Управление выделением строк в таблицах
 * Поддерживает одиночное, множественное выделение и выделение с клавишами Ctrl/Shift
 */

class TableRowSelector {
    constructor() {
        this.selectedRows = new Set();
        this.lastSelectedRow = null;
        this.isCtrlPressed = false;
        this.isShiftPressed = false;
        this.isInitialized = false;
        
        this.init();
    }
    
    init() {
        if (this.isInitialized) return;
        
        console.log('🎯 Инициализация системы выделения строк таблиц...');
        
        // Добавляем обработчики клавиш
        document.addEventListener('keydown', this.handleKeyDown.bind(this));
        document.addEventListener('keyup', this.handleKeyUp.bind(this));
        
        // Инициализируем все таблицы
        this.initializeAllTables();
        
        // Добавляем панель управления выделением
        this.addSelectionControls();
        
        this.isInitialized = true;
        console.log('✅ Система выделения строк инициализирована');
    }
    
    initializeAllTables() {
        const tables = document.querySelectorAll('.data-table, .table');
        
        tables.forEach((table, tableIndex) => {
            this.initializeTable(table, tableIndex);
        });
        
        console.log(`📊 Инициализировано таблиц: ${tables.length}`);
    }
    
    initializeTable(table, tableIndex) {
        const rows = table.querySelectorAll('tbody tr');
        
        rows.forEach((row, rowIndex) => {
            // Добавляем уникальный идентификатор строки
            if (!row.dataset.rowId) {
                row.dataset.rowId = `table-${tableIndex}-row-${rowIndex}`;
            }
            
            // Добавляем обработчик клика
            row.addEventListener('click', (e) => {
                this.handleRowClick(e, row, table);
            });
            
            // Предотвращаем выделение текста при двойном клике
            row.addEventListener('dblclick', (e) => {
                // Не перехватываем двойной клик для таблиц с редактированием
                if (table.id === 'records-table' && row.hasAttribute('data-record-id')) {
                    console.log('🎯 Двойной клик в таблице с редактированием, не перехватываем');
                    return;
                }
                e.preventDefault();
            });
        });
        
        // Добавляем обработчик для выделения всех строк
        const selectAllCheckbox = table.querySelector('.select-all-checkbox');
        if (selectAllCheckbox) {
            selectAllCheckbox.addEventListener('change', (e) => {
                this.handleSelectAll(e, table);
            });
        }
    }
    
    handleRowClick(event, row, table) {
        const rowId = row.dataset.rowId;
        
        // Если таблица отключена для выделения
        if (table.classList.contains('no-selection')) {
            return;
        }
        
        // Предотвращаем выделение при клике на ссылки или кнопки
        if (event.target.tagName === 'A' || 
            event.target.tagName === 'BUTTON' || 
            event.target.closest('a, button, input, select, textarea')) {
            return;
        }
        
        // Проверяем, есть ли обработчики редактирования для этой таблицы
        // Если есть, то не перехватываем события
        if (table.id === 'records-table' && row.hasAttribute('data-record-id')) {
            console.log('🎯 Таблица с редактированием, не перехватываем событие');
            return;
        }
        
        event.preventDefault();
        
        if (this.isShiftPressed && this.lastSelectedRow) {
            // Выделение диапазона с Shift
            this.selectRange(this.lastSelectedRow, row, table);
        } else if (this.isCtrlPressed) {
            // Множественное выделение с Ctrl
            this.toggleRowSelection(row, 'ctrl-selected');
        } else {
            // Одиночное выделение
            this.selectSingleRow(row, table);
        }
        
        this.lastSelectedRow = row;
        this.updateSelectionInfo();
    }
    
    selectSingleRow(row, table) {
        console.log('🎯 Выделяем одиночную строку:', row);
        
        // Убираем выделение со всех строк в таблице
        const allRows = table.querySelectorAll('tbody tr');
        allRows.forEach(r => {
            r.classList.remove('selected', 'multi-selected', 'ctrl-selected', 'shift-selected');
        });
        
        // Выделяем выбранную строку
        row.classList.add('selected');
        console.log('✅ Добавлен класс selected к строке:', row);
        console.log('🎨 Классы строки после выделения:', row.className);
        
        this.selectedRows.clear();
        this.selectedRows.add(row.dataset.rowId);
    }
    
    toggleRowSelection(row, selectionClass) {
        const rowId = row.dataset.rowId;
        
        if (row.classList.contains(selectionClass)) {
            // Убираем выделение
            row.classList.remove(selectionClass);
            this.selectedRows.delete(rowId);
        } else {
            // Добавляем выделение
            row.classList.add(selectionClass);
            this.selectedRows.add(rowId);
        }
    }
    
    selectRange(startRow, endRow, table) {
        const rows = Array.from(table.querySelectorAll('tbody tr'));
        const startIndex = rows.indexOf(startRow);
        const endIndex = rows.indexOf(endRow);
        
        const minIndex = Math.min(startIndex, endIndex);
        const maxIndex = Math.max(startIndex, endIndex);
        
        // Убираем предыдущее выделение
        rows.forEach(row => {
            row.classList.remove('shift-selected');
        });
        
        // Выделяем диапазон
        for (let i = minIndex; i <= maxIndex; i++) {
            const row = rows[i];
            row.classList.add('shift-selected');
            this.selectedRows.add(row.dataset.rowId);
        }
    }
    
    handleSelectAll(event, table) {
        const isChecked = event.target.checked;
        const rows = table.querySelectorAll('tbody tr');
        
        rows.forEach(row => {
            if (isChecked) {
                row.classList.add('multi-selected');
                this.selectedRows.add(row.dataset.rowId);
            } else {
                row.classList.remove('multi-selected');
                this.selectedRows.delete(row.dataset.rowId);
            }
        });
        
        this.updateSelectionInfo();
    }
    
    handleKeyDown(event) {
        if (event.key === 'Control' || event.key === 'Meta') {
            this.isCtrlPressed = true;
        }
        if (event.key === 'Shift') {
            this.isShiftPressed = true;
        }
        
        // Горячие клавиши
        if (event.key === 'a' && (event.ctrlKey || event.metaKey)) {
            event.preventDefault();
            this.selectAllVisibleRows();
        }
        
        if (event.key === 'Escape') {
            this.clearAllSelection();
        }
    }
    
    handleKeyUp(event) {
        if (event.key === 'Control' || event.key === 'Meta') {
            this.isCtrlPressed = false;
        }
        if (event.key === 'Shift') {
            this.isShiftPressed = false;
        }
    }
    
    selectAllVisibleRows() {
        const tables = document.querySelectorAll('.data-table, .table');
        
        tables.forEach(table => {
            const rows = table.querySelectorAll('tbody tr:not([style*="display: none"])');
            rows.forEach(row => {
                row.classList.add('multi-selected');
                this.selectedRows.add(row.dataset.rowId);
            });
        });
        
        this.updateSelectionInfo();
    }
    
    clearAllSelection() {
        const selectedElements = document.querySelectorAll('.selected, .multi-selected, .ctrl-selected, .shift-selected');
        
        selectedElements.forEach(element => {
            element.classList.remove('selected', 'multi-selected', 'ctrl-selected', 'shift-selected');
        });
        
        this.selectedRows.clear();
        this.lastSelectedRow = null;
        this.updateSelectionInfo();
    }
    
    addSelectionControls() {
        // Проверяем, есть ли уже панель управления
        if (document.querySelector('.selection-controls')) {
            return;
        }
        
        const controlsHtml = `
            <div class="selection-controls" id="selectionControls" style="display: none;">
                <div class="selection-info" id="selectionInfo">
                    Выбрано: 0 строк
                </div>
                <button class="btn btn-outline-primary btn-sm" onclick="tableSelector.selectAllVisibleRows()">
                    <i class="fas fa-check-square"></i> Выбрать все
                </button>
                <button class="btn btn-outline-secondary btn-sm" onclick="tableSelector.clearAllSelection()">
                    <i class="fas fa-times"></i> Снять выделение
                </button>
                <button class="btn btn-outline-info btn-sm" onclick="tableSelector.exportSelectedRows()">
                    <i class="fas fa-download"></i> Экспорт
                </button>
                <div class="ms-auto">
                    <small class="text-muted">
                        <i class="fas fa-info-circle"></i>
                        Ctrl+клик: множественное выделение | Shift+клик: диапазон | Ctrl+A: выбрать все | Esc: снять выделение
                    </small>
                </div>
            </div>
        `;
        
        // Добавляем панель в начало контента
        const mainContent = document.querySelector('.container-fluid, .container, main');
        if (mainContent) {
            mainContent.insertAdjacentHTML('afterbegin', controlsHtml);
        }
    }
    
    updateSelectionInfo() {
        const controls = document.getElementById('selectionControls');
        const info = document.getElementById('selectionInfo');
        
        if (!controls || !info) return;
        
        const count = this.selectedRows.size;
        
        if (count > 0) {
            controls.style.display = 'flex';
            info.textContent = `Выбрано: ${count} строк`;
        } else {
            controls.style.display = 'none';
        }
    }
    
    exportSelectedRows() {
        const selectedData = [];
        
        this.selectedRows.forEach(rowId => {
            const row = document.querySelector(`[data-row-id="${rowId}"]`);
            if (row) {
                const cells = row.querySelectorAll('td');
                const rowData = {};
                
                cells.forEach((cell, index) => {
                    const header = row.closest('table').querySelector(`th:nth-child(${index + 1})`);
                    const columnName = header ? header.textContent.trim() : `Column ${index + 1}`;
                    rowData[columnName] = cell.textContent.trim();
                });
                
                selectedData.push(rowData);
            }
        });
        
        if (selectedData.length === 0) {
            this.showNotification('Нет выбранных строк для экспорта', 'warning');
            return;
        }
        
        // Создаем CSV
        const csv = this.convertToCSV(selectedData);
        this.downloadCSV(csv, 'selected_rows.csv');
        
        this.showNotification(`Экспортировано ${selectedData.length} строк`, 'success');
    }
    
    convertToCSV(data) {
        if (data.length === 0) return '';
        
        const headers = Object.keys(data[0]);
        const csvRows = [headers.join(',')];
        
        data.forEach(row => {
            const values = headers.map(header => {
                const value = row[header] || '';
                // Экранируем запятые и кавычки
                return `"${value.replace(/"/g, '""')}"`;
            });
            csvRows.push(values.join(','));
        });
        
        return csvRows.join('\n');
    }
    
    downloadCSV(csv, filename) {
        const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement('a');
        
        if (link.download !== undefined) {
            const url = URL.createObjectURL(blob);
            link.setAttribute('href', url);
            link.setAttribute('download', filename);
            link.style.visibility = 'hidden';
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
        }
    }
    
    showNotification(message, type = 'info') {
        // Создаем уведомление
        const notification = document.createElement('div');
        notification.className = `alert alert-${type} alert-dismissible fade show position-fixed`;
        notification.style.cssText = `
            top: 20px;
            right: 20px;
            z-index: 9999;
            min-width: 300px;
        `;
        
        notification.innerHTML = `
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        `;
        
        document.body.appendChild(notification);
        
        // Автоматически убираем через 3 секунды
        setTimeout(() => {
            if (notification.parentNode) {
                notification.remove();
            }
        }, 3000);
    }
    
    // Метод для получения выбранных строк
    getSelectedRows() {
        return Array.from(this.selectedRows);
    }
    
    // Метод для получения данных выбранных строк
    getSelectedRowsData() {
        const data = [];
        
        this.selectedRows.forEach(rowId => {
            const row = document.querySelector(`[data-row-id="${rowId}"]`);
            if (row) {
                const cells = row.querySelectorAll('td');
                const rowData = {};
                
                cells.forEach((cell, index) => {
                    const header = row.closest('table').querySelector(`th:nth-child(${index + 1})`);
                    const columnName = header ? header.textContent.trim() : `Column ${index + 1}`;
                    rowData[columnName] = cell.textContent.trim();
                });
                
                data.push(rowData);
            }
        });
        
        return data;
    }
}

// Создаем глобальный экземпляр
let tableSelector;

// Инициализируем после загрузки DOM
document.addEventListener('DOMContentLoaded', () => {
    tableSelector = new TableRowSelector();
});

// Инициализируем для динамически загруженного контента
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
        tableSelector = new TableRowSelector();
    });
} else {
    tableSelector = new TableRowSelector();
}

// Переменная для хранения выбранных записей
let selectedRecords = new Set();

// Функция для изменения количества записей на странице
function changePerPage(newPerPage) {
    const currentUrl = new URL(window.location);
    currentUrl.searchParams.set('per_page', newPerPage);
    currentUrl.searchParams.set('page', '1'); // Сбрасываем на первую страницу
    window.location.href = currentUrl.toString();
}


// Проверка функций будет в конце файла

// Функции для серверной фильтрации
window.initializeFilter = function() {
    // Автоматический поиск при вводе с сохранением фокуса
    const searchInput = document.querySelector('input[name="search"]');
    if (searchInput) {
        // Сохраняем фокус при поиске
        searchInput.addEventListener('input', function() {
            sessionStorage.setItem('lastFocusedSearch', 'true');
        });
        
        // Восстанавливаем фокус после поиска
        if (sessionStorage.getItem('lastFocusedSearch') === 'true') {
            setTimeout(() => {
                searchInput.focus();
                sessionStorage.removeItem('lastFocusedSearch');
            }, 100);
        }
    }
};

// Функция для изменения количества записей на странице
function changePerPage(newPerPage) {
    const currentUrl = new URL(window.location);
    currentUrl.searchParams.set('per_page', newPerPage);
    currentUrl.searchParams.set('page', '1'); // Сбрасываем на первую страницу
    window.location.href = currentUrl.toString();
}

// Функция для открытия файлов
function openFile(filePath) {
    try {
        // Проверяем, что путь к файлу существует
        if (!filePath || filePath.trim() === '') {
            showNotification('❌ Путь к файлу не указан');
            return;
        }
        
        // Заменяем обратные слеши на прямые для корректной работы в браузере
        const normalizedPath = filePath.replace(/\\/g, '/');
        
        // Создаем ссылку для скачивания файла
        const link = document.createElement('a');
        if (/^s3:\/\//i.test(normalizedPath)) {
            link.href = `/api/s3_object?uri=${encodeURIComponent(normalizedPath)}`;
        } else {
            link.href = `/api/open_file?path=${encodeURIComponent(normalizedPath)}`;
        }
        link.download = normalizedPath.split('/').pop();
        link.target = '_blank';
        
        // Добавляем ссылку в DOM и кликаем по ней
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        
        showNotification('📁 Попытка открыть файл: ' + normalizedPath.split('/').pop());
        
    } catch (error) {
        console.error('Ошибка при открытии файла:', error);
        showNotification('❌ Ошибка при открытии файла: ' + error.message);
    }
}

// Функция для показа уведомлений
function showNotification(message, type = 'info') {
    // Создаем уведомление
    const notification = document.createElement('div');
    notification.className = `alert alert-${type === 'error' ? 'danger' : type === 'success' ? 'success' : 'info'} alert-dismissible fade show position-fixed`;
    notification.style.cssText = 'top: 20px; right: 20px; z-index: 9999; min-width: 300px;';
    notification.innerHTML = `
        ${message}
        <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
    `;
    
    document.body.appendChild(notification);
    
    // Автоматически скрываем через 5 секунд
    setTimeout(() => {
        if (notification.parentNode) {
            notification.remove();
        }
    }, 5000);
}

// Функции для работы с чекбоксами
function toggleSelectAll() {
    const selectAllCheckbox = document.getElementById('select-all-checkbox');
    const recordCheckboxes = document.querySelectorAll('.record-checkbox');
    
    if (selectAllCheckbox && selectAllCheckbox.checked) {
        recordCheckboxes.forEach(checkbox => {
            const value = checkbox.value;
            if (value && value !== 'null' && value !== 'undefined' && value !== '') {
                checkbox.checked = true;
                selectedRecords.add(value);
            }
        });
    } else {
        recordCheckboxes.forEach(checkbox => {
            checkbox.checked = false;
            const value = checkbox.value;
            if (value) {
                selectedRecords.delete(value);
            }
        });
    }
    updateSelectedCount();
}

function updateSelectedCount() {
    const count = selectedRecords.size;
    const selectedCountElement = document.getElementById('selected-count');
    const transferButton = document.getElementById('transfer-selected-btn');
    
    if (selectedCountElement) {
        selectedCountElement.textContent = count;
    }
    
    if (transferButton) {
        if (count > 0) {
            transferButton.style.display = 'inline-block';
        } else {
            transferButton.style.display = 'none';
        }
    }
}

// Обработчик изменения чекбоксов
document.addEventListener('change', function(e) {
    if (e.target.classList.contains('record-checkbox')) {
        const recordId = e.target.value;
        
        if (e.target.checked) {
            selectedRecords.add(recordId);
        } else {
            selectedRecords.delete(recordId);
        }
        updateSelectedCount();
    }
});

// Дополнительный обработчик для чекбокса "Выбрать все"
document.addEventListener('change', function(e) {
    if (e.target.id === 'select-all-checkbox') {
        toggleSelectAll();
    }
});

// Функция переноса выбранных записей в журнал ремонтов
async function transferSelectedToRepairLog() {
    
    if (selectedRecords.size === 0) {
        alert('❌ Пожалуйста, выберите записи для переноса');
        return;
    }
    
    if (!confirm(`Вы уверены, что хотите перенести ${selectedRecords.size} записей в журнал ремонтов?`)) {
        return;
    }
    
    try {
        // Получаем данные выбранных записей
        const selectedData = [];
        const checkboxes = document.querySelectorAll('.record-checkbox:checked');
        
        
        checkboxes.forEach((checkbox, index) => {
            const recordId = checkbox.value;
            const row = checkbox.closest('tr');
            const cells = row.querySelectorAll('td');
            
            
            // Собираем данные из строки таблицы
            const recordData = {
                app_row_id: recordId
            };
            
            // Добавляем данные из ячеек (пропускаем первую ячейку с чекбоксом)
            for (let i = 1; i < cells.length; i++) {
                const cell = cells[i];
                const columnIndex = i - 1; // Индекс столбца (без учета чекбокса)
                const columnName = document.querySelector(`th:nth-child(${i + 1})`).textContent.trim();
                
                // Получаем текст из ячейки, убирая HTML теги
                const cellText = cell.textContent || cell.innerText || '';
                recordData[columnName] = cellText.trim();
                
            }
            
            selectedData.push(recordData);
        });
        
        
        // Отправляем запрос на сервер
        
        const response = await fetch('/api/transfer_to_weld_repair_log', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                records: selectedData
            })
        });
        
        const result = await response.json();
        
        if (result.success) {
            
            // Показываем детальную информацию пользователю
            let message = `✅ Перенос завершен!\n\n`;
            message += `📊 Статистика:\n`;
            message += `• Перенесено: ${result.transferred_count} записей\n`;
            message += `• Пропущено: ${result.skipped_count} записей\n`;
            message += `• Всего выбрано: ${selectedRecords.size} записей\n\n`;
            
            if (result.skipped_count > 0) {
                message += `ℹ️ Пропущенные записи могут быть:\n`;
                message += `• Уже существующими в журнале ремонтов\n`;
                message += `• Имеющими неподходящий статус для переноса\n`;
                message += `• Не найденными в исходной таблице`;
            }
            
            alert(message);
            
            // Снимаем выделение с чекбоксов
            selectedRecords.clear();
            document.querySelectorAll('.record-checkbox').forEach(cb => cb.checked = false);
            document.getElementById('select-all-checkbox').checked = false;
            updateSelectedCount();
            
            // Перезагружаем страницу для обновления данных
            window.location.reload();
        } else {
            alert(`❌ Ошибка: ${result.message}`);
        }
        
    } catch (error) {
        console.error('Ошибка при переносе записей:', error);
        alert('Ошибка при переносе записей: ' + error.message);
    }
}

// Функции для управления столбцами
function applyColumnSelection() {
    const checkedColumns = Array.from(document.querySelectorAll('.column-checkbox-input:checked'))
        .map(cb => cb.value);
    
    const currentUrl = new URL(window.location);
    currentUrl.searchParams.set('columns', checkedColumns.join(','));
    currentUrl.searchParams.set('page', '1');
    window.location.href = currentUrl.toString();
}

function selectAllColumns() {
    document.querySelectorAll('.column-checkbox-input').forEach(cb => {
        cb.checked = true;
    });
}

function deselectAllColumns() {
    document.querySelectorAll('.column-checkbox-input').forEach(cb => {
        cb.checked = false;
    });
}

function selectDefaultColumns() {
    // Показываем все столбцы по умолчанию (как в logs_lnk_table)
    document.querySelectorAll('.column-checkbox-input').forEach(cb => {
        cb.checked = true;
    });
}

// Функции для фильтрации
function clearSearch() {
    const searchInput = document.querySelector('input[name="search"]');
    if (searchInput) {
        searchInput.value = '';
        searchInput.form.submit();
    }
}

function clearAllFilters() {
    
    // Очищаем все поля фильтров
    const filterInputs = document.querySelectorAll('.column-filter');
    filterInputs.forEach(input => {
        input.value = '';
    });
    
    // Скрываем все кнопки очистки
    const clearBtns = document.querySelectorAll('.filter-clear-btn');
    clearBtns.forEach(btn => {
        btn.style.display = 'none';
    });
    
    // Убираем активные стили
    const filterContainers = document.querySelectorAll('.filter-container');
    filterContainers.forEach(container => {
        container.classList.remove('filter-active');
    });
    
    // Очищаем объект фильтров
    columnFilters = {};
    
    // НЕ применяем фильтры автоматически - пользователь должен нажать Enter
}

function applyColumnFilter(columnName, value) {
    
    if (value.trim() === '') {
        // Если значение пустое, удаляем фильтр
        delete columnFilters[columnName];
        
        // Скрываем кнопку очистки и убираем активный стиль
        const clearBtn = document.querySelector(`[data-column="${columnName}"] .filter-clear-btn`);
        const filterContainer = document.querySelector(`[data-column="${columnName}"]`);
        
        if (clearBtn && filterContainer) {
            clearBtn.style.display = 'none';
            filterContainer.classList.remove('filter-active');
        }
    } else {
        // Добавляем фильтр
        columnFilters[columnName] = value.trim();
        
        // Показываем кнопку очистки и добавляем активный стиль
        const clearBtn = document.querySelector(`[data-column="${columnName}"] .filter-clear-btn`);
        const filterContainer = document.querySelector(`[data-column="${columnName}"]`);
        
        if (clearBtn && filterContainer) {
            clearBtn.style.display = 'inline-block';
            filterContainer.classList.add('filter-active');
        }
    }
    
    // НЕ применяем фильтры автоматически - пользователь должен нажать Enter
}

function clearColumnFilter(columnName) {
    
    // Очищаем поле ввода
    const filterInput = document.querySelector(`[data-column="${columnName}"] .column-filter`);
    if (filterInput) {
        filterInput.value = '';
    }
    
    // Удаляем фильтр
    delete columnFilters[columnName];
    
    // Скрываем кнопку очистки и убираем активный стиль
    const clearBtn = document.querySelector(`[data-column="${columnName}"] .filter-clear-btn`);
    const filterContainer = document.querySelector(`[data-column="${columnName}"]`);
    
    if (clearBtn && filterContainer) {
        clearBtn.style.display = 'none';
        filterContainer.classList.remove('filter-active');
    }
    
    // НЕ применяем фильтры автоматически - пользователь должен нажать Enter
}

function handleFilterKeyPress(event, columnName) {
    if (event.key === 'Enter') {
        applyServerFilters();
    }
}

function saveFilterFocus(columnName) {
    sessionStorage.setItem('lastFocusedFilter', columnName);
    console.log(`🔧 Сохранен фокус для фильтра: ${columnName}`);
}

// Инициализация при загрузке страницы
document.addEventListener('DOMContentLoaded', function() {
    // Отладочная информация
    // Инициализируем счетчик выбранных записей
    updateSelectedCount();
    
    // Добавляем обработчики событий
    const transferButton = document.getElementById('transfer-selected-btn');
    const selectAllCheckbox = document.getElementById('select-all-checkbox');
    
    if (transferButton) {
        transferButton.addEventListener('click', transferSelectedToRepairLog);
    }
    
    if (selectAllCheckbox) {
        selectAllCheckbox.addEventListener('change', toggleSelectAll);
    }
    
    // Добавляем обработчики для всех кнопок
    const clearSearchBtn = document.getElementById('clear-search-btn');
    if (clearSearchBtn) {
        clearSearchBtn.addEventListener('click', clearSearch);
    }
    
    const applyColumnSelectionBtn = document.getElementById('apply-column-selection-btn');
    if (applyColumnSelectionBtn) {
        applyColumnSelectionBtn.addEventListener('click', applyColumnSelection);
    }
    
    const selectAllColumnsBtn = document.getElementById('select-all-columns-btn');
    if (selectAllColumnsBtn) {
        selectAllColumnsBtn.addEventListener('click', selectAllColumns);
    }
    
    const deselectAllColumnsBtn = document.getElementById('deselect-all-columns-btn');
    if (deselectAllColumnsBtn) {
        deselectAllColumnsBtn.addEventListener('click', deselectAllColumns);
    }
    
    const selectDefaultColumnsBtn = document.getElementById('select-default-columns-btn');
    if (selectDefaultColumnsBtn) {
        selectDefaultColumnsBtn.addEventListener('click', selectDefaultColumns);
    }
    
    const perPageSelect = document.getElementById('per_page_select');
    if (perPageSelect) {
        perPageSelect.addEventListener('change', function() {
            changePerPage(this.value);
        });
    }
    
    const clearAllFiltersBtn = document.getElementById('clear-all-filters-btn');
    if (clearAllFiltersBtn) {
        clearAllFiltersBtn.addEventListener('click', clearAllFilters);
    }
    
    // Обработчики для фильтров столбцов
    const columnFilters = document.querySelectorAll('.column-filter');
    columnFilters.forEach(filter => {
        filter.addEventListener('keyup', function() {
            applyColumnFilter(this.dataset.column, this.value);
        });
        filter.addEventListener('keypress', function(e) {
            handleFilterKeyPress(e, this.dataset.column);
        });
        filter.addEventListener('focus', function() {
            saveFilterFocus(this.dataset.column);
        });
        filter.addEventListener('click', function() {
            saveFilterFocus(this.dataset.column);
        });
    });
    
    // Обработчики для кнопок очистки фильтров
    const filterClearBtns = document.querySelectorAll('.filter-clear-btn');
    filterClearBtns.forEach(btn => {
        btn.addEventListener('click', function() {
            clearColumnFilter(this.dataset.column);
        });
    });
    
    // Обработчики для кнопок открытия файлов
    const openFileBtns = document.querySelectorAll('.open-file-btn');
    openFileBtns.forEach(btn => {
        btn.addEventListener('click', function() {
            openFile(this.dataset.filePath);
        });
    });
    
    
    // Восстанавливаем фокус на последнем активном поле фильтра
    const lastFocusedFilter = sessionStorage.getItem('lastFocusedFilter');
    if (lastFocusedFilter) {
        const filterInput = document.querySelector(`[data-column="${lastFocusedFilter}"] .column-filter`);
        if (filterInput) {
            // Восстанавливаем фокус с небольшой задержкой
            setTimeout(() => {
                filterInput.focus();
                // Устанавливаем курсор в конец текста
                const length = filterInput.value.length;
                filterInput.setSelectionRange(length, length);
            }, 100);
        }
        // Очищаем сохраненный фокус
        sessionStorage.removeItem('lastFocusedFilter');
    }
    
    // Инициализация системы выделения строк
    setTimeout(() => {
        if (typeof tableSelector !== 'undefined') {
            
            // Добавляем дополнительную инициализацию для этой страницы
            const tables = document.querySelectorAll('.data-table, .table');
            tables.forEach((table, index) => {
                tableSelector.initializeTable(table, index);
            });
        }
    }, 500);
    
});

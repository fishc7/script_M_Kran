console.log('📄 extract_numbers_v2.js загружается...');
console.log('🔧 Используем делегирование событий для обхода CSP');

// Глобальные функции
window.showExtractNumbersModal = function() {
    console.log('=== showExtractNumbersModal вызвана ===');
    console.log('Попытка открыть модальное окно...');
    
    var modalElement = document.getElementById('extractNumbersModal');
    if (modalElement) {
        console.log('Модальное окно найдено, открываем...');
        var modal = new bootstrap.Modal(modalElement);
        modal.show();
        
        // Загружаем список таблиц при открытии модального окна
        loadTables();
        console.log('Модальное окно должно быть открыто');
    } else {
        console.error('❌ Модальное окно не найдено!');
        alert('Ошибка: Модальное окно не найдено!');
    }
};

// Загрузка списка таблиц
function loadTables() {
    fetch('/api/get_tables')
        .then(response => response.json())
        .then(data => {
            const tableSelect = document.getElementById('tableSelect');
            tableSelect.innerHTML = '<option value="">Выберите таблицу</option>';
            
            if (data.tables) {
                data.tables.forEach(table => {
                    const option = document.createElement('option');
                    option.value = table;
                    option.textContent = table;
                    tableSelect.appendChild(option);
                });
            }
        })
        .catch(error => {
            console.error('Ошибка загрузки таблиц:', error);
            document.getElementById('tableSelect').innerHTML = '<option value="">Ошибка загрузки</option>';
        });
}

// Загрузка столбцов таблицы
function loadColumns(tableName) {
    const columnSelect = document.getElementById('columnSelect');
    columnSelect.disabled = true;
    columnSelect.innerHTML = '<option value="">Загрузка столбцов...</option>';
    
    fetch('/api/get_columns', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ table_name: tableName })
    })
    .then(response => response.json())
    .then(data => {
        columnSelect.innerHTML = '<option value="">Выберите столбец</option>';
        
        if (data.columns) {
            data.columns.forEach(column => {
                const option = document.createElement('option');
                option.value = column;
                option.textContent = column;
                columnSelect.appendChild(option);
            });
        }
        columnSelect.disabled = false;
    })
    .catch(error => {
        console.error('Ошибка загрузки столбцов:', error);
        columnSelect.innerHTML = '<option value="">Ошибка загрузки</option>';
        columnSelect.disabled = true;
    });
}

// Обработка удаления префиксов
window.processPrefixRemoval = function() {
    const tableName = document.getElementById('tableSelect').value;
    const sourceColumn = document.getElementById('columnSelect').value;
    const mode = document.getElementById('modeSelect').value;
    const targetColumn = document.getElementById('targetColumnInput').value.trim();
    
    if (!tableName || !sourceColumn) {
        alert('Пожалуйста, выберите таблицу и столбец');
        return;
    }
    
    const processButton = document.getElementById('processButton');
    const processingStatus = document.getElementById('processingStatus');
    const statusText = document.getElementById('statusText');
    
    // Показываем статус обработки
    processButton.disabled = true;
    processingStatus.classList.remove('d-none');
    statusText.textContent = 'Обработка данных...';
    
    fetch('/api/process_table_numbers', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            table_name: tableName,
            source_column: sourceColumn,
            target_column: targetColumn || null,
            mode: mode
        })
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            statusText.textContent = data.message;
            processingStatus.className = 'alert alert-success';
            setTimeout(() => {
                bootstrap.Modal.getInstance(document.getElementById('extractNumbersModal')).hide();
            }, 2000);
        } else {
            statusText.textContent = 'Ошибка: ' + (data.error || 'Неизвестная ошибка');
            processingStatus.className = 'alert alert-danger';
        }
    })
    .catch(error => {
        console.error('Ошибка обработки:', error);
        statusText.textContent = 'Ошибка сети';
        processingStatus.className = 'alert alert-danger';
    })
    .finally(() => {
        processButton.disabled = false;
    });
};

// Инициализация при загрузке страницы
document.addEventListener('DOMContentLoaded', function() {
    console.log('🚨 === Инициализация функций извлечения чисел v2 === 🚨');
    console.log('📁 Файл extract_numbers_v2.js загружен успешно!');
    
    // Обработчик изменения фильтра по титулам
    const titleSelect = document.getElementById('titleSelect');
    if (titleSelect) {
        titleSelect.addEventListener('change', function() {
            var selectedTitle = this.value;
            var loadingIndicator = document.getElementById('loadingIndicator');
            
            // Показать индикатор загрузки
            if (loadingIndicator) {
                loadingIndicator.classList.remove('d-none');
                
                // Здесь можно добавить AJAX запрос для обновления данных
                setTimeout(function() {
                    loadingIndicator.classList.add('d-none');
                }, 1000);
            }
        });
    }
    
    // Обработчик выбора таблицы в модальном окне
    const tableSelect = document.getElementById('tableSelect');
    if (tableSelect) {
        tableSelect.addEventListener('change', function() {
            const selectedTable = this.value;
            if (selectedTable) {
                loadColumns(selectedTable);
            } else {
                document.getElementById('columnSelect').innerHTML = '<option value="">Сначала выберите таблицу</option>';
                document.getElementById('columnSelect').disabled = true;
            }
        });
    }
    
    // Обработчик выбора столбца для автоматического заполнения названия
    const columnSelect = document.getElementById('columnSelect');
    if (columnSelect) {
        columnSelect.addEventListener('change', function() {
            const selectedColumn = this.value;
            const targetColumnInput = document.getElementById('targetColumnInput');
            if (selectedColumn && targetColumnInput) {
                // Автоматически предлагаем название столбца
                const suggestedName = `_${selectedColumn}_без_S_F_`;
                targetColumnInput.value = suggestedName;
                targetColumnInput.placeholder = suggestedName;
            }
        });
    }
    
    // Обработчики для кликабельных карточек
    document.querySelectorAll('.clickable-card').forEach(function(card) {
        card.addEventListener('click', function(e) {
            e.preventDefault();
            var filter = this.getAttribute('data-filter');
            console.log('Клик по карточке с фильтром:', filter);
            
            // Добавляем визуальный эффект при клике
            this.style.transform = 'scale(0.95)';
            setTimeout(() => {
                this.style.transform = 'scale(1)';
            }, 150);
            
            // Здесь можно добавить логику фильтрации или навигации
            if (filter === 'all_defects') {
                window.location.href = '/filtered_data?filter=all';
            } else if (filter === 'rk_defects') {
                window.location.href = '/filtered_data?filter=rk';
            }
        });
    });
    
    // Обработчик для карточки "Извлечение чисел" через делегирование
    console.log('🔍 Поиск карточки "Извлечение чисел"...');
    const extractNumbersCard = document.getElementById('extractNumbersCard');
    console.log('🔍 Результат поиска:', extractNumbersCard);
    
    if (extractNumbersCard) {
        console.log('✅ Найдена карточка "Извлечение чисел"');
        console.log('🎨 Стили карточки:', extractNumbersCard.style.cssText);
        
        extractNumbersCard.addEventListener('click', function(e) {
            console.log('🎯 Клик по карточке "Извлечение чисел"');
            e.preventDefault();
            e.stopPropagation();
            
            // Добавляем визуальный эффект при клике
            this.style.transform = 'scale(0.95)';
            setTimeout(() => {
                this.style.transform = 'scale(1)';
            }, 150);
            
            // Вызываем функцию показа модального окна
            window.showExtractNumbersModal();
        });
    } else {
        console.log('❌ Карточка "Извлечение чисел" не найдена');
        console.log('🔍 Все элементы с nav-card-modern:', document.querySelectorAll('.nav-card-modern'));
        console.log('🔍 Все элементы с id:', document.querySelectorAll('[id]'));
    }
    
    // Дополнительная защита от случайного перехода на дубликаты
    document.querySelectorAll('a[href*="duplicates"]').forEach(function(link) {
        link.addEventListener('click', function(e) {
            console.log('⚠️ Попытка перехода на дубликаты:', this.href);
            // Не блокируем, но логируем для отладки
        });
    });
    
    // Обработчик для кнопки "Выполнить" в модальном окне
    const processButton = document.getElementById('processButton');
    if (processButton) {
        processButton.addEventListener('click', function(e) {
            e.preventDefault();
            window.processPrefixRemoval();
        });
    }
    
    // Универсальный обработчик для data-action атрибутов
    document.addEventListener('click', function(e) {
        const action = e.target.getAttribute('data-action');
        if (action === 'showExtractNumbersModal') {
            e.preventDefault();
            e.stopPropagation();
            window.showExtractNumbersModal();
        } else if (action === 'processPrefixRemoval') {
            e.preventDefault();
            window.processPrefixRemoval();
        }
    });
});

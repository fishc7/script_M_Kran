/**
 * Модуль для управления парсингом НАКС
 */

let currentScriptId = null;
let statusCheckInterval = null;

/**
 * Запуск парсинга НАКС
 */
async function startNaksParsing() {
    try {
        // Показываем предупреждение
        if (!confirm('Парсинг НАКС запустит браузер. Вам нужно будет решить CAPTCHA вручную. Продолжить?')) {
            return;
        }

        // Блокируем кнопку запуска
        const startBtn = document.getElementById('startParsingBtn');
        startBtn.disabled = true;
        startBtn.innerHTML = '<i class="fas fa-spinner fa-spin me-2"></i>Запуск...';

        // Показываем секции прогресса и лога
        document.getElementById('progressSection').style.display = 'block';
        document.getElementById('logSection').style.display = 'block';
        document.getElementById('resultsSection').style.display = 'none';

        // Очищаем лог
        document.getElementById('logOutput').innerHTML = '<div class="text-muted">Запуск парсинга...</div>';

        // Запускаем парсинг
        const response = await fetch('/api/naks/parse', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            }
        });

        const data = await response.json();

        if (data.success) {
            currentScriptId = data.script_id;
            addLog('✅ Парсинг запущен успешно. ID задачи: ' + currentScriptId);
            addLog('⏳ Ожидание решения CAPTCHA...');
            
            // Начинаем проверку статуса
            startStatusCheck();
        } else {
            addLog('❌ Ошибка запуска: ' + data.message, 'error');
            startBtn.disabled = false;
            startBtn.innerHTML = '<i class="fas fa-play me-2"></i>Начать парсинг НАКС';
        }

    } catch (error) {
        console.error('Ошибка запуска парсинга:', error);
        addLog('❌ Ошибка подключения к серверу: ' + error.message, 'error');
        
        const startBtn = document.getElementById('startParsingBtn');
        startBtn.disabled = false;
        startBtn.innerHTML = '<i class="fas fa-play me-2"></i>Начать парсинг НАКС';
    }
}

/**
 * Начало проверки статуса
 */
function startStatusCheck() {
    if (statusCheckInterval) {
        clearInterval(statusCheckInterval);
    }

    // Проверяем статус каждые 2 секунды
    statusCheckInterval = setInterval(checkParsingStatus, 2000);
    
    // Первая проверка сразу
    checkParsingStatus();
}

/**
 * Проверка статуса парсинга
 */
async function checkParsingStatus() {
    if (!currentScriptId) {
        return;
    }

    try {
        const response = await fetch(`/api/naks/status/${currentScriptId}`);
        const data = await response.json();

        if (!data.success) {
            addLog('❌ Ошибка получения статуса: ' + data.message, 'error');
            stopStatusCheck();
            return;
        }

        // Обновляем прогресс
        updateProgress(data.progress || 0, data.message || '', data.elapsed_time || 0);

        // Обновляем лог
        if (data.output) {
            updateLog(data.output);
        }

        if (data.errors) {
            addLog('⚠️ Ошибки: ' + data.errors, 'error');
        }

        // Проверяем статус выполнения
        if (data.status === 'completed') {
            stopStatusCheck();
            addLog('✅ Парсинг завершен успешно!', 'success');
            showResults();
            
            // Разблокируем кнопку
            const startBtn = document.getElementById('startParsingBtn');
            startBtn.disabled = false;
            startBtn.innerHTML = '<i class="fas fa-play me-2"></i>Начать парсинг НАКС';
            
        } else if (data.status === 'failed') {
            stopStatusCheck();
            addLog('❌ Парсинг завершился с ошибкой', 'error');
            
            // Разблокируем кнопку
            const startBtn = document.getElementById('startParsingBtn');
            startBtn.disabled = false;
            startBtn.innerHTML = '<i class="fas fa-play me-2"></i>Начать парсинг НАКС';
        }

    } catch (error) {
        console.error('Ошибка проверки статуса:', error);
        addLog('❌ Ошибка проверки статуса: ' + error.message, 'error');
    }
}

/**
 * Остановка проверки статуса
 */
function stopStatusCheck() {
    if (statusCheckInterval) {
        clearInterval(statusCheckInterval);
        statusCheckInterval = null;
    }
}

/**
 * Обновление прогресс-бара
 */
function updateProgress(progress, message, elapsedTime) {
    const progressBar = document.getElementById('parsingProgress');
    const progressText = document.getElementById('progressText');
    const elapsedTimeEl = document.getElementById('elapsedTime');

    progressBar.style.width = progress + '%';
    progressBar.textContent = Math.round(progress) + '%';
    progressText.textContent = message || 'Выполнение...';

    if (elapsedTime) {
        elapsedTimeEl.textContent = 'Время выполнения: ' + formatTime(elapsedTime);
    }
}

/**
 * Форматирование времени
 */
function formatTime(seconds) {
    const mins = Math.floor(seconds / 60);
    const secs = Math.floor(seconds % 60);
    return mins + 'м ' + secs + 'с';
}

/**
 * Добавление записи в лог
 */
function addLog(message, type = 'info') {
    const logOutput = document.getElementById('logOutput');
    const timestamp = new Date().toLocaleTimeString();
    
    let className = '';
    if (type === 'error') {
        className = 'text-danger';
    } else if (type === 'success') {
        className = 'text-success';
    } else if (type === 'warning') {
        className = 'text-warning';
    }

    const logEntry = document.createElement('div');
    logEntry.className = className;
    logEntry.innerHTML = `[${timestamp}] ${message}`;
    
    logOutput.appendChild(logEntry);
    
    // Прокручиваем вниз
    logOutput.scrollTop = logOutput.scrollHeight;
}

/**
 * Обновление лога из вывода скрипта
 */
function updateLog(output) {
    const logOutput = document.getElementById('logOutput');
    
    // Разбиваем вывод на строки
    const lines = output.split('\n').filter(line => line.trim());
    
    // Очищаем старый лог (кроме последних 100 строк)
    const existingLines = logOutput.children;
    if (existingLines.length > 100) {
        while (existingLines.length > 50) {
            logOutput.removeChild(existingLines[0]);
        }
    }
    
    // Добавляем новые строки
    lines.forEach(line => {
        if (line.trim()) {
            const logEntry = document.createElement('div');
            
            // Определяем тип сообщения по содержимому
            if (line.includes('❌') || line.includes('ERROR') || line.includes('Ошибка')) {
                logEntry.className = 'text-danger';
            } else if (line.includes('✅') || line.includes('SUCCESS') || line.includes('успешно')) {
                logEntry.className = 'text-success';
            } else if (line.includes('⚠️') || line.includes('WARNING')) {
                logEntry.className = 'text-warning';
            } else {
                logEntry.className = 'text-light';
            }
            
            logEntry.textContent = line;
            logOutput.appendChild(logEntry);
        }
    });
    
    // Прокручиваем вниз
    logOutput.scrollTop = logOutput.scrollHeight;
}

/**
 * Показ результатов парсинга
 */
async function showResults(data = null) {
    try {
        // Если данные не переданы, получаем их
        if (!data) {
            const response = await fetch('/api/naks/results');
            data = await response.json();
        }

        if (data.success) {
            const resultsInfo = document.getElementById('resultsInfo');
            let html = '<div class="row">';
            let hasFiles = false;

            for (const [key, fileInfo] of Object.entries(data.files)) {
                if (fileInfo.exists) {
                    hasFiles = true;
                    const sizeMB = (fileInfo.size / 1024 / 1024).toFixed(2);
                    html += `
                        <div class="col-md-4 mb-3">
                            <div class="card border-success">
                                <div class="card-body">
                                    <h6 class="card-title text-success">
                                        <i class="fas fa-file-excel me-2"></i>
                                        ${getFileName(key)}
                                    </h6>
                                    <p class="card-text small mb-1">
                                        <strong>Размер:</strong> ${sizeMB} МБ<br>
                                        <strong>Изменен:</strong> ${fileInfo.modified}
                                    </p>
                                </div>
                            </div>
                        </div>
                    `;
                }
            }
            
            if (!hasFiles) {
                html += `
                    <div class="col-12">
                        <div class="alert alert-warning">
                            <i class="fas fa-exclamation-triangle me-2"></i>
                            Файлы результатов не найдены. Запустите парсинг сначала.
                        </div>
                    </div>
                `;
            }

            html += '</div>';
            resultsInfo.innerHTML = html;
            document.getElementById('resultsSection').style.display = 'block';
        } else {
            alert('Ошибка получения результатов: ' + data.message);
        }

    } catch (error) {
        console.error('Ошибка получения результатов:', error);
        alert('Ошибка получения результатов: ' + error.message);
    }
}

/**
 * Получение читаемого имени файла
 */
function getFileName(key) {
    const names = {
        'main_file': 'naks_главное.xlsx',
        'details_file': 'naks_подробнее.xlsx',
        'merged_file': 'naks_merged.xlsx'
    };
    return names[key] || key;
}

/**
 * Проверка результатов
 */
async function checkResults() {
    try {
        // Показываем индикатор загрузки
        const btn = event.target.closest('button');
        const originalText = btn.innerHTML;
        btn.disabled = true;
        btn.innerHTML = '<i class="fas fa-spinner fa-spin me-2"></i>Загрузка...';
        
        const response = await fetch('/api/naks/results');
        const data = await response.json();
        
        btn.disabled = false;
        btn.innerHTML = originalText;
        
        if (data.success) {
            showResults(data);
            // Автоматически загружаем данные для просмотра
            loadNaksData();
        } else {
            alert('Ошибка получения результатов: ' + data.message);
        }
    } catch (error) {
        console.error('Ошибка проверки результатов:', error);
        alert('Ошибка подключения к серверу: ' + error.message);
        
        const btn = event.target.closest('button');
        btn.disabled = false;
        btn.innerHTML = '<i class="fas fa-file-excel me-2"></i>Просмотреть результаты';
    }
}

/**
 * Загрузка результатов в базу данных
 */
let loadToDbScriptId = null;
let loadToDbStatusInterval = null;

async function loadToDatabase() {
    if (!confirm('Загрузить результаты парсинга в базу данных?')) {
        return;
    }

    try {
        // Блокируем кнопку
        const btn = event.target.closest('button');
        const originalText = btn.innerHTML;
        btn.disabled = true;
        btn.innerHTML = '<i class="fas fa-spinner fa-spin me-2"></i>Загрузка...';
        
        // Показываем секцию прогресса загрузки
        const loadProgressSection = document.getElementById('loadToDbProgressSection');
        if (loadProgressSection) {
            loadProgressSection.style.display = 'block';
        }
        
        // Запускаем загрузку
        const response = await fetch('/api/naks/load_to_db', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            }
        });
        
        const data = await response.json();
        
        if (data.success) {
            loadToDbScriptId = data.script_id;
            addLog('✅ Загрузка в БД запущена. ID задачи: ' + loadToDbScriptId, 'success');
            
            // Начинаем проверку статуса загрузки
            startLoadToDbStatusCheck();
        } else {
            addLog('❌ Ошибка запуска загрузки: ' + data.message, 'error');
            btn.disabled = false;
            btn.innerHTML = originalText;
            if (loadProgressSection) {
                loadProgressSection.style.display = 'none';
            }
        }
        
    } catch (error) {
        console.error('Ошибка загрузки в БД:', error);
        addLog('❌ Ошибка подключения к серверу: ' + error.message, 'error');
        
        const btn = event.target.closest('button');
        btn.disabled = false;
        btn.innerHTML = '<i class="fas fa-database me-2"></i>Загрузить в БД';
    }
}

/**
 * Начало проверки статуса загрузки в БД
 */
function startLoadToDbStatusCheck() {
    if (loadToDbStatusInterval) {
        clearInterval(loadToDbStatusInterval);
    }
    
    // Проверяем статус каждые 2 секунды
    loadToDbStatusInterval = setInterval(checkLoadToDbStatus, 2000);
    
    // Первая проверка сразу
    checkLoadToDbStatus();
}

/**
 * Проверка статуса загрузки в БД
 */
async function checkLoadToDbStatus() {
    if (!loadToDbScriptId) {
        return;
    }
    
    try {
        const response = await fetch(`/api/naks/status/${loadToDbScriptId}`);
        const data = await response.json();
        
        if (!data.success) {
            addLog('❌ Ошибка получения статуса загрузки: ' + data.message, 'error');
            stopLoadToDbStatusCheck();
            return;
        }
        
        // Обновляем лог
        if (data.output) {
            updateLog(data.output);
        }
        
        if (data.errors) {
            addLog('⚠️ Ошибки при загрузке: ' + data.errors, 'error');
        }
        
        // Проверяем статус выполнения
        if (data.status === 'completed') {
            stopLoadToDbStatusCheck();
            addLog('✅ Загрузка в БД завершена успешно!', 'success');
            
            // Разблокируем кнопку
            const btn = document.querySelector('button[onclick="loadToDatabase()"]');
            if (btn) {
                btn.disabled = false;
                btn.innerHTML = '<i class="fas fa-database me-2"></i>Загрузить в БД';
            }
            
            // Скрываем прогресс
            const loadProgressSection = document.getElementById('loadToDbProgressSection');
            if (loadProgressSection) {
                loadProgressSection.style.display = 'none';
            }
            
            // Показываем сообщение об успехе
            alert('Загрузка в БД завершена успешно!');
            
        } else if (data.status === 'failed') {
            stopLoadToDbStatusCheck();
            addLog('❌ Загрузка в БД завершилась с ошибкой', 'error');
            
            // Разблокируем кнопку
            const btn = document.querySelector('button[onclick="loadToDatabase()"]');
            if (btn) {
                btn.disabled = false;
                btn.innerHTML = '<i class="fas fa-database me-2"></i>Загрузить в БД';
            }
            
            // Скрываем прогресс
            const loadProgressSection = document.getElementById('loadToDbProgressSection');
            if (loadProgressSection) {
                loadProgressSection.style.display = 'none';
            }
        }
        
    } catch (error) {
        console.error('Ошибка проверки статуса загрузки:', error);
        addLog('❌ Ошибка проверки статуса: ' + error.message, 'error');
    }
}

/**
 * Остановка проверки статуса загрузки в БД
 */
function stopLoadToDbStatusCheck() {
    if (loadToDbStatusInterval) {
        clearInterval(loadToDbStatusInterval);
        loadToDbStatusInterval = null;
    }
}

/**
 * Загрузка данных НАКС для просмотра
 */
let naksDataTable = null;

async function loadNaksData(source = 'auto') {
    try {
        // Показываем секцию просмотра данных
        document.getElementById('dataViewSection').style.display = 'block';
        document.getElementById('dataViewSource').textContent = 'Загрузка данных...';
        
        // Добавляем параметр source в запрос
        const url = source === 'auto' ? '/api/naks/data' : `/api/naks/data?source=${source}`;
        const response = await fetch(url);
        const data = await response.json();
        
        if (!data.success) {
            document.getElementById('dataViewSource').innerHTML = 
                `<span class="text-danger">Ошибка: ${data.message}</span>`;
            return;
        }
        
        // Обновляем информацию об источнике данных
        document.getElementById('dataViewSource').innerHTML = 
            `<strong>Источник:</strong> ${data.data_source} | <strong>${data.message}</strong>`;
        
        // Очищаем таблицу если она уже была инициализирована
        if (naksDataTable) {
            naksDataTable.destroy();
        }
        
        // Создаем заголовки таблицы
        const headerRow = document.getElementById('naksTableHeader');
        headerRow.innerHTML = '';
        data.columns.forEach(col => {
            const th = document.createElement('th');
            th.textContent = col;
            th.style.whiteSpace = 'nowrap';
            headerRow.appendChild(th);
        });
        
        // Заполняем тело таблицы
        const tbody = document.getElementById('naksTableBody');
        tbody.innerHTML = '';
        
        if (data.records.length === 0) {
            const tr = document.createElement('tr');
            tr.innerHTML = `<td colspan="${data.columns.length}" class="text-center text-muted">Нет данных для отображения</td>`;
            tbody.appendChild(tr);
        } else {
            data.records.forEach(record => {
                const tr = document.createElement('tr');
                data.columns.forEach(col => {
                    const td = document.createElement('td');
                    const value = record[col] || '';
                    td.textContent = value;
                    td.style.maxWidth = '200px';
                    td.style.overflow = 'hidden';
                    td.style.textOverflow = 'ellipsis';
                    td.style.whiteSpace = 'nowrap';
                    td.title = value; // Подсказка при наведении
                    tr.appendChild(td);
                });
                tbody.appendChild(tr);
            });
        }
        
        // Инициализируем DataTable
        naksDataTable = $('#naksDataTable').DataTable({
            language: {
                url: '//cdn.datatables.net/plug-ins/1.13.7/i18n/ru.json'
            },
            pageLength: 25,
            lengthMenu: [[10, 25, 50, 100, -1], [10, 25, 50, 100, "Все"]],
            order: [[0, 'desc']],
            scrollX: true,
            scrollY: '500px',
            scrollCollapse: true,
            fixedHeader: true,
            columnDefs: [
                {
                    targets: '_all',
                    render: function(data, type, row) {
                        if (type === 'display' && data && data.length > 50) {
                            return data.substring(0, 50) + '...';
                        }
                        return data;
                    }
                }
            ]
        });
        
        // Прокручиваем к секции просмотра данных
        document.getElementById('dataViewSection').scrollIntoView({ behavior: 'smooth', block: 'start' });
        
    } catch (error) {
        console.error('Ошибка загрузки данных НАКС:', error);
        document.getElementById('dataViewSource').innerHTML = 
            `<span class="text-danger">Ошибка подключения: ${error.message}</span>`;
    }
}

/**
 * Обновление данных НАКС
 */
function refreshNaksData() {
    // Сохраняем текущий источник или используем auto
    loadNaksData('auto');
}

/**
 * Закрытие просмотра данных
 */
function closeDataView() {
    document.getElementById('dataViewSection').style.display = 'none';
    if (naksDataTable) {
        naksDataTable.destroy();
        naksDataTable = null;
    }
}

// Очистка при разгрузке страницы
window.addEventListener('beforeunload', function() {
    stopStatusCheck();
    stopLoadToDbStatusCheck();
    if (naksDataTable) {
        naksDataTable.destroy();
    }
});


console.log('=== SCRIPTS.JS ЗАГРУЖЕН ===');

// Загрузка ETL скриптов при загрузке страницы
document.addEventListener('DOMContentLoaded', function() {
    console.log('SCRIPTS.JS: DOM loaded, initializing...');
    loadETLScripts();
    setupEventListeners();
});

// Функция загрузки ETL скриптов
function loadETLScripts() {
    fetch('/api/etl_scripts')
        .then(response => response.json())
        .then(data => {
            console.log('ETL скрипты загружены:', data);
        })
        .catch(error => {
            console.error('Ошибка загрузки ETL скриптов:', error);
        });
}

// Настройка обработчиков событий
function setupEventListeners() {
    console.log('Setting up event listeners...');
    
    // Кнопки запуска отдельных скриптов
    const scriptButtons = document.querySelectorAll('[data-script-path]');
    console.log('Found script buttons:', scriptButtons.length);
    scriptButtons.forEach(button => {
        button.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation();
            const scriptPath = this.getAttribute('data-script-path');
            const scriptName = this.getAttribute('data-script-name');
            console.log('Script button clicked!');
            console.log('Script path:', scriptPath);
            console.log('Script name:', scriptName);
            runScript(scriptPath, scriptName);
        });
    });

    // Кнопки запуска этапов
    const stageButtons = document.querySelectorAll('[data-stage-id]');
    stageButtons.forEach(button => {
        button.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation();
            const stageId = this.getAttribute('data-stage-id');
            const stageName = this.getAttribute('data-stage-name');
            runETLStage(stageId, stageName);
        });
    });

    // Кнопки запуска подкатегорий
    const subcategoryButtons = document.querySelectorAll('[data-subcategory-id]');
    subcategoryButtons.forEach(button => {
        button.addEventListener('click', function(e) {
            e.preventDefault();
            e.stopPropagation();
            const subcategoryId = this.getAttribute('data-subcategory-id');
            const subcategoryName = this.getAttribute('data-subcategory-name');
            runSubcategory(subcategoryId, subcategoryName);
        });
    });

    // Кнопка запуска всего ETL пайплайна
    const pipelineButton = document.getElementById('runETLPipeline');
    if (pipelineButton) {
        pipelineButton.addEventListener('click', function(e) {
            e.preventDefault();
            runETLPipeline();
        });
    }

    // Кнопки Extract и Transform
    const extractButton = document.getElementById('runExtractOnly');
    if (extractButton) {
        extractButton.addEventListener('click', function(e) {
            e.preventDefault();
            runExtractOnly();
        });
    }

    const transformButton = document.getElementById('runTransformOnly');
    if (transformButton) {
        transformButton.addEventListener('click', function(e) {
            e.preventDefault();
            runTransformOnly();
        });
    }

    // Кнопка показа ETL руководства
    const guideButton = document.getElementById('showETLGuide');
    if (guideButton) {
        guideButton.addEventListener('click', function(e) {
            e.preventDefault();
            showETLGuide();
        });
    }

    // Кнопки открытия папок
    const folderButtons = document.querySelectorAll('[data-folder-path]');
    folderButtons.forEach(button => {
        button.addEventListener('click', function(e) {
            e.preventDefault();
            const folderPath = this.getAttribute('data-folder-path');
            openFolder(folderPath);
        });
    });
}

// Функция запуска отдельного скрипта
function runScript(scriptPath, scriptName) {
    console.log('=== ЗАПУСК СКРИПТА ===');
    console.log('Script path:', scriptPath);
    console.log('Script name:', scriptName);
    
    const requestData = {
        script_path: scriptPath,
        script_name: scriptName
    };

    showLoading('Запуск скрипта: ' + scriptName);
    
    console.log('Sending request to /run_script with data:', requestData);
    
    fetch('/run_script', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(requestData)
    })
    .then(response => {
        console.log('Response status:', response.status);
        console.log('Response headers:', response.headers);
        return response.json();
    })
    .then(data => {
        console.log('Response data:', data);
        hideLoading();
        showResults(data);
    })
    .catch(error => {
        console.log('Fetch error:', error);
        hideLoading();
        showResults({ success: false, message: 'Ошибка запуска скрипта: ' + error.message });
    });
}

// Функция запуска пакета скриптов
function runScriptsBatch(scripts, batchName) {
    showLoading('Запуск пакета: ' + batchName);
    
    let completed = 0;
    let results = [];
    
    function runNext(index) {
        if (index >= scripts.length) {
            hideLoading();
            showResults({ 
                success: true, 
                message: `Пакет "${batchName}" завершен. Выполнено ${completed} из ${scripts.length} скриптов.`,
                details: results 
            });
            return;
        }
        
        const script = scripts[index];
        fetch('/run_script', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                script_path: script.path,
                script_name: script.name
            })
        })
        .then(response => response.json())
        .then(data => {
            completed++;
            results.push({ script: script.name, result: data });
            runNext(index + 1);
        })
        .catch(error => {
            completed++;
            results.push({ script: script.name, error: error.message });
            runNext(index + 1);
        });
    }
    
    runNext(0);
}

// Функции для работы с UI
function showLoading(message) {
    const modal = document.getElementById('loadingModal');
    const messageEl = document.getElementById('loadingMessage');
    if (modal && messageEl) {
        messageEl.textContent = message;
        const bootstrapModal = new bootstrap.Modal(modal);
        bootstrapModal.show();
    }
}

function hideLoading() {
    const modal = document.getElementById('loadingModal');
    if (modal) {
        const bootstrapModal = bootstrap.Modal.getInstance(modal);
        if (bootstrapModal) {
            bootstrapModal.hide();
        }
    }
}

function showResults(data) {
    const modal = document.getElementById('resultsModal');
    const content = document.getElementById('resultsContent');
    if (modal && content) {
        let resultHtml = '';
        
        if (data.success) {
            resultHtml = `
                <div class="alert alert-success">
                    <h4><i class="fas fa-check-circle me-2"></i>Успешно!</h4>
                    <p><strong>${data.message}</strong></p>
            `;
            
            // Отображаем вывод скрипта если есть
            if (data.output) {
                resultHtml += `
                    <div class="mt-3">
                        <h6><i class="fas fa-terminal me-2"></i>Вывод скрипта:</h6>
                        <pre class="bg-light p-3 rounded border"><code>${data.output}</code></pre>
                    </div>
                `;
            }
            
            // Отображаем ошибки если есть
            if (data.errors) {
                resultHtml += `
                    <div class="mt-3">
                        <h6><i class="fas fa-exclamation-triangle me-2 text-warning"></i>Предупреждения:</h6>
                        <pre class="bg-warning bg-opacity-10 p-3 rounded border"><code>${data.errors}</code></pre>
                    </div>
                `;
            }
            
            // Отображаем детали если есть
            if (data.details) {
                resultHtml += `
                    <div class="mt-3">
                        <h6><i class="fas fa-info-circle me-2"></i>Детали:</h6>
                        <pre class="bg-light p-3 rounded border"><code>${JSON.stringify(data.details, null, 2)}</code></pre>
                    </div>
                `;
            }
            
            resultHtml += '</div>';
        } else {
            resultHtml = `
                <div class="alert alert-danger">
                    <h4><i class="fas fa-times-circle me-2"></i>Ошибка!</h4>
                    <p><strong>${data.message}</strong></p>
            `;
            
            // Отображаем ошибки если есть
            if (data.errors) {
                resultHtml += `
                    <div class="mt-3">
                        <h6><i class="fas fa-exclamation-triangle me-2"></i>Ошибки:</h6>
                        <pre class="bg-danger bg-opacity-10 p-3 rounded border"><code>${data.errors}</code></pre>
                    </div>
                `;
            }
            
            // Отображаем детали если есть
            if (data.details) {
                resultHtml += `
                    <div class="mt-3">
                        <h6><i class="fas fa-info-circle me-2"></i>Детали:</h6>
                        <pre class="bg-light p-3 rounded border"><code>${JSON.stringify(data.details, null, 2)}</code></pre>
                    </div>
                `;
            }
            
            resultHtml += '</div>';
        }
        
        content.innerHTML = resultHtml;
        const bootstrapModal = new bootstrap.Modal(modal);
        bootstrapModal.show();
    }
}

function showETLGuide() {
    const modal = document.getElementById('etlGuideModal');
    if (modal) {
        const bootstrapModal = new bootstrap.Modal(modal);
        bootstrapModal.show();
    }
}

// Функции для запуска различных ETL процессов
function runETLStage(stageId, stageName) {
    // Получаем скрипты для данного этапа
    fetch('/api/etl_scripts')
        .then(response => response.json())
        .then(data => {
            const stage = data[stageId];
            if (stage && stage.scripts) {
                const scripts = stage.scripts.map(script => ({
                    path: script.path,
                    name: script.name
                }));
                runScriptsBatch(scripts, stageName);
            } else {
                showResults({ success: false, message: 'Скрипты для этапа не найдены' });
            }
        })
        .catch(error => {
            showResults({ success: false, message: 'Ошибка получения скриптов: ' + error.message });
        });
}

function runSubcategory(subcategoryId, subcategoryName) {
    // Получаем скрипты для данной подкатегории
    fetch('/api/etl_scripts')
        .then(response => response.json())
        .then(data => {
            let scripts = [];
            // Ищем скрипты в подкатегории
            Object.values(data).forEach(stage => {
                if (stage.subcategories && stage.subcategories[subcategoryId]) {
                    scripts = stage.subcategories[subcategoryId].scripts.map(script => ({
                        path: script.path,
                        name: script.name
                    }));
                }
            });
            
            if (scripts.length > 0) {
                runScriptsBatch(scripts, subcategoryName);
            } else {
                showResults({ success: false, message: 'Скрипты для подкатегории не найдены' });
            }
        })
        .catch(error => {
            showResults({ success: false, message: 'Ошибка получения скриптов: ' + error.message });
        });
}

function runETLPipeline() {
    // Запускаем все скрипты ETL пайплайна
    fetch('/api/etl_scripts')
        .then(response => response.json())
        .then(data => {
            let allScripts = [];
            Object.values(data).forEach(stage => {
                if (stage.scripts) {
                    allScripts = allScripts.concat(stage.scripts.map(script => ({
                        path: script.path,
                        name: script.name
                    })));
                }
            });
            
            if (allScripts.length > 0) {
                runScriptsBatch(allScripts, 'Весь ETL пайплайн');
            } else {
                showResults({ success: false, message: 'Скрипты не найдены' });
            }
        })
        .catch(error => {
            showResults({ success: false, message: 'Ошибка получения скриптов: ' + error.message });
        });
}

function runExtractOnly() {
    // Запускаем только скрипты извлечения данных
    fetch('/api/etl_scripts')
        .then(response => response.json())
        .then(data => {
            let extractScripts = [];
            if (data.extract && data.extract.scripts) {
                extractScripts = data.extract.scripts.map(script => ({
                    path: script.path,
                    name: script.name
                }));
            }
            
            if (extractScripts.length > 0) {
                runScriptsBatch(extractScripts, 'Извлечение данных');
            } else {
                showResults({ success: false, message: 'Скрипты извлечения не найдены' });
            }
        })
        .catch(error => {
            showResults({ success: false, message: 'Ошибка получения скриптов: ' + error.message });
        });
}

function runTransformOnly() {
    // Запускаем только скрипты трансформации данных
    fetch('/api/etl_scripts')
        .then(response => response.json())
        .then(data => {
            let transformScripts = [];
            if (data.transform && data.transform.scripts) {
                transformScripts = data.transform.scripts.map(script => ({
                    path: script.path,
                    name: script.name
                }));
            }
            
            if (transformScripts.length > 0) {
                runScriptsBatch(transformScripts, 'Трансформация данных');
            } else {
                showResults({ success: false, message: 'Скрипты трансформации не найдены' });
            }
        })
        .catch(error => {
            showResults({ success: false, message: 'Ошибка получения скриптов: ' + error.message });
        });
}

function openFolder(folderPath) {
    // Открываем папку в проводнике
    fetch('/open_folder', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ folder_path: folderPath })
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            showResults({ success: true, message: 'Папка открыта: ' + folderPath });
        } else {
            showResults({ success: false, message: 'Ошибка открытия папки: ' + data.message });
        }
    })
    .catch(error => {
        showResults({ success: false, message: 'Ошибка открытия папки: ' + error.message });
    });
}

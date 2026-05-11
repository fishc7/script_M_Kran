/**
 * Модуль для управления ФИО в таблице слов_клейм_факт
 * Обеспечивает выпадающий список с ФИО и возможность добавления новых ФИО
 */

class FIOManager {
    constructor() {
        this.fioList = [];
        this.isInitialized = false;
        this.init();
    }

    /**
     * Инициализация модуля
     */
    async init() {
        if (this.isInitialized) return;
        
        console.log('🔧 Инициализация FIOManager...');
        
        try {
            // Загружаем список ФИО
            await this.loadFIOList();
            
            // Инициализируем обработчики событий
            this.initEventHandlers();
            
            this.isInitialized = true;
            console.log('✅ FIOManager инициализирован');
        } catch (error) {
            console.error('❌ Ошибка инициализации FIOManager:', error);
        }
    }

    /**
     * Загрузка списка ФИО с сервера
     */
    async loadFIOList() {
        try {
            const response = await fetch('/api/fio_svar');
            const data = await response.json();
            
            if (data.success) {
                this.fioList = data.fio_list || [];
                console.log(`📋 Загружено ${this.fioList.length} ФИО`);
            } else {
                console.error('Ошибка загрузки ФИО:', data.message);
                this.fioList = [];
            }
        } catch (error) {
            console.error('Ошибка загрузки ФИО:', error);
            this.fioList = [];
        }
    }

    /**
     * Инициализация обработчиков событий
     */
    initEventHandlers() {
        // Перехватываем функцию showSlovKleimoFactEditModal сразу
        this.enhanceSlovKleimoFactModals();
    }

    /**
     * Улучшение модальных окон редактирования слов_клейм_факт
     */
    enhanceSlovKleimoFactModals() {
        // Перехватываем создание модального окна
        const originalShowModal = window.showSlovKleimoFactEditModal;
        if (originalShowModal) {
            window.showSlovKleimoFactEditModal = (record) => {
                console.log('🔧 Перехвачен вызов showSlovKleimoFactEditModal');
                this.showEnhancedSlovKleimoFactEditModal(record);
            };
        } else {
            console.log('⚠️ Функция showSlovKleimoFactEditModal не найдена');
        }
    }

    /**
     * Показ улучшенного модального окна редактирования слов_клейм_факт
     */
    showEnhancedSlovKleimoFactEditModal(record) {
        console.log('🔧 Показ улучшенного модального окна редактирования слов_клейм_факт:', record);
        
        const modalHtml = `
            <style>
                .fio-dropdown {
                    position: absolute;
                    top: 100%;
                    left: 0;
                    right: 0;
                    z-index: 1000;
                    background: white;
                    border: 1px solid #ced4da;
                    border-radius: 0.375rem;
                    box-shadow: 0 0.5rem 1rem rgba(0, 0, 0, 0.15);
                }
                .fio-dropdown .dropdown-item {
                    padding: 0.5rem 1rem;
                    cursor: pointer;
                    border-bottom: 1px solid #f8f9fa;
                }
                .fio-dropdown .dropdown-item:hover {
                    background-color: #f8f9fa;
                }
                .fio-dropdown .dropdown-item:last-child {
                    border-bottom: none;
                }
                .fio-dropdown mark {
                    background-color: #fff3cd;
                    padding: 0;
                }
            </style>
            <div class="modal fade" id="slovKleimoFactEditModal" tabindex="-1" aria-labelledby="slovKleimoFactEditModalLabel" aria-hidden="true">
                <div class="modal-dialog">
                    <div class="modal-content">
                        <div class="modal-header">
                            <h5 class="modal-title" id="slovKleimoFactEditModalLabel">
                                <i class="fas fa-edit me-2"></i>Редактирование записи слов_клейм_факт
                            </h5>
                            <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                        </div>
                        <div class="modal-body">
                            <div class="mb-3">
                                <label class="form-label">Фактическое Клеймо:</label>
                                <input type="text" class="form-control" value="${record.Фактическое_Клеймо || ''}" readonly>
                            </div>
                            <div class="mb-3">
                                <label class="form-label">ФИО:</label>
                                <div class="position-relative">
                                    <input type="text" class="form-control" id="fio-search-input" placeholder="Начните вводить ФИО для поиска..." autocomplete="off">
                                    <div class="dropdown-menu w-100" id="fio-dropdown" style="max-height: 200px; overflow-y: auto; display: none;">
                                        ${this.generateFIOOptions(record.ФИО)}
                                    </div>
                                    <input type="hidden" id="selected-fio-id" value="">
                                </div>
                                <div class="form-text">Начните вводить ФИО для поиска или выберите из списка</div>
                            </div>
                            <div class="mb-3">
                                <label class="form-label">Примечание:</label>
                                <input type="text" class="form-control" id="edit-prim" value="${record.Примечание || ''}" placeholder="Введите примечание">
                            </div>
                        </div>
                        <div class="modal-footer">
                            <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">
                                <i class="fas fa-times me-2"></i>Отмена
                            </button>
                            <button type="button" class="btn btn-primary" onclick="saveSlovKleimoFactChanges(${record.id})">
                                <i class="fas fa-save me-2"></i>Сохранить
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        `;
        
        // Удаляем существующее модальное окно, если есть
        const existingModal = document.getElementById('slovKleimoFactEditModal');
        if (existingModal) {
            existingModal.remove();
        }
        
        // Добавляем новое модальное окно
        document.body.insertAdjacentHTML('beforeend', modalHtml);
        
        // Показываем модальное окно
        const modal = new bootstrap.Modal(document.getElementById('slovKleimoFactEditModal'));
        modal.show();
        
        // Set up autocomplete functionality
        // Используем ФИО_id если есть, иначе ФИО
        const fioId = record.ФИО_id || record.ФИО;
        this.setupFIOAutocomplete(fioId);
        
        // Focus on FIO search input
        setTimeout(() => {
            const fioSearchInput = document.getElementById('fio-search-input');
            if (fioSearchInput) {
                fioSearchInput.focus();
            }
        }, 500);
    }

    /**
     * Генерация опций для выпадающего списка ФИО
     */
    generateFIOOptions(currentFIO) {
        let options = '';
        
        // Добавляем все ФИО из списка
        this.fioList.forEach(fio => {
            // Проверяем, является ли currentFIO текстом ФИО или id_fio
            const isSelected = (fio.ФИО === currentFIO || fio.id_fio == currentFIO) ? 'selected' : '';
            options += `<div class="dropdown-item fio-option" data-fio-id="${fio.id_fio}" data-fio-text="${fio.ФИО}" ${isSelected}>${fio.ФИО}</div>`;
        });
        
        // Добавляем опцию для добавления нового ФИО
        options += '<div class="dropdown-divider"></div>';
        options += '<div class="dropdown-item fio-add-new" style="color: #007bff; font-weight: bold;">➕ Добавить новое ФИО...</div>';
        
        return options;
    }

    /**
     * Обработка изменения выбора ФИО
     */
    handleFIOSelectChange() {
        const select = document.getElementById('edit-fio-select');
        if (select) {
            const selectedValue = select.value;
            if (selectedValue === 'add_new') {
                this.showAddFIOModal();
            } else {
                const selectedOption = select.options[select.selectedIndex];
                console.log('Выбрано ФИО:', selectedOption.text, 'ID:', selectedOption.value);
            }
        }
    }

    /**
     * Показ модального окна для добавления нового ФИО
     */
    showAddFIOModal() {
        const modalHtml = `
            <div class="modal fade" id="addFIOModal" tabindex="-1" aria-labelledby="addFIOModalLabel" aria-hidden="true">
                <div class="modal-dialog">
                    <div class="modal-content">
                        <div class="modal-header">
                            <h5 class="modal-title" id="addFIOModalLabel">
                                <i class="fas fa-user-plus me-2"></i>Добавить новое ФИО
                            </h5>
                            <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                        </div>
                        <div class="modal-body">
                            <div class="mb-3">
                                <label for="new-fio" class="form-label">ФИО:</label>
                                <input type="text" class="form-control" id="new-fio" placeholder="Введите ФИО" required>
                                <div class="form-text">Введите полное имя сварщика</div>
                            </div>
                        </div>
                        <div class="modal-footer">
                            <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">
                                <i class="fas fa-times me-2"></i>Отмена
                            </button>
                            <button type="button" class="btn btn-primary" onclick="fioManager.addNewFIO()">
                                <i class="fas fa-save me-2"></i>Добавить
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        `;
        
        // Удаляем существующее модальное окно, если есть
        const existingModal = document.getElementById('addFIOModal');
        if (existingModal) {
            existingModal.remove();
        }
        
        // Добавляем новое модальное окно
        document.body.insertAdjacentHTML('beforeend', modalHtml);
        
        // Показываем модальное окно
        const modal = new bootstrap.Modal(document.getElementById('addFIOModal'));
        modal.show();
        
        // Устанавливаем фокус на поле ФИО
        setTimeout(() => {
            const fioInput = document.getElementById('new-fio');
            if (fioInput) {
                fioInput.focus();
            }
        }, 500);
    }

    /**
     * Добавление нового ФИО
     */
    async addNewFIO() {
        const fioInput = document.getElementById('new-fio');
        if (!fioInput) return;
        
        const newFIO = fioInput.value.trim();
        if (!newFIO) {
            alert('Введите ФИО');
            return;
        }
        
        try {
            const response = await fetch('/api/fio_svar', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    ФИО: newFIO
                })
            });
            
            const data = await response.json();
            
            if (data.success) {
                // Добавляем новое ФИО в локальный список
                this.fioList.push({
                    id_fio: data.id_fio,
                    ФИО: newFIO
                });
                
                // Обновляем поле поиска с новым ФИО
                const searchInput = document.getElementById('fio-search-input');
                const hiddenInput = document.getElementById('selected-fio-id');
                if (searchInput && hiddenInput) {
                    searchInput.value = newFIO;
                    hiddenInput.value = data.id_fio;
                }
                
                // Закрываем модальное окно
                const modal = bootstrap.Modal.getInstance(document.getElementById('addFIOModal'));
                modal.hide();
                
                alert('ФИО успешно добавлено!');
            } else {
                alert('Ошибка добавления ФИО: ' + data.message);
            }
        } catch (error) {
            console.error('Ошибка добавления ФИО:', error);
            alert('Ошибка добавления ФИО: ' + error.message);
        }
    }

    /**
     * Обновление выпадающего списка ФИО
     */
    updateFIOSelect(selectedFIO) {
        const select = document.getElementById('edit-fio-select');
        if (!select) return;
        
        // Очищаем список опций
        select.innerHTML = '<option value="">Выберите ФИО</option>';
        
        // Добавляем все ФИО
        this.fioList.forEach(fio => {
            const option = document.createElement('option');
            option.value = fio.id_fio;
            option.textContent = fio.ФИО;
            
            if (fio.ФИО === selectedFIO || fio.id_fio == selectedFIO) {
                option.selected = true;
            }
            
            select.appendChild(option);
        });
        
        // Добавляем опцию для добавления нового ФИО
        const addNewOption = document.createElement('option');
        addNewOption.value = 'add_new';
        addNewOption.textContent = '➕ Добавить новое ФИО...';
        select.appendChild(addNewOption);
    }

    /**
     * Настройка автопоиска ФИО
     */
    setupFIOAutocomplete(currentFIO) {
        const searchInput = document.getElementById('fio-search-input');
        const dropdown = document.getElementById('fio-dropdown');
        const hiddenInput = document.getElementById('selected-fio-id');
        
        if (!searchInput || !dropdown || !hiddenInput) return;
        
        // Устанавливаем текущее значение, если есть
        if (currentFIO) {
            const currentFioObj = this.fioList.find(fio => 
                fio.ФИО === currentFIO || fio.id_fio == currentFIO
            );
            if (currentFioObj) {
                searchInput.value = currentFioObj.ФИО;
                hiddenInput.value = currentFioObj.id_fio;
            }
        }
        
        // Обработчик ввода текста
        searchInput.addEventListener('input', (e) => {
            const query = e.target.value.toLowerCase().trim();
            
            if (query.length === 0) {
                dropdown.style.display = 'none';
                hiddenInput.value = '';
                return;
            }
            
            // Фильтруем ФИО по запросу
            const filteredFios = this.fioList.filter(fio => 
                fio.ФИО.toLowerCase().includes(query)
            );
            
            // Обновляем dropdown
            this.updateDropdownOptions(filteredFios, query);
            dropdown.style.display = 'block';
        });
        
        // Обработчик клика по опции
        dropdown.addEventListener('click', (e) => {
            if (e.target.classList.contains('fio-option')) {
                const fioId = e.target.dataset.fioId;
                const fioText = e.target.dataset.fioText;
                
                searchInput.value = fioText;
                hiddenInput.value = fioId;
                dropdown.style.display = 'none';
            } else if (e.target.classList.contains('fio-add-new')) {
                this.showAddFIOModal();
            }
        });
        
        // Обработчик потери фокуса
        searchInput.addEventListener('blur', () => {
            // Небольшая задержка, чтобы клик по dropdown успел сработать
            setTimeout(() => {
                dropdown.style.display = 'none';
            }, 200);
        });
        
        // Обработчик клавиш
        searchInput.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                dropdown.style.display = 'none';
            } else if (e.key === 'Enter') {
                e.preventDefault();
                const firstOption = dropdown.querySelector('.fio-option');
                if (firstOption) {
                    firstOption.click();
                }
            }
        });
    }
    
    /**
     * Обновление опций в dropdown
     */
    updateDropdownOptions(filteredFios, query) {
        const dropdown = document.getElementById('fio-dropdown');
        if (!dropdown) return;
        
        let html = '';
        
        // Добавляем отфильтрованные ФИО
        filteredFios.forEach(fio => {
            const highlightedText = this.highlightText(fio.ФИО, query);
            html += `<div class="dropdown-item fio-option" data-fio-id="${fio.id_fio}" data-fio-text="${fio.ФИО}">${highlightedText}</div>`;
        });
        
        // Если ничего не найдено, предлагаем добавить новое
        if (filteredFios.length === 0) {
            html += `<div class="dropdown-item fio-add-new" style="color: #007bff; font-weight: bold;">➕ Добавить "${query}" как новое ФИО</div>`;
        } else {
            html += '<div class="dropdown-divider"></div>';
            html += '<div class="dropdown-item fio-add-new" style="color: #007bff; font-weight: bold;">➕ Добавить новое ФИО...</div>';
        }
        
        dropdown.innerHTML = html;
    }
    
    /**
     * Подсветка текста в результатах поиска
     */
    highlightText(text, query) {
        if (!query) return text;
        
        const regex = new RegExp(`(${query})`, 'gi');
        return text.replace(regex, '<mark>$1</mark>');
    }

    /**
     * Получение выбранного ФИО для сохранения
     */
    getSelectedFIO() {
        const hiddenInput = document.getElementById('selected-fio-id');
        if (!hiddenInput) return '';
        
        return hiddenInput.value || '';
    }
}

// Создаем глобальный экземпляр менеджера ФИО
window.fioManager = new FIOManager();

// Дополнительный перехват функции после загрузки DOM
document.addEventListener('DOMContentLoaded', function() {
    console.log('🔧 DOM загружен, проверяем перехват функции...');
    
    // Проверяем, что функция перехвачена
    if (window.showSlovKleimoFactEditModal && window.fioManager) {
        const originalFunction = window.showSlovKleimoFactEditModal;
        window.showSlovKleimoFactEditModal = function(record) {
            console.log('🔧 Перехвачен вызов showSlovKleimoFactEditModal через DOMContentLoaded');
            window.fioManager.showEnhancedSlovKleimoFactEditModal(record);
        };
        console.log('✅ Функция showSlovKleimoFactEditModal успешно перехвачена');
    } else {
        console.log('⚠️ Функция showSlovKleimoFactEditModal не найдена для перехвата');
    }
});

// Переопределяем функцию сохранения изменений в слов_клейм_факт
window.saveSlovKleimoFactChanges = async function(recordId) {
    try {
        const fioValue = fioManager.getSelectedFIO();
        const primValue = document.getElementById('edit-prim').value;
        
        // Проверяем, что ФИО выбрано
        if (!fioValue) {
            alert('Пожалуйста, выберите ФИО');
            return;
        }
        
        const response = await fetch(`/api/slov_kleimo_fact/${recordId}`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                ФИО: fioValue,
                Примечание: primValue
            })
        });
        
        const data = await response.json();
        
        if (data.success) {
            alert('Запись успешно обновлена!');
            
            // Закрываем модальное окно
            const modal = bootstrap.Modal.getInstance(document.getElementById('slovKleimoFactEditModal'));
            modal.hide();
            
            // Перезагружаем страницу для обновления данных
            window.location.reload();
        } else {
            alert('Ошибка обновления: ' + data.message);
        }
        
    } catch (error) {
        console.error('Ошибка обновления записи:', error);
        alert('Ошибка обновления записи: ' + error.message);
    }
};

console.log('✅ Модуль FIOManager загружен');
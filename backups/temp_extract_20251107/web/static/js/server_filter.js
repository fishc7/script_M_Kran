/**
 * Серверная фильтрация таблиц
 * Обеспечивает поиск по всей базе данных, а не только по видимым данным
 */

class ServerTableFilter {
    constructor(tableName, options = {}) {
        this.tableName = tableName;
        this.options = {
            perPage: 10,
            searchDelay: 500,
            ...options
        };
        
        this.currentPage = 1;
        this.currentSearch = '';
        this.selectedColumns = '';
        this.isLoading = false;
        
        this.init();
    }
    
    init() {
        this.bindEvents();
        this.updateStats();
    }
    
    bindEvents() {
        // Обработчик поиска с задержкой
        const searchInput = document.getElementById('tableFilter');
        if (searchInput) {
            let searchTimeout;
            searchInput.addEventListener('input', (e) => {
                clearTimeout(searchTimeout);
                searchTimeout = setTimeout(() => {
                    this.performSearch(e.target.value);
                }, this.options.searchDelay);
            });
        }
        
        // Обработчик кнопки очистки
        const clearBtn = document.getElementById('clearFilterBtn');
        if (clearBtn) {
            clearBtn.addEventListener('click', () => {
                this.clearSearch();
            });
        }
        
        // Обработчик формы поиска
        const searchForm = document.getElementById('searchForm');
        if (searchForm) {
            searchForm.addEventListener('submit', (e) => {
                e.preventDefault();
                const searchValue = searchInput ? searchInput.value : '';
                this.performSearch(searchValue);
            });
        }
    }
    
    async performSearch(query, page = 1) {
        if (this.isLoading) return;
        
        this.isLoading = true;
        this.currentSearch = query;
        this.currentPage = page;
        
        this.showLoading();
        
        try {
            const params = new URLSearchParams({
                q: query,
                page: page,
                columns: this.selectedColumns
            });
            
            const response = await fetch(`/api/table/${this.tableName}/search?${params}`);
            const data = await response.json();
            
            if (data.success) {
                this.updateTable(data.records);
                this.updatePagination(data);
                this.updateStats(data.total_records);
            } else {
                this.showError(data.error || 'Ошибка поиска');
            }
        } catch (error) {
            console.error('Ошибка поиска:', error);
            this.showError('Ошибка соединения с сервером');
        } finally {
            this.hideLoading();
            this.isLoading = false;
        }
    }
    
    updateTable(records) {
        const tbody = document.querySelector('#dataTable tbody');
        if (!tbody) return;
        
        tbody.innerHTML = '';
        
        if (records.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="100%" class="text-center text-muted py-4">
                        <i class="fas fa-search fa-2x mb-2"></i>
                        <h5>Результаты не найдены</h5>
                        <p>Попробуйте изменить поисковый запрос</p>
                    </td>
                </tr>
            `;
            return;
        }
        
        records.forEach(record => {
            const row = document.createElement('tr');
            row.className = 'data-row';
            
            Object.values(record).forEach(value => {
                const cell = document.createElement('td');
                cell.className = 'searchable-cell';
                
                if (value === null || value === undefined) {
                    cell.innerHTML = '<span class="text-muted">NULL</span>';
                } else {
                    const strValue = String(value);
                    if (strValue.length > 50) {
                        cell.innerHTML = `<span title="${strValue}">${strValue.substring(0, 50)}...</span>`;
                    } else {
                        cell.textContent = strValue;
                    }
                }
                
                row.appendChild(cell);
            });
            
            tbody.appendChild(row);
        });
    }
    
    updatePagination(data) {
        const paginationContainer = document.querySelector('.pagination');
        if (!paginationContainer) return;
        
        const { current_page, total_pages } = data;
        
        let paginationHTML = '';
        
        // Кнопка "Предыдущая"
        if (current_page > 1) {
            paginationHTML += `
                <li class="page-item">
                    <a class="page-link" href="#" onclick="serverFilter.goToPage(${current_page - 1})">
                        <i class="fas fa-chevron-left"></i> Предыдущая
                    </a>
                </li>
            `;
        }
        
        // Номера страниц
        const startPage = Math.max(1, current_page - 2);
        const endPage = Math.min(total_pages, current_page + 2);
        
        for (let p = startPage; p <= endPage; p++) {
            paginationHTML += `
                <li class="page-item ${p === current_page ? 'active' : ''}">
                    <a class="page-link" href="#" onclick="serverFilter.goToPage(${p})">${p}</a>
                </li>
            `;
        }
        
        // Кнопка "Следующая"
        if (current_page < total_pages) {
            paginationHTML += `
                <li class="page-item">
                    <a class="page-link" href="#" onclick="serverFilter.goToPage(${current_page + 1})">
                        Следующая <i class="fas fa-chevron-right"></i>
                    </a>
                </li>
            `;
        }
        
        paginationContainer.innerHTML = paginationHTML;
    }
    
    updateStats(totalRecords = null) {
        const statsElement = document.getElementById('filterStats');
        if (!statsElement) return;
        
        if (totalRecords !== null) {
            if (this.currentSearch) {
                statsElement.innerHTML = `Найдено: <strong>${totalRecords.toLocaleString()}</strong> записей`;
            } else {
                statsElement.innerHTML = `Всего: <strong>${totalRecords.toLocaleString()}</strong> записей`;
            }
        }
    }
    
    goToPage(page) {
        this.performSearch(this.currentSearch, page);
    }
    
    clearSearch() {
        const searchInput = document.getElementById('tableFilter');
        if (searchInput) {
            searchInput.value = '';
        }
        this.performSearch('');
    }
    
    showLoading() {
        const searchBtn = document.getElementById('searchBtn');
        const searchBtnText = document.getElementById('searchBtnText');
        
        if (searchBtn) {
            searchBtn.disabled = true;
        }
        if (searchBtnText) {
            searchBtnText.textContent = 'Поиск...';
        }
    }
    
    hideLoading() {
        const searchBtn = document.getElementById('searchBtn');
        const searchBtnText = document.getElementById('searchBtnText');
        
        if (searchBtn) {
            searchBtn.disabled = false;
        }
        if (searchBtnText) {
            searchBtnText.textContent = 'Поиск';
        }
    }
    
    showError(message) {
        const tbody = document.querySelector('#dataTable tbody');
        if (tbody) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="100%" class="text-center text-danger py-4">
                        <i class="fas fa-exclamation-triangle fa-2x mb-2"></i>
                        <h5>Ошибка</h5>
                        <p>${message}</p>
                    </td>
                </tr>
            `;
        }
    }
}

// Глобальная переменная для доступа к фильтру
let serverFilter;

// Инициализация при загрузке страницы
document.addEventListener('DOMContentLoaded', function() {
    // Получаем название таблицы из URL
    const pathParts = window.location.pathname.split('/');
    const tableName = pathParts[pathParts.length - 1];
    
    if (tableName && tableName !== 'table') {
        serverFilter = new ServerTableFilter(tableName);
    }
});


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Импортер данных из Excel в SQLite с графическим интерфейсом
Поддерживает сохранение и загрузку шаблонов сопоставления столбцов
Исправленная версия с улучшенной обработкой ошибок и производительностью
"""

import sys
import sqlite3
import json
import os
import locale
import warnings
from typing import Optional, Dict, Any
from PyQt5.QtWidgets import (QApplication, QMainWindow, QVBoxLayout, QWidget, QPushButton, 
                             QFileDialog, QLabel, QComboBox, QTableWidget, QTableWidgetItem,
                             QHBoxLayout, QMessageBox, QSpinBox, QGroupBox, QInputDialog,
                             QProgressBar, QTextEdit, QSplitter, QCheckBox)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QFont
import pandas as pd

# Импортируем модуль очистки Unicode
try:
    from .unicode_cleaner import clean_dataframe_unicode, get_unicode_cleaning_stats
except ImportError:
    # Если не можем импортировать как модуль, импортируем напрямую
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from unicode_cleaner import clean_dataframe_unicode, get_unicode_cleaning_stats

# Настройка кодировки для Windows
if sys.platform.startswith('win'):
    try:
        locale.setlocale(locale.LC_ALL, 'ru_RU.UTF-8')
    except:
        try:
            locale.setlocale(locale.LC_ALL, 'Russian_Russia.1251')
        except:
            pass
    os.environ['PYTHONIOENCODING'] = 'utf-8'

# Подавляем предупреждения pandas
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
warnings.filterwarnings('ignore', category=FutureWarning)

class ImportWorker(QThread):
    """Рабочий поток для импорта данных"""
    progress = pyqtSignal(int)
    status = pyqtSignal(str)
    finished = pyqtSignal(bool, str)
    
    def __init__(self, excel_data: pd.DataFrame, mapping: Dict[str, str], 
                 table_name: str, db_file: str, clean_unicode: bool = True):
        super().__init__()
        self.excel_data = excel_data
        self.mapping = mapping
        self.table_name = table_name
        self.db_file = db_file
        self.clean_unicode = clean_unicode
    
    def run(self):
        try:
            self.status.emit("Подготовка данных...")
            self.progress.emit(10)
            
            # Проверяем доступность столбцов и фильтруем mapping
            available_columns = list(self.excel_data.columns)
            valid_mapping = {}
            
            for excel_col, sqlite_col in self.mapping.items():
                if excel_col in available_columns:
                    valid_mapping[excel_col] = sqlite_col
                else:
                    self.status.emit(f"Предупреждение: столбец '{excel_col}' не найден в данных")
            
            if not valid_mapping:
                raise ValueError("Нет доступных столбцов для импорта")
            
            # Подготавливаем данные для импорта
            data_to_import = self.excel_data[list(valid_mapping.keys())].rename(columns=valid_mapping)
            
            # Автоматическая очистка Unicode символов
            if self.clean_unicode:
                self.status.emit("Очистка Unicode символов...")
                self.progress.emit(20)
                
                # Получаем статистику до очистки
                stats_before = get_unicode_cleaning_stats(data_to_import)
                
                # Очищаем данные
                data_to_import = clean_dataframe_unicode(data_to_import)
                
                # Получаем статистику после очистки
                stats_after = get_unicode_cleaning_stats(data_to_import)
                
                if stats_before['problematic_chars_found'] > 0:
                    self.status.emit(f"Очищено {stats_before['problematic_chars_found']} проблемных символов")
            
            # Очищаем данные от NaN значений
            data_to_import = data_to_import.fillna('')
            
            self.status.emit("Импорт в базу данных...")
            self.progress.emit(60 if self.clean_unicode else 50)
            
            # Создаем новое соединение в этом потоке
            with sqlite3.connect(self.db_file) as conn:
                # Импортируем в SQLite
                data_to_import.to_sql(
                    self.table_name,
                    conn,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
            
            self.progress.emit(100)
            self.status.emit("Импорт завершен успешно!")
            self.finished.emit(True, f"Импортировано {len(data_to_import)} записей")
            
        except Exception as e:
            self.finished.emit(False, f"Ошибка импорта: {str(e)}")

class ExcelToSQLiteImporter(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Импорт данных из Excel в SQLite")
        self.setGeometry(100, 100, 1200, 900)
        
        # Инициализация переменных
        self.excel_file: Optional[str] = None
        self.excel_data: Optional[pd.DataFrame] = None
        self.excel_raw_data: Optional[pd.DataFrame] = None
        self.excel_sheets: Optional[list] = None
        self.current_sheet: Optional[str] = None
        self.db_file: Optional[str] = None
        self.conn: Optional[sqlite3.Connection] = None
        self.current_template: Optional[str] = None
        self.templates_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mapping_templates")
        self.import_worker: Optional[ImportWorker] = None
        
        self.setup_ui()
        self.setup_templates()
    
    def setup_ui(self):
        """Настройка пользовательского интерфейса"""
        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        
        # Основной layout
        main_layout = QVBoxLayout()
        self.central_widget.setLayout(main_layout)
        
        # Создаем сплиттер для разделения интерфейса
        splitter = QSplitter(Qt.Orientation.Horizontal)
        main_layout.addWidget(splitter)
        
        # Левая панель - настройки
        left_panel = QWidget()
        left_layout = QVBoxLayout()
        left_panel.setLayout(left_layout)
        splitter.addWidget(left_panel)
        
        # Правая панель - превью и логи
        right_panel = QWidget()
        right_layout = QVBoxLayout()
        right_panel.setLayout(right_layout)
        splitter.addWidget(right_panel)
        
        # Настройка левой панели
        self.setup_left_panel(left_layout)
        
        # Настройка правой панели
        self.setup_right_panel(right_layout)
        
        # Устанавливаем пропорции сплиттера
        splitter.setSizes([400, 800])
    
    def setup_left_panel(self, layout):
        """Настройка левой панели с элементами управления"""
        # Группа для шаблонов
        self.templates_group = QGroupBox("Управление шаблонами")
        self.templates_layout = QHBoxLayout()
        self.templates_group.setLayout(self.templates_layout)
        layout.addWidget(self.templates_group)
        
        # Выпадающий список шаблонов
        self.template_combo = QComboBox()
        self.template_combo.setMinimumWidth(200)
        self.templates_layout.addWidget(QLabel("Шаблон:"))
        self.templates_layout.addWidget(self.template_combo)
        
        # Кнопки управления шаблонами
        self.save_template_btn = QPushButton("Сохранить текущий")
        self.save_template_btn.clicked.connect(self.save_mapping_template)
        self.templates_layout.addWidget(self.save_template_btn)
        
        self.load_template_btn = QPushButton("Применить выбранный")
        self.load_template_btn.clicked.connect(self.load_selected_template)
        self.templates_layout.addWidget(self.load_template_btn)
        
        self.delete_template_btn = QPushButton("Удалить шаблон")
        self.delete_template_btn.clicked.connect(self.delete_template)
        self.templates_layout.addWidget(self.delete_template_btn)
        
        # Выбор файла Excel
        self.file_group = QGroupBox("Файл Excel")
        self.file_layout = QVBoxLayout()
        self.file_group.setLayout(self.file_layout)
        layout.addWidget(self.file_group)
        
        self.file_label = QLabel("Файл не выбран")
        self.file_label.setWordWrap(True)
        self.file_layout.addWidget(self.file_label)
        
        self.select_file_btn = QPushButton("Выбрать файл Excel")
        self.select_file_btn.clicked.connect(self.select_excel_file)
        self.file_layout.addWidget(self.select_file_btn)
        
        # Выбор листа Excel
        self.sheet_label = QLabel("Лист Excel:")
        self.file_layout.addWidget(self.sheet_label)
        
        self.sheet_combo = QComboBox()
        self.sheet_combo.currentTextChanged.connect(self.on_sheet_changed)
        self.file_layout.addWidget(self.sheet_combo)
        
        # Настройки чтения Excel
        self.settings_group = QGroupBox("Настройки импорта")
        self.settings_layout = QHBoxLayout()
        self.settings_group.setLayout(self.settings_layout)
        layout.addWidget(self.settings_group)
        
        self.header_row_label = QLabel("Строка с заголовками:")
        self.settings_layout.addWidget(self.header_row_label)
        
        self.header_row_spin = QSpinBox()
        self.header_row_spin.setMinimum(0)
        self.header_row_spin.setValue(0)
        self.settings_layout.addWidget(self.header_row_spin)
        
        self.data_start_label = QLabel("Данные начинаются со строки:")
        self.settings_layout.addWidget(self.data_start_label)
        
        self.data_start_spin = QSpinBox()
        self.data_start_spin.setMinimum(1)
        self.data_start_spin.setValue(1)
        self.settings_layout.addWidget(self.data_start_spin)
        
        self.update_preview_btn = QPushButton("Обновить превью")
        self.update_preview_btn.clicked.connect(self.update_excel_preview)
        self.settings_layout.addWidget(self.update_preview_btn)
        
        # Чекбокс для автоматической очистки Unicode
        self.clean_unicode_checkbox = QCheckBox("Автоматически очищать Unicode символы")
        self.clean_unicode_checkbox.setChecked(True)  # По умолчанию включено
        self.clean_unicode_checkbox.setToolTip("Удаляет проблемные символы (японские, эмодзи, градусы и т.д.)")
        self.settings_layout.addWidget(self.clean_unicode_checkbox)
        
        # База данных SQLite
        self.db_group = QGroupBox("База данных SQLite")
        self.db_layout = QVBoxLayout()
        self.db_group.setLayout(self.db_layout)
        layout.addWidget(self.db_group)
        
        self.db_label = QLabel("База данных не выбрана")
        self.db_label.setWordWrap(True)
        self.db_layout.addWidget(self.db_label)
        
        self.select_db_btn = QPushButton("Выбрать базу данных")
        self.select_db_btn.clicked.connect(self.select_sqlite_db)
        self.db_layout.addWidget(self.select_db_btn)
        
        self.table_label = QLabel("Целевая таблица:")
        self.db_layout.addWidget(self.table_label)
        
        self.table_combo = QComboBox()
        self.table_combo.currentTextChanged.connect(self.update_mapping_table)
        self.db_layout.addWidget(self.table_combo)
        
        # Сопоставление столбцов
        self.mapping_group = QGroupBox("Сопоставление столбцов (SQLite → Excel)")
        self.mapping_layout = QVBoxLayout()
        self.mapping_group.setLayout(self.mapping_layout)
        layout.addWidget(self.mapping_group)
        
        self.mapping_table = QTableWidget()
        self.mapping_table.setColumnCount(2)
        self.mapping_table.setHorizontalHeaderLabels(["Столбец SQLite", "Столбец Excel (буква)"])
        self.mapping_layout.addWidget(self.mapping_table)
        
        # Прогресс бар
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)
        
        # Кнопка импорта
        self.import_btn = QPushButton("Импортировать данные")
        self.import_btn.clicked.connect(self.import_data)
        self.import_btn.setEnabled(False)
        layout.addWidget(self.import_btn)
    
    def setup_right_panel(self, layout):
        """Настройка правой панели с превью и логами"""
        # Превью данных Excel
        self.preview_group = QGroupBox("Превью данных Excel")
        self.preview_layout = QVBoxLayout()
        self.preview_group.setLayout(self.preview_layout)
        layout.addWidget(self.preview_group)
        
        self.excel_table = QTableWidget()
        self.preview_layout.addWidget(self.excel_table)
        
        # Лог операций
        self.log_group = QGroupBox("Лог операций")
        self.log_layout = QVBoxLayout()
        self.log_group.setLayout(self.log_layout)
        layout.addWidget(self.log_group)
        
        self.log_text = QTextEdit()
        self.log_text.setMaximumHeight(150)
        self.log_text.setFont(QFont("Consolas", 9))
        self.log_layout.addWidget(self.log_text)
    
    def log_message(self, message: str):
        """Добавляет сообщение в лог"""
        from datetime import datetime
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.append(f"[{timestamp}] {message}")
    
    def setup_templates(self):
        """Инициализация системы шаблонов"""
        # Создаем директорию для шаблонов, если ее нет
        os.makedirs(self.templates_dir, exist_ok=True)
        self.update_templates_list()
    
    def update_templates_list(self):
        """Обновляет список доступных шаблонов в выпадающем меню"""
        try:
            templates = [f[:-5] for f in os.listdir(self.templates_dir) if f.endswith('.json')]
            self.template_combo.clear()
            self.template_combo.addItems(templates)
            self.log_message(f"Загружено шаблонов: {len(templates)}")
        except Exception as e:
            self.log_message(f"Ошибка загрузки шаблонов: {str(e)}")
    
    def save_mapping_template(self):
        """Сохранение текущего шаблона сопоставления"""
        if not self.table_combo.currentText():
            QMessageBox.warning(self, "Ошибка", "Не выбрана целевая таблица")
            return
        
        if self.mapping_table.rowCount() == 0:
            QMessageBox.warning(self, "Ошибка", "Нет данных для сохранения шаблона")
            return
        
        # Получаем имя шаблона от пользователя
        template_name, ok = QInputDialog.getText(
            self, 
            "Сохранение шаблона", 
            "Введите имя шаблона:",
            text=f"{os.path.basename(self.excel_file or 'unnamed')}_{self.table_combo.currentText()}"
        )
        
        if not ok or not template_name:
            return
        
        try:
            # Собираем данные шаблона (простой формат)
            template_data = {}
            
            # Сохраняем сопоставления (используем буквенные обозначения столбцов)
            for row in range(self.mapping_table.rowCount()):
                item = self.mapping_table.item(row, 0)
                if item is None:
                    continue
                    
                sqlite_col = item.text().split(' ')[0]
                combo = self.mapping_table.cellWidget(row, 1)
                if combo is None:
                    continue
                    
                excel_col_combo = combo.currentText()
                
                if excel_col_combo != "-- Не импортировать --":
                    # Извлекаем буквенное обозначение столбца (A, B, C...)
                    excel_col_letter = excel_col_combo.split(' ')[0]
                    template_data[sqlite_col] = excel_col_letter
            
            # Сохраняем в файл
            template_path = os.path.join(self.templates_dir, f"{template_name}.json")
            with open(template_path, 'w', encoding='utf-8') as f:
                json.dump(template_data, f, ensure_ascii=False, indent=2)
            
            self.current_template = template_name
            self.update_templates_list()
            self.template_combo.setCurrentText(template_name)
            self.log_message(f"Шаблон '{template_name}' сохранен")
            QMessageBox.information(self, "Успех", f"Шаблон '{template_name}' успешно сохранен")
        except Exception as e:
            self.log_message(f"Ошибка сохранения шаблона: {str(e)}")
            QMessageBox.critical(self, "Ошибка", f"Не удалось сохранить шаблон: {str(e)}")
    
    def load_selected_template(self):
        """Загрузка выбранного шаблона"""
        template_name = self.template_combo.currentText()
        if not template_name:
            return
        
        self.load_template(template_name)
    
    def load_template(self, template_name: str):
        """Загрузка шаблона по имени"""
        template_path = os.path.join(self.templates_dir, f"{template_name}.json")
        try:
            with open(template_path, 'r', encoding='utf-8') as f:
                template_data = json.load(f)
            
            # Применяем сопоставления после обновления таблицы
            self.current_template = template_name
            self.log_message(f"Шаблон '{template_name}' загружен, применяем сопоставления...")
            
            # Добавляем большую задержку, чтобы таблица сопоставления точно успела обновиться
            from PyQt5.QtCore import QTimer
            QTimer.singleShot(500, self.apply_template_mappings)
            
            QMessageBox.information(self, "Успех", f"Шаблон '{template_name}' успешно загружен")
        except Exception as e:
            self.log_message(f"Ошибка загрузки шаблона: {str(e)}")
            QMessageBox.critical(self, "Ошибка", f"Не удалось загрузить шаблон: {str(e)}")
    
    def delete_template(self):
        """Удаление выбранного шаблона"""
        template_name = self.template_combo.currentText()
        if not template_name:
            return
        
        reply = QMessageBox.question(
            self, 
            "Удаление шаблона", 
            f"Вы уверены, что хотите удалить шаблон '{template_name}'?",
            QMessageBox.Yes | QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            template_path = os.path.join(self.templates_dir, f"{template_name}.json")
            try:
                os.remove(template_path)
                self.update_templates_list()
                self.log_message(f"Шаблон '{template_name}' удален")
                QMessageBox.information(self, "Успех", f"Шаблон '{template_name}' удален")
            except Exception as e:
                self.log_message(f"Ошибка удаления шаблона: {str(e)}")
                QMessageBox.critical(self, "Ошибка", f"Не удалось удалить шаблон: {str(e)}")
    
    def select_excel_file(self):
        """Выбор файла Excel"""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Выберите файл Excel", "", "Excel Files (*.xlsx *.xls)"
        )
        
        if file_path:
            self.excel_file = file_path
            self.file_label.setText(f"Выбран файл: {os.path.basename(file_path)}")
            self.log_message(f"Выбран файл Excel: {os.path.basename(file_path)}")
            self.load_excel_sheets()
    
    def load_excel_sheets(self):
        """Загрузка списка листов из Excel файла"""
        if self.excel_file:
            try:
                self.log_message("Загрузка списка листов...")
                
                # Получаем список всех листов
                excel_file = pd.ExcelFile(self.excel_file)
                self.excel_sheets = excel_file.sheet_names
                
                # Заполняем выпадающий список листов
                self.sheet_combo.clear()
                self.sheet_combo.addItems(self.excel_sheets)
                
                # Выбираем первый лист по умолчанию
                if self.excel_sheets:
                    self.current_sheet = self.excel_sheets[0]
                    self.sheet_combo.setCurrentText(self.current_sheet)
                    self.log_message(f"Найдено листов: {len(self.excel_sheets)}")
                    self.load_excel_data()
                else:
                    self.log_message("В файле не найдено листов")
                    
            except Exception as e:
                self.log_message(f"Ошибка загрузки листов: {str(e)}")
                QMessageBox.critical(self, "Ошибка", f"Не удалось прочитать листы файла: {str(e)}")
    
    def on_sheet_changed(self, sheet_name: str):
        """Обработка изменения выбранного листа"""
        if sheet_name and sheet_name != self.current_sheet:
            self.current_sheet = sheet_name
            self.log_message(f"Выбран лист: {sheet_name}")
            self.load_excel_data()
    
    def load_excel_data(self):
        """Загрузка данных из Excel файла"""
        if self.excel_file and self.current_sheet:
            try:
                self.log_message(f"Загрузка данных из листа '{self.current_sheet}'...")
                
                # Читаем весь лист как есть
                self.excel_raw_data = pd.read_excel(
                    self.excel_file, 
                    sheet_name=self.current_sheet,
                    header=None, 
                    engine='openpyxl'
                )
                
                # Устанавливаем максимальные значения для спинбоксов
                max_row = len(self.excel_raw_data)
                self.header_row_spin.setMaximum(max_row - 1)
                self.data_start_spin.setMaximum(max_row)
                
                # Автоматически определяем строку с заголовками
                self.detect_header_row()
                
                self.update_excel_preview()
                self.log_message(f"Данные загружены: {max_row} строк")
            except Exception as e:
                self.log_message(f"Ошибка загрузки Excel: {str(e)}")
                QMessageBox.critical(self, "Ошибка", f"Не удалось прочитать файл Excel: {str(e)}")
    
    def detect_header_row(self):
        """Автоматическое определение строки с заголовками"""
        try:
            if self.excel_raw_data is None:
                return
                
            # Простая эвристика для определения строки с заголовками
            for i in range(min(5, len(self.excel_raw_data))):
                row = self.excel_raw_data.iloc[i]
                # Проверяем, что большинство ячеек в строке содержат текст
                text_cells = sum(1 for cell in row if isinstance(cell, str) and len(str(cell).strip()) > 0)
                if text_cells >= len(row) * 0.7:  # 70% ячеек содержат текст
                    self.header_row_spin.setValue(i)
                    self.data_start_spin.setValue(i + 1)
                    self.log_message(f"Автоопределена строка заголовков: {i}")
                    break
        except Exception as e:
            self.log_message(f"Ошибка автоопределения заголовков: {str(e)}")
    
    def update_excel_preview(self):
        """Обновление превью данных Excel"""
        if self.excel_raw_data is not None:
            header_row = self.header_row_spin.value()
            data_start = self.data_start_spin.value()
            
            try:
                self.log_message("Обновление превью Excel...")
                
                # Читаем данные с учетом выбранных строк
                skiprows = None
                if data_start > header_row + 1:
                    skiprows = range(header_row + 1, data_start - 1)
                
                self.excel_data = pd.read_excel(
                    self.excel_file,
                    sheet_name=self.current_sheet,
                    header=header_row,
                    skiprows=skiprows,
                    engine='openpyxl'
                )
                
                # Удаляем пустые строки и столбцы
                if self.excel_data is not None:
                    self.excel_data.dropna(how='all', inplace=True)
                    self.excel_data.dropna(how='all', axis=1, inplace=True)
                    
                    # Очищаем имена столбцов от "Unnamed"
                    new_columns = []
                    for i, col in enumerate(self.excel_data.columns):
                        if str(col).startswith('Unnamed'):
                            new_columns.append(f'Column_{i+1}')
                        else:
                            new_columns.append(str(col))
                    self.excel_data.columns = new_columns
                
                # Отображаем превью
                self.show_excel_preview()
                self.update_mapping_table()
                
                # Применяем сопоставления из текущего шаблона, если он загружен
                if self.current_template and self.mapping_table.rowCount() > 0:
                    self.apply_template_mappings()
                
                if self.excel_data is not None:
                    self.log_message(f"Превью обновлено: {len(self.excel_data)} строк, {len(self.excel_data.columns)} столбцов")
            except Exception as e:
                self.log_message(f"Ошибка обновления превью: {str(e)}")
                QMessageBox.critical(self, "Ошибка", f"Не удалось обновить превью: {str(e)}")
    
    def show_excel_preview(self):
        """Отображение превью данных Excel"""
        if self.excel_data is not None:
            # Показываем максимум 50 строк для производительности
            preview_rows = min(50, len(self.excel_data))
            self.excel_table.setRowCount(preview_rows)
            self.excel_table.setColumnCount(len(self.excel_data.columns))
            
            # Заголовки с буквенными обозначениями
            headers = []
            for i, col in enumerate(self.excel_data.columns):
                col_letter = self.get_column_letter(i)
                headers.append(f"{col_letter} ({col})")
            self.excel_table.setHorizontalHeaderLabels(headers)
            
            # Заполняем данные
            for i in range(preview_rows):
                for j in range(len(self.excel_data.columns)):
                    value = str(self.excel_data.iloc[i, j])
                    if value == 'nan':
                        value = ''
                    item = QTableWidgetItem(value)
                    self.excel_table.setItem(i, j, item)
    
    def get_column_letter(self, col_idx: int) -> str:
        """Конвертирует индекс столбца в буквенное обозначение (A, B, ..., Z, AA, AB, ...)"""
        letters = []
        while col_idx >= 0:
            letters.append(chr(ord('A') + (col_idx % 26)))
            col_idx = (col_idx // 26) - 1
        return ''.join(reversed(letters))
    
    def select_sqlite_db(self):
        """Выбор файла базы данных SQLite"""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Выберите файл базы данных SQLite", "", "SQLite Databases (*.db *.sqlite)"
        )
        
        if file_path:
            self.db_file = file_path
            self.db_label.setText(f"Выбрана база данных: {os.path.basename(file_path)}")
            self.log_message(f"Выбрана база данных: {os.path.basename(file_path)}")
            
            try:
                self.conn = sqlite3.connect(file_path)
                self.update_table_list()
                self.log_message("Подключение к базе данных установлено")
            except Exception as e:
                self.log_message(f"Ошибка подключения к БД: {str(e)}")
                QMessageBox.critical(self, "Ошибка", f"Не удалось подключиться к базе данных: {str(e)}")
    
    def update_table_list(self):
        """Обновление списка таблиц в базе данных"""
        if self.conn:
            try:
                cursor = self.conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = cursor.fetchall()
                
                self.table_combo.clear()
                for table in tables:
                    self.table_combo.addItem(table[0])
                
                self.log_message(f"Найдено таблиц в БД: {len(tables)}")
            except Exception as e:
                self.log_message(f"Ошибка получения списка таблиц: {str(e)}")
    
    def update_mapping_table(self):
        """Обновление таблицы сопоставления столбцов"""
        if self.excel_data is not None and self.conn is not None and self.table_combo.currentText():
            table_name = self.table_combo.currentText()
            cursor = self.conn.cursor()
            
            try:
                # Получаем информацию о столбцах в целевой таблице
                cursor.execute(f"PRAGMA table_info({table_name})")
                sqlite_columns_info = cursor.fetchall()
                sqlite_columns = [column[1] for column in sqlite_columns_info]
                
                # Получаем буквенные обозначения столбцов Excel
                excel_columns = self.excel_data.columns
                excel_letters = [self.get_column_letter(i) for i in range(len(excel_columns))]
                
                # Настраиваем таблицу сопоставления
                self.mapping_table.setRowCount(len(sqlite_columns))
                
                for i, (sqlite_col, col_type) in enumerate(zip(
                    sqlite_columns,
                    [column[2] for column in sqlite_columns_info]
                )):
                    # Столбец SQLite (с типом)
                    item_sqlite = QTableWidgetItem(f"{sqlite_col} ({col_type})")
                    item_sqlite.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
                    self.mapping_table.setItem(i, 0, item_sqlite)
                    
                    # Выпадающий список для столбцов Excel
                    combo = QComboBox()
                    combo.addItem("-- Не импортировать --")
                    combo.addItems([f"{letter} ({name})" for letter, name in zip(excel_letters, excel_columns)])
                    self.mapping_table.setCellWidget(i, 1, combo)
                
                # Проверяем, можно ли включить кнопку импорта
                self.check_import_ready()
                self.log_message(f"Сопоставление обновлено: {len(sqlite_columns)} столбцов")
            except Exception as e:
                self.log_message(f"Ошибка обновления сопоставления: {str(e)}")
                QMessageBox.critical(self, "Ошибка", f"Не удалось получить информацию о таблице: {str(e)}")
    
    def check_import_ready(self):
        """Проверяет готовность к импорту"""
        ready = (self.excel_file is not None and 
                self.db_file is not None and 
                bool(self.table_combo.currentText()) and
                self.mapping_table.rowCount() > 0)
        
        self.import_btn.setEnabled(ready)
    
    def import_data(self):
        """Импорт данных в базу данных"""
        if not self.excel_file or not self.db_file or not self.table_combo.currentText():
            QMessageBox.warning(self, "Ошибка", "Пожалуйста, выберите файл Excel, базу данных и целевую таблицу")
            return
        
        table_name = self.table_combo.currentText()
        mapping = {}
        
        # Собираем сопоставление столбцов
        for row in range(self.mapping_table.rowCount()):
            item = self.mapping_table.item(row, 0)
            if item is None:
                continue
                
            sqlite_col = item.text().split(' ')[0]  # Извлекаем имя столбца без типа
            combo = self.mapping_table.cellWidget(row, 1)
            if combo is None:
                continue
                
            excel_col_combo = combo.currentText()
            
            if excel_col_combo != "-- Не импортировать --":
                # Извлекаем имя столбца Excel из комбо-бокса (формат "A (ColumnName)")
                excel_col = excel_col_combo.split(' ')[1][1:-1]  # Извлекаем часть в скобках
                # Проверяем, что столбец существует в данных
                if self.excel_data is not None and excel_col in self.excel_data.columns:
                    mapping[excel_col] = sqlite_col
                else:
                    self.log_message(f"Предупреждение: столбец '{excel_col}' не найден в данных Excel")
        
        if not mapping:
            QMessageBox.warning(self, "Ошибка", "Не выбрано ни одного столбца для импорта")
            return
        
        try:
            # Проверяем, что данные готовы
            if self.excel_data is None:
                QMessageBox.warning(self, "Ошибка", "Данные Excel не загружены")
                return
                
            if self.conn is None:
                QMessageBox.warning(self, "Ошибка", "Соединение с базой данных не установлено")
                return
                
            # Создаем рабочий поток для импорта
            clean_unicode = self.clean_unicode_checkbox.isChecked()
            self.import_worker = ImportWorker(self.excel_data, mapping, table_name, self.db_file, clean_unicode)
            self.import_worker.progress.connect(self.progress_bar.setValue)
            self.import_worker.status.connect(self.log_message)
            self.import_worker.finished.connect(self.import_finished)
            
            # Показываем прогресс бар и блокируем интерфейс
            self.progress_bar.setVisible(True)
            self.import_btn.setEnabled(False)
            self.progress_bar.setValue(0)
            
            # Запускаем импорт
            self.import_worker.start()
            
        except Exception as e:
            self.log_message(f"Ошибка запуска импорта: {str(e)}")
            QMessageBox.critical(self, "Ошибка", f"Не удалось запустить импорт: {str(e)}")
    
    def import_finished(self, success: bool, message: str):
        """Обработка завершения импорта"""
        self.progress_bar.setVisible(False)
        self.import_btn.setEnabled(True)
        
        if success:
            self.log_message(message)
            QMessageBox.information(self, "Успех", message)
        else:
            self.log_message(message)
            QMessageBox.critical(self, "Ошибка", message)
     
    def apply_template_mappings(self):
        """Применяет сопоставления из текущего шаблона"""
        self.log_message(f"=== НАЧАЛО ПРИМЕНЕНИЯ ШАБЛОНА ===")
        self.log_message(f"Текущий шаблон: {self.current_template}")
        self.log_message(f"Количество строк в таблице: {self.mapping_table.rowCount()}")
        
        if not self.current_template:
            self.log_message("✗ Нет текущего шаблона")
            return
            
        if self.mapping_table.rowCount() == 0:
            # Если таблица сопоставления еще не готова, пробуем еще раз через небольшую задержку
            self.log_message("Таблица сопоставления пуста, пробуем через 200мс...")
            from PyQt5.QtCore import QTimer
            QTimer.singleShot(200, self.apply_template_mappings)
            return
        
        try:
            template_path = os.path.join(self.templates_dir, f"{self.current_template}.json")
            self.log_message(f"Путь к шаблону: {template_path}")
            
            with open(template_path, 'r', encoding='utf-8') as f:
                template_data = json.load(f)
            
            applied_count = 0
            
            # Подробное логирование для отладки
            self.log_message(f"Применение шаблона '{self.current_template}'")
            self.log_message(f"Найдено сопоставлений в шаблоне: {len(template_data)}")
            self.log_message(f"Сопоставления: {template_data}")
            
            # Применяем сопоставления к таблице
            for row in range(self.mapping_table.rowCount()):
                item = self.mapping_table.item(row, 0)
                if item is None:
                    self.log_message(f"Строка {row}: нет элемента SQLite")
                    continue
                    
                sqlite_col = item.text().split(' ')[0]
                self.log_message(f"Строка {row}: SQLite столбец = '{sqlite_col}'")
                
                combo = self.mapping_table.cellWidget(row, 1)
                if combo is None:
                    self.log_message(f"Строка {row}: нет комбо-бокса")
                    continue
                
                # Ищем соответствующее сопоставление в шаблоне
                if sqlite_col in template_data:
                    excel_col_letter = template_data[sqlite_col]  # Буквенное обозначение (A, B, C...)
                    self.log_message(f"Ищем столбец '{excel_col_letter}' для SQLite столбца '{sqlite_col}'")
                    
                    # Ищем этот столбец в комбо-боксе по буквенному обозначению
                    found = False
                    for i in range(combo.count()):
                        combo_text = combo.itemText(i)
                        self.log_message(f"  Вариант {i}: '{combo_text}'")
                        if combo_text.startswith(excel_col_letter + " "):  # Ищем по началу строки
                            combo.setCurrentIndex(i)
                            applied_count += 1
                            found = True
                            self.log_message(f"✓ Найден и применен: {combo_text}")
                            break
                    
                    if not found:
                        self.log_message(f"✗ Столбец '{excel_col_letter}' не найден в комбо-боксе")
                        # Показываем все доступные варианты
                        available_options = [combo.itemText(j) for j in range(combo.count())]
                        self.log_message(f"Доступные варианты: {available_options}")
                else:
                    # Если сопоставления нет, устанавливаем "Не импортировать"
                    combo.setCurrentIndex(0)
                    self.log_message(f"Столбец '{sqlite_col}' не найден в шаблоне")
            
            self.log_message(f"Сопоставления из шаблона '{self.current_template}' применены ({applied_count} столбцов)")
            self.log_message(f"=== КОНЕЦ ПРИМЕНЕНИЯ ШАБЛОНА ===")
        except Exception as e:
            self.log_message(f"Ошибка применения сопоставлений: {str(e)}")
            import traceback
            self.log_message(f"Детали ошибки: {traceback.format_exc()}")
    
    def closeEvent(self, event):
        """Обработка закрытия приложения"""
        if self.conn:
            self.conn.close()
        if self.import_worker and self.import_worker.isRunning():
            self.import_worker.terminate()
            self.import_worker.wait()
        event.accept()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = ExcelToSQLiteImporter()
    window.show()
    sys.exit(app.exec_())

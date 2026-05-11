import sys
import os
import sqlite3
from PyQt5.QtWidgets import (QApplication, QMainWindow, QVBoxLayout, QHBoxLayout, 
                             QWidget, QTableWidget, QTableWidgetItem, QPushButton, 
                             QLabel, QComboBox, QMessageBox, QHeaderView, QLineEdit,
                             QProgressBar, QSplitter, QFrame, QGroupBox)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QFont, QColor
from PyQt5 import QtCore

class DuplicatesEditor(QMainWindow):
    """Форма для редактирования таблиц дубликатов"""
    
    def __init__(self, db_path):
        super().__init__()
        self.db_path = db_path
        self.table_data = []
        self.filtered_data = []
        self.current_filter = ""
        self.current_table = "duplicates_wl_report_smr"
        self.available_tables = [
            ("duplicates_wl_report_smr", "Дубликаты wl_report_smr"),
            ("duplicates_wl_china", "Дубликаты wl_china")
        ]
        self.status_options = {
            "OK": "Оставить | Keep",
            "R-D": "Дефектный стык удален | Removed - Defective",
            "R-M": "Удален из-за несоосности | Removed - Misalignment",
            "R-R": "Демонтирован для переделки | Removed - Rework"
        }
        self.init_ui()
        self.load_data()
    
    def init_ui(self):
        self.setWindowTitle("Редактор дубликатов сварных швов")
        self.setGeometry(100, 100, 1600, 800)
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout()
        title_label = QLabel("Редактирование дубликатов сварных швов")
        title_label.setFont(QFont("Arial", 16, QFont.Bold))
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        title_label.setStyleSheet("color: #2c3e50; margin: 10px;")
        main_layout.addWidget(title_label)
        control_panel = QGroupBox("Управление")
        control_layout = QHBoxLayout()
        # Выбор таблицы
        table_label = QLabel("Таблица:")
        table_label.setFont(QFont("Arial", 10))
        self.table_selector = QComboBox()
        for tbl, desc in self.available_tables:
            self.table_selector.addItem(desc, tbl)
        self.table_selector.currentIndexChanged.connect(lambda idx: self.on_table_changed(idx))
        # Поиск
        search_label = QLabel("Поиск:")
        search_label.setFont(QFont("Arial", 10))
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Введите текст для поиска...")
        self.search_input.textChanged.connect(self.filter_data)
        self.search_input.setMinimumWidth(300)
        self.refresh_btn = QPushButton("🔄 Обновить")
        self.refresh_btn.setFont(QFont("Arial", 10, QFont.Bold))
        self.refresh_btn.clicked.connect(self.load_data)
        self.show_empty_btn = QPushButton("📝 Только пустые")
        self.show_empty_btn.setFont(QFont("Arial", 10, QFont.Bold))
        self.show_empty_btn.clicked.connect(self.show_empty_status_only)
        self.show_all_btn = QPushButton("📋 Все записи")
        self.show_all_btn.setFont(QFont("Arial", 10, QFont.Bold))
        self.show_all_btn.clicked.connect(self.show_all_records)
        self.save_btn = QPushButton("💾 Сохранить изменения")
        self.save_btn.setFont(QFont("Arial", 10, QFont.Bold))
        self.save_btn.clicked.connect(self.save_changes)
        
        self.update_smr_from_excel_btn = QPushButton("📊 Обновить SMR из Excel")
        self.update_smr_from_excel_btn.setFont(QFont("Arial", 10, QFont.Bold))
        self.update_smr_from_excel_btn.setStyleSheet("background-color: #27ae60; color: white;")
        self.update_smr_from_excel_btn.clicked.connect(self.update_smr_from_excel)
        print("✅ Кнопка SMR создана и привязана к функции")
        
        self.update_china_from_excel_btn = QPushButton("📊 дубли wl китайский")
        self.update_china_from_excel_btn.setFont(QFont("Arial", 10, QFont.Bold))
        self.update_china_from_excel_btn.setStyleSheet("background-color: #e67e22; color: white;")
        self.update_china_from_excel_btn.clicked.connect(self.update_china_from_excel)
        print("✅ Кнопка China создана и привязана к функции")
        
        self.export_to_excel_btn = QPushButton("📥 Выгрузка в Excel")
        self.export_to_excel_btn.setFont(QFont("Arial", 10, QFont.Bold))
        self.export_to_excel_btn.setStyleSheet("background-color: #3498db; color: white;")
        self.export_to_excel_btn.clicked.connect(self.export_to_excel)
        

        
        self.close_btn = QPushButton("❌ Закрыть")
        self.close_btn.setFont(QFont("Arial", 10, QFont.Bold))
        self.close_btn.clicked.connect(self.close)
        
        control_layout.addWidget(table_label)
        control_layout.addWidget(self.table_selector)
        control_layout.addWidget(search_label)
        control_layout.addWidget(self.search_input)
        control_layout.addStretch()
        control_layout.addWidget(self.refresh_btn)
        control_layout.addWidget(self.show_empty_btn)
        control_layout.addWidget(self.show_all_btn)
        control_layout.addWidget(self.save_btn)
        control_layout.addWidget(self.update_smr_from_excel_btn)
        control_layout.addWidget(self.update_china_from_excel_btn)
        control_layout.addWidget(self.export_to_excel_btn)
        control_layout.addWidget(self.close_btn)
        control_panel.setLayout(control_layout)
        main_layout.addWidget(control_panel)
        self.info_label = QLabel("Загрузка данных...")
        self.info_label.setFont(QFont("Arial", 10))
        self.info_label.setStyleSheet("color: #7f8c8d; margin: 5px;")
        main_layout.addWidget(self.info_label)
        self.table = QTableWidget()
        self.table.setFont(QFont("Arial", 9))
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setEditTriggers(QTableWidget.DoubleClicked)
        header = self.table.horizontalHeader()
        if header:
            header.setStretchLastSection(True)
            header.setSectionResizeMode(QHeaderView.Interactive)
        vertical_header = self.table.verticalHeader()
        if vertical_header:
            vertical_header.setVisible(False)
        self.table.itemChanged.connect(self.on_item_changed)
        main_layout.addWidget(self.table)
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        main_layout.addWidget(self.progress_bar)
        central_widget.setLayout(main_layout)
    
    def on_table_changed(self, idx):
        self.current_table = self.table_selector.currentData()
        self.load_data()
    
    def load_data(self):
        try:
            print(f"Загрузка данных из таблицы: {self.current_table}")
            self.progress_bar.setVisible(True)
            self.progress_bar.setValue(0)
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Получаем данные из выбранной таблицы с правильными названиями столбцов
            # Ограничиваем количество записей для производительности
            limit = 1000  # Максимум 1000 записей за раз
            
            if self.current_table == 'duplicates_wl_report_smr':
                query = f'''
                    SELECT 
                        rowid,
                        "Титул",
                        "_Стыка",
                        "Дата_сварки",
                        "ЛИНИЯ",
                        "_ISO",
                        "_Номер_стыка",
                        "duplicate_group_id",
                        "duplicate_count",
                        "extraction_date",
                        "original_id_smr",
                        "_Что_со_стыком_повторяющимся??!!"
                    FROM {self.current_table}
                    ORDER BY "_ISO", "_Номер_стыка"
                    LIMIT {limit}
                '''
            else:  # duplicates_wl_china
                query = f'''
                    SELECT 
                        rowid,
                        "блок_",
                        "Номер_сварного_шва",
                        "Дата_сварки",
                        "_Линии",
                        "Номер_чертежа",
                        "_Номер_сварного_шва",
                        "duplicate_group_id",
                        "duplicate_count",
                        "extraction_date",
                        "original_id_china",
                        "_Что_со_стыком_повторяющимся??!!"
                    FROM {self.current_table}
                    ORDER BY "Номер_чертежа", "_Номер_сварного_шва"
                    LIMIT {limit}
                '''
            
            print(f"Выполняем запрос: {query[:100]}...")
            cursor.execute(query)
            self.table_data = cursor.fetchall()
            print(f"Получено записей: {len(self.table_data)}")
            conn.close()
            self.progress_bar.setValue(50)
            
            # Показываем предупреждение, если записей много
            if len(self.table_data) >= 1000:
                QMessageBox.information(self, "Информация", 
                    f"Загружено {len(self.table_data)} записей (ограничение для производительности).\n"
                    "Для просмотра всех записей используйте фильтры или экспорт в Excel.")
            
            headers = [
                "ID", "Титул", "Стык", "Дата сварки", "Линия", 
                "ISO", "Номер стыка", "Группа", "Кол-во", "Дата", "Original ID", "Статус стыка"
            ]
            
            self.table.setColumnCount(len(headers))
            self.table.setHorizontalHeaderLabels(headers)
            self.progress_bar.setValue(75)
            
            print("Заполняем таблицу...")
            self.filter_data()
            self.progress_bar.setValue(100)
            self.progress_bar.setVisible(False)
            print("Загрузка данных завершена")
            
        except Exception as e:
            print(f"Ошибка при загрузке данных: {e}")
            self.progress_bar.setVisible(False)
            QMessageBox.critical(self, "Ошибка", f"Ошибка при загрузке данных: {str(e)}")
    
    def filter_data(self):
        search_text = self.search_input.text().lower()
        if search_text:
            self.current_filter = search_text
            self.filtered_data = [
                row for row in self.table_data
                if any(search_text in str(cell).lower() for cell in row)
            ]
        else:
            if self.current_filter == "empty_status":
                self.show_empty_status_only()
                return
            else:
                self.filtered_data = self.table_data.copy()
        self.populate_table()
    
    def populate_table(self):
        try:
            print(f"Заполнение таблицы: {len(self.filtered_data)} строк")
            self.table.setRowCount(len(self.filtered_data))
            
            for row_idx, row_data in enumerate(self.filtered_data):
                if row_idx % 100 == 0:  # Показываем прогресс каждые 100 строк
                    print(f"Обрабатываем строку {row_idx}/{len(self.filtered_data)}")
                
                for col_idx, cell_data in enumerate(row_data):
                    if col_idx == 11:  # Столбец "_Что_со_стыком_повторяющимся??!!"
                        combo = QComboBox()
                        combo.addItem("", "")
                        for code, description in self.status_options.items():
                            combo.addItem(f"{code} | {description}", code)
                        current_value = str(cell_data) if cell_data else ""
                        found_index = 0
                        for i in range(combo.count()):
                            if combo.itemData(i) == current_value:
                                found_index = i
                                break
                        combo.setCurrentIndex(found_index)
                        combo.currentTextChanged.connect(
                            lambda text, row=row_idx: self.on_combo_changed_with_highlight(row, text)
                        )
                        self.table.setCellWidget(row_idx, col_idx, combo)
                    else:
                        item = QTableWidgetItem(str(cell_data) if cell_data else "")
                        item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
                        self.table.setItem(row_idx, col_idx, item)
            
            print("Обновляем информацию о записях...")
            total_records = len(self.table_data)
            filtered_records = len(self.filtered_data)
            filter_info = ""
            if self.current_filter == "empty_status":
                filter_info = " | Фильтр: Только пустые статусы"
            elif self.current_filter:
                filter_info = f" | Фильтр: '{self.current_filter}'"
            self.info_label.setText(
                f"Всего записей: {total_records} | "
                f"Отображено: {filtered_records}" + filter_info
            )
            
            print("Настраиваем размеры столбцов...")
            self.table.resizeColumnsToContents()
            self.table.setColumnWidth(0, 60)
            self.table.setColumnWidth(1, 120)
            self.table.setColumnWidth(2, 100)
            self.table.setColumnWidth(6, 120)
            self.table.setColumnWidth(7, 80)
            self.table.setColumnWidth(8, 80)
            self.table.setColumnWidth(9, 100)
            self.table.setColumnWidth(10, 120)
            self.table.setColumnWidth(11, 250)
            print("Заполнение таблицы завершено")
            
        except Exception as e:
            print(f"Ошибка при заполнении таблицы: {e}")
            QMessageBox.critical(self, "Ошибка", f"Ошибка при заполнении таблицы: {str(e)}")
    
    def on_combo_changed_with_highlight(self, row, text):
        if " | " in text:
            code = text.split(" | ")[0]
        else:
            code = ""
        if row < len(self.filtered_data):
            old_value = self.filtered_data[row][11] if len(self.filtered_data[row]) > 11 else ""
            row_data = list(self.filtered_data[row])
            row_data[11] = code
            self.filtered_data[row] = tuple(row_data)
        self.highlight_row(row, True)
    
    def on_combo_changed(self, row, text):
        if " | " in text:
            code = text.split(" | ")[0]
        else:
            code = ""
        if row < len(self.filtered_data):
            row_data = list(self.filtered_data[row])
            row_data[11] = code
            self.filtered_data[row] = tuple(row_data)
    
    def on_item_changed(self, item):
        pass
    
    def highlight_row(self, row, highlight=True):
        if 0 <= row < self.table.rowCount():
            if highlight:
                self.table.selectRow(row)
                self.table.scrollToItem(self.table.item(row, 0))
            else:
                self.table.clearSelection()
    
    def show_empty_status_only(self):
        self.current_filter = "empty_status"
        self.filtered_data = [
            row for row in self.table_data
            if not row[11] or str(row[11]).strip() == ""
        ]
        self.populate_table()
        self.search_input.clear()
    
    def show_all_records(self):
        self.current_filter = ""
        self.filtered_data = self.table_data.copy()
        self.populate_table()
        self.search_input.clear()
    
    def save_changes(self):
        try:
            changes_count = 0
            updates = []
            original_data_dict = {row[0]: row for row in self.table_data}
            for row_idx, row_data in enumerate(self.filtered_data):
                rowid = row_data[0]
                original_row = original_data_dict.get(rowid)
                if original_row and row_data[11] != original_row[11]:
                    changes_count += 1
                    updates.append((rowid, row_data[11]))
            if changes_count == 0:
                QMessageBox.information(self, "Информация", "Нет изменений для сохранения.")
                return
            reply = QMessageBox.question(
                self, "Подтверждение", 
                f"Сохранить {changes_count} изменений в базе данных?",
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No
            )
            if reply == QMessageBox.Yes:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                for rowid, new_status in updates:
                    cursor.execute(f'''
                        UPDATE {self.current_table} 
                        SET "_Что_со_стыком_повторяющимся??!!" = ?
                        WHERE rowid = ?
                    ''', (new_status, rowid))
                conn.commit()
                conn.close()
                self.load_data()
                QMessageBox.information(
                    self, "Успех", 
                    f"Успешно сохранено {changes_count} изменений в базе данных."
                )
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Ошибка при сохранении изменений: {str(e)}")
            print(f"Ошибка сохранения: {e}")

    def update_smr_from_excel(self):
        """Обновляет данные SMR из Excel файла через запуск скрипта"""
        try:
            import subprocess
            import sys
            import os
            
            # Определяем путь к скрипту
            if getattr(sys, 'frozen', False):
                # Если запущено из EXE
                base_path = os.path.dirname(sys.executable)
            else:
                # Если запущено из .py
                base_path = os.path.dirname(os.path.abspath(__file__))
            
            script_path = os.path.join(base_path, 'loud_M_Kran_Kingesepp', 'update_duplicates_from_excel.py')
            
            if not os.path.exists(script_path):
                QMessageBox.critical(self, "Ошибка", f"Скрипт не найден: {script_path}")
                return
            
            # Определяем Python интерпретатор
            if getattr(sys, 'frozen', False):
                # Если запущено из EXE, используем python.exe из той же папки
                python_exe = os.path.join(os.path.dirname(sys.executable), 'python.exe')
                if not os.path.exists(python_exe):
                    # Если python.exe нет рядом, используем системный Python
                    python_exe = 'python'
            else:
                python_exe = sys.executable
            
            print(f"🚀 Запуск скрипта обновления SMR: {script_path}")
            print(f"🐍 Используем Python: {python_exe}")
            
            # Настраиваем переменные окружения
            env = os.environ.copy()
            env['PYTHONIOENCODING'] = 'utf-8'
            env['PYTHONLEGACYWINDOWSSTDIO'] = 'utf-8'
            
            # Запускаем скрипт через subprocess
            result = subprocess.run(
                [python_exe, script_path],
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace',
                cwd=os.path.dirname(script_path),
                env=env,
                timeout=300  # 5 минут таймаут
            )
            
            # Выводим результат
            if result.stdout:
                print(f"📤 Вывод скрипта:\n{result.stdout}")
            if result.stderr:
                print(f"⚠️ Ошибки скрипта:\n{result.stderr}")
            
            if result.returncode == 0:
                QMessageBox.information(self, "Успех", f"Обновление SMR завершено успешно!")
                self.load_data()  # Перезагружаем данные
            else:
                error_msg = result.stderr if result.stderr else "Неизвестная ошибка"
                QMessageBox.warning(self, "Ошибка", f"Скрипт завершился с ошибкой:\n{error_msg}")
                
        except subprocess.TimeoutExpired:
            QMessageBox.warning(self, "Таймаут", "Скрипт выполнялся слишком долго и был прерван")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Ошибка при запуске скрипта: {str(e)}")

    def update_china_from_excel(self):
        """Обновляет данные China из Excel файла через запуск скрипта"""
        try:
            import subprocess
            import sys
            import os
            
            # Определяем путь к скрипту
            if getattr(sys, 'frozen', False):
                # Если запущено из EXE
                base_path = os.path.dirname(sys.executable)
            else:
                # Если запущено из .py
                base_path = os.path.dirname(os.path.abspath(__file__))
            
            script_path = os.path.join(base_path, 'loud_M_Kran_Kingesepp', 'update_duplicates_china_from_excel.py')
            
            if not os.path.exists(script_path):
                QMessageBox.critical(self, "Ошибка", f"Скрипт не найден: {script_path}")
                return
            
            # Определяем Python интерпретатор
            if getattr(sys, 'frozen', False):
                # Если запущено из EXE, используем python.exe из той же папки
                python_exe = os.path.join(os.path.dirname(sys.executable), 'python.exe')
                if not os.path.exists(python_exe):
                    # Если python.exe нет рядом, используем системный Python
                    python_exe = 'python'
            else:
                python_exe = sys.executable
            
            print(f"🚀 Запуск скрипта обновления China: {script_path}")
            print(f"🐍 Используем Python: {python_exe}")
            
            # Настраиваем переменные окружения
            env = os.environ.copy()
            env['PYTHONIOENCODING'] = 'utf-8'
            env['PYTHONLEGACYWINDOWSSTDIO'] = 'utf-8'
            
            # Запускаем скрипт через subprocess
            result = subprocess.run(
                [python_exe, script_path],
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace',
                cwd=os.path.dirname(script_path),
                env=env,
                timeout=300  # 5 минут таймаут
            )
            
            # Выводим результат
            if result.stdout:
                print(f"📤 Вывод скрипта:\n{result.stdout}")
            if result.stderr:
                print(f"⚠️ Ошибки скрипта:\n{result.stderr}")
            
            if result.returncode == 0:
                QMessageBox.information(self, "Успех", f"Обновление China завершено успешно!")
                self.load_data()  # Перезагружаем данные
            else:
                error_msg = result.stderr if result.stderr else "Неизвестная ошибка"
                QMessageBox.warning(self, "Ошибка", f"Скрипт завершился с ошибкой:\n{error_msg}")
                
        except subprocess.TimeoutExpired:
            QMessageBox.warning(self, "Таймаут", "Скрипт выполнялся слишком долго и был прерван")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Ошибка при запуске скрипта: {str(e)}")

    def export_to_excel(self):
        """Экспортирует данные в Excel файл"""
        try:
            from PyQt5.QtWidgets import QFileDialog
            import pandas as pd
            from datetime import datetime
            import os
            
            # Получаем данные для экспорта
            if not self.filtered_data:
                QMessageBox.warning(self, "Предупреждение", "Нет данных для экспорта")
                return
            
            # Определяем заголовки в зависимости от таблицы
            if self.current_table == 'duplicates_wl_report_smr':
                headers = [
                    "ID", "Титул", "Стык", "Дата сварки", "Линия", 
                    "ISO", "Номер стыка", "Группа", "Кол-во", "Дата", "Original ID", "Статус стыка"
                ]
            else:  # duplicates_wl_china
                headers = [
                    "ID", "Блок", "Номер сварного шва", "Дата сварки", "Линии", 
                    "Номер чертежа", "Номер сварного шва", "Группа", "Кол-во", "Дата", "Original ID", "Статус стыка"
                ]
            
            # Создаем DataFrame
            df = pd.DataFrame(self.filtered_data, columns=headers)
            
            # Преобразуем статусы в читаемый вид
            status_mapping = {v: k for k, v in self.status_options.items()}
            df['Статус стыка'] = df['Статус стыка'].map(lambda x: status_mapping.get(x, x))
            
            # Предлагаем место для сохранения
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            default_filename = f"дубликаты_{self.current_table}_{timestamp}.xlsx"
            
            file_path, _ = QFileDialog.getSaveFileName(
                self, 
                "Сохранить дубликаты в Excel", 
                default_filename,
                "Excel файлы (*.xlsx);;Все файлы (*)"
            )
            
            if file_path:
                # Сохраняем в Excel
                with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                    df.to_excel(writer, sheet_name='Дубликаты', index=False)
                    
                    # Автоматически подгоняем ширину столбцов
                    worksheet = writer.sheets['Дубликаты']
                    for column in worksheet.columns:
                        max_length = 0
                        column_letter = column[0].column_letter
                        for cell in column:
                            try:
                                if len(str(cell.value)) > max_length:
                                    max_length = len(str(cell.value))
                            except:
                                pass
                        adjusted_width = min(max_length + 2, 50)
                        worksheet.column_dimensions[column_letter].width = adjusted_width
                
                QMessageBox.information(
                    self, 
                    "Успех", 
                    f"Данные успешно экспортированы в файл:\n{file_path}\n\n"
                    f"Экспортировано записей: {len(df)}"
                )
                
        except ImportError:
            QMessageBox.critical(
                self, 
                "Ошибка", 
                "Для экспорта в Excel требуется установить pandas и openpyxl:\n"
                "pip install pandas openpyxl"
            )
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Ошибка при экспорте в Excel: {str(e)}")




def main():
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    if getattr(sys, 'frozen', False):
        script_dir = os.path.dirname(sys.executable)
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, 'BD_Kingisepp', 'M_Kran_Kingesepp.db')
    if not os.path.exists(db_path):
        QMessageBox.critical(None, "Ошибка", f"База данных не найдена: {db_path}")
        return
    window = DuplicatesEditor(db_path)
    window.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main() 
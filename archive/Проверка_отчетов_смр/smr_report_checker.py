import sys
import os
import sqlite3
import pandas as pd
import re
from PyQt5.QtWidgets import (QApplication, QMainWindow, QPushButton, QVBoxLayout, 
                             QHBoxLayout, QWidget, QLabel, QMessageBox, QFileDialog,
                             QTextEdit, QProgressBar, QGroupBox, QLineEdit)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QFont

class SmrReportChecker(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Проверка отчетов СМР")
        self.setGeometry(100, 100, 1000, 700)
        
        # Устанавливаем флаги окна для лучшего отображения
        self.setWindowFlags(Qt.WindowType.Window)
        
        # Устанавливаем атрибут для предотвращения автоматического закрытия
        self.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, False)
        
        # Центрируем окно на экране
        self.center_window()
        
        # Обработчик закрытия окна уже определен в closeEvent
        
        # Пути к базе данных
        self.base_path = self.find_base_path()
        self.db_path = os.path.join(self.base_path, 'BD_Kingisepp', 'M_Kran_Kingesepp.db')
        
        # Переменные
        self.excel_file_path = None
        self.excel_data = None
        
        self.init_ui()
        
    def find_base_path(self):
        """Находит базовую папку проекта"""
        if getattr(sys, 'frozen', False):
            # Если запущено из EXE
            base_path = os.path.dirname(sys.executable)
        else:
            # Если запущено из .py
            base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        return base_path
    
    def closeEvent(self, event):
        """Обработчик закрытия окна"""
        print("Форма проверки отчетов СМР закрывается...")
        # Принимаем событие закрытия
        event.accept()
    
    def center_window(self):
        """Центрирует окно на экране"""
        try:
            screen = QApplication.primaryScreen()
            if screen:
                screen_geometry = screen.geometry()
                x = (screen_geometry.width() - self.width()) // 2
                y = (screen_geometry.height() - self.height()) // 2
                self.move(x, y)
        except Exception:
            # Если не удалось центрировать, используем стандартную позицию
            pass
        
    def init_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        layout = QVBoxLayout()
        
        # Заголовок
        title_label = QLabel("Проверка отчетов СМР")
        title_label.setFont(QFont("Arial", 16, QFont.Bold))
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        title_label.setStyleSheet("color: #2c3e50; margin: 10px;")
        layout.addWidget(title_label)
        
        # Группа выбора файла
        file_group = QGroupBox("📁 Выбор Excel файла")
        file_group.setFont(QFont("Arial", 12, QFont.Bold))
        file_layout = QVBoxLayout()
        
        # Поле для отображения пути к файлу
        self.file_path_label = QLabel("Файл не выбран")
        self.file_path_label.setStyleSheet("color: #7f8c8d; padding: 5px; border: 1px solid #bdc3c7; border-radius: 5px;")
        file_layout.addWidget(self.file_path_label)
        
        # Кнопка выбора файла
        select_file_btn = QPushButton("📂 Выбрать Excel файл")
        select_file_btn.setFont(QFont("Arial", 11))
        select_file_btn.setMinimumHeight(40)
        select_file_btn.setStyleSheet("""
            QPushButton {
                background-color: #3498db;
                color: white;
                border: none;
                border-radius: 8px;
                padding: 10px;
            }
            QPushButton:hover {
                background-color: #2980b9;
            }
        """)
        select_file_btn.clicked.connect(self.select_excel_file)
        file_layout.addWidget(select_file_btn)
        
        file_group.setLayout(file_layout)
        layout.addWidget(file_group)
        
        # Группа информации о файле
        info_group = QGroupBox("📊 Информация о файле")
        info_group.setFont(QFont("Arial", 12, QFont.Bold))
        info_layout = QVBoxLayout()
        
        self.info_text = QTextEdit()
        self.info_text.setMaximumHeight(150)
        self.info_text.setFont(QFont("Consolas", 9))
        self.info_text.setStyleSheet("background-color: #f8f9fa; border: 1px solid #dee2e6;")
        info_layout.addWidget(self.info_text)
        
        info_group.setLayout(info_layout)
        layout.addWidget(info_group)
        
        # Группа проверки
        check_group = QGroupBox("🔍 Проверка данных")
        check_group.setFont(QFont("Arial", 12, QFont.Bold))
        check_layout = QVBoxLayout()
        
        # Кнопка проверки
        self.check_btn = QPushButton("🔍 Начать проверку")
        self.check_btn.setFont(QFont("Arial", 11))
        self.check_btn.setMinimumHeight(40)
        self.check_btn.setStyleSheet("""
            QPushButton {
                background-color: #27ae60;
                color: white;
                border: none;
                border-radius: 8px;
                padding: 10px;
            }
            QPushButton:hover {
                background-color: #229954;
            }
            QPushButton:disabled {
                background-color: #bdc3c7;
            }
        """)
        self.check_btn.clicked.connect(self.start_check)
        self.check_btn.setEnabled(False)
        check_layout.addWidget(self.check_btn)
        
        # Прогресс бар
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setStyleSheet("""
            QProgressBar {
                border: 2px solid #bdc3c7;
                border-radius: 5px;
                text-align: center;
            }
            QProgressBar::chunk {
                background-color: #3498db;
                border-radius: 3px;
            }
        """)
        check_layout.addWidget(self.progress_bar)
        
        check_group.setLayout(check_layout)
        layout.addWidget(check_group)
        
        # Группа результатов
        results_group = QGroupBox("📋 Результаты проверки")
        results_group.setFont(QFont("Arial", 12, QFont.Bold))
        results_layout = QVBoxLayout()
        
        self.results_text = QTextEdit()
        self.results_text.setFont(QFont("Consolas", 9))
        self.results_text.setStyleSheet("background-color: white; border: 1px solid #dee2e6;")
        results_layout.addWidget(self.results_text)
        
        results_group.setLayout(results_layout)
        layout.addWidget(results_group)
        
        # Кнопка сохранения
        self.save_btn = QPushButton("💾 Сохранить результат")
        self.save_btn.setFont(QFont("Arial", 11))
        self.save_btn.setMinimumHeight(40)
        self.save_btn.setStyleSheet("""
            QPushButton {
                background-color: #f39c12;
                color: white;
                border: none;
                border-radius: 8px;
                padding: 10px;
            }
            QPushButton:hover {
                background-color: #e67e22;
            }
            QPushButton:disabled {
                background-color: #bdc3c7;
            }
        """)
        self.save_btn.clicked.connect(self.save_result)
        self.save_btn.setEnabled(False)
        layout.addWidget(self.save_btn)
        
        central_widget.setLayout(layout)
        
        # Приветственное сообщение
        self.results_text.append("🚀 Система проверки отчетов СМР готова к работе!\n")
        self.results_text.append("📁 Выберите Excel файл для проверки\n")
        self.results_text.append("📂 По умолчанию будет открыта папка: D:\\МК_Кран\\МК_Кран_Кингесеп\\СМР\\Проверка отчетов\n")
        self.results_text.append("🔍 Система проверит соответствие данных столбцов 'ЛИНИЯ' и 'Чертеж' с базой данных\n")
        
    def select_excel_file(self):
        """Выбор Excel файла"""
        # Путь к папке с отчетами СМР
        default_path = r"D:\МК_Кран\МК_Кран_Кингесеп\СМР\Проверка отчетов"
        
        # Проверяем существование папки
        if not os.path.exists(default_path):
            default_path = ""
        
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Выберите Excel файл",
            default_path,
            "Excel Files (*.xlsx *.xls)"
        )
        
        if file_path:
            self.excel_file_path = file_path
            self.file_path_label.setText(f"Выбран файл: {os.path.basename(file_path)}")
            self.file_path_label.setStyleSheet("color: #27ae60; padding: 5px; border: 1px solid #27ae60; border-radius: 5px;")
            
            # Загружаем информацию о файле
            self.load_file_info()
            
            # Активируем кнопку проверки
            self.check_btn.setEnabled(True)
            
    def load_file_info(self):
        """Загружает информацию о выбранном файле"""
        try:
            # Читаем Excel файл
            df = pd.read_excel(self.excel_file_path)
            
            # Сохраняем данные
            self.excel_data = df
            
            # Формируем информацию
            info_text = f"📊 Информация о файле:\n"
            info_text += f"📁 Путь: {self.excel_file_path}\n"
            info_text += f"📋 Количество строк: {len(df)}\n"
            info_text += f"📊 Количество столбцов: {len(df.columns)}\n\n"
            
            info_text += f"📋 Столбцы в файле:\n"
            for i, col in enumerate(df.columns, 1):
                info_text += f"  {i}. {col}\n"
            
            # Проверяем наличие необходимых столбцов
            info_text += f"\n🔍 Проверка необходимых столбцов:\n"
            
            required_columns = ['ЛИНИЯ', 'Чертеж']
            missing_columns = []
            
            for col in required_columns:
                if col in df.columns:
                    info_text += f"  ✅ {col} - найден\n"
                else:
                    info_text += f"  ❌ {col} - НЕ НАЙДЕН\n"
                    missing_columns.append(col)
            
            if missing_columns:
                info_text += f"\n⚠️ ВНИМАНИЕ: Отсутствуют необходимые столбцы: {', '.join(missing_columns)}\n"
                info_text += f"Проверка может быть неполной!\n"
            
            self.info_text.setText(info_text)
            
        except Exception as e:
            self.info_text.setText(f"❌ Ошибка при чтении файла: {str(e)}")
            self.check_btn.setEnabled(False)
            
    def start_check(self):
        """Начинает проверку данных"""
        if not self.excel_data is not None:
            QMessageBox.warning(self, "Ошибка", "Сначала выберите Excel файл!")
            return
            
        # Проверяем наличие необходимых столбцов
        required_columns = ['ЛИНИЯ', 'Чертеж']
        missing_columns = [col for col in required_columns if col not in self.excel_data.columns]
        
        if missing_columns:
            QMessageBox.warning(self, "Ошибка", f"В файле отсутствуют необходимые столбцы: {', '.join(missing_columns)}")
            return
            
        # Проверяем существование базы данных
        if not os.path.exists(self.db_path):
            QMessageBox.critical(self, "Ошибка", f"База данных не найдена: {self.db_path}")
            return
            
        # Запускаем проверку в отдельном потоке
        self.check_thread = CheckThread(self.excel_data, self.db_path)
        self.check_thread.progress_signal.connect(self.update_progress)
        self.check_thread.result_signal.connect(self.show_results)
        self.check_thread.finished_signal.connect(self.check_finished)
        
        # Настраиваем интерфейс
        self.check_btn.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, len(self.excel_data))
        self.progress_bar.setValue(0)
        
        # Очищаем результаты
        self.results_text.clear()
        self.results_text.append("🔍 Начинаем проверку данных...\n")
        
        # Запускаем поток
        self.check_thread.start()
        
    def update_progress(self, value):
        """Обновляет прогресс бар"""
        self.progress_bar.setValue(value)
        
    def show_results(self, results):
        """Показывает результаты проверки"""
        self.results_text.append("📋 Результаты проверки:\n")
        self.results_text.append("=" * 50 + "\n")
        
        # Статистика
        total_rows = len(results)
        ok_count = sum(1 for r in results if r['status'] == 'OK')
        error_count = sum(1 for r in results if r['status'] == 'НЕ верно ЛИНИЯ или номер ISO ПРОВЕРИТЬ!!!')
        
        self.results_text.append(f"📊 Статистика:\n")
        self.results_text.append(f"  Всего строк: {total_rows}\n")
        self.results_text.append(f"  ✅ Совпадений: {ok_count}\n")
        self.results_text.append(f"  ❌ НЕ верно ЛИНИЯ или номер ISO ПРОВЕРИТЬ!!!: {error_count}\n")
        self.results_text.append(f"  📈 Процент успеха: {(ok_count/total_rows*100):.1f}%\n\n")
        
        # Детальные результаты
        self.results_text.append("📋 Детальные результаты:\n")
        self.results_text.append("-" * 50 + "\n")
        
        for i, result in enumerate(results, 1):
            if result['status'] == 'OK':
                status_icon = "✅"
            else:
                status_icon = "❌"
            self.results_text.append(f"{i}. {status_icon} Строка {result['row']}: {result['message']}\n")
            
        # Сохраняем результаты для сохранения
        self.check_results = results
        self.checked_data = self.excel_data.copy()
        
        # Добавляем столбец с результатами
        self.checked_data['Коментарий_ОГС'] = [r['status'] for r in results]
        
    def check_finished(self):
        """Обработка завершения проверки"""
        self.progress_bar.setVisible(False)
        self.check_btn.setEnabled(True)
        self.save_btn.setEnabled(True)
        
        self.results_text.append("\n✅ Проверка завершена!\n")
        self.results_text.append("💾 Используйте кнопку 'Сохранить результат' для сохранения файла с результатами\n")
        
    def save_result(self):
        """Сохраняет результат проверки"""
        if not hasattr(self, 'checked_data'):
            QMessageBox.warning(self, "Ошибка", "Нет данных для сохранения!")
            return
        
        # Путь к папке с отчетами СМР
        default_save_path = r"D:\МК_Кран\МК_Кран_Кингесеп\СМР\Проверка отчетов"
        
        # Проверяем существование папки
        if not os.path.exists(default_save_path):
            default_save_path = os.path.dirname(self.excel_file_path) if self.excel_file_path else ""
        
        # Формируем имя файла для сохранения
        if self.excel_file_path:
            base_name = os.path.splitext(os.path.basename(self.excel_file_path))[0]
            default_file_name = f"{base_name}_проверено.xlsx"
            default_full_path = os.path.join(default_save_path, default_file_name)
        else:
            default_full_path = os.path.join(default_save_path, "результат_проверки.xlsx")
            
        # Предлагаем место для сохранения
        save_path, _ = QFileDialog.getSaveFileName(
            self,
            "Сохранить результат проверки",
            default_full_path,
            "Excel Files (*.xlsx)"
        )
        
        if save_path:
            try:
                # Сохраняем файл
                self.checked_data.to_excel(save_path, index=False)
                
                QMessageBox.information(self, "Успех", f"Результат сохранен в файл:\n{save_path}")
                
                # Показываем статистику в результатах
                ok_count = sum(1 for r in self.check_results if r['status'] == 'OK')
                error_count = sum(1 for r in self.check_results if r['status'] == 'НЕ верно ЛИНИЯ или номер ISO ПРОВЕРИТЬ!!!')
                total_count = len(self.check_results)
                
                self.results_text.append(f"\n💾 Файл сохранен: {os.path.basename(save_path)}\n")
                self.results_text.append(f"📊 Итоговая статистика:\n")
                self.results_text.append(f"  ✅ OK: {ok_count} записей\n")
                self.results_text.append(f"  ❌ НЕ верно ЛИНИЯ или номер ISO ПРОВЕРИТЬ!!!: {error_count} записей\n")
                self.results_text.append(f"  📋 Всего: {total_count} записей ({(ok_count/total_count*100):.1f}% совпадений)\n")
                
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", f"Ошибка при сохранении файла: {str(e)}")

class CheckThread(QThread):
    """Поток для выполнения проверки данных"""
    progress_signal = pyqtSignal(int)
    result_signal = pyqtSignal(list)
    finished_signal = pyqtSignal()
    
    def __init__(self, excel_data, db_path):
        super().__init__()
        self.excel_data = excel_data
        self.db_path = db_path
        
    def run(self):
        """Выполняет проверку данных"""
        try:
            # Подключаемся к базе данных
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Проверяем существование таблицы Log_Piping_PTO
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Log_Piping_PTO'")
            if not cursor.fetchone():
                self.result_signal.emit([{
                    'row': 1,
                    'status': 'НЕ верно ЛИНИЯ или номер ISO ПРОВЕРИТЬ!!!',
                    'message': 'Таблица Log_Piping_PTO не найдена в базе данных'
                }])
                conn.close()
                self.finished_signal.emit()
                return
            
            # Получаем данные из базы
            cursor.execute("SELECT Линия, ключь_жср_смр FROM Log_Piping_PTO WHERE Линия IS NOT NULL AND ключь_жср_смр IS NOT NULL")
            db_data = cursor.fetchall()
            
            # Создаем словарь для быстрого поиска
            db_dict = {}
            for line, key in db_data:
                if line and key:
                    # Очищаем данные от пробелов
                    clean_line = str(line).strip()
                    clean_key = str(key).strip()
                    if clean_line and clean_key:
                        db_dict[(clean_line, clean_key)] = True
            
            conn.close()
            
            # Проверяем данные из Excel
            results = []
            
            for index, row in self.excel_data.iterrows():
                # Получаем значения из Excel
                excel_line = str(row['ЛИНИЯ']).strip() if pd.notna(row['ЛИНИЯ']) else ''
                excel_drawing = str(row['Чертеж']).strip() if pd.notna(row['Чертеж']) else ''
                
                # Обрабатываем номер чертежа согласно требованиям
                processed_drawing = self.process_drawing_number(excel_drawing)
                
                # Проверяем совпадение
                if excel_line and processed_drawing:
                    if (excel_line, processed_drawing) in db_dict:
                        status = 'OK'
                        message = f"ЛИНИЯ='{excel_line}', Чертеж='{excel_drawing}' → '{processed_drawing}' - найдено в БД"
                    else:
                        status = 'НЕ верно ЛИНИЯ или номер ISO ПРОВЕРИТЬ!!!'
                        message = f"ЛИНИЯ='{excel_line}', Чертеж='{excel_drawing}' → '{processed_drawing}' - НЕ найдено в БД"
                else:
                    status = 'НЕ верно ЛИНИЯ или номер ISO ПРОВЕРИТЬ!!!'
                    message = f"Пустые значения: ЛИНИЯ='{excel_line}', Чертеж='{excel_drawing}'"
                
                results.append({
                    'row': index + 1,
                    'status': status,
                    'message': message
                })
                
                # Обновляем прогресс
                self.progress_signal.emit(index + 1)
            
            # Отправляем результаты
            self.result_signal.emit(results)
            
        except Exception as e:
            # В случае ошибки отправляем сообщение об ошибке
            self.result_signal.emit([{
                'row': 1,
                'status': 'НЕ верно ЛИНИЯ или номер ISO ПРОВЕРИТЬ!!!',
                'message': f'Ошибка при проверке: {str(e)}'
            }])
        
        finally:
            self.finished_signal.emit()
    
    def process_drawing_number(self, drawing_number):
        """Обрабатывает номер чертежа согласно требованиям"""
        if not drawing_number:
            return ''
        
        # Убираем кавычки
        drawing_number = drawing_number.strip().strip('"\'')
        
        # Примеры обработки:
        # 70-12-107(10) → 70-12-107
        # 70-13-01(13) → 70-13-1
        
        # Удаляем номер в скобках в конце
        pattern = r'^(.*?)\(\d+\)$'
        match = re.match(pattern, drawing_number)
        if match:
            drawing_number = match.group(1)
        
        # Обрабатываем специальные случаи
        # 70-13-01 → 70-13-1 (убираем ведущий ноль)
        pattern2 = r'^(\d+-\d+)-0(\d+)$'
        match2 = re.match(pattern2, drawing_number)
        if match2:
            drawing_number = f"{match2.group(1)}-{int(match2.group(2))}"
        
        return drawing_number.strip()

def main():
    app = QApplication(sys.argv)
    app.setStyle('Fusion')  # Современный стиль
    
    window = SmrReportChecker()
    
    # Показываем окно
    window.show()
    
    # Принудительно поднимаем окно на передний план
    window.raise_()
    window.activateWindow()
    
    # Добавляем небольшую задержку для гарантии отображения
    import time
    time.sleep(0.1)
    
    # Еще раз активируем окно
    window.raise_()
    window.activateWindow()
    
    # Добавляем отладочную информацию
    print("🚀 Форма проверки отчетов СМР запущена")
    print(f"📱 Окно видимо: {window.isVisible()}")
    print(f"📱 Окно активно: {window.isActiveWindow()}")
    print(f"📱 Геометрия окна: {window.geometry()}")
    print(f"📱 Позиция окна: {window.pos()}")
    
    # Запускаем главный цикл приложения
    sys.exit(app.exec_())

if __name__ == '__main__':
    main() 
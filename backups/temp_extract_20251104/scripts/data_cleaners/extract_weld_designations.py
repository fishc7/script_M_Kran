import sys
import os
import sqlite3
import re
from PyQt5.QtWidgets import QApplication, QDialog, QVBoxLayout, QLabel, QComboBox, QPushButton, QMessageBox

# Определяем путь к базе данных
try:
    DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'BD_Kingisepp', 'M_Kran_Kingesepp.db')
except NameError:
    # Если __file__ не определен, используем относительный путь
    DB_PATH = os.path.join('..', 'BD_Kingisepp', 'M_Kran_Kingesepp.db')
NEW_COL = '_ОБОЗНАЧЕНИЕ_ШВАРНОГО_ШВА'

def get_weld_type(text):
    """Определяет тип шва по паттерну в тексте"""
    if not text:
        return ""
    text_str = str(text).strip().upper()
    # По таблице соответствий и шаблонам:
    if re.match(r'^\d+CW$', text_str):
        return "Стык вырезан или переварен из-за ошибки проектирования или по другой причине"
    if re.match(r'^F\d+CW$', text_str):
        return "Стык вырезан или переварен из-за ошибки проектирования или по другой причине"
    if re.match(r'^S\d+CW$', text_str):
        return "Стык вырезан или переварен из-за ошибки проектирования или по другой причине"
    if re.match(r'^\d+$', text_str):
        return "Монтажный шов"
    if re.match(r'^F\d+$', text_str):
        return "Монтажный шов"
    if re.match(r'^S\d+$', text_str):
        return "Цеховой шов"
    if re.match(r'^F\d+R$', text_str):
        return "Ремонтный шов"
    if re.match(r'^S\d+R$', text_str):
        return "Ремонтный шов"
    if re.match(r'^F\d+A$', text_str):
        return "Дополнительный стык"
    if re.match(r'^F\d+B$', text_str):
        return "Дополнительный стык"
    if re.match(r'^S\d+A$', text_str):
        return "Дополнительный стык"
    if re.match(r'^S\d+B$', text_str):
        return "Дополнительный стык"
    if re.match(r'^F\d+GW$', text_str):
        return "Гарантийный стык"
    if re.match(r'^S\d+R2$', text_str):
        return "Повторно ремонтируемый шов"
    
    if re.match(r'^PS\d+$', text_str):
        return "Шов приварки опоры к трубопроводу"
    if re.match(r'^F\d+RW$', text_str):
        return "После ремонта (или вырезан)"
    if re.match(r'^S\d+RW$', text_str):
        return "После ремонта (или вырезан)"
    return "не верна маркировка согласно процедуре или не внесено обозначение в скрипт"

def process_table_weld_designations(cursor, table_name, column_name):
    """Обрабатывает таблицу для извлечения обозначений швов"""
    print(f"Обработка таблицы {table_name}, столбец {column_name}...")
    
    # Добавляем столбец, если его нет
    cursor.execute(f'PRAGMA table_info("{table_name}")')
    columns = [row[1] for row in cursor.fetchall()]
    if NEW_COL not in columns:
        try:
            cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{NEW_COL}" TEXT')
            print(f"Добавлен новый столбец '{NEW_COL}' в {table_name}")
        except Exception as e:
            print(f"Ошибка при добавлении столбца в {table_name}: {e}")
            return 0
    
    # Получаем данные для обработки
    cursor.execute(f'SELECT rowid, "{column_name}" FROM "{table_name}" WHERE "{column_name}" IS NOT NULL')
    rows = cursor.fetchall()
    print(f"Найдено записей для обработки в {table_name}: {len(rows)}")
    
    # Обрабатываем записи
    updated = 0
    for rowid, value in rows:
        if value is None:
            continue
        
        weld_type = get_weld_type(value)
        cursor.execute(f'UPDATE "{table_name}" SET "{NEW_COL}" = ? WHERE rowid = ?', (weld_type, rowid))
        updated += 1
    
    print(f"Обработано записей в {table_name}: {updated}")
    return updated

def run_script():
    """Функция для запуска скрипта через систему запуска"""
    print("=== Извлечение типов сварных швов (описания) ===")
    
    # Подключаемся к базе данных
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Определяем таблицы и столбцы для обработки
        tables_to_process = [
            ('wl_china', 'Номер_сварного_шва'),
            ('wl_report_smr', '_Стыка'),
            ('work_order_log_NDT', 'Номер_стыка_Welded_joint_No_'),
            ('logs_lnk', 'Номер_стыка')
        ]
        
        total_updated = 0
        
        for table_name, column_name in tables_to_process:
            # Проверяем существование таблицы
            cursor.execute('SELECT name FROM sqlite_master WHERE type="table" AND name=?', (table_name,))
            if not cursor.fetchone():
                print(f"Таблица '{table_name}' не существует, пропускаем...")
                continue
            
            # Проверяем существование столбца
            cursor.execute(f'PRAGMA table_info("{table_name}")')
            columns = [row[1] for row in cursor.fetchall()]
            if column_name not in columns:
                print(f"Столбец '{column_name}' не найден в таблице '{table_name}', пропускаем...")
                continue
            
            # Обрабатываем таблицу
            updated = process_table_weld_designations(cursor, table_name, column_name)
            total_updated += updated
        
        # Сохраняем изменения
        conn.commit()
        print(f"\n=== Извлечение завершено успешно ===")
        print(f"Всего обработано записей: {total_updated}")
        
    except Exception as e:
        print(f"Произошла ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

class ExtractWeldDesignationDialog(QDialog):
    def __init__(self, conn):
        super().__init__()
        self.conn = conn
        self.setWindowTitle('Добавить обозначение сварного шва')
        self.resize(400, 200)
        layout = QVBoxLayout()
        self.setLayout(layout)

        self.label1 = QLabel('Выберите таблицу:')
        layout.addWidget(self.label1)
        self.table_combo = QComboBox()
        layout.addWidget(self.table_combo)

        self.label2 = QLabel('Выберите столбец:')
        layout.addWidget(self.label2)
        self.column_combo = QComboBox()
        layout.addWidget(self.column_combo)

        self.run_btn = QPushButton('Выполнить')
        layout.addWidget(self.run_btn)
        self.run_btn.clicked.connect(self.process)

        self.table_combo.currentTextChanged.connect(self.update_columns)
        self.load_tables()

    def load_tables(self):
        cur = self.conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cur.fetchall()]
        self.table_combo.addItems(tables)
        if tables:
            self.update_columns(tables[0])

    def update_columns(self, table_name):
        self.column_combo.clear()
        cur = self.conn.cursor()
        try:
            cur.execute(f'PRAGMA table_info("{table_name}")')
            columns = [row[1] for row in cur.fetchall()]
            self.column_combo.addItems(columns)
        except Exception as e:
            pass

    def process(self):
        table = self.table_combo.currentText()
        column = self.column_combo.currentText()
        if not table or not column:
            QMessageBox.warning(self, 'Ошибка', 'Выберите таблицу и столбец!')
            return
        
        # Обрабатываем выбранную таблицу
        updated = process_table_weld_designations(self.conn.cursor(), table, column)
        self.conn.commit()
        QMessageBox.information(self, 'Готово', f'Обновлено строк: {updated}')
        self.close()

def main():
    # Проверяем, не импортируется ли модуль
    if __name__ != "__main__":
        return
    
    app = QApplication(sys.argv)
    conn = sqlite3.connect(DB_PATH)
    dlg = ExtractWeldDesignationDialog(conn)
    dlg.exec_()
    conn.close()

if __name__ == '__main__':
    main() 
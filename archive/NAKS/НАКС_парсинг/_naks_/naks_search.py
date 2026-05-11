import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import time
import os
import subprocess
import sys

def run_script():
    """
    Основная функция для запуска парсинга НАКС через script_runner
    """
    # Путь для сохранения файлов (используем относительный путь от корня проекта)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(current_dir))))
    SAVE_PATH = os.path.join(project_root, 'NAKS', 'НАКС_парсинг')
    
    # Создаем директорию, если она не существует
    os.makedirs(SAVE_PATH, exist_ok=True)
    
    # Инициализация браузера (НЕ headless, чтобы пользователь мог решить CAPTCHA)
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    # НЕ добавляем --headless, чтобы браузер был виден для решения CAPTCHA
    driver = webdriver.Chrome(options=options)
    driver.get("https://naks.ru/registry/personal/")

    # Ждём ручного прохождения CAPTCHA или автоматически продолжаем
    print("Пожалуйста, решите CAPTCHA вручную в браузере...")
    print("Скрипт продолжит выполнение автоматически через 30 секунд...")
    time.sleep(30)  # Ждем 30 секунд для ручного решения CAPTCHA
    print("Продолжаем выполнение скрипта...")

    # Сбор основных данных из таблицы
    try:
        # Ждём загрузки таблицы
        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.XPATH, "//*[@id='app_registry_personal']/div/table/tbody"))
        )

        # Получаем строки таблицы
        rows = driver.find_elements(By.XPATH, "//*[@id='app_registry_personal']/div/table/tbody/tr")
        table_data = []

        for i, row in enumerate(rows):
            cols = [col.text.strip() for col in row.find_elements(By.TAG_NAME, "td")]

            print(f"Строка {i}: {len(cols)} столбцов")

            # Пропустить строки с недостаточным количеством столбцов
            if len(cols) < 12:
                print(f"❌ Пропущена строка {i}: недостаточно столбцов ({len(cols)})")
                continue

            table_data.append(cols[:13])  # Обрезаем до 13 столбцов, если нужно

        # Названия колонок (соответствуют 13 ячейкам)
        column_names = [
            "ФИО", "Шифр клейма", "Место работы", "Должность", "Номер удостоверения",
            "Доп. Атт.", "AЦ", "AП", "Дата аттестации",
            "Окончание срока действия удостоверения", "Cрок продления", "Вид деятельности", "Область аттестации (Подробнее)"
        ]

        # Пропускаем первую строку (если это заголовок, а не данные)
        if table_data and "Фамилия" in table_data[0][0]:
            table_data = table_data[1:]

        df = pd.DataFrame(table_data, columns=column_names)

        # Сохраняем в Excel
        file_path = os.path.join(SAVE_PATH, "naks_главное.xlsx")
        df.to_excel(file_path, index=False, engine="openpyxl")
        print(f"✅ Данные успешно сохранены в файл {file_path}")

    except Exception as e:
        print(f"❌ Произошла ошибка при сборе основных данных: {e}")

    # Сбор дополнительных данных из модальных окон
    results = []

    try:
        # Собираем все ссылки "подробнее"
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//a[contains(text(), 'подробнее')]"))
        )
        links = driver.find_elements(By.XPATH, "//a[contains(text(), 'подробнее')]")
        print(f"🔍 Найдено {len(links)} ссылок 'подробнее'.")

        for i in range(len(links)):
            try:
                # Снова находим все ссылки (иначе StaleElementReferenceException)
                links = driver.find_elements(By.XPATH, "//a[contains(text(), 'подробнее')]")
                driver.execute_script("arguments[0].scrollIntoView(true);", links[i])
                time.sleep(0.5)
                links[i].click()
                print(f"✅ Открываем запись {i+1}...")

                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, "oblast_att"))
                )
                time.sleep(2)  # Ждем полной загрузки

                # Получаем HTML модального окна
                modal_html = driver.find_element(By.ID, "oblast_att").get_attribute("outerHTML")
                if modal_html:
                    soup = BeautifulSoup(modal_html, 'html.parser')

                    # Извлекаем заголовок из модального окна
                    title_element = soup.find('h4', class_="modal-title")
                    title = title_element.text.strip() if title_element else "Без названия"

                    # Извлекаем все таблицы
                    tables = soup.find_all("table", class_="table-detailPersonal")
                    
                    print(f"🔍 Найдено таблиц в модальном окне: {len(tables)}")

                    if len(tables) < 2:
                        print(f"⚠️ Меньше двух таблиц у записи {i+1}, пропущено.")
                        continue

                    # Извлекаем данные из первой таблицы
                    info_data = {}
                    print(f"📊 Обрабатываем первую таблицу (строк: {len(tables[0].find_all('tr'))})")
                    for row_idx, row in enumerate(tables[0].find_all("tr")):
                        cols = row.find_all("td")
                        print(f"   Строка {row_idx}: {len(cols)} колонок")
                        if len(cols) == 2:
                            key = cols[0].text.strip().rstrip(":")
                            value = cols[1].text.strip()
                            info_data[key] = value
                            print(f"   ✅ {key} = {value}")

                    # Извлекаем данные из второй таблицы (с учетом colspan)
                    weld_data = {}
                    print(f"📊 Обрабатываем вторую таблицу (строк: {len(tables[1].find_all('tr'))})")
                    
                    # Словарь для группировки данных по ключам
                    grouped_data = {}
                    
                    for row_idx, row in enumerate(tables[1].find_all("tr")):
                        cols = row.find_all("td")
                        print(f"   Строка {row_idx}: {len(cols)} колонок - {[col.text.strip() for col in cols]}")
                        
                        if len(cols) == 4:  # Для строк с четырьмя колонками
                            key = cols[0].text.strip()
                            value1 = cols[1].text.strip()
                            value2 = cols[2].text.strip()
                            value3 = cols[3].text.strip()
                            
                            # Группируем данные по ключу
                            if key not in grouped_data:
                                grouped_data[key] = []
                            grouped_data[key].extend([value1, value2, value3])
                            print(f"📋 Обработана строка с 4 колонками: {key} = [{value1}, {value2}, {value3}]")
                            
                        elif len(cols) == 3:  # Для строк с тремя колонками
                            key = cols[0].text.strip()
                            value1 = cols[1].text.strip()
                            value2 = cols[2].text.strip()
                            
                            # Группируем данные по ключу
                            if key not in grouped_data:
                                grouped_data[key] = []
                            grouped_data[key].extend([value1, value2])
                            print(f"📋 Обработана строка с 3 колонками: {key} = [{value1}, {value2}]")
                            
                        elif len(cols) == 2:  # Для строк с двумя колонками
                            key = cols[0].text.strip()
                            value = cols[1].text.strip()
                            
                            # Для строк с 2 колонками сохраняем как есть
                            weld_data[key] = value
                            print(f"📋 Обработана строка с 2 колонками: {key} = {value}")
                            
                        else:
                            print(f"⚠️ Пропущена строка с {len(cols)} колонками: {[col.text.strip() for col in cols]}")
                    
                    # Преобразуем сгруппированные данные в weld_data
                    for key, values in grouped_data.items():
                        # Убираем дублирующиеся значения
                        unique_values = []
                        for value in values:
                            if value not in unique_values:
                                unique_values.append(value)
                        
                        # Объединяем все уникальные значения в одну строку через разделитель
                        if len(unique_values) == 1:
                            weld_data[key] = unique_values[0]
                        else:
                            # Объединяем все значения через " | "
                            weld_data[key] = " | ".join(unique_values)

                    combined = {**info_data, **weld_data, "_УДОСТОВИРЕНИЕ_НАКС_": title}
                    results.append(combined)
                else:
                    print(f"⚠️ Не удалось получить HTML для записи {i+1}")

                # Закрываем модалку
                close_button = driver.find_element(By.XPATH, "//button[@class='close']")
                close_button.click()
                time.sleep(1)

            except Exception as e:
                print(f"❌ Ошибка при обработке записи {i+1}: {e}")
                continue

        # Сохраняем в Excel
        if results:
            df_additional = pd.DataFrame(results)
            file_path = os.path.join(SAVE_PATH, "naks_подробнее.xlsx")
            df_additional.to_excel(file_path, index=False)
            print(f"✅ Все данные сохранены в '{file_path}'.")
            
            # Автоматически запускаем объединение файлов
            print("\n🔄 Запускаем объединение файлов...")
            try:
                # Путь к скрипту объединения
                join_script_path = os.path.join(os.path.dirname(__file__), "join_naks_files_excel_merged.py")
                
                # Запускаем скрипт объединения
                # Используем cp1251 для Windows и errors='replace' для безопасной обработки некорректных символов
                result = subprocess.run([sys.executable, join_script_path], 
                                      capture_output=True, 
                                      text=True,
                                      encoding='cp1251',
                                      errors='replace',
                                      cwd=os.path.dirname(__file__))
                
                if result.returncode == 0:
                    print("✅ Объединение файлов выполнено успешно!")
                    if result.stdout:
                        print("Вывод скрипта объединения:")
                        print(result.stdout)
                else:
                    print("❌ Ошибка при объединении файлов:")
                    print(result.stderr)
                
            except Exception as e:
                print(f"❌ Ошибка при запуске скрипта объединения: {e}")
        else:
            print("⚠️ Данные не были собраны.")

    except Exception as e:
        print(f"❌ Произошла ошибка при сборе дополнительных данных: {e}")

    # Закрываем браузер
    driver.quit()
    print("✅ Парсинг НАКС завершен успешно!")

# Запуск скрипта, если он выполняется напрямую (не через script_runner)
if __name__ == "__main__":
    run_script()

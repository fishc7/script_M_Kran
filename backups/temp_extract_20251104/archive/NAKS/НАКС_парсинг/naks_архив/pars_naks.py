import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Инициализация браузера
driver = webdriver.Chrome()

# Открываем сайт
driver.get("https://naks.ru/registry/personal/")

# Ждём ручного прохождения CAPTCHA
input("Пожалуйста, решите CAPTCHA вручную и нажмите Enter, когда закончите...")

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
        "Доп. Атт.", "Место аттестации", "Дата аттестации", "Окончание срока",
        "Срок продления", "Вид деятельности", "Область аттестации", "Подробнее"
    ]

    # Пропускаем первую строку (если это заголовок, а не данные)
    if table_data and "Фамилия" in table_data[0][0]:
        table_data = table_data[1:]

    df = pd.DataFrame(table_data, columns=column_names)

    # Сохраняем в Excel
    df.to_excel("output_data.xlsx", index=False, engine="openpyxl")
    print("✅ Данные успешно сохранены в файл output_data.xlsx")

except Exception as e:
    print(f"❌ Произошла ошибка: {e}")

finally:
    driver.quit()

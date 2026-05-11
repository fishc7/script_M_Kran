from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import pandas as pd
import time

# Запуск браузера
options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")
driver = webdriver.Chrome(options=options)
driver.get("https://naks.ru/registry/personal/")

input("Решите CAPTCHA и нажмите Enter...")

# Собираем все ссылки "подробнее"
try:
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, "//a[contains(text(), 'подробнее')]"))
    )
    links = driver.find_elements(By.XPATH, "//a[contains(text(), 'подробнее')]")
    print(f"🔍 Найдено {len(links)} ссылок 'подробнее'.")
except Exception:
    print("❌ Не удалось найти ссылки 'подробнее'.")
    driver.quit()
    exit()

results = []

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
        soup = BeautifulSoup(modal_html, 'html.parser')
        
        # Извлекаем заголовок из модального окна
        title = soup.find('h4', class_="modal-title").text.strip() if soup.find('h4', class_="modal-title") else "Без названия"
        
        # Извлекаем все таблицы
        tables = soup.find_all("table", class_="table-detailPersonal")

        if len(tables) < 2:
            print(f"⚠️ Меньше двух таблиц у записи {i+1}, пропущено.")
            continue

        # Извлекаем данные из первой таблицы
        info_data = {}
        for row in tables[0].find_all("tr"):
            cols = row.find_all("td")
            if len(cols) == 2:
                key = cols[0].text.strip().rstrip(":")
                value = cols[1].text.strip()
                info_data[key] = value

        # Извлекаем данные из второй таблицы (с учетом colspan)
        weld_data = {}
        for row in tables[1].find_all("tr"):
            cols = row.find_all("td")
            if len(cols) == 3:  # Для строк с тремя колонками
                key = cols[0].text.strip()
                value1 = cols[1].text.strip()
                value2 = cols[2].text.strip()
                weld_data[key] = (value1, value2)
            elif len(cols) == 2:  # Для строк с двумя колонками
                key = cols[0].text.strip()
                value = cols[1].text.strip()
                weld_data[key] = value

        combined = {**info_data, **weld_data, "_УДОСТОВИРЕНИЕ_НАКС_": title}
        results.append(combined)

        # Закрываем модалку
        close_button = driver.find_element(By.XPATH, "//button[@class='close']")
        close_button.click()
        time.sleep(1)

    except Exception as e:
        print(f"❌ Ошибка при обработке записи {i+1}: {e}")
        continue

# Сохраняем в Excel
if results:
    df = pd.DataFrame(results)
    df.to_excel("НАКС_парсинг/naks_подробнее.xlsx", index=False)
    print("✅ Все данные сохранены в 'naks_подробнее.xlsx'.")
else:
    print("⚠️ Данные не были собраны.")

driver.quit()

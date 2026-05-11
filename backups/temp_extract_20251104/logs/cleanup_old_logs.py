import os
import time

# Папка с логами
LOG_DIR = os.path.dirname(os.path.abspath(__file__))
# Сколько дней хранить логи
DAYS_TO_KEEP = 14

now = time.time()

for filename in os.listdir(LOG_DIR):
    if filename.endswith('.log'):
        filepath = os.path.join(LOG_DIR, filename)
        if os.path.isfile(filepath):
            file_age_days = (now - os.path.getmtime(filepath)) / 86400
            if file_age_days > DAYS_TO_KEEP:
                print(f'Removing old log: {filename}')
                os.remove(filepath)
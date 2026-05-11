import re

def extract_joint_number(joint_text):
    """
    Извлекает номер стыка из текста
    Убирает ведущие нули из извлеченных чисел.
    """
    if not joint_text:
        return None
    
    try:
        # Безопасное преобразование в строку
        if isinstance(joint_text, bytes):
            joint_text = joint_text.decode('utf-8', errors='ignore')
        else:
            joint_text = str(joint_text)
        
        # Дополнительная очистка от проблемных символов
        joint_text = ''.join(char for char in joint_text if ord(char) < 128 or char.isdigit())
        joint_text = joint_text.strip()
        
        match = re.search(r'\d+', joint_text)
        if match:
            number = match.group()
            # Убираем ведущие нули
            return str(int(number))
    except Exception as e:
        # Игнорируем все ошибки
        pass
    
    return None

def clean_joint_number(joint_text):
    """
    Улучшенная функция очистки номера стыка
    Удаляет префиксы S/F и ведущие нули
    """
    if not joint_text:
        return None
    
    try:
        # Безопасное преобразование в строку
        if isinstance(joint_text, bytes):
            joint_text = joint_text.decode('utf-8', errors='ignore')
        else:
            joint_text = str(joint_text)
        
        # Дополнительная очистка от проблемных символов
        joint_text = ''.join(char for char in joint_text if ord(char) < 128 or char.isdigit())
        joint_text = joint_text.strip()
        
        # Удаляем S или F в начале, пробелы, дефисы
        cleaned = re.sub(r'^[SF]\s*-?\s*', '', joint_text, flags=re.IGNORECASE)
        cleaned = cleaned.replace(' ', '')  # Убираем все пробелы
        
        # Удаляем все ведущие нули, но оставляем хотя бы одну цифру
        cleaned = re.sub(r'^0+', '', cleaned)
        
        # Если после удаления нулей ничего не осталось, возвращаем '0'
        if not cleaned:
            cleaned = '0'
        
        return cleaned
    except Exception as e:
        # Игнорируем все ошибки
        pass
    
    return None

def extract_and_clean_joint_number(joint_text):
    """
    Комбинированная функция: извлекает число и очищает его от префиксов S/F
    """
    if not joint_text:
        return None
    
    try:
        # Безопасное преобразование в строку
        if isinstance(joint_text, bytes):
            joint_text = joint_text.decode('utf-8', errors='ignore')
        else:
            joint_text = str(joint_text)
        
        # Дополнительная очистка от проблемных символов
        joint_text = ''.join(char for char in joint_text if ord(char) < 128 or char.isdigit())
        joint_text = joint_text.strip()
        
        # Сначала пытаемся извлечь число с префиксом S/F
        match = re.search(r'[SF]\s*-?\s*(\d+)', joint_text, flags=re.IGNORECASE)
        if match:
            number = match.group(1)
            # Убираем ведущие нули
            return str(int(number))
        
        # Если нет префикса S/F, ищем любые цифры
        match = re.search(r'(\d+)', joint_text)
        if match:
            number = match.group(1)
            # Убираем ведущие нули
            return str(int(number))
    except Exception as e:
        # Игнорируем все ошибки
        pass
    
    return None 
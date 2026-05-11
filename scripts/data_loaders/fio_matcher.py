"""
Модуль для сопоставления ФИО между таблицами.

Использует Python-логику для более гибкого и поддерживаемого сопоставления.
Поддерживает fuzzy matching для обработки опечаток и вариаций написания.
"""

import re
from typing import Optional, List, Tuple
from difflib import SequenceMatcher

# Пытаемся импортировать библиотеки для fuzzy matching
try:
    from rapidfuzz import fuzz, process
    HAS_RAPIDFUZZ = True
except ImportError:
    try:
        from fuzzywuzzy import fuzz
        from fuzzywuzzy import process
        HAS_RAPIDFUZZ = False
    except ImportError:
        try:
            from thefuzz import fuzz, process
            HAS_RAPIDFUZZ = False
        except ImportError:
            fuzz = None
            process = None
            HAS_RAPIDFUZZ = False


class FIOMatcher:
    """Класс для сопоставления ФИО с использованием различных стратегий."""
    # Явный словарь исправлений частых опечаток фамилий.
    # Ключи/значения в нижнем регистре и без "ё".
    TYPO_SURNAME_MAP = {
        "шребин": "щербина",
        "щербин": "щербина",
        "кобосян": "кобаснян",
        "кобоснян": "кобаснян",
        "абдурасулов": "абдулрасулов",
        "абдрасувов": "абдрасулов",
    }
    TYPO_FIRSTNAME_MAP = {
        "адилжан": "алимжан",
        "адильжан": "алимжан",
    }
    
    def __init__(self, target_fios_with_ids: List[Tuple[str, int]], fuzzy_threshold: int = 85):
        """
        Инициализация сопоставителя.
        
        Args:
            target_fios_with_ids: Список кортежей (ФИО, id_fio) для сопоставления (из ФИО_свар)
            fuzzy_threshold: Порог схожести для fuzzy matching (0-100, по умолчанию 85)
        """
        self.target_fios_with_ids = [(fio.strip(), fio_id) for fio, fio_id in target_fios_with_ids if fio and fio.strip()]
        self.target_fios = [fio for fio, _ in self.target_fios_with_ids]
        self.fuzzy_threshold = fuzzy_threshold
        # Создаем словарь для быстрого поиска id_fio по ФИО
        self._fio_to_id = {fio: fio_id for fio, fio_id in self.target_fios_with_ids}
        # Нормализуем целевые ФИО для быстрого поиска
        self._normalized_targets = {
            self._normalize_for_comparison(fio): (fio, fio_id)
            for fio, fio_id in self.target_fios_with_ids
        }
    
    @staticmethod
    def _normalize_for_comparison(fio: str) -> str:
        """
        Нормализует ФИО для сравнения (заменяет ё на е, приводит к нижнему регистру).
        
        Args:
            fio: Исходное ФИО
            
        Returns:
            Нормализованное ФИО
        """
        if not fio:
            return ""
        normalized = fio.replace("ё", "е").replace("Ё", "Е")
        # Приводим "Власов.Ю", "Ю.А." и подобные формы к единым токенам.
        normalized = re.sub(r"([А-Яа-яA-Za-z])\.([А-Яа-яA-Za-z])", r"\1 \2", normalized)
        normalized = re.sub(r"[.,;:]+", " ", normalized)
        normalized = re.sub(r"\s+", " ", normalized)
        normalized = normalized.lower().strip()

        # Исправляем опечатки фамилии (первый токен), если она в словаре.
        parts = normalized.split()
        if parts:
            parts[0] = FIOMatcher.TYPO_SURNAME_MAP.get(parts[0], parts[0])
        # Исправляем частые вариации/опечатки имени (второй токен), если есть.
        if len(parts) > 1:
            parts[1] = FIOMatcher.TYPO_FIRSTNAME_MAP.get(parts[1], parts[1])
        return " ".join(parts)
    
    @staticmethod
    def _split_fio(fio: str) -> Tuple[str, str, str]:
        """
        Разбивает ФИО на части: фамилия, имя, отчество.
        
        Args:
            fio: ФИО для разбора
            
        Returns:
            Кортеж (фамилия, имя, отчество)
        """
        parts = fio.strip().split()
        surname = parts[0] if len(parts) > 0 else ""
        first_name = parts[1] if len(parts) > 1 else ""
        patronymic = parts[2] if len(parts) > 2 else ""
        return surname, first_name, patronymic
    
    @staticmethod
    def _is_initial(word: str) -> bool:
        """
        Проверяет, является ли слово инициалом (одна буква или одна буква с точкой).
        
        Args:
            word: Слово для проверки
            
        Returns:
            True, если это инициал
        """
        word = word.strip().replace(".", "")
        return len(word) == 1 and word.isalpha()

    @staticmethod
    def _surname_similar(left: str, right: str, threshold: float = 0.74) -> bool:
        """Проверяет, похожи ли фамилии (для обработки опечаток)."""
        if not left or not right:
            return False
        if left == right:
            return True
        return SequenceMatcher(None, left, right).ratio() >= threshold
    
    def _match_by_partial(self, source_fio: str) -> Optional[int]:
        """
        Стратегия 1: Частичное совпадение (одно ФИО содержится в другом).
        
        Args:
            source_fio: Исходное ФИО для сопоставления
            
        Returns:
            id_fio найденного ФИО или None
        """
        source_normalized = self._normalize_for_comparison(source_fio)
        
        for target_fio, target_id in self.target_fios_with_ids:
            target_normalized = self._normalize_for_comparison(target_fio)
            
            # Проверяем, содержится ли одно в другом
            if source_normalized in target_normalized or target_normalized in source_normalized:
                return target_id
        
        return None
    
    def _match_by_surname_and_initials(self, source_fio: str) -> Optional[int]:
        """
        Стратегия 2: Сопоставление по фамилии и инициалам.
        Например: "Жуков В.Н." -> "Жуков Виктор Николаевич"
                  "Кубко Ю А" -> "Кубко Юрий Александрович"
        
        Args:
            source_fio: Исходное ФИО для сопоставления
            
        Returns:
            id_fio найденного ФИО или None
        """
        source_parts = self._split_fio(source_fio)
        source_surname, source_first, source_patronymic = source_parts
        
        if not source_surname:
            return None
        
        source_surname_norm = self._normalize_for_comparison(source_surname)
        source_first_norm = self._normalize_for_comparison(source_first)
        source_patronymic_norm = self._normalize_for_comparison(source_patronymic)

        # Поддержка формата "Фамилия И.О." после нормализации -> "фамилия и о"
        # В таком случае оба инициала оказываются во втором токене.
        first_tokens = source_first_norm.split()
        if not source_patronymic_norm and len(first_tokens) >= 2 and all(len(t) == 1 for t in first_tokens[:2]):
            source_first_norm = first_tokens[0]
            source_patronymic_norm = first_tokens[1]
        
        # Должно быть хотя бы имя или инициал имени
        if not source_first_norm:
            return None
        
        # Ищем совпадения по фамилии
        for target_fio, target_id in self.target_fios_with_ids:
            target_parts = self._split_fio(target_fio)
            target_surname, target_first, target_patronymic = target_parts
            
            if len(target_parts) < 3:  # В целевом ФИО должно быть минимум 3 слова
                continue
            
            target_surname_norm = self._normalize_for_comparison(target_surname)
            
            # Фамилии должны совпадать или быть очень похожими (опечатки).
            if not self._surname_similar(source_surname_norm, target_surname_norm):
                continue
            
            target_first_norm = self._normalize_for_comparison(target_first)

            # Проверяем имя:
            # - для инициалов: первая буква,
            # - для полного имени: совпадение полного токена.
            source_first_token = source_first_norm.replace(" ", "")
            source_first_is_initial = len(source_first_token) == 1
            if source_first_is_initial:
                if not target_first_norm or target_first_norm[0] != source_first_token[0]:
                    continue
            else:
                if target_first_norm != source_first_norm:
                    continue
            
            # Если есть инициал отчества в исходном ФИО, проверяем его
            if source_patronymic_norm:
                target_patronymic_norm = self._normalize_for_comparison(target_patronymic)
                source_patronymic_token = source_patronymic_norm.replace(" ", "")
                source_patronymic_is_initial = len(source_patronymic_token) == 1
                if source_patronymic_is_initial:
                    if not target_patronymic_norm or target_patronymic_norm[0] != source_patronymic_token[0]:
                        continue
                else:
                    if target_patronymic_norm != source_patronymic_norm:
                        continue
            
            # Все условия выполнены
            return target_id
        
        return None
    
    def _match_by_fuzzy(self, source_fio: str) -> Optional[int]:
        """
        Стратегия 3: Fuzzy matching для обработки опечаток и вариаций.
        
        Args:
            source_fio: Исходное ФИО для сопоставления
            
        Returns:
            id_fio найденного ФИО или None
        """
        if not fuzz or not process:
            return None
        
        source_normalized = self._normalize_for_comparison(source_fio)
        
        # Используем fuzzy matching для поиска наиболее похожего ФИО
        try:
            # Создаем список нормализованных ФИО для поиска
            normalized_fios = [self._normalize_for_comparison(fio) for fio, _ in self.target_fios_with_ids]
            
            if HAS_RAPIDFUZZ:
                # rapidfuzz использует другой API
                best_match = process.extractOne(
                    source_normalized,
                    normalized_fios,
                    scorer=fuzz.ratio
                )
            else:
                # fuzzywuzzy/thefuzz
                best_match = process.extractOne(
                    source_normalized,
                    normalized_fios
                )
            
            if best_match and best_match[1] >= self.fuzzy_threshold:
                # Находим id_fio по нормализованному ФИО
                matched_normalized = best_match[0]
                for target_fio, target_id in self.target_fios_with_ids:
                    if self._normalize_for_comparison(target_fio) == matched_normalized:
                        return target_id
        except Exception:
            # Если fuzzy matching не работает, возвращаем None
            pass
        
        return None
    
    def match(self, source_fio: str) -> Optional[int]:
        """
        Сопоставляет исходное ФИО с целевыми ФИО, используя различные стратегии.
        
        Стратегии применяются в следующем порядке:
        1. Частичное совпадение
        2. Сопоставление по фамилии и инициалам
        3. Fuzzy matching (для опечаток и вариаций)
        
        Args:
            source_fio: Исходное ФИО для сопоставления
            
        Returns:
            id_fio найденного ФИО или None, если совпадение не найдено
        """
        if not source_fio or not source_fio.strip():
            return None
        
        source_fio = source_fio.strip()
        
        # Стратегия 1: Частичное совпадение
        match = self._match_by_partial(source_fio)
        if match:
            return match
        
        # Стратегия 2: Сопоставление по фамилии и инициалам
        match = self._match_by_surname_and_initials(source_fio)
        if match:
            return match
        
        # Стратегия 3: Fuzzy matching
        match = self._match_by_fuzzy(source_fio)
        if match:
            return match
        
        return None


def create_fio_matcher_from_db(conn, table_name: str = "ФИО_свар", column_name: str = "ФИО", id_column: str = "id_fio", fuzzy_threshold: int = 85) -> FIOMatcher:
    """
    Создает FIOMatcher из данных базы данных.
    
    Args:
        conn: Подключение к базе данных
        table_name: Имя таблицы с целевыми ФИО
        column_name: Имя столбца с ФИО
        id_column: Имя столбца с id_fio
        fuzzy_threshold: Порог схожести для fuzzy matching (0-100, по умолчанию 85)
        
    Returns:
        Экземпляр FIOMatcher
    """
    cursor = conn.cursor()
    cursor.execute(f'SELECT "{column_name}", "{id_column}" FROM "{table_name}" WHERE "{column_name}" IS NOT NULL AND TRIM("{column_name}") != ""')
    target_fios_with_ids = [(row[0], row[1]) for row in cursor.fetchall()]
    return FIOMatcher(target_fios_with_ids, fuzzy_threshold=fuzzy_threshold)


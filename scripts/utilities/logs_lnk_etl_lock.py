# -*- coding: utf-8 -*-
"""Межпроцессная блокировка полной загрузки logs_lnk (Windows/Linux).

Два одновременных запуска load_lnk_data / полного обновления журнала приводят к
перемешиванию stdout, «Всего записей: 0» между DROP и INSERT чужого процесса и
раздвоению таблицы. Каталог-замок создаётся атомарно через os.mkdir.
"""

from __future__ import annotations

import os
import time


class LogsLnkEtlLock:
    def __init__(self, db_path: str, wait_sec: float = 7200.0, poll_sec: float = 0.3):
        self.path = os.path.abspath(db_path) + ".logs_lnk_etl.dir_lock"
        self.wait_sec = float(wait_sec)
        self.poll_sec = float(poll_sec)
        self._held = False

    def acquire(self) -> None:
        deadline = time.time() + self.wait_sec
        while True:
            try:
                os.mkdir(self.path)
                self._held = True
                return
            except FileExistsError:
                if time.time() > deadline:
                    raise TimeoutError(
                        "Уже выполняется загрузка logs_lnk (другой процесс или зависший замок). "
                        f"Каталог-замок: {self.path}\n"
                        "Дождитесь окончания или удалите этот каталог вручную, если предыдущий запуск упал."
                    )
                time.sleep(self.poll_sec)
            except OSError as e:
                if time.time() > deadline:
                    raise TimeoutError(f"Не удалось создать замок {self.path}: {e}") from e
                time.sleep(self.poll_sec)

    def release(self) -> None:
        if not self._held:
            return
        try:
            os.rmdir(self.path)
        except OSError:
            pass
        self._held = False

    def __enter__(self) -> LogsLnkEtlLock:
        self.acquire()
        return self

    def __exit__(self, *args) -> None:
        self.release()

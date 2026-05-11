# script_M_Kran

Точки запуска (этап 1 очистки корня):

- `launch.bat` — основной запуск приложения.
- `stop_servers.bat` — остановка запущенных сервисов.

Дополнительные CLI-скрипты перенесены в `scripts/cli/`:

- `scripts/cli/backup_manager.bat`
- `scripts/cli/launch_visible.bat`
- `scripts/cli/launch_vue.bat`
- `scripts/cli/restore_interactive.bat`

Вспомогательные Python-скрипты перенесены из корня в `scripts/tools/`:

- `scripts/tools/check_syntax.py`
- `scripts/tools/temp_query.py`
- `scripts/tools/copy_table_to_correct_db.py`
- `scripts/tools/restore_database.py`
- `scripts/tools/load_naks_to_db.py`

Прямые root-скрипты `restore_database.py` и `load_naks_to_db.py` удалены: используйте версии из `scripts/tools/`.
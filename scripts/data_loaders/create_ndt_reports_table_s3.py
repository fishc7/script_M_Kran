#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Сканирование заключений НК в S3-совместимом хранилище (Selectel и др.)
и заполнение таблицы folder_NDT_Report (как create_ndt_reports_table.py, но источник — бакет).

Переменные окружения (или аргументы CLI):
  S3_ENDPOINT_URL — URL API (Selectel: часто региональный, напр. https://s3.ru-3.storage.selcloud.ru;
    точный URL — в панели бакета «Подключение»)
  S3_BUCKET — имя бакета
  AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY — ключи (стандарт boto3)

Опционально:
  S3_PREFIX — префикс ключей, по умолчанию «НК/Заключения/» (пустая строка = весь бакет)
  AWS_SESSION_TOKEN — если используете временные ключи

При запуске скрипта из корня проекта подхватывается файл .env (python-dotenv),
если пакет установлен. Уже заданные в системе переменные не перезаписываются.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
utilities_dir = os.path.join(project_root, "scripts", "utilities")
if utilities_dir not in sys.path:
    sys.path.insert(0, utilities_dir)
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)


def _load_dotenv() -> None:
    env_path = os.path.join(project_root, ".env")
    if not os.path.isfile(env_path):
        return
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv(env_path, override=False)


_load_dotenv()

try:
    from db_utils import get_database_path
except ImportError:
    from scripts.utilities.db_utils import get_database_path

try:
    from create_ndt_reports_table import (
        create_ndt_reports_table,
        extract_conclusion_number,
        update_ndt_reports_table,
    )
except ImportError:
    import importlib.util

    _spec = importlib.util.spec_from_file_location(
        "create_ndt_reports_table",
        os.path.join(current_dir, "create_ndt_reports_table.py"),
    )
    _mod = importlib.util.module_from_spec(_spec)
    assert _spec.loader is not None
    _spec.loader.exec_module(_mod)
    create_ndt_reports_table = _mod.create_ndt_reports_table
    extract_conclusion_number = _mod.extract_conclusion_number
    update_ndt_reports_table = _mod.update_ndt_reports_table

try:
    import boto3
    from botocore.client import BaseClient
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError as e:
    boto3 = None  # type: ignore
    BaseClient = Any  # type: ignore
    BotoCoreError = ClientError = Exception  # type: ignore
    _BOTO_IMPORT_ERROR = e
else:
    _BOTO_IMPORT_ERROR = None

SUPPORTED_EXTENSIONS = (".pdf", ".doc", ".docx", ".xls", ".xlsx", ".txt", ".rtf")


def setup_logging() -> logging.Logger:
    log_dir = os.path.join(project_root, "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_filename = os.path.join(
        log_dir,
        f"create_ndt_reports_table_s3_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
    )
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_filename, encoding="utf-8"),
            logging.StreamHandler(),
        ],
        force=True,
    )
    return logging.getLogger(__name__)


def _normalize_prefix(prefix: Optional[str]) -> str:
    if not prefix:
        return ""
    prefix = prefix.replace("\\", "/").strip("/")
    return prefix + "/" if prefix else ""


def _s3_uri(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"


def make_s3_client(
    endpoint_url: str,
    region_name: Optional[str] = None,
) -> BaseClient:
    if boto3 is None:
        raise RuntimeError(
            "Нужен пакет boto3: pip install boto3. "
            f"Импорт: {_BOTO_IMPORT_ERROR}"
        )
    session = boto3.session.Session()
    kwargs: Dict[str, Any] = {
        "service_name": "s3",
        "endpoint_url": endpoint_url.rstrip("/"),
    }
    if region_name:
        kwargs["region_name"] = region_name
    return session.client(**kwargs)


def list_ndt_objects_s3(
    client: BaseClient,
    bucket: str,
    prefix: str,
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    """Возвращает список словарей file_name, full_path (s3 URI), _Номер_заключений."""
    files_info: List[Dict[str, Any]] = []
    paginator = client.get_paginator("list_objects_v2")
    norm_prefix = _normalize_prefix(prefix) if prefix else ""

    try:
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=norm_prefix)
        for page in page_iterator:
            for obj in page.get("Contents") or []:
                key = obj.get("Key") or ""
                if not key or key.endswith("/"):
                    continue
                name = os.path.basename(key)
                ext = os.path.splitext(name)[1].lower()
                if ext not in SUPPORTED_EXTENSIONS:
                    continue
                files_info.append(
                    {
                        "file_name": name,
                        "full_path": _s3_uri(bucket, key),
                        "_Номер_заключений": extract_conclusion_number(name),
                    }
                )
    except (ClientError, BotoCoreError) as e:
        logger.error("Ошибка S3 API: %s", e)
        raise

    logger.info("Найдено %s объектов (поддерживаемые типы) в s3://%s/%s", len(files_info), bucket, norm_prefix)
    return files_info


def _env_config(args: argparse.Namespace) -> Dict[str, Optional[str]]:
    endpoint = args.endpoint or os.environ.get("S3_ENDPOINT_URL") or os.environ.get("AWS_ENDPOINT_URL")
    bucket = args.bucket or os.environ.get("S3_BUCKET") or os.environ.get("AWS_S3_BUCKET")
    prefix = args.prefix
    if prefix is None:
        prefix = os.environ.get("S3_PREFIX", "НК/Заключения/")
    region = args.region or os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("S3_REGION")
    return {"endpoint": endpoint, "bucket": bucket, "prefix": prefix, "region": region}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="folder_NDT_Report из S3 (Selectel и др.)")
    p.add_argument("--endpoint", help="S3 endpoint URL (иначе S3_ENDPOINT_URL)")
    p.add_argument("--bucket", help="Имя бакета (иначе S3_BUCKET)")
    p.add_argument(
        "--prefix",
        default=None,
        help="Префикс ключей (иначе S3_PREFIX; по умолчанию в коде: НК/Заключения/)",
    )
    p.add_argument("--region", help="Регион для подписи (опционально)")
    p.add_argument("--dry-run", action="store_true", help="Только листинг, без записи в БД")
    return p.parse_args()


def main() -> bool:
    logger = setup_logging()
    args = parse_args()
    cfg = _env_config(args)

    if not cfg["endpoint"] or not cfg["bucket"]:
        logger.error(
            "Задайте endpoint и bucket: переменные S3_ENDPOINT_URL и S3_BUCKET "
            "или аргументы --endpoint и --bucket"
        )
        return False

    try:
        client = make_s3_client(cfg["endpoint"], cfg["region"])
    except RuntimeError as e:
        logger.error("%s", e)
        return False

    try:
        files_info = list_ndt_objects_s3(
            client, cfg["bucket"], cfg["prefix"] or "", logger
        )
    except (ClientError, BotoCoreError):
        return False

    if not files_info:
        logger.warning(
            "Подходящих объектов не найдено — таблица folder_NDT_Report не меняется "
            "(как при пустой локальной папке в create_ndt_reports_table.py)"
        )

    if args.dry_run:
        for i, row in enumerate(files_info[:20]):
            logger.info("  %s | %s", row["file_name"], row["full_path"])
        if len(files_info) > 20:
            logger.info("  ... и ещё %s записей", len(files_info) - 20)
        return True

    if not files_info:
        return True

    db_path = get_database_path()
    if not db_path:
        logger.error("Не удалось определить путь к базе данных")
        return False

    if not create_ndt_reports_table(db_path):
        return False

    inserted = update_ndt_reports_table(db_path, files_info)
    logger.info("Готово: вставлено записей: %s", inserted)
    return True


def run_script() -> bool:
    """Точка входа для запуска из GUI, как у create_ndt_reports_table.run_script."""
    return main()


if __name__ == "__main__":
    ok = main()
    sys.exit(0 if ok else 1)

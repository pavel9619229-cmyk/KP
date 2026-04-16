#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Запускается локально (на ПК) по расписанию.
Тянет данные из 1C через staged pipeline, сохраняет снапшот и пушит в GitHub.

Запуск: python tools/refresh_seed.py
"""

import json
import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# Добавляем корень проекта в sys.path, чтобы импортировать api_proxy
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Импортируем staged pipeline из api_proxy
from api_proxy import (
    fetch_rows_from_odata,
    RUNTIME_DATA_FILE,
    RUNTIME_META_FILE,
    SEED_DATA_FILE,
)

REPO_ROOT = ROOT


def save_and_push(rows: list) -> None:
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    runtime_path = REPO_ROOT / RUNTIME_DATA_FILE
    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    with runtime_path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    meta_path = REPO_ROOT / RUNTIME_META_FILE
    with meta_path.open("w", encoding="utf-8") as f:
        json.dump({"updatedAt": now_str, "rowCount": len(rows)}, f, ensure_ascii=False, indent=2)

    seed_path = REPO_ROOT / SEED_DATA_FILE
    seed_path.parent.mkdir(parents=True, exist_ok=True)
    with seed_path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    print(f"[{now_str}] Сохранено {len(rows)} строк → {runtime_path.name}, {seed_path.name}")

    # git add + commit + push
    try:
        subprocess.run(
            ["git", "-C", str(REPO_ROOT), "add",
             RUNTIME_DATA_FILE, RUNTIME_META_FILE, SEED_DATA_FILE],
            check=True,
        )
        commit_msg = f"seed: refresh {len(rows)} rows at {now_str}"
        result = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "commit", "-m", commit_msg],
            capture_output=True, text=True,
        )
        if "nothing to commit" in result.stdout + result.stderr:
            print("git: данные не изменились, коммит не нужен")
        else:
            print(f"git commit: {result.stdout.strip()}")
            subprocess.run(["git", "-C", str(REPO_ROOT), "push"], check=True)
            print("git push: успешно")
    except subprocess.CalledProcessError as exc:
        print(f"git ошибка: {exc}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh KP snapshot from local 1C access")
    parser.add_argument(
        "--skip-heavy",
        action="store_true",
        help="Skip heavy stage6 (invoice/payment group enrichment)",
    )
    args = parser.parse_args()

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Запуск staged refresh...")
    rows = fetch_rows_from_odata(include_stage6=not args.skip_heavy)
    if not rows:
        print("Получено 0 строк — данные не сохранены, push не выполнен")
        sys.exit(1)
    print(f"Получено {len(rows)} строк")
    save_and_push(rows)


if __name__ == "__main__":
    main()

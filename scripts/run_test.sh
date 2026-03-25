#!/bin/bash
set -e

echo "=== Подготовка тестовых данных ==="
python3 scripts/prepare_test.py

echo "=== Создание симлинков ==="
python3 scripts/link_test_files.py

echo "=== Запуск транскрибации (Docker) ==="
cd "$(dirname "$0")/.."  # переходим в корень проекта
docker compose run --rm -v /tmp/whisperx_input:/input -v /tmp/whisperx_output:/output whisperx

echo "=== Обновление JSON ==="
python3 scripts/update_test_json.py

echo "=== Готово. Результат в /storage/data/test_answers.json ==="
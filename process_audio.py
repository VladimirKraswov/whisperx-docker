#!/usr/bin/env python3
"""
Обработка первых 30 аудиофайлов из /storage/data/audio длительностью >5 сек,
транскрибация через WhisperX (Docker) и сохранение результата в output.json.
"""

import json
import os
import subprocess
import sys
from pathlib import Path
import wave

AUDIO_DIR = Path("/storage/data/audio")
OUTPUT_JSON = Path("/storage/data/output.json")
TEMP_INPUT_DIR = Path("/tmp/whisperx_input")
TEMP_OUTPUT_DIR = Path("/tmp/whisperx_output")
DURATION_THRESHOLD = 5.0   # секунды
MAX_FILES = 30

def check_ffprobe():
    """Проверяет доступность ffprobe."""
    try:
        subprocess.run(['ffprobe', '-version'], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False

def get_duration_ffprobe(filepath: Path) -> float:
    """Получает длительность через ffprobe."""
    result = subprocess.run(
        ['ffprobe', '-v', 'error', '-show_entries', 'format=duration',
         '-of', 'default=noprint_wrappers=1:nokey=1', str(filepath)],
        capture_output=True, text=True, check=True
    )
    return float(result.stdout.strip())

def get_duration_wave(filepath: Path) -> float:
    """Пытается получить длительность через wave (только для настоящих WAV)."""
    try:
        with wave.open(str(filepath), 'rb') as wav:
            frames = wav.getnframes()
            rate = wav.getframerate()
            return frames / rate
    except Exception:
        return 0.0

def get_duration(filepath: Path) -> float:
    """Обёртка: сначала ffprobe, если не работает, пробует wave."""
    if check_ffprobe():
        try:
            return get_duration_ffprobe(filepath)
        except (subprocess.CalledProcessError, ValueError):
            # если ffprobe не смог, пробуем wave
            return get_duration_wave(filepath)
    else:
        # если ffprobe нет, просто используем wave
        return get_duration_wave(filepath)

def find_audio_files():
    """Находит все .wav файлы, фильтрует по длительности, возвращает первые MAX_FILES."""
    wav_files = sorted(AUDIO_DIR.glob("*.wav"))  # сортировка по имени
    valid = []
    ffprobe_available = check_ffprobe()
    if not ffprobe_available:
        print("Внимание: ffprobe не найден. Фильтрация по длительности отключена.")
    for f in wav_files:
        if ffprobe_available:
            duration = get_duration(f)
            if duration > DURATION_THRESHOLD:
                valid.append(f)
        else:
            valid.append(f)  # берём все файлы
        if len(valid) >= MAX_FILES:
            break
    return valid

def extract_user_id(filename: Path) -> int:
    parts = filename.stem.split('_')
    if parts and parts[0].isdigit():
        return int(parts[0])
    return 0

def build_initial_json(files):
    records = []
    for f in files:
        size_kb = os.path.getsize(f) // 1024
        records.append({
            "user_id": extract_user_id(f),
            "question": "",
            "quiz_id": None,
            "size": size_kb,
            "file_name": str(f),
            "answer": ""
        })
    return records

def symlink_files(files, target_dir):
    target_dir.mkdir(parents=True, exist_ok=True)
    for f in files:
        link_path = target_dir / f.name
        if not link_path.exists():
            os.symlink(f, link_path)

def run_docker_transcription():
    cmd = [
        "docker", "compose", "run", "--rm",
        "-v", f"{TEMP_INPUT_DIR}:/input",
        "-v", f"{TEMP_OUTPUT_DIR}:/output",
        "whisperx"
    ]
    print("Запуск транскрибации в Docker...")
    subprocess.run(cmd, check=True)

def update_json_with_transcriptions(records):
    for rec in records:
        base = Path(rec["file_name"]).stem
        txt_path = TEMP_OUTPUT_DIR / f"{base}.txt"
        if txt_path.exists():
            with open(txt_path, 'r', encoding='utf-8') as f:
                rec["answer"] = f.read().strip()
        else:
            print(f"Предупреждение: нет транскрипции для {base}")
    return records

def main():
    print("Шаг 1: Поиск аудиофайлов...")
    files = find_audio_files()
    print(f"Найдено {len(files)} файлов (берём первые {MAX_FILES})")

    if not files:
        print("Нет подходящих файлов. Выход.")
        return

    print("Шаг 2: Формирование начального JSON...")
    records = build_initial_json(files)

    # Сохраняем промежуточный JSON (опционально)
    with open("/tmp/input_files.json", 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print("Шаг 3: Создание симлинков во входную папку Docker...")
    symlink_files(files, TEMP_INPUT_DIR)

    print("Шаг 4: Запуск транскрибации (может занять время)...")
    try:
        run_docker_transcription()
    except subprocess.CalledProcessError as e:
        print(f"Ошибка Docker: {e}")
        sys.exit(1)

    print("Шаг 5: Обновление JSON полученными текстами...")
    updated = update_json_with_transcriptions(records)

    print(f"Шаг 6: Сохранение итогового JSON в {OUTPUT_JSON}")
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(updated, f, ensure_ascii=False, indent=2)

    print("Готово.")

if __name__ == "__main__":
    main()
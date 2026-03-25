#!/usr/bin/env python3
import json
import os
import subprocess
import sys
import shutil
from pathlib import Path

AUDIO_DIR = Path("/storage/data/audio")
OUTPUT_JSON = Path("/storage/data/output.json")
TEMP_INPUT_DIR = Path("/tmp/whisperx_input")
TEMP_OUTPUT_DIR = Path("/tmp/whisperx_output")
DURATION_THRESHOLD = 5.0
MAX_FILES = 30
WHISPERX_IMAGE = "whisperx-docker:latest"

def check_ffprobe():
    try:
        subprocess.run(['ffprobe', '-version'], capture_output=True, check=True)
        return True
    except:
        return False

def get_duration_ffprobe(filepath):
    result = subprocess.run(
        ['ffprobe', '-v', 'error', '-show_entries', 'format=duration',
         '-of', 'default=noprint_wrappers=1:nokey=1', str(filepath)],
        capture_output=True, text=True, check=True
    )
    return float(result.stdout.strip())

def get_duration(filepath):
    if check_ffprobe():
        try:
            return get_duration_ffprobe(filepath)
        except:
            return 0.0
    else:
        print("Внимание: ffprobe не найден. Длительность не проверяется.")
        return float('inf')

def find_audio_files():
    wav_files = sorted(AUDIO_DIR.glob("*.wav"))
    valid = []
    for f in wav_files:
        duration = get_duration(f)
        if duration > DURATION_THRESHOLD:
            valid.append(f)
            if len(valid) >= MAX_FILES:
                break
    return valid

def extract_user_id(filename):
    parts = filename.stem.split('_')
    return int(parts[0]) if parts and parts[0].isdigit() else 0

def symlink_files(files, target_dir):
    if target_dir.exists():
        shutil.rmtree(target_dir)
    target_dir.mkdir(parents=True)
    for f in files:
        os.symlink(f, target_dir / f.name)

def run_docker_transcription():
    print("Запуск транскрибации через Docker (это может занять время)...")
    cmd = [
        "docker", "run", "--rm",
        "-v", f"{TEMP_INPUT_DIR}:/input",
        "-v", f"{TEMP_OUTPUT_DIR}:/output",
        WHISPERX_IMAGE,
        "--input_dir", "/input",
        "--output_dir", "/output"
    ]
    result = subprocess.run(cmd, capture_output=False)
    if result.returncode != 0:
        print("Ошибка при выполнении Docker. Смотрите вывод выше.")
        sys.exit(1)

def build_json_from_transcriptions():
    """Создает JSON на основе файлов .txt в TEMP_OUTPUT_DIR."""
    records = []
    for txt_file in TEMP_OUTPUT_DIR.glob("*.txt"):
        base = txt_file.stem  # имя без расширения
        # Ищем соответствующий .wav в исходной папке или во временной папке
        wav_path = AUDIO_DIR / f"{base}.wav"
        if not wav_path.exists():
            # может быть в TEMP_INPUT_DIR (симлинк)
            wav_path = TEMP_INPUT_DIR / f"{base}.wav"
            if not wav_path.exists():
                print(f"Предупреждение: не найден исходный аудиофайл для {base}")
                continue
        size_kb = os.path.getsize(wav_path) // 1024
        with open(txt_file, 'r', encoding='utf-8') as f:
            answer = f.read().strip()
        records.append({
            "user_id": extract_user_id(wav_path),
            "question": "",
            "quiz_id": None,
            "size": size_kb,
            "file_name": str(wav_path),
            "answer": answer
        })
    return records

def main():
    print("Шаг 1: Поиск аудиофайлов длительностью >5 секунд...")
    files = find_audio_files()
    print(f"Найдено {len(files)} файлов (берём первые {MAX_FILES})")
    if not files:
        print("Нет подходящих файлов. Выход.")
        return

    print("Шаг 2: Создание симлинков во входную папку Docker...")
    symlink_files(files, TEMP_INPUT_DIR)

    print("Шаг 3: Запуск транскрибации...")
    run_docker_transcription()

    print("Шаг 4: Формирование JSON из транскрипций...")
    records = build_json_from_transcriptions()
    if not records:
        print("Не удалось сформировать записи. Выход.")
        return

    print(f"Шаг 5: Сохранение в {OUTPUT_JSON}")
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"Готово. Обработано {len(records)} файлов.")

if __name__ == "__main__":
    main()
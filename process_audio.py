#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Пакетная транскрибация аудиофайлов (.wav) длительностью >5 секунд с помощью WhisperX в Docker.
Результат сохраняется в JSON-файл с полями: user_id, question (из answers_full.json),
quiz_id (из answers_full.json), size (КБ), file_name, answer (транскрипция).
"""

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import List, Dict, Any, Optional

# ----------------------------- Конфигурация по умолчанию -----------------------------
DEFAULT_AUDIO_DIR = Path("/storage/data/audio")
DEFAULT_ANSWERS_JSON = Path("/storage/data/answers_full.json")
DEFAULT_OUTPUT_JSON = Path("/storage/data/output.json")
DEFAULT_DURATION_THRESHOLD = 5.0          # секунды
DEFAULT_MAX_FILES = 30
DEFAULT_WHISPERX_IMAGE = "whisperx-docker:latest"
DEFAULT_MODEL = os.getenv("WHISPERX_MODEL", "small")   # на CPU small работает быстрее
DEFAULT_LANGUAGE = "ru"                   # язык аудио (можно изменить)

# Настройка логирования (вывод в консоль и в файл)
LOG_FILE = Path("/tmp/whisperx_process.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)


<<<<<<< HEAD
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Orchestrator for WhisperX Docker transcription")
    parser.add_argument("--input-dir", default="./input", help="Directory with source audio files")
    parser.add_argument("--output-json", default="./output.json", help="Path to the final output JSON file")
<<<<<<< HEAD
    parser.add_argument("--answers-json", default="/storage/data/answers_full.json", help="Path to the source answers/metadata JSON")
    parser.add_argument("--model", default="large-v3", help="Whisper model to use")
    parser.add_argument("--language", default="ru", help="Language code (ru, en, auto, etc.)")
=======
    parser.add_argument("--answers-json", default="answers_full.json", help="Path to the source answers/metadata JSON")
    parser.add_argument("--model", default="large-v3", help="Whisper model to use")
>>>>>>> 0907c7c (Complete WhisperX service refactor and new orchestrator)
    parser.add_argument("--min-size", type=float, default=10, help="Minimum file size in KB to process")
    parser.add_argument("--max-files", type=int, default=30, help="Maximum number of files to process in one batch")
    parser.add_argument("--cache-dir", default="./whisperx-cache", help="Path to HuggingFace cache directory")
    parser.add_argument("--keep-temp", action="store_true", help="Keep temporary directories after processing")
    parser.add_argument("--device", choices=["cuda", "cpu", "auto"], default="auto", help="Device to use (default: auto)")
    return parser
=======
def parse_args() -> argparse.Namespace:
    """Разбор аргументов командной строки."""
    parser = argparse.ArgumentParser(
        description="Транскрибация аудиофайлов через WhisperX (Docker) и сохранение в JSON."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_AUDIO_DIR,
        help=f"Папка с исходными аудиофайлами .wav (по умолчанию: {DEFAULT_AUDIO_DIR})"
    )
    parser.add_argument(
        "--answers-json",
        type=Path,
        default=DEFAULT_ANSWERS_JSON,
        help=f"Исходный JSON с вопросами (по умолчанию: {DEFAULT_ANSWERS_JSON})"
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=DEFAULT_OUTPUT_JSON,
        help=f"Выходной JSON-файл (по умолчанию: {DEFAULT_OUTPUT_JSON})"
    )
    parser.add_argument(
        "--model",
        type=str,
        default=DEFAULT_MODEL,
        help=f"Модель Whisper (tiny, base, small, medium, large-v3); по умолчанию: {DEFAULT_MODEL}"
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=DEFAULT_MAX_FILES,
        help=f"Максимальное количество файлов для обработки (по умолчанию: {DEFAULT_MAX_FILES})"
    )
    parser.add_argument(
        "--min-duration",
        type=float,
        default=DEFAULT_DURATION_THRESHOLD,
        help=f"Минимальная длительность файла в секундах (по умолчанию: {DEFAULT_DURATION_THRESHOLD})"
    )
    parser.add_argument(
        "--language",
        type=str,
        default=DEFAULT_LANGUAGE,
        help=f"Язык аудио (по умолчанию: {DEFAULT_LANGUAGE})"
    )
    parser.add_argument(
        "--keep-temp",
        action="store_true",
        help="Не удалять временные папки после завершения (для отладки)"
    )
    parser.add_argument(
        "--docker-image",
        type=str,
        default=DEFAULT_WHISPERX_IMAGE,
        help=f"Имя Docker-образа (по умолчанию: {DEFAULT_WHISPERX_IMAGE})"
    )
    return parser.parse_args()
>>>>>>> 3c8de68 (fix)


def check_ffprobe() -> bool:
    """Проверка наличия ffprobe в системе."""
    try:
        subprocess.run(["ffprobe", "-version"], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        log.warning("ffprobe не найден. Фильтрация по длительности отключена.")
        return False


def get_duration_ffprobe(filepath: Path) -> float:
    """Получение длительности через ffprobe."""
    result = subprocess.run(
        [
            "ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", str(filepath)
        ],
        capture_output=True,
        text=True,
        check=True
    )
    return float(result.stdout.strip())


def get_duration(filepath: Path) -> float:
    """Возвращает длительность файла в секундах (0 при ошибке)."""
    if check_ffprobe():
        try:
            return get_duration_ffprobe(filepath)
        except Exception as e:
            log.error(f"Ошибка ffprobe для {filepath.name}: {e}")
            return 0.0
    # Если ffprobe нет, считаем все файлы подходящими
    return float("inf")


def find_audio_files(audio_dir: Path, min_duration: float, max_files: int) -> List[Path]:
    """Возвращает список файлов .wav длительностью > min_duration (первые max_files)."""
    wav_files = sorted(audio_dir.glob("*.wav"))
    log.info(f"Найдено {len(wav_files)} .wav файлов в {audio_dir}")

    valid = []
    for f in wav_files:
        duration = get_duration(f)
        if duration > min_duration:
            valid.append(f)
            log.debug(f"{f.name}: {duration:.2f} сек")
            if len(valid) >= max_files:
                break
    log.info(f"Отобрано {len(valid)} файлов (первые {max_files} длительностью > {min_duration} сек)")
    return valid


def load_answers_index(answers_json_path: Path) -> Dict[str, Dict[str, Any]]:
    """Загружает answers_full.json и строит индекс по file_name."""
    if not answers_json_path.exists():
        log.warning(f"Файл {answers_json_path} не найден. Вопросы будут пустыми.")
        return {}
    try:
        with open(answers_json_path, "r", encoding="utf-8") as f:
            records = json.load(f)
        index = {}
        for rec in records:
            file_name = rec.get("file_name")
            if file_name:
                index[file_name] = rec
        log.info(f"Загружено {len(records)} записей из {answers_json_path}, построен индекс.")
        return index
    except Exception as e:
        log.error(f"Ошибка загрузки {answers_json_path}: {e}")
        return {}

<<<<<<< HEAD
def extract_user_id(filename: str) -> int:
<<<<<<< HEAD
=======
    # Ожидается формат "ID_anything.wav"
>>>>>>> 0907c7c (Complete WhisperX service refactor and new orchestrator)
    parts = filename.split('_')
=======

def extract_user_id(filepath: Path) -> int:
    """Извлекает user_id из имени файла (первая часть до '_')."""
    parts = filepath.stem.split("_")
>>>>>>> 3c8de68 (fix)
    if parts and parts[0].isdigit():
        return int(parts[0])
    return 0


def copy_files_to_temp(files: List[Path], target_dir: Path) -> None:
    """Копирует файлы во временную папку."""
    if target_dir.exists():
        shutil.rmtree(target_dir)
    target_dir.mkdir(parents=True)
    for f in files:
        dest = target_dir / f.name
        shutil.copy2(f, dest)
    log.info(f"Скопировано {len(files)} файлов в {target_dir}")


def check_docker_image(image_name: str) -> bool:
    """Проверяет наличие Docker-образа локально."""
    try:
        result = subprocess.run(
            ["docker", "image", "inspect", image_name],
            capture_output=True,
            check=False
        )
        return result.returncode == 0
    except FileNotFoundError:
        log.error("Docker не установлен или недоступен в PATH.")
        return False


def run_docker_transcription(input_dir: Path, output_dir: Path, image: str, model: str, language: str) -> None:
    """Запускает контейнер WhisperX с транскрибацией."""
    log.info("Запуск Docker-контейнера для транскрибации...")
    cmd = [
        "docker", "run", "--rm",
        "-v", f"{input_dir}:/input",
        "-v", f"{output_dir}:/output",
        "-e", f"WHISPERX_MODEL={model}",
        "-e", f"WHISPERX_LANGUAGE={language}",
        image,
        "--input_dir", "/input",
        "--output_dir", "/output"
    ]
    try:
        subprocess.run(cmd, check=True)
        log.info("Транскрибация завершена успешно.")
    except subprocess.CalledProcessError as e:
        log.error(f"Ошибка при выполнении Docker: {e}")
        sys.exit(1)


def build_json_from_transcriptions(output_dir: Path, audio_dir: Path, answers_index: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Создаёт список записей JSON на основе файлов .txt в output_dir, дополняя вопросами из индекса."""
    records = []
    for txt_file in output_dir.glob("*.txt"):
        if txt_file.stat().st_size == 0:
            log.warning(f"Пустой файл транскрипции: {txt_file.name}")
            continue
        base = txt_file.stem
        wav_path = audio_dir / f"{base}.wav"
        if not wav_path.exists():
            log.warning(f"Не найден исходный аудиофайл для {base}")
            continue

        # Получаем вопрос из индекса (по полному пути к файлу)
        answer_record = answers_index.get(str(wav_path), {})
        question_text = answer_record.get("question", "")
        quiz_id = answer_record.get("quiz_id", None)

        size_kb = wav_path.stat().st_size // 1024
        with open(txt_file, "r", encoding="utf-8") as f:
            answer = f.read().strip()
        records.append({
            "user_id": extract_user_id(wav_path),
            "question": question_text,
            "quiz_id": quiz_id,
            "size": size_kb,
            "file_name": str(wav_path),
            "answer": answer
        })
    log.info(f"Сформировано {len(records)} записей из {len(list(output_dir.glob('*.txt')))} .txt файлов.")
    return records


def main() -> None:
    args = parse_args()

    # 1. Проверка Docker
    if not check_docker_image(args.docker_image):
        log.error(f"Docker-образ {args.docker_image} не найден. Выполните `make first-run` для сборки.")
        sys.exit(1)

    # 2. Загрузка индекса вопросов
    answers_index = load_answers_index(args.answers_json)

    # 3. Поиск аудиофайлов
    audio_files = find_audio_files(args.input_dir, args.min_duration, args.max_files)
    if not audio_files:
        log.warning("Нет подходящих файлов. Выход.")
        return

<<<<<<< HEAD
    logger.info(f"Found {len(found_files)} files for processing")

<<<<<<< HEAD
    # 2. Загрузка метаданных из answers_full.json
    answers_index = {}
    if answers_json_path.exists():
        try:
            with open(answers_json_path, "r", encoding="utf-8") as f:
                answers_list = json.load(f)
            # Индексируем по полному пути к файлу
            for item in answers_list:
                fpath = item.get("file_name")
                if fpath:
                    answers_index[Path(fpath).resolve()] = item
            logger.info(f"Loaded {len(answers_index)} metadata records from {answers_json_path}")
        except Exception as e:
            logger.warning(f"Could not read answers JSON: {e}")
    else:
        logger.warning(f"Answers JSON not found at {answers_json_path}. Questions and quiz_id will be empty.")

    # 3. Создание временных папок
=======
    # 2. Создание временных папок
>>>>>>> 0907c7c (Complete WhisperX service refactor and new orchestrator)
    temp_input_dir = tempfile.mkdtemp(prefix="whisperx_in_")
    temp_output_dir = tempfile.mkdtemp(prefix="whisperx_out_")
    logger.info(f"Created temporary directories: {temp_input_dir}, {temp_output_dir}")

    try:
<<<<<<< HEAD
        # 4. Копирование файлов
=======
        # 3. Копирование файлов
>>>>>>> 0907c7c (Complete WhisperX service refactor and new orchestrator)
        for f in found_files:
            logger.info(f"Copying {f.name} to temporary directory...")
            shutil.copy2(f, Path(temp_input_dir) / f.name)

<<<<<<< HEAD
        # 5. Запуск Docker
=======
        # 4. Запуск Docker
>>>>>>> 0907c7c (Complete WhisperX service refactor and new orchestrator)
        log_filename = f"whisperx_docker_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        logger.info(f"Starting Docker container. Logs will be saved to {log_filename}")
=======
    # 4. Создание временных папок
    temp_input = Path(tempfile.mkdtemp(prefix="whisperx_input_"))
    temp_output = Path(tempfile.mkdtemp(prefix="whisperx_output_"))
    log.info(f"Временные папки: input={temp_input}, output={temp_output}")

    try:
        # 5. Копирование файлов
        copy_files_to_temp(audio_files, temp_input)

        # 6. Запуск транскрибации
        run_docker_transcription(temp_input, temp_output, args.docker_image, args.model, args.language)
>>>>>>> 3c8de68 (fix)

        # 7. Формирование JSON с вопросами
        records = build_json_from_transcriptions(temp_output, args.input_dir, answers_index)
        if not records:
            log.warning("Не удалось получить транскрипции для файлов. Проверьте логи Docker.")
            return

<<<<<<< HEAD
        docker_cmd = [
            "docker", "run", "--rm"
        ]

        if use_gpu:
            docker_cmd.extend(["--gpus", "all"])
            logger.info("Using GPU for Docker container")
        else:
            logger.info("Using CPU for Docker container")

        docker_cmd.extend([
            "-v", f"{temp_input_dir}:/input",
            "-v", f"{temp_output_dir}:/output",
            "-v", f"{cache_dir}:/root/.cache/huggingface",
            DEFAULT_IMAGE,
            "--input_dir", "/input",
            "--output_dir", "/output",
<<<<<<< HEAD
            "--model", args.model,
            "--language", args.language
=======
            "--model", args.model
>>>>>>> 0907c7c (Complete WhisperX service refactor and new orchestrator)
        ])

        if args.device != "auto":
            docker_cmd.extend(["--device", args.device])

        with open(log_filename, "w", encoding="utf-8") as log_file:
            process = subprocess.Popen(
                docker_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True
            )
            for line in process.stdout:
                log_file.write(line)
                log_file.flush()
<<<<<<< HEAD
=======
                # Также выводим в консоль для прогресса
>>>>>>> 0907c7c (Complete WhisperX service refactor and new orchestrator)
                print(line.strip(), flush=True)

            process.wait()

        if process.returncode != 0:
            logger.error(f"Docker container exited with error code {process.returncode}")
<<<<<<< HEAD
            # Продолжаем, чтобы собрать хотя бы частичные результаты
        else:
            logger.info("Docker transcription completed successfully")

        # 6. Сбор результатов и объединение с метаданными
        final_records = []
        for txt_file in Path(temp_output_dir).glob("*.txt"):
            audio_name = txt_file.stem
            # Ищем оригинальный файл среди найденных
            orig_file = next((f for f in found_files if f.stem == audio_name), None)
            if not orig_file:
                logger.warning(f"Original file for {txt_file.name} not found, skipping.")
=======
            # Не выходим сразу, попробуем собрать то, что успели
        else:
            logger.info("Docker transcription completed successfully")

        # 5. Сбор результатов и объединение с метаданными
        answers_data = {}
        if answers_json_path.exists():
            try:
                # Читаем только если нужно, и эффективно.
                # С 450к записями это может занять память, поэтому выводим лог.
                logger.info(f"Loading metadata from {answers_json_path}...")
                with open(answers_json_path, "r", encoding="utf-8") as af:
                    answers_list = json.load(af)
                    if isinstance(answers_list, list):
                        logger.info(f"Loaded {len(answers_list)} metadata records")
                        # Индексируем по имени файла для быстрого поиска
                        for item in answers_list:
                            fname = item.get("file_name")
                            if fname:
                                answers_data[Path(fname).name] = item.get("question", "")
            except Exception as e:
                logger.warning(f"Could not read answers JSON: {e}")

        final_records = []
        for txt_file in Path(temp_output_dir).glob("*.txt"):
            audio_name = txt_file.stem
            # Ищем оригинальный путь файла среди найденных
            orig_file = next((f for f in found_files if f.stem == audio_name), None)
            if not orig_file:
>>>>>>> 0907c7c (Complete WhisperX service refactor and new orchestrator)
                continue

            with open(txt_file, "r", encoding="utf-8") as f:
                answer_text = f.read().strip()

<<<<<<< HEAD
            # Поиск метаданных по полному пути
            meta = answers_index.get(orig_file.resolve(), {})
            question = meta.get("question", "")
            quiz_id = meta.get("quiz_id", None)

            final_records.append({
                "user_id": extract_user_id(orig_file.name),
                "question": question,
                "quiz_id": quiz_id,
=======
            final_records.append({
                "user_id": extract_user_id(orig_file.name),
                "question": answers_data.get(orig_file.name, ""),
                "quiz_id": None,
>>>>>>> 0907c7c (Complete WhisperX service refactor and new orchestrator)
                "size": int(orig_file.stat().st_size / 1024),
                "file_name": str(orig_file),
                "answer": answer_text
            })

        if final_records:
            logger.info(f"Saving {len(final_records)} records to {output_json_path}")
            output_json_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_json_path, "w", encoding="utf-8") as f:
                json.dump(final_records, f, ensure_ascii=False, indent=2)
        else:
            logger.warning("No transcription results found to save.")
=======
        # 8. Сохранение JSON
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        with open(args.output_json, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
        log.info(f"Результат сохранён в {args.output_json}")
>>>>>>> 3c8de68 (fix)

    finally:
        # 9. Удаление временных папок (если не указано --keep-temp)
        if not args.keep_temp:
            shutil.rmtree(temp_input, ignore_errors=True)
            shutil.rmtree(temp_output, ignore_errors=True)
            log.debug("Временные папки удалены.")
        else:
            log.info(f"Временные папки оставлены: {temp_input}, {temp_output}")


if __name__ == "__main__":
<<<<<<< HEAD
    process_batch()
=======
    process_batch()
>>>>>>> 0907c7c (Complete WhisperX service refactor and new orchestrator)

#!/usr/bin/env python3
<<<<<<< HEAD
# -*- coding: utf-8 -*-

"""
Пакетная транскрибация аудиофайлов (.wav) с помощью WhisperX в Docker.
Используется модель large-v3 для максимального качества.
Фильтрация файлов по минимальному размеру (байты) – отсеиваются пустые или очень короткие записи.
Результат сохраняется в JSON с вопросами из answers_full.json.
"""

=======
>>>>>>> 0907c7c (Complete WhisperX service refactor and new orchestrator)
import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
<<<<<<< HEAD
from typing import List, Dict, Any

# ----------------------------- Конфигурация по умолчанию -----------------------------
DEFAULT_AUDIO_DIR = Path("/storage/data/audio")
DEFAULT_ANSWERS_JSON = Path("/storage/data/answers_full.json")
DEFAULT_OUTPUT_JSON = Path("/storage/data/output.json")
DEFAULT_MIN_SIZE_BYTES = 10000          # 10 КБ – минимальный размер файла для обработки
DEFAULT_MAX_FILES = 30
DEFAULT_WHISPERX_IMAGE = "whisperx-docker:latest"
DEFAULT_MODEL = "large-v3"              # максимальное качество
DEFAULT_LANGUAGE = "ru"                 # язык аудио

# Настройка логирования
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
        "--min-size",
        type=int,
        default=DEFAULT_MIN_SIZE_BYTES,
        help=f"Минимальный размер файла в байтах для обработки (по умолчанию: {DEFAULT_MIN_SIZE_BYTES})"
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


def find_audio_files(audio_dir: Path, min_size_bytes: int, max_files: int) -> List[Path]:
    """
    Возвращает список файлов .wav, размер которых >= min_size_bytes.
    Останавливается, как только набрано max_files файлов.
    """
    if not audio_dir.exists():
        log.error(f"Директория {audio_dir} не существует.")
        return []

    wav_files = sorted(audio_dir.glob("*.wav"))
    log.info(f"Найдено {len(wav_files)} .wav файлов в {audio_dir}")

    valid = []
    for f in wav_files:
        size = f.stat().st_size
        if size >= min_size_bytes:
            valid.append(f)
            log.debug(f"{f.name}: {size} байт")
            if len(valid) >= max_files:
                break
    log.info(f"Отобрано {len(valid)} файлов (первые {max_files} с размером >= {min_size_bytes} байт)")
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


def extract_user_id(filepath: Path) -> int:
    """Извлекает user_id из имени файла (первая часть до '_')."""
    parts = filepath.stem.split("_")
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
=======
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout
)
logger = logging.getLogger("whisperx-orchestrator")

DEFAULT_IMAGE = "whisperx-docker:latest"

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Orchestrator for WhisperX Docker transcription")
    parser.add_argument("--input-dir", default="./input", help="Directory with source audio files")
    parser.add_argument("--output-json", default="./output.json", help="Path to the final output JSON file")
    parser.add_argument("--answers-json", default="answers_full.json", help="Path to the source answers/metadata JSON")
    parser.add_argument("--model", default="large-v3", help="Whisper model to use")
    parser.add_argument("--min-size", type=float, default=10, help="Minimum file size in KB to process")
    parser.add_argument("--max-files", type=int, default=30, help="Maximum number of files to process in one batch")
    parser.add_argument("--cache-dir", default="./whisperx-cache", help="Path to HuggingFace cache directory")
    parser.add_argument("--keep-temp", action="store_true", help="Keep temporary directories after processing")
    parser.add_argument("--device", choices=["cuda", "cpu", "auto"], default="auto", help="Device to use (default: auto)")
    return parser

def is_nvidia_gpu_available() -> bool:
    try:
        subprocess.run(["nvidia-smi"], capture_output=True, check=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False

def check_docker_image(image_name: str) -> bool:
>>>>>>> 0907c7c (Complete WhisperX service refactor and new orchestrator)
    try:
        result = subprocess.run(
            ["docker", "image", "inspect", image_name],
            capture_output=True,
<<<<<<< HEAD
=======
            text=True,
>>>>>>> 0907c7c (Complete WhisperX service refactor and new orchestrator)
            check=False
        )
        return result.returncode == 0
    except FileNotFoundError:
<<<<<<< HEAD
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

    # 3. Поиск аудиофайлов по размеру
    audio_files = find_audio_files(args.input_dir, args.min_size, args.max_files)
    if not audio_files:
        log.warning("Нет подходящих файлов. Выход.")
        return

    # 4. Создание временных папок
    temp_input = Path(tempfile.mkdtemp(prefix="whisperx_input_"))
    temp_output = Path(tempfile.mkdtemp(prefix="whisperx_output_"))
    log.info(f"Временные папки: input={temp_input}, output={temp_output}")

    try:
        # 5. Копирование файлов
        copy_files_to_temp(audio_files, temp_input)

        # 6. Запуск транскрибации
        run_docker_transcription(temp_input, temp_output, args.docker_image, args.model, args.language)

        # 7. Формирование JSON с вопросами
        records = build_json_from_transcriptions(temp_output, args.input_dir, answers_index)
        if not records:
            log.warning("Не удалось получить транскрипции для файлов. Проверьте логи Docker.")
            return

        # 8. Сохранение JSON
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        with open(args.output_json, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
        log.info(f"Результат сохранён в {args.output_json}")

    finally:
        # 9. Удаление временных папок (если не указано --keep-temp)
        if not args.keep_temp:
            shutil.rmtree(temp_input, ignore_errors=True)
            shutil.rmtree(temp_output, ignore_errors=True)
            log.debug("Временные папки удалены.")
        else:
            log.info(f"Временные папки оставлены: {temp_input}, {temp_output}")


if __name__ == "__main__":
    main()
=======
        logger.error("Docker command not found. Is Docker installed?")
        return False

def extract_user_id(filename: str) -> int:
    # Ожидается формат "ID_anything.wav"
    parts = filename.split('_')
    if parts and parts[0].isdigit():
        return int(parts[0])
    return 0

def process_batch():
    parser = build_parser()
    args = parser.parse_args()

    input_dir = Path(args.input_dir).resolve()
    output_json_path = Path(args.output_json).resolve()
    answers_json_path = Path(args.answers_json).resolve()
    cache_dir = Path(args.cache_dir).resolve()
    cache_dir.mkdir(parents=True, exist_ok=True)

    if not input_dir.exists():
        logger.error(f"Input directory does not exist: {input_dir}")
        sys.exit(1)

    if not check_docker_image(DEFAULT_IMAGE):
        logger.error(f"Docker image {DEFAULT_IMAGE} not found. Please build it first.")
        sys.exit(1)

    # 1. Поиск файлов по размеру
    audio_extensions = {".mp3", ".wav", ".flac", ".m4a", ".ogg", ".opus"}
    found_files = []
    for item in sorted(input_dir.iterdir()):
        if item.is_file() and item.suffix.lower() in audio_extensions:
            size_kb = item.stat().st_size / 1024
            if size_kb >= args.min_size:
                found_files.append(item)
                if len(found_files) >= args.max_files:
                    break

    if not found_files:
        logger.info(f"No suitable audio files found in {input_dir} (min size {args.min_size} KB)")
        return

    logger.info(f"Found {len(found_files)} files for processing")

    # 2. Создание временных папок
    temp_input_dir = tempfile.mkdtemp(prefix="whisperx_in_")
    temp_output_dir = tempfile.mkdtemp(prefix="whisperx_out_")
    logger.info(f"Created temporary directories: {temp_input_dir}, {temp_output_dir}")

    try:
        # 3. Копирование файлов
        for f in found_files:
            logger.info(f"Copying {f.name} to temporary directory...")
            shutil.copy2(f, Path(temp_input_dir) / f.name)

        # 4. Запуск Docker
        log_filename = f"whisperx_docker_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        logger.info(f"Starting Docker container. Logs will be saved to {log_filename}")

        use_gpu = False
        if args.device == "cuda":
            use_gpu = True
        elif args.device == "auto":
            use_gpu = is_nvidia_gpu_available()

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
            "--model", args.model
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
                # Также выводим в консоль для прогресса
                print(line.strip(), flush=True)

            process.wait()

        if process.returncode != 0:
            logger.error(f"Docker container exited with error code {process.returncode}")
            # Не выходим сразу, попробуем собрать то, что успели
        else:
            logger.info("Docker transcription completed successfully")

        # 5. Сбор результатов и объединение с метаданными
        answers_data = {}
        if answers_json_path.exists():
            try:
                with open(answers_json_path, "r", encoding="utf-8") as af:
                    answers_list = json.load(af)
                    # Индексируем по file_name или как-то еще.
                    # Но в оригинале answers_full.json может иметь разную структуру.
                    # Предположим, это список объектов с file_name или мы сопоставляем по ID.
                    if isinstance(answers_list, list):
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
                continue

            with open(txt_file, "r", encoding="utf-8") as f:
                answer_text = f.read().strip()

            final_records.append({
                "user_id": extract_user_id(orig_file.name),
                "question": answers_data.get(orig_file.name, ""),
                "quiz_id": None,
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

    finally:
        if not args.keep_temp:
            logger.info("Cleaning up temporary directories...")
            shutil.rmtree(temp_input_dir, ignore_errors=True)
            shutil.rmtree(temp_output_dir, ignore_errors=True)
        else:
            logger.info(f"Keeping temporary directories: {temp_input_dir}, {temp_output_dir}")

if __name__ == "__main__":
    process_batch()
>>>>>>> 0907c7c (Complete WhisperX service refactor and new orchestrator)

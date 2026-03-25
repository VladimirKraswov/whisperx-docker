#!/usr/bin/env python3
import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from datetime import datetime

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
    parser.add_argument("--input-dir", default="/storage/data/audio", help="Directory with source audio files")
    parser.add_argument("--output-json", default="/storage/data/output.json", help="Path to the final output JSON file")
    parser.add_argument("--answers-json", default="/storage/data/answers_full.json", help="Path to the source answers/metadata JSON")
    parser.add_argument("--model", default="large-v3", help="Whisper model to use")
    parser.add_argument("--language", default="ru", help="Language code (ru, en, auto, etc.)")
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
    try:
        result = subprocess.run(
            ["docker", "image", "inspect", image_name],
            capture_output=True,
            text=True,
            check=False
        )
        return result.returncode == 0
    except FileNotFoundError:
        logger.error("Docker command not found. Is Docker installed?")
        return False

def extract_user_id(filename: str) -> int:
    parts = filename.split('_')
    if parts and parts[0].isdigit():
        return int(parts[0])
    return 0

def load_answers_index(answers_path: Path, base_dir: Path) -> dict:
    """
    Загружает answers_full.json и строит индекс для быстрого поиска.
    Ключи:
      - полный путь (resolve)
      - имя файла (если нет полного пути)
    """
    if not answers_path.exists():
        logger.warning(f"Answers JSON not found at {answers_path}")
        return {}
    try:
        with open(answers_path, "r", encoding="utf-8") as f:
            records = json.load(f)
        logger.info(f"Loaded {len(records)} records from {answers_path}")
        index = {}
        for rec in records:
            fpath = rec.get("file_name")
            if not fpath:
                continue
            # Попробуем получить абсолютный путь
            p = Path(fpath)
            if not p.is_absolute():
                # Если путь относительный, пробуем сделать его абсолютным относительно base_dir
                p = (base_dir / p).resolve()
            else:
                p = p.resolve()
            # Сохраняем по полному пути
            index[str(p)] = {
                "question": rec.get("question", ""),
                "quiz_id": rec.get("quiz_id", None)
            }
            # Также сохраняем по имени файла для случая, если полный путь не совпадёт
            index[p.name] = {
                "question": rec.get("question", ""),
                "quiz_id": rec.get("quiz_id", None)
            }
        logger.info(f"Built index with {len(index)} entries (including filename keys)")
        return index
    except Exception as e:
        logger.warning(f"Could not read answers JSON: {e}")
        return {}

def get_metadata(file_path: Path, index: dict) -> tuple:
    """
    Возвращает (question, quiz_id) для файла, пытаясь найти по полному пути,
    затем по имени файла.
    """
    abs_path = str(file_path.resolve())
    if abs_path in index:
        rec = index[abs_path]
        return rec["question"], rec["quiz_id"]
    if file_path.name in index:
        rec = index[file_path.name]
        return rec["question"], rec["quiz_id"]
    return "", None

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

    # 2. Загрузка индекса метаданных
    answers_index = load_answers_index(answers_json_path, input_dir)

    # 3. Создание временных папок
    temp_input_dir = tempfile.mkdtemp(prefix="whisperx_in_")
    temp_output_dir = tempfile.mkdtemp(prefix="whisperx_out_")
    logger.info(f"Created temporary directories: {temp_input_dir}, {temp_output_dir}")

    try:
        # 4. Копирование файлов
        for f in found_files:
            logger.info(f"Copying {f.name} to temporary directory...")
            shutil.copy2(f, Path(temp_input_dir) / f.name)

        # 5. Запуск Docker
        log_filename = f"whisperx_docker_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        logger.info(f"Starting Docker container. Logs will be saved to {log_filename}")

        use_gpu = False
        if args.device == "cuda":
            use_gpu = True
        elif args.device == "auto":
            use_gpu = is_nvidia_gpu_available()

        docker_cmd = ["docker", "run", "--rm"]
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
            "--model", args.model,
            "--language", args.language
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
                print(line.strip(), flush=True)

            process.wait()

        if process.returncode != 0:
            logger.error(f"Docker container exited with error code {process.returncode}")
        else:
            logger.info("Docker transcription completed successfully")

        # 6. Сбор результатов
        final_records = []
        for txt_file in Path(temp_output_dir).glob("*.txt"):
            audio_name = txt_file.stem
            # Ищем оригинальный файл среди найденных
            orig_file = next((f for f in found_files if f.stem == audio_name), None)
            if not orig_file:
                logger.warning(f"Original file for {txt_file.name} not found, skipping.")
                continue

            with open(txt_file, "r", encoding="utf-8") as f:
                answer_text = f.read().strip()

            question, quiz_id = get_metadata(orig_file, answers_index)

            final_records.append({
                "user_id": extract_user_id(orig_file.name),
                "question": question,
                "quiz_id": quiz_id,
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
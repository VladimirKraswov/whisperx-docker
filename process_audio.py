#!/usr/bin/env python3
import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
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
    parser.add_argument("--min-size", type=float, default=0, help="Minimum file size in KB to process (0 = no filter)")
    parser.add_argument("--batch-size", type=int, default=50, help="Number of files to process in one batch")
    parser.add_argument("--cache-dir", default="./whisperx-cache", help="Path to HuggingFace cache directory")
    parser.add_argument("--progress-file", default="processed_files.txt", help="File to store processed filenames (for resuming)")
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

def load_processed(progress_file: Path) -> set:
    """Загружает список уже обработанных имён файлов из progress_file."""
    if progress_file.exists():
        with open(progress_file, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f if line.strip())
    return set()

def save_processed(progress_file: Path, processed: set):
    """Сохраняет список обработанных имён файлов в progress_file."""
    with open(progress_file, "w", encoding="utf-8") as f:
        for name in sorted(processed):
            f.write(name + "\n")

def process_batch():
    parser = build_parser()
    args = parser.parse_args()

    input_dir = Path(args.input_dir).resolve()
    output_json_path = Path(args.output_json).resolve()
    answers_json_path = Path(args.answers_json).resolve()
    cache_dir = Path(args.cache_dir).resolve()
    progress_file = Path(args.progress_file).resolve()
    cache_dir.mkdir(parents=True, exist_ok=True)

    if not input_dir.exists():
        logger.error(f"Input directory does not exist: {input_dir}")
        sys.exit(1)

    if not check_docker_image(DEFAULT_IMAGE):
        logger.error(f"Docker image {DEFAULT_IMAGE} not found. Please build it first.")
        sys.exit(1)

    # 1. Загружаем индекс answers_full.json
    answers_index = {}
    if answers_json_path.exists():
        try:
            with open(answers_json_path, "r", encoding="utf-8") as f:
                answers_list = json.load(f)
            logger.info(f"Loaded {len(answers_list)} records from {answers_json_path}")
            for item in answers_list:
                fpath = item.get("file_name")
                if fpath:
                    resolved = Path(fpath).resolve()
                    answers_index[resolved] = {
                        "question": item.get("question", ""),
                        "quiz_id": item.get("quiz_id", None)
                    }
            logger.info(f"Indexed {len(answers_index)} unique file paths.")
        except Exception as e:
            logger.error(f"Failed to load answers JSON: {e}")
            sys.exit(1)
    else:
        logger.error(f"Answers JSON not found at {answers_json_path}. Cannot proceed.")
        sys.exit(1)

    # 2. Определяем все потенциальные файлы для обработки (те, что есть в индексе и подходят по размеру)
    audio_extensions = {".mp3", ".wav", ".flac", ".m4a", ".ogg", ".opus"}
    all_candidates = []
    for audio_path in sorted(answers_index.keys()):
        if not audio_path.exists():
            continue
        if audio_path.suffix.lower() not in audio_extensions:
            continue
        size_kb = audio_path.stat().st_size / 1024
        if size_kb >= args.min_size:
            all_candidates.append(audio_path)

    logger.info(f"Total candidates: {len(all_candidates)}")

    # 3. Загружаем уже обработанные файлы
    processed_names = load_processed(progress_file)
    # Отфильтровываем уже обработанные
    remaining = [f for f in all_candidates if f.name not in processed_names]
    logger.info(f"Already processed: {len(processed_names)} files, remaining: {len(remaining)}")

    if not remaining:
        logger.info("All files already processed. Nothing to do.")
        return

    # Берём первые batch_size
    batch = remaining[:args.batch_size]
    logger.info(f"Processing batch of {len(batch)} files (batch size = {args.batch_size})")

    # 4. Создаём временные папки
    temp_input_dir = tempfile.mkdtemp(prefix="whisperx_in_")
    temp_output_dir = tempfile.mkdtemp(prefix="whisperx_out_")
    logger.info(f"Created temporary directories: {temp_input_dir}, {temp_output_dir}")

    try:
        # 5. Копируем выбранные файлы во временную папку (вместо симлинков)
        for f in batch:
            dest = Path(temp_input_dir) / f.name
            shutil.copy2(f, dest)
            logger.debug(f"Copied {f.name} to {dest}")

        # 6. Запускаем Docker
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
            logger.info("Using GPU")
        else:
            logger.info("Using CPU")

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

        # Запускаем и ждём завершения, выводя логи в консоль и в файл
        with open(log_filename, "w", encoding="utf-8") as log_file:
            process = subprocess.Popen(
                docker_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )
            for line in process.stdout:
                log_file.write(line)
                log_file.flush()
                print(line.strip(), flush=True)   # выводим в консоль
            process.wait()

        if process.returncode != 0:
            logger.error(f"Docker exited with error code {process.returncode}")
            # Продолжаем, чтобы забрать то, что успели
        else:
            logger.info("Docker transcription completed successfully")

        # 7. Собираем результаты из выходной папки и обновляем JSON
        # Загружаем существующие записи
        existing_records = []
        if output_json_path.exists():
            try:
                with open(output_json_path, "r", encoding="utf-8") as f:
                    existing_records = json.load(f)
            except Exception:
                pass
        saved_names = {rec["file_name"] for rec in existing_records}

        new_records = []
        for txt_file in Path(temp_output_dir).glob("*.txt"):
            audio_name = txt_file.stem
            orig_file = next((f for f in batch if f.stem == audio_name), None)
            if not orig_file:
                continue
            if str(orig_file) in saved_names:
                logger.debug(f"Record for {orig_file.name} already exists, skipping.")
                continue

            with open(txt_file, "r", encoding="utf-8") as f:
                answer = f.read().strip()
            meta = answers_index.get(orig_file.resolve(), {})
            record = {
                "user_id": extract_user_id(orig_file.name),
                "question": meta.get("question", ""),
                "quiz_id": meta.get("quiz_id", None),
                "size": int(orig_file.stat().st_size / 1024),
                "file_name": str(orig_file),
                "answer": answer
            }
            new_records.append(record)
            saved_names.add(str(orig_file))

        if new_records:
            existing_records.extend(new_records)
            with open(output_json_path, "w", encoding="utf-8") as f:
                json.dump(existing_records, f, ensure_ascii=False, indent=2)
            logger.info(f"Added {len(new_records)} new records to {output_json_path}")
            # Обновляем progress file
            for f in batch:
                processed_names.add(f.name)
            save_processed(progress_file, processed_names)
            logger.info(f"Progress saved to {progress_file}")
        else:
            logger.warning("No new transcription results found.")

    finally:
        if not args.keep_temp:
            logger.info("Cleaning up temporary directories...")
            shutil.rmtree(temp_input_dir, ignore_errors=True)
            shutil.rmtree(temp_output_dir, ignore_errors=True)
        else:
            logger.info(f"Keeping temporary directories: {temp_input_dir}, {temp_output_dir}")

    # Сообщаем, сколько осталось
    remaining_after = len(remaining) - len(batch)
    logger.info(f"Processed {len(batch)} files. Remaining: {remaining_after}. Run again to continue.")

if __name__ == "__main__":
    process_batch()
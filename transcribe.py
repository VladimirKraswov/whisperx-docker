import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import requests
import torch
import whisperx
import yaml


DEFAULT_CONFIG_PATH = Path("/app/default_config.yaml")
DEFAULT_AUDIO_EXTENSIONS = [".mp3", ".wav", ".flac", ".m4a", ".ogg", ".opus"]


def str_to_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def load_yaml_from_path_or_url(path_or_url: str | None) -> dict[str, Any]:
    if not path_or_url:
        return {}

    if path_or_url.startswith(("http://", "https://")):
        response = requests.get(path_or_url, timeout=30)
        response.raise_for_status()
        data = yaml.safe_load(response.text)
        return data or {}

    path = Path(path_or_url)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path_or_url}")

    with path.open("r", encoding="utf-8") as file:
        data = yaml.safe_load(file)
    return data or {}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Batch audio transcription with WhisperX",
    )

    parser.add_argument("input_dir_pos", nargs="?", help="Input directory (positional compatibility mode)")
    parser.add_argument("output_dir_pos", nargs="?", help="Output directory (positional compatibility mode)")

    parser.add_argument("--config", help="Path or URL to YAML config")
    parser.add_argument("--input_dir", help="Directory with audio files")
    parser.add_argument("--output_dir", help="Directory to save outputs")
    parser.add_argument("--model", help="Whisper model name, e.g. large-v3")
    parser.add_argument("--language", help='Language code, e.g. "ru", "en", or "auto"')
    parser.add_argument("--device", help="cuda or cpu")
    parser.add_argument("--compute_type", help="float16, int8, int8_float16, etc.")
    parser.add_argument("--batch_size", type=int, help="Batch size for transcription")
    parser.add_argument("--audio_extensions", nargs="+", help="List of audio extensions")
    parser.add_argument("--align", action=argparse.BooleanOptionalAction, default=None, help="Enable word alignment")
    parser.add_argument("--diarize", action=argparse.BooleanOptionalAction, default=None, help="Enable speaker diarization")
    parser.add_argument("--output_format", choices=["txt", "json"], help="Output format")
    parser.add_argument("--save_segments", action=argparse.BooleanOptionalAction, default=None, help="Save segments JSON alongside TXT")
    parser.add_argument("--min_speakers", type=int, help="Minimum speakers for diarization")
    parser.add_argument("--max_speakers", type=int, help="Maximum speakers for diarization")
    parser.add_argument("--hf_token", help="Hugging Face token for diarization models")
    parser.add_argument("--download_root", help="Model download/cache directory")

    return parser


def load_env_overrides() -> dict[str, Any]:
    mapping = {
        "WHISPERX_INPUT_DIR": "input_dir",
        "WHISPERX_OUTPUT_DIR": "output_dir",
        "WHISPERX_MODEL": "model",
        "WHISPERX_LANGUAGE": "language",
        "WHISPERX_DEVICE": "device",
        "WHISPERX_COMPUTE_TYPE": "compute_type",
        "WHISPERX_BATCH_SIZE": "batch_size",
        "WHISPERX_ALIGN": "align",
        "WHISPERX_DIARIZE": "diarize",
        "WHISPERX_OUTPUT_FORMAT": "output_format",
        "WHISPERX_SAVE_SEGMENTS": "save_segments",
        "WHISPERX_AUDIO_EXTENSIONS": "audio_extensions",
        "WHISPERX_MIN_SPEAKERS": "min_speakers",
        "WHISPERX_MAX_SPEAKERS": "max_speakers",
        "WHISPERX_HF_TOKEN": "hf_token",
        "WHISPERX_DOWNLOAD_ROOT": "download_root",
        "HF_TOKEN": "hf_token",
        "HUGGINGFACE_TOKEN": "hf_token",
    }

    bool_keys = {"align", "diarize", "save_segments"}
    int_keys = {"batch_size", "min_speakers", "max_speakers"}

    result: dict[str, Any] = {}
    for env_name, config_key in mapping.items():
        value = os.getenv(env_name)
        if value is None or value == "":
            continue

        if config_key in bool_keys:
            result[config_key] = str_to_bool(value)
        elif config_key in int_keys:
            result[config_key] = int(value)
        elif config_key == "audio_extensions":
            result[config_key] = [item.strip() for item in value.split(",") if item.strip()]
        else:
            result[config_key] = value

    return result


def merge_config(
    default_cfg: dict[str, Any],
    file_cfg: dict[str, Any],
    env_cfg: dict[str, Any],
    cli_cfg: dict[str, Any],
) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for cfg in (default_cfg, file_cfg, env_cfg, cli_cfg):
        for key, value in cfg.items():
            if value is not None:
                merged[key] = value
    return merged


def normalize_config(config: dict[str, Any]) -> dict[str, Any]:
    if not config.get("audio_extensions"):
        config["audio_extensions"] = list(DEFAULT_AUDIO_EXTENSIONS)

    config["audio_extensions"] = [str(ext).lower() for ext in config["audio_extensions"]]

    if not config.get("input_dir"):
        raise ValueError("input_dir is required")
    if not config.get("output_dir"):
        raise ValueError("output_dir is required")

    if config.get("language", "").lower() == "auto":
        config["language"] = "auto"

    if config.get("device") == "cuda" and not torch.cuda.is_available():
        print("[!] CUDA requested but not available. Falling back to CPU.")
        config["device"] = "cpu"

    if config.get("device") == "cpu" and config.get("compute_type") in {"float16", "bfloat16"}:
        print("[!] compute_type=float16 is not suitable for CPU. Switching to int8.")
        config["compute_type"] = "int8"

    if config.get("diarize") and not config.get("hf_token"):
        raise ValueError("Diarization requires hf_token / WHISPERX_HF_TOKEN / HF_TOKEN")

    return config


def discover_audio_files(input_dir: Path, extensions: list[str]) -> list[Path]:
    found: list[Path] = []
    for item in sorted(input_dir.iterdir()):
        if not item.is_file():
            continue
        if item.suffix.lower() in extensions:
            found.append(item)
    return found


def save_txt(output_file: Path, result: dict[str, Any]) -> None:
    with output_file.open("w", encoding="utf-8") as file:
        for segment in result.get("segments", []):
            text = str(segment.get("text", "")).strip()
            if text:
                file.write(text + "\n")


def save_json(output_file: Path, result: dict[str, Any]) -> None:
    with output_file.open("w", encoding="utf-8") as file:
        json.dump(result, file, indent=2, ensure_ascii=False)


def save_segments_json(output_file: Path, result: dict[str, Any]) -> None:
    payload = {
        "language": result.get("language"),
        "segments": result.get("segments", []),
    }
    with output_file.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2, ensure_ascii=False)


def build_runtime_config(raw_args: list[str]) -> dict[str, Any]:
    parser = build_parser()
    args = parser.parse_args(raw_args)

    default_cfg = load_yaml_from_path_or_url(str(DEFAULT_CONFIG_PATH))
    file_cfg = load_yaml_from_path_or_url(args.config)
    env_cfg = load_env_overrides()

    cli_cfg = {
        key: value
        for key, value in vars(args).items()
        if key not in {"config", "input_dir_pos", "output_dir_pos"} and value is not None
    }

    if args.input_dir_pos and "input_dir" not in cli_cfg:
        cli_cfg["input_dir"] = args.input_dir_pos
    if args.output_dir_pos and "output_dir" not in cli_cfg:
        cli_cfg["output_dir"] = args.output_dir_pos

    merged = merge_config(default_cfg, file_cfg, env_cfg, cli_cfg)
    return normalize_config(merged)


def main() -> None:
    try:
        config = build_runtime_config(sys.argv[1:])
    except Exception as exc:
        print(f"[!] Configuration error: {exc}")
        sys.exit(2)

    input_dir = Path(config["input_dir"]).expanduser().resolve()
    output_dir = Path(config["output_dir"]).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    if not input_dir.exists():
        print(f"[!] Input directory does not exist: {input_dir}")
        sys.exit(2)

    audio_files = discover_audio_files(input_dir, config["audio_extensions"])
    if not audio_files:
        print(f"[!] No audio files found in {input_dir}")
        sys.exit(0)

    print("[*] Effective configuration:")
    print(json.dumps(
        {
            "input_dir": str(input_dir),
            "output_dir": str(output_dir),
            "model": config["model"],
            "language": config["language"],
            "device": config["device"],
            "compute_type": config["compute_type"],
            "batch_size": config["batch_size"],
            "align": config["align"],
            "diarize": config["diarize"],
            "output_format": config["output_format"],
            "save_segments": config["save_segments"],
            "audio_extensions": config["audio_extensions"],
            "download_root": config.get("download_root"),
        },
        ensure_ascii=False,
        indent=2,
    ))

    load_language = None if config["language"] == "auto" else config["language"]

    print(f"[*] Loading WhisperX model: {config['model']} on {config['device']} ({config['compute_type']})")
    model = whisperx.load_model(
        config["model"],
        config["device"],
        compute_type=config["compute_type"],
        language=load_language,
        download_root=config.get("download_root"),
    )

    align_cache: dict[str, tuple[Any, Any]] = {}
    diarize_model = None

    if config["diarize"]:
        from whisperx.diarize import DiarizationPipeline

        print("[*] Initializing diarization pipeline")
        diarize_model = DiarizationPipeline(
            token=config["hf_token"],
            device=config["device"],
        )

    processed = 0
    failed = 0

    for audio_path in audio_files:
        print(f"\n[*] Processing: {audio_path.name}")

        try:
            audio = whisperx.load_audio(str(audio_path))
            result = model.transcribe(
                audio,
                batch_size=int(config["batch_size"]),
                language=load_language,
            )

            if config["align"]:
                result_language = result.get("language")
                if not result_language:
                    raise RuntimeError("Alignment requested but transcription did not return language")

                if result_language not in align_cache:
                    print(f"[*] Loading align model for language: {result_language}")
                    align_cache[result_language] = whisperx.load_align_model(
                        language_code=result_language,
                        device=config["device"],
                    )

                model_a, metadata = align_cache[result_language]
                result = whisperx.align(
                    result["segments"],
                    model_a,
                    metadata,
                    audio,
                    config["device"],
                    return_char_alignments=False,
                )

            if config["diarize"] and diarize_model is not None:
                diarize_kwargs: dict[str, Any] = {}
                if config.get("min_speakers") is not None:
                    diarize_kwargs["min_speakers"] = int(config["min_speakers"])
                if config.get("max_speakers") is not None:
                    diarize_kwargs["max_speakers"] = int(config["max_speakers"])

                diarize_segments = diarize_model(audio, **diarize_kwargs)
                result = whisperx.assign_word_speakers(diarize_segments, result)

            base_name = audio_path.stem
            output_format = config["output_format"]

            if output_format == "txt":
                output_file = output_dir / f"{base_name}.txt"
                save_txt(output_file, result)
                print(f"[*] Saved text: {output_file}")

                if config["save_segments"]:
                    segments_file = output_dir / f"{base_name}.segments.json"
                    save_segments_json(segments_file, result)
                    print(f"[*] Saved segments JSON: {segments_file}")

            elif output_format == "json":
                output_file = output_dir / f"{base_name}.json"
                save_json(output_file, result)
                print(f"[*] Saved JSON: {output_file}")

            processed += 1

        except Exception as exc:
            failed += 1
            print(f"[!] Error processing {audio_path.name}: {exc}")

    print(f"\n[*] Done. Processed: {processed}, failed: {failed}, total: {len(audio_files)}")


if __name__ == "__main__":
    main()
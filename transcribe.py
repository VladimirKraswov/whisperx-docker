import os
import argparse
import json
import whisperx
import torch
from pathlib import Path

def main():
    parser = argparse.ArgumentParser(description='Universal audio transcription tool using whisperX')
    parser.add_argument('--input_dir', required=True, help='Directory containing audio files')
    parser.add_argument('--output_dir', required=True, help='Directory to save transcriptions')
    parser.add_argument('--model', default='faster-whisper/large-v3', help='Whisper model ID')
    parser.add_argument('--language', default='ru', help='Language code (e.g., "ru", "en"). Use "auto" for detection.')
    parser.add_argument('--device', default='cuda' if torch.cuda.is_available() else 'cpu', help='Device (cuda/cpu)')
    parser.add_argument('--compute_type', default='float16', help='Compute type (float16, int8, etc.)')
    parser.add_argument('--batch_size', type=int, default=128, help='Batch size for transcription')
    parser.add_argument('--audio_extensions', nargs='+', default=['.mp3', '.wav', '.flac', '.m4a', '.ogg', '.opus'], help='Supported audio extensions')
    parser.add_argument('--align', action='store_true', help='Perform alignment to get word-level timestamps')
    parser.add_argument('--output_format', choices=['txt', 'json'], default='txt', help='Output format')
    parser.add_argument('--save_segments', action='store_true', help='Include segments in output (always true for JSON)')

    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    print(f"[*] Loading model: {args.model} on {args.device} ({args.compute_type})...")

    # If language is auto, we don't pass it to load_model if we want it to detect per file,
    # but whisperx.load_model normally needs it for some optimizations?
    # Actually, whisperx.load_model(..., language=...) is optional.
    load_language = None if args.language == 'auto' else args.language
    model = whisperx.load_model(args.model, args.device, compute_type=args.compute_type, language=load_language)

    # Discover audio files
    audio_files = []
    for ext in args.audio_extensions:
        audio_files.extend(list(Path(args.input_dir).glob(f"*{ext}")))
        audio_files.extend(list(Path(args.input_dir).glob(f"*{ext.upper()}")))

    if not audio_files:
        print(f"[!] No audio files found in {args.input_dir}")
        return

    print(f"[*] Found {len(audio_files)} audio files.")

    for audio_path in audio_files:
        print(f"\n[*] Processing: {audio_path.name}")
        try:
            # 1. Transcribe
            audio = whisperx.load_audio(str(audio_path))
            result = model.transcribe(audio, batch_size=args.batch_size, language=load_language)

            # 2. Align if requested
            if args.align:
                print(f"[*] Aligning: {audio_path.name}")
                model_a, metadata = whisperx.load_align_model(language_code=result["language"], device=args.device)
                result = whisperx.align(result["segments"], model_a, metadata, audio, args.device, return_char_alignments=False)

            # 3. Save result
            base_name = audio_path.stem
            if args.output_format == 'txt':
                output_file = os.path.join(args.output_dir, f"{base_name}.txt")
                with open(output_file, 'w', encoding='utf-8') as f:
                    for segment in result["segments"]:
                        f.write(segment["text"].strip() + "\n")
                print(f"[*] Saved text to: {output_file}")

            elif args.output_format == 'json':
                output_file = os.path.join(args.output_dir, f"{base_name}.json")
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(result, f, indent=2, ensure_ascii=False)
                print(f"[*] Saved JSON to: {output_file}")

        except Exception as e:
            print(f"[!] Error processing {audio_path.name}: {e}")

if __name__ == "__main__":
    main()

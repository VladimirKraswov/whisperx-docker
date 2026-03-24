import os
import argparse
import whisperx
import torch
from pathlib import Path

def main():
    parser = argparse.ArgumentParser(description='Транскрибация аудиофайлов из папки с помощью whisperX')
    parser.add_argument('input_dir', help='Каталог с аудиозаписями')
    parser.add_argument('output_dir', help='Каталог для сохранения текстов')
    parser.add_argument('--model', default='small', help='Модель whisper (tiny, base, small, medium, large-v2, large-v3) или faster-whisper/*')
    parser.add_argument('--device', default='cuda' if torch.cuda.is_available() else 'cpu', help='Устройство (cuda/cpu)')
    parser.add_argument('--batch_size', type=int, default=16, help='Размер пакета для выравнивания')
    parser.add_argument('--language', default='ru', help='Язык аудио (auto для автоопределения)')
    parser.add_argument('--compute_type', default='float16' if torch.cuda.is_available() else 'int8', help='Тип вычислений')
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    print(f"Загрузка модели {args.model} на устройство {args.device}...")
    model = whisperx.load_model(args.model, args.device, compute_type=args.compute_type, language=args.language)

    audio_extensions = ('.mp3', '.wav', '.flac', '.m4a', '.ogg', '.opus')
    audio_files = [f for f in os.listdir(args.input_dir) if f.lower().endswith(audio_extensions)]

    if not audio_files:
        print("В указанной папке не найдено аудиофайлов.")
        return

    for audio_file in audio_files:
        audio_path = os.path.join(args.input_dir, audio_file)
        print(f"\nОбработка: {audio_file}")

        result = model.transcribe(audio_path, batch_size=args.batch_size)

        base_name = Path(audio_file).stem
        output_file = os.path.join(args.output_dir, f"{base_name}.txt")

        with open(output_file, 'w', encoding='utf-8') as f:
            for segment in result["segments"]:
                f.write(segment["text"].strip() + "\n")
        print(f"Сохранён текст: {output_file}")

if __name__ == "__main__":
    main()
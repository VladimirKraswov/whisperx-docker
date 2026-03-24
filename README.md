docker run --rm --gpus all \
  -v /путь/к/аудио:/input \
  -v /путь/к/текстам:/output \
  -v whisperx-cache:/root/.cache/huggingface \
  whisperx-gpu /input /output --model faster-whisper/large-v3 --language ru --device cuda --batch_size 32
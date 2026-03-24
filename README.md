# Универсальный Docker-образ для транскрибации аудио с помощью whisperX

Этот инструмент позволяет легко транскрибировать аудиофайлы в каталоге, используя [whisperX](https://github.com/m-bain/whisperX). Оптимизирован для GPU (RTX 5090 и другие).

## Особенности
- **Гибкая конфигурация**: через файл (локальный или удалённый), переменные окружения или аргументы командной строки.
- **Высокая производительность**: использование `faster-whisper` и поддержка CUDA 12.
- **Поддержка различных форматов**: вывод в TXT или JSON.
- **Автоматизация**: запуск одной командой `docker run`.

## Быстрый старт (с дефолтными настройками)
По умолчанию используется модель `large-v3`, `batch_size: 128`, язык `ru`.

```bash
docker run --rm --gpus all \
  -v /путь/к/аудио:/input \
  -v /путь/к/текстам:/output \
  -v whisperx-cache:/root/.cache/huggingface \
  whisperx-transcriber \
  --input /input \
  --output /output
```

## Использование с конфигурационным файлом

### Локальный конфиг
Вы можете смонтировать свой `config.yaml` в контейнер:

```bash
docker run --rm --gpus all \
  -v /путь/к/аудио:/input \
  -v /путь/к/текстам:/output \
  -v /путь/к/my_config.yaml:/config.yaml \
  whisperx-transcriber \
  --config /config.yaml \
  --input /input \
  --output /output
```

### Удалённый конфиг (по URL)
```bash
docker run --rm --gpus all \
  -v /путь/к/аудио:/input \
  -v /путь/к/текстам:/output \
  whisperx-transcriber \
  --config https://example.com/my_config.yaml \
  --input /input \
  --output /output
```

## Настройка через переменные окружения
Вы можете переопределить любой параметр, используя префикс `WHISPERX_`:

```bash
docker run --rm --gpus all \
  -e WHISPERX_MODEL=medium \
  -e WHISPERX_BATCH_SIZE=32 \
  -v /путь/к/аудио:/input \
  -v /путь/к/текстам:/output \
  whisperx-transcriber \
  --input /input \
  --output /output
```

## Доступные параметры
| Параметр | Описание | Дефолт |
|----------|----------|---------|
| `model` | ID модели whisper | `faster-whisper/large-v3` |
| `language`| Язык (ru, en, auto) | `ru` |
| `device` | Устройство (cuda/cpu) | `cuda` |
| `batch_size` | Размер батча | `128` |
| `align` | Выполнять ли выравнивание | `false` |
| `output_format` | Формат вывода (txt/json) | `txt` |

## Приоритет параметров
Аргументы командной строки > Переменные окружения > Файл конфигурации > Дефолтные настройки.

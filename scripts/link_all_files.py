import json
import os

test_json = '/storage/data/answers_all.json'
input_dir = '/tmp/whisperx_input'

os.makedirs(input_dir, exist_ok=True)

with open(test_json, 'r') as f:
    data = json.load(f)

for rec in data:
    src = rec['file_name']
    if os.path.exists(src):
        dst = os.path.join(input_dir, os.path.basename(src))
        if not os.path.exists(dst):
            os.symlink(src, dst)
    else:
        print(f"Файл не найден: {src}")

print(f"Созданы ссылки для {len(data)} файлов")
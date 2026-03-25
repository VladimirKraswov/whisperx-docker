import json
import os

unknown_dir = '/storage/data/categorized_by_stack/unknown'
output_json = '/storage/data/answers_all.json'

files = []
for f in sorted(os.listdir(unknown_dir)):
    if not f.endswith('.wav'):
        continue
    # Извлекаем user_id из имени файла (первая часть до подчёркивания)
    parts = f.split('_')
    if len(parts) >= 2 and parts[0].isdigit():
        user_id = int(parts[0])
    else:
        user_id = None
    full_path = os.path.join(unknown_dir, f)
    size_kb = os.path.getsize(full_path) // 1024
    files.append({
        "user_id": user_id,
        "question": "",
        "quiz_id": None,
        "size": size_kb,
        "file_name": full_path,
        "answer": ""
    })

print(f"Найдено {len(files)} файлов")

with open(output_json, 'w', encoding='utf-8') as f:
    json.dump(files, f, ensure_ascii=False, indent=2)

print(f"Создано {len(files)} записей в {output_json}")
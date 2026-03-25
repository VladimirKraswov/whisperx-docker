import json
import os

unknown_dir = '/storage/data/categorized_by_stack/unknown'
audio_answers_json = '/storage/data/audio_answers.json'
output_json = '/storage/data/test_answers.json'

files = []
for f in sorted(os.listdir(unknown_dir)):
    if not f.endswith('.wav'):
        continue
    path = os.path.join(unknown_dir, f)
    if os.path.getsize(path) > 5000:
        files.append(f)
        if len(files) == 30:
            break

print(f"Найдено {len(files)} файлов")

with open(audio_answers_json, 'r', encoding='utf-8') as f:
    all_records = json.load(f)

index = {}
for rec in all_records:
    user = rec.get('user')
    h = rec.get('hash')
    if user and h:
        key = f"{user}_{h}"
        index[key] = rec

test_records = []
for f in files:
    base = os.path.splitext(f)[0]
    key = base
    if key in index:
        rec = index[key]
        test_records.append({
            "user_id": rec['user'],
            "question": rec.get('question_text', ''),
            "quiz_id": rec.get('quiz_id'),
            "size": os.path.getsize(os.path.join(unknown_dir, f)) // 1024,
            "file_name": os.path.join(unknown_dir, f),
            "answer": ""
        })
    else:
        print(f"Внимание: для файла {f} нет данных в audio_answers.json")

with open(output_json, 'w', encoding='utf-8') as f:
    json.dump(test_records, f, ensure_ascii=False, indent=2)

print(f"Создано {len(test_records)} записей в {output_json}")
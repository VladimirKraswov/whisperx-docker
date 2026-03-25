import json
import os

test_json = '/storage/data/test_answers.json'
output_dir = '/tmp/whisperx_output'

with open(test_json, 'r', encoding='utf-8') as f:
    data = json.load(f)

for rec in data:
    base = os.path.splitext(os.path.basename(rec['file_name']))[0]
    txt_path = os.path.join(output_dir, base + '.txt')
    if os.path.exists(txt_path):
        with open(txt_path, 'r', encoding='utf-8') as tf:
            rec['answer'] = tf.read().strip()
    else:
        print(f"Не найден txt для {base}")

with open(test_json, 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print("JSON обновлён")
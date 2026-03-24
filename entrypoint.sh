#!/bin/bash
set -e

DEFAULT_CONFIG="/app/default_config.yaml"
CONFIG_FILE_ARG=""

# Find --config argument manually to download it before merging
for i in "${!@}"; do
  if [[ "${!i}" == "--config" ]]; then
    j=$((i+1))
    CONFIG_FILE_ARG="${!j}"
    break
  fi
done

# If config is a URL, download it
if [[ "$CONFIG_FILE_ARG" =~ ^https?:// ]]; then
  echo "[*] Downloading config from $CONFIG_FILE_ARG..."
  curl -s -o /tmp/downloaded_config.yaml "$CONFIG_FILE_ARG"
  CONFIG_FILE_ARG="/tmp/downloaded_config.yaml"
fi

# Merge everything using Python and produce the final command string
# We pass CLI arguments to python script via sys.argv
FINAL_CMD=$(python3 - "$CONFIG_FILE_ARG" "$@" <<'EOF'
import yaml
import os
import sys
import argparse
import shlex

def load_yaml(path):
    if path and os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    return {}

config_path_arg = sys.argv[1]
cli_args_list = sys.argv[2:]

# 1. Defaults
config = load_yaml('/app/default_config.yaml')

# 2. Config File (if specified via --config)
if config_path_arg:
    file_config = load_yaml(config_path_arg)
    config.update(file_config)

# 3. Environment variables (WHISPERX_*)
env_mapping = {
    'WHISPERX_INPUT_DIR': 'input_dir',
    'WHISPERX_OUTPUT_DIR': 'output_dir',
    'WHISPERX_MODEL': 'model',
    'WHISPERX_LANGUAGE': 'language',
    'WHISPERX_DEVICE': 'device',
    'WHISPERX_COMPUTE_TYPE': 'compute_type',
    'WHISPERX_BATCH_SIZE': 'batch_size',
    'WHISPERX_ALIGN': 'align',
    'WHISPERX_OUTPUT_FORMAT': 'output_format',
    'WHISPERX_SAVE_SEGMENTS': 'save_segments',
    'WHISPERX_AUDIO_EXTENSIONS': 'audio_extensions'
}

for env_var, key in env_mapping.items():
    val = os.getenv(env_var)
    if val is not None:
        if val.lower() in ['true', 'yes', '1']:
            config[key] = True
        elif val.lower() in ['false', 'no', '0']:
            config[key] = False
        elif val.isdigit():
            config[key] = int(val)
        elif key == 'audio_extensions':
            config[key] = [ext.strip() for ext in val.split(',')]
        else:
            config[key] = val

# 4. CLI Arguments (passed via sys.argv[2:])
parser = argparse.ArgumentParser()
parser.add_argument('--input', dest='input_dir')
parser.add_argument('--output', dest='output_dir')
parser.add_argument('--model')
parser.add_argument('--language')
parser.add_argument('--device')
parser.add_argument('--compute_type')
parser.add_argument('--batch_size', type=int)
parser.add_argument('--align', action='store_true', default=None)
parser.add_argument('--output_format')
parser.add_argument('--save_segments', action='store_true', default=None)
parser.add_argument('--audio_extensions', nargs='+')
parser.add_argument('--config')

known_cli_args, _ = parser.parse_known_args(cli_args_list)
cli_dict = {k: v for k, v in vars(known_cli_args).items() if v is not None}

# Special handling for boolean flags to ensure they can be overridden
if known_cli_args.align is True: config['align'] = True
if known_cli_args.save_segments is True: config['save_segments'] = True

config.update({k: v for k, v in cli_dict.items() if k not in ['align', 'save_segments', 'config']})

# Build the final command
cmd = ['python3', '/app/transcribe.py']
if config.get('input_dir'): cmd.extend(['--input_dir', str(config['input_dir'])])
if config.get('output_dir'): cmd.extend(['--output_dir', str(config['output_dir'])])
if config.get('model'): cmd.extend(['--model', str(config['model'])])
if config.get('language'): cmd.extend(['--language', str(config['language'])])
if config.get('device'): cmd.extend(['--device', str(config['device'])])
if config.get('compute_type'): cmd.extend(['--compute_type', str(config['compute_type'])])
if config.get('batch_size'): cmd.extend(['--batch_size', str(config['batch_size'])])
if config.get('align'): cmd.append('--align')
if config.get('output_format'): cmd.extend(['--output_format', str(config['output_format'])])
if config.get('save_segments'): cmd.append('--save_segments')
if config.get('audio_extensions'):
    cmd.append('--audio_extensions')
    cmd.extend([str(ext) for ext in config['audio_extensions']])

print(shlex.join(cmd))
EOF
)

echo "[*] Executing: $FINAL_CMD"
eval exec "$FINAL_CMD"

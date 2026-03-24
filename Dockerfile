FROM nvidia/cuda:12.1-base-ubuntu22.04

RUN apt-get update && apt-get install -y \
    python3 python3-pip ffmpeg build-essential git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements-opt.txt .
RUN pip3 install --no-cache-dir -r requirements-opt.txt
COPY transcribe_folder.py .

ENTRYPOINT ["python3", "transcribe_folder.py"]
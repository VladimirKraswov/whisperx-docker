FROM whisperx-docker-ai-base:latest

WORKDIR /app
ENV PATH="/venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

COPY requirements.app.txt /tmp/requirements.app.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r /tmp/requirements.app.txt

COPY default_config.yaml /app/default_config.yaml
COPY entrypoint.sh /app/entrypoint.sh
COPY transcribe.py /app/transcribe.py

RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
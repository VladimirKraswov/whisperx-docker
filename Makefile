.PHONY: first-run base run down clean-images logs

first-run:
	docker compose build whisperx-base
	docker compose build whisperx

base:
	docker compose build whisperx-base

run:
	docker compose run --rm whisperx

down:
	docker compose down

logs:
	docker compose logs -f whisperx

clean-images:
	docker image rm -f whisperx-docker:latest whisperx-docker-ai-base:latest || true
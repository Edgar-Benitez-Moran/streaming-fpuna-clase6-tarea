.PHONY: install edit run check docker-run

install:
	uv sync

edit:
	uv run marimo edit notebook.py

run:
	uv run marimo run notebook.py

check:
	uv run ruff check notebook.py
	uv run marimo check --strict notebook.py

docker-run:
	docker compose up --build notebook

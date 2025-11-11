SHELL := powershell.exe
.ONESHELL:

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f bitcoind

lint:
	poetry run ruff check .

fmt:
	poetry run ruff format .

type:
	poetry run mypy src

test:
	poetry run pytest -q

ingest:
	poetry run onchain ingest catchup --max-blocks 2000

verify:
	poetry run onchain ingest verify --date 2020-05-11

docs:
	poetry run onchain-metrics docs
	poetry run mkdocs build --strict

setup:
	python3.12 -m venv .venv

install:
	pip install --upgrade pip &&\
	pip install -r requirements.txt

dbt-debug:
	set -a && . ./.env && set +a && cd dbt && dbt debug --profiles-dir .

dbt-run:
	set -a && . ./.env && set +a && cd dbt && dbt run --profiles-dir .

pipeline:
	python -m flows.pipeline


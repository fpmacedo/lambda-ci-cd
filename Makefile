

init:
	pip install poetry
	poetry install
	pre-commit install

test:
	poetry run pytest

ci-test:
	poetry run pytest
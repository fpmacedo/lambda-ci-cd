

init:
	pip install poetry
	poetry install

test:
	poetry run pytest

ci-test:
	poetry run pytest

ci-setup:
	pip install poetry
	poetry install
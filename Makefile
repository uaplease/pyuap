.PHONY: ruff

ruff:
	poetry run ruff format pyuap tests

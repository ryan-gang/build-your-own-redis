lint:
	python -m black app/
	python -m isort app/ --skip .history --skip venv
	python -m flake8 app/ || true
	python -m mypy app/ --explicit-package-bases || true
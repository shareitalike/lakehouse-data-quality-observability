.PHONY: install test lint format clean

install:
	pip install -r requirements.txt
	pip install pytest black isort flake8

test:
	pytest tests/ --junitxml=test-results.xml -v

format:
	black .
	isort .

lint:
	black --check .
	isort --check-only .
	flake8 . --max-line-length=100 --extend-ignore=E203

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache

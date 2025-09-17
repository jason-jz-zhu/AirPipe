.PHONY: help install install-dev test test-cov lint format clean run-example

help:
	@echo "Available commands:"
	@echo "  make install       Install AirPipe"
	@echo "  make install-dev   Install AirPipe with development dependencies"
	@echo "  make test          Run unit tests"
	@echo "  make test-cov      Run tests with coverage"
	@echo "  make lint          Run linting checks"
	@echo "  make format        Format code with black and isort"
	@echo "  make clean         Clean up generated files"
	@echo "  make run-example   Run the basic example pipeline"

install:
	pip install -r requirements.txt
	pip install -e .

install-dev:
	pip install -r requirements-dev.txt
	pip install -e .

test:
	python -m pytest tests/ -v

test-cov:
	python -m pytest tests/ --cov=airpipe --cov-report=html --cov-report=term

lint:
	flake8 airpipe/ --max-line-length=100 --ignore=E203,W503
	mypy airpipe/ --ignore-missing-imports

format:
	black airpipe/ tests/ examples/ --line-length=100
	isort airpipe/ tests/ examples/ --profile=black --line-length=100

clean:
	rm -rf build/ dist/ *.egg-info
	rm -rf .pytest_cache/ .coverage htmlcov/
	rm -rf artifacts/ output/ logs/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete

run-example:
	@mkdir -p output
	python examples/basic_pipeline.py
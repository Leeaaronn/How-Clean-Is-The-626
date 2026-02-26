.PHONY: ingest filter_626 stage core marts validate test

ingest:
	python -m src.ingest

filter_626:
	python -m src.filter_626

stage:
	python -m src.stage

core:
	python -m src.core

marts:
	python -m src.marts

validate:
	python -m src.validate

test:
	python -m pytest tests/test_quality.py -v

.PHONY: ingest filter_626 stage core

ingest:
	python -m src.ingest

filter_626:
	python -m src.filter_626

stage:
	python -m src.stage

core:
	python -m src.core

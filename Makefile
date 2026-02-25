.PHONY: ingest filter_626 stage

ingest:
	python -m src.ingest

filter_626:
	python -m src.filter_626

stage:
	python -m src.stage

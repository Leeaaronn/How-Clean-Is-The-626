.PHONY: ingest filter_626

ingest:
	python -m src.ingest

filter_626:
	python -m src.filter_626

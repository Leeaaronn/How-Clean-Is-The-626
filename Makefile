.PHONY: ingest filter_626 stage core marts geo_near_me validate test

ingest:
	python3 -m src.ingest

filter_626:
	python3 -m src.filter_626

stage:
	python3 -m src.stage

core:
	python3 -m src.core

marts:
	python3 -m src.marts

geo_near_me:
	python3 -m src.geo_near_me

validate:
	python3 -m src.validate

test:
	python3 -m pytest tests/test_quality.py -v
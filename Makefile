# Makefile for Storacha Spark Pipeline

.PHONY: help install test run clean docker-build docker-run

help:
	@echo "Commands:"
	@echo "  install        : install dependencies"
	@echo "  test           : run tests"
	@echo "  run            : run the pipeline"
	@echo "  clean          : remove temporary files"
	@echo "  docker-build   : build the docker image"
	@echo "  docker-run     : run the pipeline in a docker container"

install:
	pip install -r requirements.txt

test:
	pytest

run:
	python spark_storacha_pipeline.py input_data.txt

clean:
	rm -rf __pycache__ .pytest_cache spark_output

docker-build:
	docker build -t storacha-spark-pipeline .

docker-run:
	docker run --rm -v $(shell pwd)/input_data.txt:/app/input_data.txt --env-file .env storacha-spark-pipeline python spark_storacha_pipeline.py input_data.txt

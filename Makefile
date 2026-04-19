#################################################################################
# GLOBALS                                                                       #
#################################################################################

PROJECT_NAME = rating-movie
PYTHON_VERSION = 3.13
PYTHON_INTERPRETER = python

#################################################################################
# COMMANDS                                                                      #
#################################################################################


## Install Python dependencies
.PHONY: install
install:
	pip install -e .

## Delete all compiled Python files
.PHONY: clean
clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete

## Format source code with ruff
.PHONY: format
format:
	ruff format . | isort .

## Run tests
.PHONY: test
test:
	python -m pytest tests

## Run all
.PHONY: all
all: dataset train predict plot

.PHONY: dataset
dataset:
	python -m src/dataset.py

.PHONY: train
train:
	python -m src/modeling/train.py

.PHONY: predict
predict:
	python -m src/modeling/predict.py

.PHONY: plot
plot:
	python -m src/plot.py


## Set up Python interpreter environment
.PHONY: env
env:
	conda create --name $(PROJECT_NAME) python=$(PYTHON_VERSION) -y
	@echo ">>> conda env created. Activate with:\nconda activate $(PROJECT_NAME)"

# Activate the conda environment
.PHONY: activate
activate:
	conda init
	conda activate $(PROJECT_NAME)

#################################################################################
# Self Documenting Commands                                                     #
#################################################################################

.DEFAULT_GOAL := help

define PRINT_HELP_PYSCRIPT
import re, sys; \
lines = '\n'.join([line for line in sys.stdin]); \
matches = re.findall(r'\n## (.*)\n[\s\S]+?\n([a-zA-Z_-]+):', lines); \
print('Available rules:\n'); \
print('\n'.join(['{:25}{}'.format(*reversed(match)) for match in matches]))
endef
export PRINT_HELP_PYSCRIPT

help:
	@$(PYTHON_INTERPRETER) -c "${PRINT_HELP_PYSCRIPT}" < $(MAKEFILE_LIST)

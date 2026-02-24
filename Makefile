
# paths
SRC_DIR := src

export PYTHONPATH := $(SRC_DIR):$(PYTHONPATH)

run:
	python3 -m bodine.broker.main

subscribe:
	@echo "Running subscribe example..."
	python3 -m bodine.examples.subscribe

publish:
	@echo "Running produce example..."
	python3 -m bodine.examples.publish

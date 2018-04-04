test:
	SNUBA_SETTINGS=test py.test

install-dependencies:
	@pip install -q -r requirements.txt
	@python setup.py -q develop

.PHONY: test install-dependencies

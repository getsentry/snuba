UNAME := $(shell uname -s)

ifeq ($(UNAME),Darwin)
	librdkafka_cmd = install-librdkafka-homebrew
else
	librdkafka_cmd = install-librdkafka-src
endif

.PHONY: test install-dependencies install-dev-dependencies install-librdkafka install-librdkafka-homebrew install-librdkafka-src-

develop: install-dev-dependencies setup-git

setup-git:
	pre-commit install

test:
	SNUBA_SETTINGS=test py.test -vv

install-dependencies:
	pip install -e .

install-dev-dependencies:
	pip install -e ".[dev]"

install-librdkafka-homebrew:
	brew install librdkafka

install-librdkafka-src:
	mkdir tmp-build-librdkafka && \
	cd tmp-build-librdkafka && \
	curl -L https://github.com/edenhill/librdkafka/archive/v0.11.4.tar.gz -O && \
	tar xf v0.11.4.tar.gz && \
	cd librdkafka-0.11.4 && \
	./configure --prefix=/usr && \
	make && \
	sudo PREFIX=/usr make install && \
	cd .. && \
	cd .. && \
	rm -rf tmp-build-librdkafka

install-librdkafka: $(librdkafka_cmd)

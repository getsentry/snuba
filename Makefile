UNAME := $(shell uname -s)

ifeq ($(UNAME),Darwin)
	librdkafka_cmd = install-librdkafka-homebrew
else
	librdkafka_cmd = install-librdkafka-src
endif

test:
	SNUBA_SETTINGS=test py.test -vv

install-python-dependencies:
	$(eval PY_VERSION = $(shell python -c "import sys; print(sys.version_info[0])"))
	@pip install -q -r requirements-py$(PY_VERSION).txt
	@python setup.py -q develop

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

.PHONY: test install-python-dependencies install-librdkafka install-librdkafka-homebrew install-librdkafka-src-

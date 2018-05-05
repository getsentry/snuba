test:
	SNUBA_SETTINGS=test py.test

install-python-dependencies:
	@pip install -q -r requirements.txt
	@python setup.py -q develop

install-librdkafka:
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

.PHONY: test install-python-dependencies install-librdkafka

UNAME := $(shell uname -s)

ifeq ($(UNAME),Darwin)
	librdkafka_cmd = install-librdkafka-homebrew
else
	librdkafka_cmd = install-librdkafka-src
endif

.PHONY: test install-python-dependencies install-librdkafka install-librdkafka-homebrew install-librdkafka-src-

develop: install-python-dependencies setup-git

setup-git:
	pre-commit install

test:
	SNUBA_SETTINGS=test py.test -vv

install-python-dependencies:
	pip install -e .

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

define REDIS_CLUSTER_NODE1_CONF
daemonize yes
port 7000
cluster-node-timeout 5000
pidfile /tmp/redis_cluster_node1.pid
logfile /tmp/redis_cluster_node1.log
save ""
appendonly no
cluster-enabled yes
cluster-config-file /tmp/redis_cluster_node1.conf
endef

define REDIS_CLUSTER_NODE2_CONF
daemonize yes
port 7001
cluster-node-timeout 5000
pidfile /tmp/redis_cluster_node2.pid
logfile /tmp/redis_cluster_node2.log
save ""
appendonly no
cluster-enabled yes
cluster-config-file /tmp/redis_cluster_node2.conf
endef

define REDIS_CLUSTER_NODE3_CONF
daemonize yes
port 7002
cluster-node-timeout 5000
pidfile /tmp/redis_cluster_node3.pid
logfile /tmp/redis_cluster_node3.log
save ""
appendonly no
cluster-enabled yes
cluster-config-file /tmp/redis_cluster_node3.conf
endef

export REDIS_CLUSTER_NODE1_CONF
export REDIS_CLUSTER_NODE2_CONF
export REDIS_CLUSTER_NODE3_CONF

travis-start-redis-cluster:
	# Start all cluster nodes
	echo "$$REDIS_CLUSTER_NODE1_CONF" | redis-server -
	echo "$$REDIS_CLUSTER_NODE2_CONF" | redis-server -
	echo "$$REDIS_CLUSTER_NODE3_CONF" | redis-server -
	sleep 5
	# Join all nodes in the cluster
	echo yes | redis-cli --cluster create 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002
	sleep 5

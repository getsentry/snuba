# Single image used both as the Flex Template launcher and as the Dataflow
# Python SDK worker container (sdk_container_image), per
# https://cloud.google.com/dataflow/docs/guides/templates/configuring-flex-templates#use_custom_container_images
#
# Workers and the launcher VM run with --disable-public-ips, so nothing may be
# fetched from PyPI / Maven Central at runtime: Python deps, a JRE (needed at
# launch to expand the cross-language KafkaIO / BigQuery Storage Write
# transforms) and the Beam expansion-service jars are baked in here.
ARG BEAM_VERSION=2.75.0
ARG PY_VERSION=3.11

FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest AS launcher

FROM apache/beam_python${PY_VERSION}_sdk:${BEAM_VERSION}
ARG BEAM_VERSION

COPY --from=launcher /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Pre-stage the expansion-service jars exactly where
# apache_beam.utils.subprocess_server.JavaJarServer looks for them
# (~/.apache_beam/cache/jars/<basename of the Maven URL>).
RUN set -eux; \
    cache=/root/.apache_beam/cache/jars; mkdir -p "$cache"; \
    base=https://repo.maven.apache.org/maven2/org/apache/beam; \
    for a in beam-sdks-java-io-expansion-service beam-sdks-java-io-google-cloud-platform-expansion-service; do \
      curl -fsSL -o "$cache/$a-${BEAM_VERSION}.jar" "$base/$a/${BEAM_VERSION}/$a-${BEAM_VERSION}.jar"; \
    done

WORKDIR /template
COPY requirements.txt pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir -r requirements.txt \
 && pip install --no-cache-dir --no-deps . \
 && pip check
COPY main.py ./

ENV FLEX_TEMPLATE_PYTHON_PY_FILE=/template/main.py
# Everything is already installed; stop the launcher from pip-installing/staging.
ENV PIP_NO_DEPS=True

# Worker (SDK harness) entrypoint. The Flex launcher overrides this with
# /opt/google/dataflow/python_template_launcher.
ENTRYPOINT ["/opt/apache/beam/boot"]

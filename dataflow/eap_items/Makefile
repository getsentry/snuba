# Build / run helpers for the EAP items Dataflow Flex Template (s4s2).
# Nothing here runs automatically; every GCP target needs valid gcloud auth.

PROJECT        ?= sentry-s4s2
REGION         ?= us-east1
# Artifact Registry repo does NOT exist yet (see README "Infra prerequisites").
AR_REPO        ?= dataflow
IMAGE_NAME     ?= eap-items-dataflow
TAG            ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)
IMAGE          ?= $(REGION)-docker.pkg.dev/$(PROJECT)/$(AR_REPO)/$(IMAGE_NAME):$(TAG)
BUCKET         ?= gs://sentry-dataflow-eap-s4s2
TEMPLATE_PATH  ?= $(BUCKET)/templates/eap-items-dataflow-$(TAG).json
SERVICE_ACCOUNT?= dataflow-eap@$(PROJECT).iam.gserviceaccount.com
SUBNETWORK     ?= regions/$(REGION)/subnetworks/sentry-default
JOB_NAME       ?= eap-items-bigquery
BOOTSTRAP      ?= kafka-spans-0.us-east1-b.c.sentry-s4s2.internal.:9092,kafka-spans-1.us-east1-c.c.sentry-s4s2.internal.:9092,kafka-spans-2.us-east1-d.c.sentry-s4s2.internal.:9092
SAMPLE_RATE    ?= 1.0
MAX_WORKERS    ?= 4
MACHINE_TYPE   ?= n2-standard-4

VENV ?= .venv
PY   := $(VENV)/bin/python

.PHONY: venv test image push template run cancel

venv:
	uv venv --python 3.11 $(VENV)
	uv pip install --python $(PY) -e '.[test]'

test:
	$(PY) -m pytest -q

image:
	docker build --platform linux/amd64 -t $(IMAGE) .

push: image
	docker push $(IMAGE)

template:
	gcloud dataflow flex-template build $(TEMPLATE_PATH) \
	  --project=$(PROJECT) \
	  --image=$(IMAGE) \
	  --sdk-language=PYTHON \
	  --metadata-file=metadata.json

run:
	gcloud dataflow flex-template run $(JOB_NAME) \
	  --project=$(PROJECT) \
	  --region=$(REGION) \
	  --template-file-gcs-location=$(TEMPLATE_PATH) \
	  --service-account-email=$(SERVICE_ACCOUNT) \
	  --subnetwork=$(SUBNETWORK) \
	  --disable-public-ips \
	  --staging-location=$(BUCKET)/staging \
	  --temp-location=$(BUCKET)/temp \
	  --worker-machine-type=$(MACHINE_TYPE) \
	  --max-workers=$(MAX_WORKERS) \
	  --enable-streaming-engine \
	  --additional-experiments=use_runner_v2 \
	  --parameters='^~^sdk_container_image=$(IMAGE)~sdk_location=container~bootstrap_servers=$(BOOTSTRAP)~sample_rate=$(SAMPLE_RATE)~dead_letter_path=$(BUCKET)/dead-letter/$(JOB_NAME)/'

cancel:
	gcloud dataflow jobs list --project=$(PROJECT) --region=$(REGION) --status=active \
	  --filter="name=$(JOB_NAME)" --format="value(id)" | \
	  xargs -r -n1 gcloud dataflow jobs drain --project=$(PROJECT) --region=$(REGION)

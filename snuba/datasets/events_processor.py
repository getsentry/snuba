from typing import Any, Mapping, MutableMapping

import logging
import _strptime  # NOQA fixes _strptime deferred import issue

from snuba.clickhouse.columns import ColumnSet
from snuba.consumers.types import KafkaMessageMetadata
from snuba.datasets.events_format import extract_http, extract_user
from snuba.datasets.events_processor_base import EventsProcessorBase, InsertEvent
from snuba.processor import (
    _boolify,
    _floatify,
    _hashify,
    _unicodify,
)


logger = logging.getLogger("snuba.processor")


class EventsProcessor(EventsProcessorBase):
    def __init__(self, promoted_tag_columns: ColumnSet):
        self._promoted_tag_columns = promoted_tag_columns

    def extract_promoted_tags(
        self, output: MutableMapping[str, Any], tags: Mapping[str, Any],
    ) -> None:
        output.update(
            {
                col.name: _unicodify(tags.get(col.name, None))
                for col in self._promoted_tag_columns
            }
        )

    def _should_process(self, event: InsertEvent) -> bool:
        return True

    def _extract_event_id(
        self, output: MutableMapping[str, Any], event: InsertEvent,
    ) -> None:
        output["event_id"] = event["event_id"]

    def extract_custom(
        self,
        output: MutableMapping[str, Any],
        event: InsertEvent,
        metadata: KafkaMessageMetadata,
    ) -> None:
        data = event.get("data", {})

        output["message"] = _unicodify(event["message"])

        # USER REQUEST GEO
        user = data.get("user", data.get("sentry.interfaces.User", None)) or {}
        extract_user(output, user)

        geo = user.get("geo", None) or {}
        self.extract_geo(output, geo)

        request = data.get("request", data.get("sentry.interfaces.Http", None)) or {}
        http_data: MutableMapping[str, Any] = {}
        extract_http(http_data, request)
        output["http_method"] = http_data["http_method"]
        output["http_referer"] = http_data["http_referer"]

        output["primary_hash"] = _hashify(event["primary_hash"])
        output["hierarchical_hashes"] = list(
            _hashify(x) for x in data.get("hierarchical_hashes") or ()
        )

        output["culprit"] = _unicodify(data.get("culprit", None))
        output["type"] = _unicodify(data.get("type", None))
        output["title"] = _unicodify(data.get("title", None))

    def extract_tags_custom(
        self,
        output: MutableMapping[str, Any],
        event: InsertEvent,
        tags: Mapping[str, Any],
        metadata: KafkaMessageMetadata,
    ) -> None:
        pass

    def extract_promoted_contexts(
        self,
        output: MutableMapping[str, Any],
        contexts: Mapping[str, Any],
        tags: Mapping[str, Any],
    ) -> None:
        app_ctx = contexts.get("app", None) or {}
        output["app_device"] = _unicodify(tags.get("app.device", None))
        app_ctx.pop("device_app_hash", None)  # tag=app.device

        os_ctx = contexts.get("os", None) or {}
        output["os"] = _unicodify(tags.get("os", None))
        output["os_name"] = _unicodify(tags.get("os.name", None))
        os_ctx.pop("name", None)  # tag=os and/or os.name
        os_ctx.pop("version", None)  # tag=os
        output["os_rooted"] = _boolify(tags.get("os.rooted", None))
        os_ctx.pop("rooted", None)  # tag=os.rooted
        output["os_build"] = _unicodify(os_ctx.pop("build", None))
        output["os_kernel_version"] = _unicodify(os_ctx.pop("kernel_version", None))

        runtime_ctx = contexts.get("runtime", None) or {}
        output["runtime"] = _unicodify(tags.get("runtime", None))
        output["runtime_name"] = _unicodify(tags.get("runtime.name", None))
        runtime_ctx.pop("name", None)  # tag=runtime and/or runtime.name
        runtime_ctx.pop("version", None)  # tag=runtime

        browser_ctx = contexts.get("browser", None) or {}
        output["browser"] = _unicodify(tags.get("browser", None))
        output["browser_name"] = _unicodify(tags.get("browser.name", None))
        browser_ctx.pop("name", None)  # tag=browser and/or browser.name
        browser_ctx.pop("version", None)  # tag=browser

        device_ctx = contexts.get("device", None) or {}
        output["device"] = _unicodify(tags.get("device", None))
        device_ctx.pop("model", None)  # tag=device
        output["device_family"] = _unicodify(tags.get("device.family", None))
        device_ctx.pop("family", None)  # tag=device.family
        output["device_name"] = _unicodify(device_ctx.pop("name", None))
        output["device_brand"] = _unicodify(device_ctx.pop("brand", None))
        output["device_locale"] = _unicodify(device_ctx.pop("locale", None))
        output["device_uuid"] = _unicodify(device_ctx.pop("uuid", None))
        output["device_model_id"] = _unicodify(device_ctx.pop("model_id", None))
        output["device_arch"] = _unicodify(device_ctx.pop("arch", None))
        output["device_battery_level"] = _floatify(
            device_ctx.pop("battery_level", None)
        )
        output["device_orientation"] = _unicodify(device_ctx.pop("orientation", None))
        output["device_simulator"] = _boolify(device_ctx.pop("simulator", None))
        output["device_online"] = _boolify(device_ctx.pop("online", None))
        output["device_charging"] = _boolify(device_ctx.pop("charging", None))

    def extract_geo(
        self, output: MutableMapping[str, Any], geo: Mapping[str, Any]
    ) -> None:
        output["geo_country_code"] = _unicodify(geo.get("country_code", None))
        output["geo_region"] = _unicodify(geo.get("region", None))
        output["geo_city"] = _unicodify(geo.get("city", None))

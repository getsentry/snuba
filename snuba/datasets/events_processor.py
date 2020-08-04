from typing import Any, Mapping, MutableMapping, Optional

import logging
import _strptime  # NOQA fixes _strptime deferred import issue

from snuba.clickhouse.columns import ColumnSet
from snuba.consumer import KafkaMessageMetadata
from snuba.datasets.events_format import extract_user
from snuba.datasets.events_processor_base import EventsProcessorBase, Event
from snuba.processor import (
    _as_dict_safe,
    _boolify,
    _floatify,
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

    def _should_process(self, event: Event) -> bool:
        return True

    def _extract_event_id(
        self, output: MutableMapping[str, Any], event: Event,
    ) -> None:
        output["event_id"] = event["data"]["event_id"]

    def extract_custom(
        self,
        output: MutableMapping[str, Any],
        event: Event,
        metadata: Optional[KafkaMessageMetadata] = None,
    ) -> None:
        data = event.get("data", {})
        # The following concerns the change to message/search_message
        # There are 2 Scenarios:
        #   Pre-rename:
        #        - Payload contains:
        #             "message": "a long search message"
        #        - Does NOT contain a `search_message` property
        #        - "message" value saved in `message` column
        #        - `search_message` column nonexistent or Null
        #   Post-rename:
        #        - Payload contains:
        #             "search_message": "a long search message"
        #        - Optionally the payload's "data" dict (event body) contains:
        #             "message": "short message"
        #        - "search_message" value stored in `search_message` column
        #        - "message" value stored in `message` column
        #
        output["search_message"] = _unicodify(event.get("search_message", None))
        if output["search_message"] is None:
            # Pre-rename scenario, we expect to find "message" at the top level
            output["message"] = _unicodify(event["message"])
        else:
            # Post-rename scenario, we check in case we have the optional
            # "message" in the event body.
            output["message"] = _unicodify(data.get("message", None))  # type: ignore

        # USER REQUEST GEO
        user = (
            data.get(
                "user", data.get("sentry.interfaces.User", None)  # type: ignore
            )
            or {}
        )
        extract_user(output, user)

        geo = user.get("geo", None) or {}
        self.extract_geo(output, geo)

        http = (
            data.get(
                "request", data.get("sentry.interfaces.Http", None)  # type: ignore
            )
            or {}
        )  # types: ignore
        self.extract_http(output, http)

    def extract_tags_custom(
        self,
        output: MutableMapping[str, Any],
        event: Event,
        tags: Mapping[str, Any],
        metadata: Optional[KafkaMessageMetadata] = None,
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

    def extract_contexts_custom(
        self,
        output: MutableMapping[str, Any],
        event: Event,
        tags: Mapping[str, Any],
        metadata: Optional[KafkaMessageMetadata] = None,
    ) -> None:
        pass

    def extract_geo(self, output, geo):
        output["geo_country_code"] = _unicodify(geo.get("country_code", None))
        output["geo_region"] = _unicodify(geo.get("region", None))
        output["geo_city"] = _unicodify(geo.get("city", None))

    def extract_http(self, output, http):
        output["http_method"] = _unicodify(http.get("method", None))
        http_headers = _as_dict_safe(http.get("headers", None))
        output["http_referer"] = _unicodify(http_headers.get("Referer", None))

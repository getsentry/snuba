import json
import os
from typing import Any, MutableMapping

from snuba import settings


def write_settings_to_json() -> str:
    """
    Write settings to json file, return the file path.
    """
    settings_json = get_settings_json()

    tmp_dir = os.path.join(settings.ROOT_REPO_PATH, "tmp")
    file_path = os.path.join(tmp_dir, "settings.json")

    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)

    with open(file_path, "w") as f:
        f.write(settings_json)

    return file_path


def get_settings_json() -> str:
    settings_vars = [
        setting for setting in dir(settings) if is_settings_variable(setting)
    ]

    settings_json: MutableMapping[str, Any] = {}

    for setting in settings_vars:
        settings_json[setting] = getattr(settings, setting)

    return json.dumps(settings_json, indent=4, cls=SetEncoder)


def is_settings_variable(name: str) -> bool:
    if name.startswith("_"):
        return False

    return name.upper() == name


class SetEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)

def _load_settings(obj=locals()):
    """Load settings from the path provided in the SNUBA_SETTINGS environment
    variable. Defaults to `./snuba/settings_base.py`. Users can provide a
    short name like `test` that will be expanded to `settings_test.py` in the
    main Snuba directory, or they can provide a full absolute path such as
    `/foo/bar/my_settings.py`."""

    import importlib
    import importlib.util
    import os

    settings = os.environ.get('SNUBA_SETTINGS', 'base')
    if not settings.startswith('/') and not settings.startswith('settings_'):
        settings = '.settings_%s' % settings
    elif not settings.endswith('.py'):
        settings += '.py'

    if settings.startswith('/'):
        # Code below is adapted from https://stackoverflow.com/a/41595552/90297S
        settings_spec = importlib.util.spec_from_file_location('snuba.settings.custom', settings)
        settings_module = importlib.util.module_from_spec(settings_spec)
        settings_spec.loader.exec_module(settings_module)
    else:
        settings_module = importlib.import_module(settings, 'snuba')

    for attr in dir(settings_module):
        if attr.isupper():
            obj[attr] = getattr(settings_module, attr)


_load_settings()

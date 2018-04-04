def _load_settings(obj=locals()):
    """Load settings from the path provided in the SNUBA_SETTINGS environment
    variable. Defaults to `./snuba/settings_base.py`. Users can provide a
    short name like `test` that will be expanded to `settings_test.py` in the
    main Snuba directory, or they can provide a full absolute path such as
    `/foo/bar/my_settings.py`."""

    import os
    import imp

    path = os.path.dirname(__file__)

    settings = os.environ.get('SNUBA_SETTINGS', 'base')
    if not settings.startswith('/') and not settings.startswith('settings_'):
        settings = 'settings_%s' % settings
    if not settings.endswith('.py'):
        settings += '.py'

    settings = os.path.join(path, settings)
    settings = imp.load_source('snuba.settings', settings)

    for attr in dir(settings):
        if attr.isupper():
            obj[attr] = getattr(settings, attr)


_load_settings()

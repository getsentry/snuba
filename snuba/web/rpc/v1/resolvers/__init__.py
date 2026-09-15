import os

from snuba.utils.registered_class import import_submodules_in_directory

_TO_IMPORT = {}

for f in os.listdir(os.path.dirname(os.path.realpath(__file__))):
    if f.startswith("R_"):
        _TO_IMPORT[f] = os.path.join(os.path.dirname(os.path.realpath(__file__)), f)


for v, module_path in _TO_IMPORT.items():
    import_submodules_in_directory(module_path, f"snuba.web.rpc.v1.resolvers.{v}")

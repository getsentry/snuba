import os

from snuba.utils.registered_class import import_submodules_in_directory

import_submodules_in_directory(os.path.dirname(os.path.realpath(__file__)), "snuba.replacers")

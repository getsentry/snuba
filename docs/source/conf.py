# Configuration file for the Sphinx documentation builder.
#

# -- Project information -----------------------------------------------------

project = "Snuba"
copyright = "2021, Sentry Team and Contributors"
author = "Sentry Team and Contributors"

release = "23.6.0"


# -- General configuration ---------------------------------------------------

extensions = ["sphinx.ext.githubpages", "sphinx.ext.intersphinx", "myst_parser"]

# This is relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
exclude_patterns = ["build"]

source_suffix = {
    ".rst": "restructuredtext",
    ".txt": "markdown",
    ".md": "markdown",
}

# -- Options for HTML output -------------------------------------------------

html_theme = "alabaster"

html_static_path = ["_static"]

html_logo = "_static/snuba.svg"

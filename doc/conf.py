# flake8: noqa

# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
from datetime import date
from rbc import __version__


# -- Project information -----------------------------------------------------

project = 'RBC'
copyright = f'2018-{date.today().year}, Quansight RBC Developers'
author = 'Xnd-Project Developers'

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The short X.Y version.
version = __version__.split('+')[0]
# The full version, including alpha/beta/rc tags.
release = __version__


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.

# append _ext so that one can use "numbaext"
sys.path.append(os.path.abspath("./_ext"))

extensions = [
    'sphinx.ext.intersphinx',
    'sphinx.ext.doctest',
    'sphinx.ext.extlinks',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.coverage',
    'sphinx.ext.napoleon',
    'sphinx.ext.autosectionlabel',
    'numbadoc',
]

# autosummary configuration
autosummary_generate = True
autodoc_typehints = "description"

html_copy_source = False

# Napoleon configurations
napoleon_google_docstring = False
napoleon_numpy_docstring = True

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'pydata_sphinx_theme'

# The Xnd logo
html_logo = "images/xndlogo.png"

html_theme_options = {
    "github_url": "https://github.com/xnd-project/rbc",
    "use_edit_page_button": True,
    "logo": {
        "image_light": html_logo,
        "image_dark": html_logo,
    },
    # https://github.com/pydata/pydata-sphinx-theme/issues/1220
    "icon_links": [],
}

html_context = {
    "github_user": "xnd-project",
    "github_repo": "rbc",
    "github_version": "main",
    "doc_path": "doc",
}

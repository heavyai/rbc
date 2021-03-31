import os
import sys
import docutils  # noqa: E402


sys.path.insert(0, os.path.abspath('../'))
from rbc import __version__

# -- General configuration ------------------------------------------------

# If your documentation needs a minimal Sphinx version, state it here.
#
# needs_sphinx = '1.0'

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.

extensions = [
    'sphinx.ext.intersphinx',
    'sphinx.ext.doctest',
    'sphinx.ext.extlinks',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.coverage',
    'sphinx.ext.napoleon',
    'sphinx_rtd_theme'
]

napoleon_google_docstring = False
napoleon_numpy_docstring = True

# If true, `todo` and `todoList` produce output, else they produce nothing.
todo_include_todos = False

autosummary_generate = True
autosummary_imported_members = False
autosummary_generate_overwrite = False

autodoc_default_options = {
    'inherited-members': None,
}

# The suffix(es) of source filenames.
# You can specify multiple suffix as a list of string:
#
# source_suffix = ['.rst', '.md']
source_suffix = '.rst'

# The master toctree document.
master_doc = 'index'

# General information about the project.
project = "rbc"
copyright = "2021, Xnd-Project"
author = "Xnd Developers"

# The version info for the project you're documenting, acts as replacement for
# |version| and |release|, also used in various other places throughout the
# built documents.
#
# The short X.Y version.
version = __version__.split('+')[0]
# The full version, including alpha/beta/rc tags.
release = __version__

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This patterns also effect to html_static_path and html_extra_path
exclude_patterns = ['doc', '_build']

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'


# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

primary_domain = 'py'
add_function_parentheses = False

# -- Options for HTML output ----------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_rtd_theme"

# Theme options are theme-specific and customize the look and feel of a theme
# further.  For a list of options available for each theme, see the
# documentation.
#
html_theme_options = {
    'canonical_url': 'https://xnd.io/',
    'analytics_id': '',
    'logo_only': True,
    'display_version': True,
    'prev_next_buttons_location': 'bottom',
    # Toc options
    'collapse_navigation': True,
    'sticky_navigation': True,
    'navigation_depth': 4,
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

html_context = {
    "display_github": False,
    # Add 'Edit on Github' link instead of 'View page source'
    "last_updated": True,
    "commit": False,
}

html_show_sourcelink = False

extlinks = {
    'issue': ('https://github.com/xnd-project/rbc/issues/%s', 'GH#'),
    'pr': ('https://github.com/xnd-project/rbc/pull/%s', 'GH#')
}

html_logo = "images/xndlogo.png"


def setup(app):
    app.add_crossref_type(
        'topic', 'topic', 'single: %s', docutils.nodes.strong
    )

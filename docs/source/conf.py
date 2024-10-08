# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
import sphinx_pdj_theme
import django_valkey


project = "django-valkey"
copyright = "2024, amirreza"
author = "amirreza"
release = django_valkey.__version__

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = []

templates_path = ["_templates"]
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output


html_theme = "sphinx_pdj_theme"
html_static_path = [sphinx_pdj_theme.get_html_theme_path()]

html_sidebars = {
    "**": ["globaltoc.html", "sourcelink.html", "searchbox.html", "relations.html"],
}

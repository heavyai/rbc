{{ fullname | escape | underline }}

.. rubric:: Description
.. automodule:: {{ fullname }}
.. currentmodule:: {{ fullname }}

{% if classes %}
.. rubric:: Classes
.. autosummary::
    :toctree:
    {% for class in classes %}
        {{ class }}
    {% endfor %}
{% endif %}

{% if fullname == "rbc.externals.libdevice" %}
.. rubric:: Functions
.. autosummary::
    :toctree:
    {% for function in members %}
        {% if "__nv" in function %}
            {{ function }}
        {% endif %}
    {% endfor %}
{% elif functions %}
.. rubric:: Functions
.. autosummary::
    :toctree:
    {% for function in functions %}
        {{ function }}
    {% endfor %}
{% endif %}

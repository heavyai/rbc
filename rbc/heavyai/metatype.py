"""Implements type meta
"""

__all__ = ['HeavyDBMetaType']


class HeavyDBMetaType(type):

    class_names = set()

    def __init__(cls, name, bases, dct):
        type(cls).class_names.add(name)

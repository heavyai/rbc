# SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

"""Implements type meta
"""

__all__ = ['HeavyDBMetaType']


class HeavyDBMetaType(type):

    class_names = set()

    def __init__(cls, name, bases, dct):
        type(cls).class_names.add(name)

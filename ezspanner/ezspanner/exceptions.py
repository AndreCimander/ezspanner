# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals


class ObjectDoesNotExist(Exception):
    pass


class ModelError(Exception):
    pass


class FieldError(ModelError):
    pass


class IndexError(ModelError):
    pass


# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals


class EzSpannerException(Exception):
    pass


class ObjectDoesNotExist(EzSpannerException):
    pass


class ModelError(EzSpannerException):
    pass


class QueryError(EzSpannerException):
    pass


class QueryJoinError(QueryError):
    pass


class FieldError(ModelError):
    pass


class SpannerIndexError(ModelError):
    pass


# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
import logging

from google.cloud.spanner import types


class SpannerField(object):
    type = None

    length_required = False

    def __init__(self, null=False, length=None, default=None):
        self.length = length
        if self.length_required and not self.length:
            raise ValueError("This field requires a length param!")

        self.null = null
        self.default = default

    def from_db(self, value):
        return value

    def to_db(self, value):
        return value

    def get_type(self):
        """ get spanner param type based on field type for proper data sanitation. """
        spanner_type = getattr(types, self.type+'_PARAM_TYPE')
        if not spanner_type:
            raise ValueError("invalid spanner type specified: %s" % self.type)
        return spanner_type


class IntField(SpannerField):
    type = 'INT64'


class BoolField(SpannerField):
    type = 'BOOL'


class TimestampField(SpannerField):
    type = 'TIMESTAMP'


class StringField(SpannerField):
    type = 'STRING'
    length_required = True

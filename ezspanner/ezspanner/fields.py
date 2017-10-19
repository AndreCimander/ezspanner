# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
import logging

from google.cloud.spanner import types


class SpannerField(object):
    type = None

    length_required = False

    def __init__(self, null=False, length=None, default=None, name='', choices=None):
        self.length = length
        if self.length_required and not self.length:
            raise ValueError("This field requires a length param!")

        self.null = null
        self.default = default
        self.choices = None

        # set by contribute_to_class
        self.model = None
        self.name = name
        self.column = self.name

    def __str__(self):
        """ Return "model_label.field_name". """
        model = self.model
        return '%s.%s' % (model._meta.object_name, self.name)

    def __repr__(self):
        """
        Displays the module, class and name of the field.
        """
        path = '%s.%s' % (self.__class__.__module__, self.__class__.__name__)
        name = getattr(self, 'name', None)
        if name is not None:
            return '<%s: %s>' % (path, name)
        return '<%s>' % path

    def from_db(self, value):
        return value

    def to_db(self, value):
        return value

    def contribute_to_class(self, cls, name, check_if_already_added=False):
        self.set_attributes_from_name(name)
        self.model = cls
        cls._meta.add_field(self, check_if_already_added=check_if_already_added)
        # if self.choices:
        #     setattr(cls, 'get_%s_display' % self.name,
        #             curry(cls._get_FIELD_display, field=self))

    def set_attributes_from_name(self, name):
        if not self.name:
            self.name = name
        self.column = self.name

    def get_spanner_type(self):
        """ get spanner param type based on field type for proper data sanitation. """
        spanner_type = getattr(types, self.type+'_PARAM_TYPE')
        if not spanner_type:
            raise ValueError("invalid spanner type specified: %s" % self.type)
        return spanner_type

    def get_type(self):
        if self.type in {'STRING', 'BYTES'}:
            return self.type + '(%s)' % self.length
        return self.type

    def stmt_create(self, field_model_name):
        return """`%(field_model_name)s` %(type)s %(null)s""" % {
            'type': self.get_type(),
            'null': 'NULL' if self.null else 'NOT NULL',
            'field_model_name': field_model_name
        }


class IntField(SpannerField):
    type = 'INT64'


class BoolField(SpannerField):
    type = 'BOOL'


class TimestampField(SpannerField):
    type = 'TIMESTAMP'


class StringField(SpannerField):
    type = 'STRING'
    length_required = True

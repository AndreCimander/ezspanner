# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
import logging

from google.cloud.spanner import types

from .helper import NOT_PROVIDED, Empty


class SpannerField(object):
    type = None

    length_required = False
    
    # These track each time a Field instance is created. Used to retain order.
    # The auto_creation_counter is used for index_fields that Django implicitly
    # creates, creation_counter is used for all user-specified index_fields.
    creation_counter = 0
    auto_creation_counter = -1

    def __init__(self, null=False, length=None, default=None, name='', choices=None, auto_created=False):
        self.length = length
        if self.length_required and not self.length:
            raise ValueError("This field requires a length param!")

        self.null = null
        self.default = default
        self.choices = choices

        # set by contribute_to_class
        self.model = None
        self.name = name
        self.column = self.name
        
        # Adjust the appropriate creation counter, and save our local copy.
        if auto_created:
            self.creation_counter = SpannerField.auto_creation_counter
            SpannerField.auto_creation_counter -= 1
        else:
            self.creation_counter = SpannerField.creation_counter
            SpannerField.creation_counter += 1

    def __str__(self):
        """ Return "model_label.field_name". """
        model = self.model
        return '%s.%s' % (model._meta.object_name, self.name)

    def __repr__(self):
        """
        Displays the module, class and name of the SpannerField.
        """
        path = '%s.%s' % (self.__class__.__module__, self.__class__.__name__)
        name = getattr(self, 'name', None)
        if name is not None:
            return '<%s: %s>' % (path, name)
        return '<%s>' % path

    def __eq__(self, other):
        # Needed for @total_ordering
        if isinstance(other, SpannerField):
            return self.creation_counter == other.creation_counter
        return NotImplemented

    def __lt__(self, other):
        # This is needed because bisect does not take a comparison function.
        if isinstance(other, SpannerField):
            return self.creation_counter < other.creation_counter
        return NotImplemented

    def __hash__(self):
        return hash(self.creation_counter)
    
    def __copy__(self):
        # We need to avoid hitting __reduce__, so define this
        # slightly weird copy construct.
        obj = Empty()
        obj.__class__ = self.__class__
        obj.__dict__ = self.__dict__.copy()
        return obj

    def has_default(self):
        """
        Returns a boolean of whether this field has a default value.
        """
        return self.default is not NOT_PROVIDED

    def get_default(self):
        """
        Returns the default value for this field.
        """
        if self.has_default():
            if callable(self.default):
                return self.default()
            return self.default
        return ""

    def value_from_object(self, obj):
        """
        Returns the value of this field in the given model instance.
        """
        return getattr(obj, self.name)

    def from_db(self, value):
        return value

    def to_db(self, model_instance, add):
        """
        Returns field's value just before saving.
        """
        return getattr(model_instance, self.name)

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


class IntField(SpannerField):
    type = 'INT64'


class BoolField(SpannerField):
    type = 'BOOL'


class TimestampField(SpannerField):
    type = 'TIMESTAMP'


class StringField(SpannerField):
    type = 'STRING'
    length_required = True

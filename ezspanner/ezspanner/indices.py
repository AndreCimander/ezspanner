# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

from collections import OrderedDict

from future.builtins import *

from .helper import Empty


class SpannerIndex(object):
    """

    Example usage:

    ```
    class ExampleModel(ezspanner.SpannerModel):
        class Meta:
            table = 'example'
            pk = ['example_field']

            indices = [
                # create descending sorted index over id_a that stores additional index_fields
                SpannerIndex('index_name', '-id_a', storing=['field_x', 'field_y'])
            ]

    ```

    """
    def __init__(self, name, fields, unique=False, storing=None, interleave=None):
        """

        :type name: str|unicode
        :param name: name of the index

        :type unique: bool
        :param unique: should the index be unique?

        :type storing: None|list
        :param storing: store additional values in the index-table to avoid hitting the full table.

        :type interleave: None|ezspanner.models.SpannerModelBase
        :param interleave: you can interleave an Index in the parent's table.

        :type unique: bool
        :param unique: should this index be a unique index? (default: false)
        """
        self.name = name
        self.model = None
        self.unique = unique
        self.storing = storing or []
        # raw field strings containing -'s to indicate descending sort
        self.fields = fields
        self.interleave = interleave

        if self.interleave:
            self.fields = self.interleave._meta.primary.fields + self.fields
            self.build_fields_with_sort()

        # build field list with sort based on fields
        self.fields_with_sort = None
        self.build_fields_with_sort()

    def __str__(self):
        """ Return index name with model information. """
        return '%s.%s' % (self.model._meta.object_name, self.name)

    def __repr__(self):
        """
        Displays the module, class and name of the SpannerField.
        """
        path = '%s.%s' % (self.__class__.__module__, self.__class__.__name__)
        name = getattr(self, 'name', None)
        if name is not None:
            return '%s <%s: %s>' % (self, path, self.fields)
        return '<%s>' % path

    def __copy__(self):
        # We need to avoid hitting __reduce__, so define this
        # slightly weird copy construct.
        obj = Empty()
        obj.__class__ = self.__class__
        obj.__dict__ = self.__dict__.copy()
        return obj

    def contribute_to_class(self, cls, name):
        self.model = cls
        cls._meta.add_index(self)

    def validate(self):
        pass

    def build_fields_with_sort(self):
        self.index_fields = OrderedDict()
        for field in self.fields:
            if field[0] == '-':
                self.index_fields[field[1:]] = ' DESC'
            else:
                self.index_fields[field] = ''

    def get_field_names(self):
        return self.index_fields.keys()

    def get_fields_with_sort(self):
        return ['`%s` %s' % (field_name, field_sort) for field_name, field_sort in self.index_fields.items()]


class PrimaryKey(SpannerIndex):
    """ Special index that acts as primary key, assigned to SpannerModel.Meta.primary. """
    def __init__(self, fields, parent=None, **kwargs):
        name = 'primary'
        self.parent = parent
        super(PrimaryKey, self).__init__(name, fields, **kwargs)

    def set_parent(self, parent):
        # if we inherited a pk from another model and the parent is already set: don't re-add the parent's pk fields
        # fixme: this could also be handled by SpannerIndex.interleave
        if self.parent == parent:
            return
        self.parent = parent
        self.fields = parent.fields + self.fields
        self.build_fields_with_sort()

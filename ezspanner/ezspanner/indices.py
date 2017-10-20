# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

from collections import OrderedDict

from future.builtins import *

from ezspanner.helper import Empty


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
    def __init__(self, name, fields, **kwargs):
        """

        :type name: str|unicode
        :param name: name of the index

        :param args: field names for the index.You can use "-" to indicate a descending sort for the field, e.g.
        "-field_id" -> "`field_id` DESC".

        :type unique: bool
        :param unique: should this index be a unique index? (default: false)
        """
        self.name = name
        self.model = None
        self.unique = kwargs.get('unique', False)
        self.storing = kwargs.get('storing', [])
        self.fields = fields
        self.index_fields = OrderedDict()
        for field in fields:
            if field[0] == '-':
                self.index_fields[field[1:]] = ' DESC'
            else:
                self.index_fields[field] = ''

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

    def get_field_names(self):
        return self.index_fields.keys()

    def stmt_drop(self):
        """
        Create index drop statement.

        :type model: SpannerModel

        :rtype: unicode
        :returns: drop index string
        """
        return 'DROP INDEX `%(name)s` ON %(table)s;' % {
            'name': self.name,
            'table': self.model.Meta.table
        }

    def stmt_create(self):
        """
        Create index creation statement.

        :type model: SpannerModel

        :rtype: unicode
        :returns: create index string
        """
        fields = ['`%s` %s' % (field_name, field_sort) for field_name, field_sort in self.index_fields.items()]
        return 'CREATE%(unique)s INDEX `%(name)s` ON %(table)s (%(fields)s)%(storing)s;' % {
            'name': self.name,
            'table': self.model.Meta.table,
            'index_fields': ', '.join(fields),
            'unique': ' UNIQUE' if self.unique else '',
            'storing': '' if not self.storing else ' STORING(%s)' % ', '.join(['`%s`' % f for f in self.storing]),
        }


class PrimaryKey(SpannerIndex):
    """ Special index that acts as primary key, assigned in SpannerModel.Meta.pk. """
    def __init__(self, fields, **kwargs):
        name = 'primary'
        super(PrimaryKey, self).__init__(name, fields, **kwargs)

    def stmt_drop(self):
        # we can't drop primary indices
        return ''

    def stmt_create(self):
        # we only need a comma separated list of index_fields
        return ', '.join(['`%s` %s' % (field_name, field_sort) for field_name, field_sort in self.index_fields.items()])

# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

from collections import OrderedDict

from future.builtins import *


class SpannerIndex(object):
    """

    Example usage:

    ```
    class ExampleModel(ezspanner.SpannerModel):
        class Meta:
            table = 'example'
            pk = ['example_field']

            indices = [
                # create descending sorted index over id_a that stores additional fields
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
        self.unique = kwargs.get('unique', False)
        self.storing = kwargs.get('storing', [])
        self.fields = OrderedDict()
        for field in fields:
            if field[0] == '-':
                self.fields[field[1:]] = ' DESC'
            else:
                self.fields[field] = ''

    def stmt_drop(self, model):
        """
        Create index drop statement.

        :type model: SpannerModel

        :rtype: unicode
        :returns: drop index string
        """
        return 'DROP INDEX `%(name)s` ON %(table)s;' % {
            'name': self.name,
            'table': model.Meta.table
        }

    def stmt_create(self, model):
        """
        Create index creation statement.

        :type model: SpannerModel

        :rtype: unicode
        :returns: create index string
        """
        fields = ['`%s` %s' % (field_name, field_sort) for field_name, field_sort in self.fields.items()]
        return 'CREATE%(unique)s INDEX `%(name)s` ON %(table)s (%(fields)s)%(storing)s;' % {
            'name': self.name,
            'table': model.Meta.table,
            'fields': ', '.join(fields),
            'unique': ' UNIQUE' if self.unique else '',
            'storing': '' if not self.storing else ' STORING(%s)' % ', '.join(['`%s`' % f for f in self.storing]),
        }


class PrimaryKey(SpannerIndex):
    """ Special index that acts as primary key, assigned in SpannerModel.Meta.pk. """
    def __init__(self, fields, **kwargs):
        name = 'primary'
        super(PrimaryKey, self).__init__(name, fields, **kwargs)

    def stmt_drop(self, model):
        # we can't drop primary indices
        return ''

    def stmt_create(self, model):
        # we only need a comma separated list of fields
        return ', '.join(['`%s` %s' % (field_name, field_sort) for field_name, field_sort in self.fields.items()])

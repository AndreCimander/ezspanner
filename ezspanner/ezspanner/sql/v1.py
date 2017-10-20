# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
from future.builtins import *


class SQLTable(object):

    def __init__(self, model):
        from ezspanner.models import SpannerModelBase
        assert isinstance(model, SpannerModelBase)
        self.model = model

    def stmt_create(self, include_indices=True):
        """

        :param include_indices:
        :rtype: list
        """
        parent_table_sql = ''

        fields = self.model._meta.get_fields()
        model = self.model
        model_meta = model._meta

        if model_meta.parent:
            # build interleave sql
            parent_table_sql = ' INTERLEAVE IN `%(parent_table)s `' % {'parent_table': model_meta.parent._meta.table}

            # add on delete, default to CASCADE
            if model_meta.parent_on_delete == 'CASCADE':
                parent_table_sql += ' ON DELETE CASCADE'
            else:
                parent_table_sql += ' ON DELETE NO ACTION'

        primary_key_fields = model_meta.primary.get_fields_with_sort()

        ddl_statements = [
            """CREATE TABLE `%(table)s` (\n%(field_definitions)s\n) PRIMARY KEY (\n%(primary_key_fields)s) %(parent_table_sql)s;""" % {
                'table': model_meta.table,
                'field_definitions': ',\n'.join([SQLField(field).stmt_create()[0]
                                                for field in fields]),
                'primary_key_fields': ', '.join(primary_key_fields),
                'parent_table_sql': parent_table_sql
            }]

        if include_indices:
            # todo: create indices
            for index in model_meta.indices:
                ddl_statements.extend(SQLIndex(index).stmt_create())

        return ddl_statements

    def stmt_delete(self):
        return ["""DROP TABLE `(%table)s` """ % {
            'table': self.model._meta.table,
        }]


class SQLField(object):

    def __init__(self, field):
        from ezspanner.fields import SpannerField
        assert isinstance(field, SpannerField)
        self.field = field
        self.model = field.model

    def stmt_create(self):
        """
        :rtype: list
        """
        return ["""`%(field_model_name)s` %(type)s %(null)s""" % {
            'type': self.field.get_type(),
            'null': 'NULL' if self.field.null else 'NOT NULL',
            'field_model_name': self.field.name
        }]


class SQLIndex(object):

    def __init__(self, index):
        from ezspanner.indices import SpannerIndex
        assert isinstance(index, SpannerIndex)
        self.index = index

    def stmt_drop(self):
        """
        Create index drop statement.

        :rtype: list
        :returns: drop index string
        """
        return ['DROP INDEX `%(name)s` ON %(table)s;' % {
            'name': self.index.name,
            'table': self.index.model._meta.table
        }]

    def stmt_create(self):
        """
        Create index creation statement.

        :rtype: list
        :returns: create index string
        """
        index = self.index
        fields = ['`%s` %s' % (field_name, field_sort) for field_name, field_sort in index.index_fields.items()]
        return ['CREATE%(unique)s INDEX `%(name)s` ON `%(table)s` (%(index_fields)s)%(storing)s;' % {
            'name': index.name,
            'table': index.model._meta.table,
            'index_fields': ', '.join(fields),
            'unique': ' UNIQUE' if index.unique else '',
            'storing': '' if not index.storing else ' STORING(%s)' % ', '.join(['`%s`' % f for f in index.storing]),
        }]
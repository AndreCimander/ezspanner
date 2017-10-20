# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
from future.builtins import *


class SQLTable(object):

    def __init__(self, model):
        from ezspanner.models import SpannerModelBase
        assert isinstance(model, SpannerModelBase)
        self.model = model

    def build_stmt_create(self, include_indices=True):
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
                'field_definitions': ',\n'.join([field.stmt_create(field.name)
                                                for field in fields]),
                'primary_key_fields': ', '.join(primary_key_fields),
                'parent_table_sql': parent_table_sql
            }]

        if include_indices:
            # todo: create indices
            # ddl_statements.extend(cls.stmt_create_indices())
            pass

        return ddl_statements

        pass


class SQLField(object):

    def __init__(self, field):
        from ezspanner.fields import SpannerField
        assert isinstance(field, SpannerField)
        self.field = field
        self.model = field.model


class SQLIndex(object):
    pass

# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
from collections import defaultdict, OrderedDict
import logging

from google.cloud import spanner

from .connection import Connection
from .query import SpannerQueryset
from .fields import SpannerField


class SpannerModelRegistry(object):

    registered_models = defaultdict(dict)

    @classmethod
    def register(cls, spanner_class, init_priority=None):
        """

        :type spanner_class:
        :param spanner_class:

        :type init_priority: int|None
        :param init_priority: set the init priority, 0 == highest, 9==lowest. If you have parent-child
        tables you are required to set the parent tables to a higher priority to ensure proper table creation.

        """
        # make sure priority is between 0 and 9, default to 9
        init_priority = init_priority if init_priority and init_priority <= 9 else 9
        init_priority = init_priority if init_priority >= 0 else 0

        assert isinstance(spanner_class(), SpannerModel)
        cls.registered_models[init_priority][spanner_class.__name__] = spanner_class.__class__

    @classmethod
    def create_tables(cls, connection_id=None):
        ddl_statements = []
        for i in range(0, 10):
            for spanner_class in cls.registered_models[i].values():
                assert isinstance(spanner_class, SpannerModel)
                ddl_statements.extend(spanner_class.stmt_create_table())

        database = Connection.get(connection_id=connection_id)
        database.update_ddl(ddl_statements=ddl_statements).result()

    @classmethod
    def drop_tables(cls, connection_id=None):
        ddl_statements = []
        for i in range(0, 10):
            for spanner_class in cls.registered_models[i].values():
                assert isinstance(spanner_class, SpannerModel)
                ddl_statements.extend(spanner_class.stmt_drop_table())

        database = Connection.get(connection_id=connection_id)
        database.update_ddl(ddl_statements=ddl_statements).result()


def register(init_priority=None):
    """
    Decorator that registers a spanner model in the registry.

    :type init_priority: int|None
    :param init_priority: init priority for parent-child relations. 0 = highest, 9 = lowest priority
    """

    def _model_admin_wrapper(spanner_class):
        SpannerModelRegistry.register(spanner_class, init_priority=init_priority)
        return spanner_class

    return _model_admin_wrapper


class SpannerModel(object):
    table_name = ''
    table_parent = None
    table_parent_on_delete = 'CASCADE'
    table_pk = None
    table_indices = []

    # todo: move to config
    instance_id = ''
    database_id = ''

    @classmethod
    def verify_table(cls):
        obj = cls()
        if not obj.table_name:
            raise ValueError("%s: table_name not defined!")

        if not obj.table_pk or not isinstance(obj.table_pk, (list, tuple)):
            raise ValueError("%s: table_pk must be a list of field names!" % cls.__name__)

        from .fields import SpannerField
        for field in obj.table_pk:
            if not isinstance(getattr(obj, field), SpannerField):
                raise ValueError("%s: table_pk %s must be valid SpannerFields!" % (cls.__name__, field))

    #
    # helper methods for create / drop
    #

    @classmethod
    def create_table(cls, connection_id=None):
        ddl_statements = cls.stmt_create_table()
        database = Connection.get(connection_id=connection_id)
        database.update_ddl(ddl_statements=ddl_statements).result()

    @classmethod
    def drop_table(cls, connection_id=None):
        ddl_statements = cls.stmt_drop_table()
        database = Connection.get(connection_id=connection_id)
        database.update_ddl(ddl_statements=ddl_statements).result()

    @classmethod
    def stmt_create_table(cls, include_indices=True):
        """


        :param include_indices:

        :rtype: list
        """
        parent_table_sql = ''
        fields = ['`%s`' % f for f in cls().get_field_names()]

        fields_pk = cls.table_pk
        if cls.table_parent:  # add parent primary keys
            fields_pk = cls.table_parent.table_pk + fields_pk

            parent_table_sql = 'INTERLEAVE IN `%(parent_table)s`' % {'parent_table': cls.table_parent}

        fields_pk = ['`%s`' % f for f in fields_pk]

        ddl_statements = [
            """CREATE TABLE `%(table)s` (%(fields)s) PRIMARY KEY (%(fields_pk)s), (%(parent_table_sql)s)""" % {
                'table': cls.table_name,
                'fields': fields,
                'fields_pk': fields_pk,
                'parent_table_sql': parent_table_sql
            }]

        if include_indices:
            ddl_statements.extend(cls.stmt_create_indices())

        return ddl_statements

    @classmethod
    def stmt_drop_table(cls):
        return """DROP TABLE `(%table)s` """ % {
            'table': cls.table_name,
        }

    @classmethod
    def stmt_create_indices(cls):
        return [index.stmt_create(cls) for index in cls.table_indices]

    @classmethod
    def stmt_drop_indices(cls):
        return [index.stmt_drop(cls) for index in cls.table_indices]

    #
    # misc helper methods
    #

    @property
    def objects(self):
        return SpannerQueryset(self)

    def get_pk_fields(self):
        """ Create primary key field list, resolve parent-child relationships. """
        pk_fields = self.table_pk
        if self.table_parent:
            pk_fields = self.table_parent.get_pk_fields() + pk_fields

        return pk_fields

    def get_indices(self):
        return OrderedDict([
            (attr_key, getattr(self, attr_key)) for attr_key in dir(self)
            if isinstance(getattr(self, attr_key), Index)
        ])

    def get_fields(self):
        return OrderedDict([
            (attr_key, getattr(self, attr_key)) for attr_key in dir(self)
            if isinstance(getattr(self, attr_key), SpannerField)
        ])

    def get_field_names(self):
        return [attr_key for attr_key in dir(self)
                if isinstance(getattr(self, attr_key), SpannerField)]


class Index(object):

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
        self.storing = kwargs.get('media_id', [])
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
            'table': model.table_name
        }

    def stmt_create(self, model):
        """
        Create index creation statement.

        :type model: SpannerModel

        :rtype: unicode
        :returns: create index string
        """
        fields = ['`%s`%s' % (field_name, field_sort) for field_name, field_sort in self.fields.items()]
        return 'CREATE%(unique)s INDEX `%(name)s` ON %(table)s (%(fields)s)%(storing)s;' % {
            'name': self.name,
            'table': model.table_name,
            'fields': ', '.join(fields),
            'unique': ' UNIQUE' if self.unique else '',
            'storing': '' if not self.storing else ' STORING(%s)' % ', '.join(['`%s`' % f for f in self.storing]),
        }

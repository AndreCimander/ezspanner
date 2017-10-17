# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
from collections import defaultdict, OrderedDict
import logging

from google.cloud import spanner

from ezspanner.helper import get_valid_instance_from_class
from .connection import Connection
from .query import SpannerQueryset
from .fields import SpannerField


class SpannerModelRegistry(object):
    """
    Helper class that validates SpannerModels and helps with global operations on all registered models
    (e.g. create/drop tables).

    """
    registered_models = {}

    @classmethod
    def register(cls, spanner_class):
        """

        :type spanner_class:
        :param spanner_class:

        """
        spanner_instance = spanner_class()
        assert isinstance(spanner_instance, SpannerModel)

        # verify model
        spanner_class.verify_table()

        cls.registered_models[spanner_class.__name__] = spanner_instance

    @classmethod
    def get_registered_models_in_correct_order(cls):
        prio_dict = defaultdict(list)
        for class_instance in cls.registered_models.values():
            init_prio = cls._get_prio(class_instance)
            prio_dict[init_prio].append(class_instance)

        for i in range(0, 10):
            for o in prio_dict[i]:
                yield o

    @staticmethod
    def _get_prio(model_class, i=0):
        while getattr(model_class.Meta, 'parent', False):
            i += 1
            model_class = model_class.Meta.parent
            if not model_class.Meta.parent or i >= 9:
                break
        return i

    @classmethod
    def create_table_statements(cls):
        ddl_statements = []
        for spanner_class in cls.get_registered_models_in_correct_order():
            assert isinstance(spanner_class, SpannerModel)
            ddl_statements.extend(spanner_class.stmt_create_table())
        return ddl_statements

    @classmethod
    def create_tables(cls, connection_id=None):
        ddl_statements = cls.create_table_statements()
        database = Connection.get(connection_id=connection_id)
        database.update_ddl(ddl_statements=ddl_statements).result()

    @classmethod
    def drop_tables(cls, connection_id=None):
        ddl_statements = []
        for spanner_class in cls.registered_models.values():
            assert isinstance(spanner_class, SpannerModel)
            ddl_statements.extend(spanner_class.stmt_drop_table())

        database = Connection.get(connection_id=connection_id)
        database.update_ddl(ddl_statements=ddl_statements).result()


def register():
    """
    Decorator that registers a spanner model in the registry.
    """

    def _model_admin_wrapper(spanner_class):
        SpannerModelRegistry.register(spanner_class)
        return spanner_class

    return _model_admin_wrapper


class SpannerModel(object):

    class Meta:
        """ default Meta class with all available attributes. """
        table = ''
        pk = None
        indices = []

        parent = None
        parent_on_delete = 'CASCADE'

    @classmethod
    def verify_table(cls):
        obj = cls()
        if not hasattr(obj.Meta, 'table') or not obj.Meta.table:
            raise ValueError("%s: Meta.name not defined!")

        if not hasattr(obj.Meta, 'pk') or not isinstance(obj.Meta.pk, (list, tuple)):
            raise ValueError("%s: Meta.pk must be field-name-string or  a list of field names!" % cls.__name__)

        if obj.has_parent():
            get_valid_instance_from_class(obj.Meta.parent, valid_class_types=(SpannerModel,))

        from .fields import SpannerField
        for field in obj.Meta.pk:
            if not isinstance(getattr(obj, field), SpannerField):
                raise ValueError("%s: table_pk %s must be valid SpannerFields!" % (cls.__name__, field))

    @classmethod
    def get_table_name(cls):
        if cls.Meta.table:
            return cls.Meta.table
        else:
            c = cls.__mro__[0]
            name = c.__module__ + "." + c.__name__
            return name

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

        :type include_indices: bool
        :param include_indices: add index creation statements to ddl_statements?

        :rtype: list
        """
        parent_table_sql = ''

        fields = cls.get_fields()
        if cls.has_parent():
            fields = OrderedDict(cls.Meta.parent.get_fields_pk().items() + fields.items())

            # build interleave sql
            parent_table_sql = ' INTERLEAVE IN `%(parent_table)s `' % {'parent_table': cls.Meta.parent.Meta.table}

            # add on delete, default to CASCADE
            if not hasattr(cls.Meta, 'parent_on_delete') or cls.Meta.parent_on_delete == 'CASCADE':
                parent_table_sql += ' ON DELETE CASCADE'
            else:
                parent_table_sql += ' ON DELETE NO ACTION'

        primary_key_fields = cls.get_field_names_pk()

        ddl_statements = [
            """CREATE TABLE `%(table)s` (\n%(field_definitions)s\n) PRIMARY KEY (\n%(primary_key_fields)s) %(parent_table_sql)s;""" % {
                'table': cls.Meta.table,
                'field_definitions': ',\n'.join([field.stmt_create(field_name)
                                                for field_name, field in fields.items()]),
                'primary_key_fields': ', '.join(primary_key_fields),
                'parent_table_sql': parent_table_sql
            }]

        if include_indices:
            ddl_statements.extend(cls.stmt_create_indices())

        return ddl_statements

    @classmethod
    def stmt_drop_table(cls):
        return """DROP TABLE `(%table)s` """ % {
            'table': cls.Meta.table,
        }

    @classmethod
    def stmt_create_indices(cls):
        return [index.stmt_create(cls) for index in getattr(cls.Meta, 'indices', [])]

    @classmethod
    def stmt_drop_indices(cls):
        return [index.stmt_drop(cls) for index in getattr(cls.Meta, 'indices', [])]

    @property
    def objects(self):
        return SpannerQueryset(self)

    #
    # misc helper methods
    #

    @classmethod
    def has_parent(cls):
        return hasattr(cls.Meta, 'parent') and cls.Meta.parent

    @classmethod
    def get_fields(cls):
        """

        :rtype: OrderedDict[str, SpannerField]
        """
        return OrderedDict([
            (attr_key, getattr(cls, attr_key)) for attr_key in dir(cls)
            if isinstance(getattr(cls, attr_key), SpannerField)
        ])

    @classmethod
    def get_fields_pk(cls):
        """
        Get primary key fields (includes parent table's primary key fields).

        :rtype: OrderedDict[str, SpannerField]
        """
        fields = cls.get_fields()

        pk_fields = [(pk, fields[pk]) for pk in cls.Meta.pk]
        if cls.has_parent():
            pk_fields = cls.Meta.parent.get_fields_pk().items() + pk_fields

        return OrderedDict(pk_fields)

    @classmethod
    def get_field_names_pk(cls):
        """
        Create primary key field list, resolve parent-child relationships and support ASC/DESC

        :rtype: list[str]
        """
        return cls.get_fields_pk().keys()

    @classmethod
    def get_fields_with_parent_pks(cls):
        """
        Get fields plus the primary key fields from all parent models.

        :rtype: OrderedDict[str, SpannerField]
        """
        pass

    @classmethod
    def get_indices(cls):
        """

        :rtype: OrderedDict[str, SpannerIndex]
        """
        return OrderedDict([
            (i.name, i) for i in getattr(cls.Meta, 'indices', [])
        ])


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

# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

import copy
import inspect
from bisect import bisect
from collections import defaultdict, OrderedDict

import six
from itertools import chain

from google.cloud import spanner

from ezspanner.exceptions import ObjectDoesNotExist, FieldError
from ezspanner.helper import get_valid_instance_from_class, subclass_exception
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

        :type spanner_class: SpannerModelBase
        :param spanner_class:

        """
        assert isinstance(spanner_class, SpannerModelBase)

        # verify model
        # todo: refactor and move verification to SpannerModelBase
        # spanner_class.verify_table()

        # test for table name collisions
        registered_class = cls.registered_models.get(spanner_class._meta.table)
        if registered_class and registered_class != spanner_class:
            raise ValueError("SpannerModel.meta.table collision: %s %s" % (registered_class, spanner_class))

        cls.registered_models[spanner_class._meta.table] = spanner_class

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


DEFAULT_NAMES = ('table', 'pk', 'parent', 'parent_on_delete', 'indices', 'abstract')


class SpannerModelMeta(object):

    def __init__(self, meta):
        self.model = None
        self.meta = meta
        self.table = ''
        self.local_fields = []
        self.lookup_fields = {}
        self.pk_fields = []
        self.parent = None
        self.abstract = False
        self.pk = []

    def contribute_to_class(self, cls, name):

        cls._meta = self
        self.model = cls
        # First, construct the default values for these options.
        self.object_name = cls.__name__
        self.model_name = self.object_name.lower()

        # Store the original user-defined values for each option,
        # for use when serializing the model definition
        self.original_attrs = {}

        # Next, apply any overridden values from 'class Meta'.
        if self.meta:
            meta_attrs = self.meta.__dict__.copy()
            for name in self.meta.__dict__:
                # Ignore any private attributes that Django doesn't care about.
                # NOTE: We can't modify a dictionary's contents while looping
                # over it, so we loop over the *original* dictionary instead.
                if name.startswith('_'):
                    del meta_attrs[name]
            for attr_name in DEFAULT_NAMES:
                if attr_name in meta_attrs:
                    setattr(self, attr_name, meta_attrs.pop(attr_name))
                    self.original_attrs[attr_name] = getattr(self, attr_name)
                elif hasattr(self.meta, attr_name):
                    setattr(self, attr_name, getattr(self.meta, attr_name))
                    self.original_attrs[attr_name] = getattr(self, attr_name)

            # Any leftover attributes must be invalid.
            if meta_attrs != {}:
                raise TypeError("'class Meta' got invalid attribute(s): %s" % ','.join(meta_attrs.keys()))
        del self.meta

        if not self.abstract and not self.table:
            raise ValueError("%s must define a Meta.table!" % cls)

    def add_field(self, field, check_if_already_added=False):
        if check_if_already_added and self.lookup_fields.get(field.name):
            return

        self.lookup_fields[field.name] = field
        self.local_fields.insert(bisect(self.local_fields, field), field)

    def interleave_with_parent(self, parent):
        self.add_parent_pk_definition(parent._meta.get_parent_pk_definition())
        for field in parent._meta.get_fields_pk():
            new_field = copy.deepcopy(field)
            new_field.contribute_to_class(self.model, field.name,check_if_already_added=True)

    def get_parent_pk_definition(self):
        return self.pk

    def add_parent_pk_definition(self, pk_field_list):
        # prepend parent pk to own pk definition
        self.pk = pk_field_list + self.pk

    def get_fields(self):
        return self.local_fields

    def get_fields_pk(self):
        return self.pk_fields

    def _prepare(cls, model):
        """
        Do some more magic once self._meta has been populated.
        """

        # convert field names to fields
        # todo: create primary key index
        for field_id in cls.pk:
            if field_id[0] == '-':
                order = 'DESC'
                field_id = field_id[1:]
            else:
                order = 'ASC'

            cls.pk_fields.append(cls.lookup_fields[field_id])
        pass


class SpannerModelBase(type):
    """
    Metaclass for all models, heavily borrowed from Django's awesome ORM.
    Gosh, this "thin SQL wrapper for Cloud Spanner" is really getting out of hand... FML -.-
    """
    # fixme: implement proxy model (later)

    def __new__(cls, name, bases, attrs):
        super_new = super(SpannerModelBase, cls).__new__

        # Also ensure initialization is only performed for subclasses of Model
        # (excluding Model class itself).
        parents = [b for b in bases if isinstance(b, SpannerModelBase)]
        if not parents:
            return super_new(cls, name, bases, attrs)

        # Create the class.
        module = attrs.pop('__module__')
        new_class = super_new(cls, name, bases, {'__module__': module})
        attr_meta = attrs.pop('Meta', None)

        if not attr_meta:
            meta = getattr(new_class, 'Meta', None)
        else:
            meta = attr_meta
        base_meta = getattr(new_class, '_meta', None)

        new_class.add_to_class('_meta', SpannerModelMeta(meta))

        # add DoesNotExist exception
        new_class.add_to_class(
            'DoesNotExist',
            subclass_exception(
                str('DoesNotExist'),
                tuple(
                    x.DoesNotExist for x in parents if hasattr(x, '_meta') and not x._meta.abstract
                ) or (ObjectDoesNotExist,),
                module,
                attached_to=new_class))

        # Add all attributes to the class.
        for obj_name, obj in attrs.items():
            new_class.add_to_class(obj_name, obj)

        all_fields = chain(
            new_class._meta.local_fields
        )
        # All the fields of any type declared on this model
        field_names = {f.name for f in all_fields}

        # interleave parent
        interleave_with = new_class._meta.parent

        # Do the appropriate setup for any model parents.
        for base in parents:
            if not hasattr(base, '_meta'):
                # Things without _meta aren't functional models, so they're
                # uninteresting parents.
                continue

            # only allow one _meta.parent!
            if interleave_with and base._meta.parent and interleave_with != base._meta.parent:
                raise ValueError("%s may only have one Meta.parent, found different parent in %s!" % (new_class, base))

            if not interleave_with:
                interleave_with = base._meta.parent

            # todo: decide: do we inherit ._meta.pk from other bases?
            # todo: decide: do we inherit ._meta.indices?
            # add parent's field to model
            parent_fields = base._meta.local_fields
            for field in parent_fields:
                # Check for clashes between locally declared fields and those
                # on the base classes (we cannot handle shadowed fields at the
                # moment).
                if field.name in field_names:
                    raise FieldError(
                        'Local field %r in class %r clashes '
                        'with field of similar name from '
                        'base class %r' % (field.name, name, base.__name__)
                    )

                new_field = copy.deepcopy(field)
                new_class.add_to_class(field.name, new_field)

        # interleave table with parent table
        if interleave_with:
            new_class._meta.interleave_with_parent(interleave_with)

        # register class in registry (if not abstract)
        if not new_class._meta.abstract:
            SpannerModelRegistry.register(new_class)

        new_class._prepare()
        return new_class

    def add_to_class(cls, name, value):
        # We should call the contribute_to_class method only if it's bound
        if not inspect.isclass(value) and hasattr(value, 'contribute_to_class'):
            value.contribute_to_class(cls, name)
        else:
            setattr(cls, name, value)

    def _prepare(cls):
        """
        Creates some methods once self._meta has been populated.
        """
        opts = cls._meta
        opts._prepare(cls)


class SpannerModel(six.with_metaclass(SpannerModelBase)):

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

        # validate indices
        # pk
        if not hasattr(obj.Meta, 'pk') or not isinstance(obj.Meta.pk, (list, tuple)):
            raise ValueError("%s: Meta.pk must be field-name-string or  a list of field names!" % cls.__name__)
        # other indices
        # todo:
        pass

        # validate parent
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

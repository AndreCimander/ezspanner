# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
import copy
import inspect
from bisect import bisect
from collections import defaultdict, OrderedDict
import six
from itertools import chain

from .exceptions import ObjectDoesNotExist, FieldError, ModelError
from .helper import subclass_exception
from .connection import Connection
from .query import SpannerQuerySet
from .sql import v1 as sql_v1


class SpannerModelRegistry(object):
    """
    Helper class that validates SpannerModels and helps with global operations on all registered models
    (e.g. create/drop tables).

    """
    registered_models = OrderedDict()

    @classmethod
    def register(cls, spanner_class):
        """

        :type spanner_class: SpannerModelBase
        :param spanner_class:

        """
        assert isinstance(spanner_class, SpannerModelBase)

        # test for table name collisions
        registered_class = cls.registered_models.get(spanner_class._meta.table)
        if registered_class and registered_class != spanner_class:
            raise ModelError("SpannerModel.meta.table collision: %s %s" % (registered_class, spanner_class))

        cls.registered_models[spanner_class._meta.table] = spanner_class

    @classmethod
    def get_registered_models_prio_dict(cls):
        prio_dict = defaultdict(list)
        for class_instance in cls.registered_models.values():
            init_prio = cls._get_prio(class_instance)
            prio_dict[init_prio].append(class_instance)
        return prio_dict

    @classmethod
    def get_registered_models_in_correct_order(cls):
        prio_dict = cls.get_registered_models_prio_dict()
        for i in range(0, 10):
            for o in prio_dict[i]:
                yield o

    @staticmethod
    def _get_prio(model_class, i=0):
        while model_class._meta.parent:
            i += 1
            model_class = model_class._meta.parent
            if i >= 9:
                break
        return i

    @classmethod
    def create_table_statements(cls):
        ddl_statements = []
        for spanner_class in cls.get_registered_models_in_correct_order():
            builder = sql_v1.SQLTable(spanner_class)
            ddl_statements.extend(builder.stmt_create())
        return ddl_statements

    @classmethod
    def create_tables(cls, connection_id=None):
        ddl_statements = cls.create_table_statements()
        database = Connection.get(connection_id=connection_id)
        database.update_ddl(ddl_statements=ddl_statements).result()

    @classmethod
    def delete_table_statements(cls):
        ddl_statements = []
        for spanner_class in cls.get_registered_models_in_correct_order():
            builder = sql_v1.SQLTable(spanner_class)
            ddl_statements.extend(builder.stmt_delete())
        return ddl_statements

    @classmethod
    def drop_tables(cls, connection_id=None):
        ddl_statements = cls.drop_table_statements()
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


DEFAULT_NAMES = ('table', 'pk', 'parent', 'parent_on_delete', 'indices', 'indices_inherit', 'abstract')


class ModelState(object):
    """
    A class for storing instance state
    """
    def __init__(self, db=None):
        self.db = db
        # If true, uniqueness validation checks will consider this a new, as-yet-unsaved object.
        # Necessary for correct validation of new instances of objects with explicit (non-auto) PKs.
        # This impacts validation only; it has no effect on the actual save.
        self.adding = True


class SpannerModelMeta(object):

    def __init__(self, meta):
        self.model = None
        self.meta = meta
        self.table = ''
        self.local_fields = []
        self.field_lookup = {}

        self.pk = []
        self.primary = None

        self.indices = []
        self.index_lookup = {}
        self.inherit_indices = True
        
        self.parent = None
        self.parent_on_delete = 'CASCADE'
        self.abstract = False

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
        if check_if_already_added and self.field_lookup.get(field.name):
            return

        self.field_lookup[field.name] = field
        self.local_fields.insert(bisect(self.local_fields, field), field)

    def add_index(self, index):
        from .indices import SpannerIndex, PrimaryKey
        assert isinstance(index, SpannerIndex)
        if self.index_lookup.get(index.name):
            return

        self.index_lookup[index.name] = index

        # don't assign the primary key to the indices list
        if isinstance(index, PrimaryKey):
            self.primary = index
        else:
            self.indices.append(index)

    def interleave_with_parent(self, parent):
        # set parent on model meta
        self.parent = parent

        # copy all primary key field names that needs to be copied to this model
        for field in parent._meta.primary.get_field_names():
            new_field = copy.deepcopy(parent._meta.field_lookup[field])
            new_field.contribute_to_class(self.model, new_field.name, check_if_already_added=True)

        # add parent primary
        self.primary.set_parent(parent._meta.primary)

    def get_fields(self):
        return self.local_fields

    def _prepare(cls, model):
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

        # All the index_fields of any type declared on this model
        field_names = {f.name for f in all_fields}

        # interleave parent
        interleave_with = new_class._meta.parent

        # add indices to class
        for index in new_class._meta.indices:
            new_index = index
            new_class.add_to_class(new_index.name, new_index)

        # Do the appropriate setup for any model parents.
        pk_inherited_from_parent = False
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

            # add class parent's fields to model
            parent_fields = base._meta.local_fields
            for field in parent_fields:
                # Check for clashes between locally declared index_fields and those
                # on the base classes (we cannot handle shadowed index_fields at the
                # moment).
                if field.name in field_names:
                    raise FieldError(
                        'Local field %r in class %r clashes '
                        'with field of similar name from '
                        'base class %r' % (field.name, name, base.__name__)
                    )

                new_field = copy.deepcopy(field)
                new_class.add_to_class(field.name, new_field)
            
            # assign first parent's pk if this class has no pk defined
            # make sure that our parent's only have one primary key, otherwise we run into problems.
            if base._meta.pk and pk_inherited_from_parent:
                raise ModelError("%s may only inherit one primary key!" % new_class)
            if base._meta.pk and not new_class._meta.pk:
                new_class._meta.pk = base._meta.pk
                pk_inherited_from_parent = True

            # copy parent's indices if this class defines to indices.
            if new_class._meta.inherit_indices:
                for index in base._meta.indices:
                    new_index = copy.deepcopy(index)
                    new_class.add_to_class(new_index.name, new_index)

        # convert primary key field list/tuple to PrimaryKey index
        if isinstance(new_class._meta.pk, (list, tuple)):
            from .indices import PrimaryKey
            new_index = PrimaryKey(fields=new_class._meta.pk)
            new_class.add_to_class(new_index.name, new_index)

        # interleave table with parent table (also copies&prepends _meta.pk index_fields from parent)
        if interleave_with:
            new_class._meta.interleave_with_parent(interleave_with)

        # register class in registry (if not abstract)
        if not new_class._meta.abstract:
            SpannerModelRegistry.register(new_class)
        setattr(new_class, 'objects', SpannerQuerySet(new_class))
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


@six.python_2_unicode_compatible
class SpannerModel(six.with_metaclass(SpannerModelBase)):

    # default queryset to shut automatic code analysis up
    objects = SpannerQuerySet()

    def __init__(self, *args, **kwargs):
        # Set up the storage for instance state
        self._state = ModelState()

        # There is a rather weird disparity here; if kwargs, it's set, then args
        # overrides it. It should be one or the other; don't duplicate the work
        # The reason for the kwargs check is that standard iterator passes in by
        # args, and instantiation for iteration is 33% faster.
        args_len = len(args)
        if args_len > len(self._meta.local_fields):
            # Daft, but matches old exception sans the err msg.
            raise IndexError("Number of args exceeds number of index_fields")

        if not kwargs:
            fields_iter = iter(self._meta.local_fields)
            # The ordering of the zip calls matter - zip throws StopIteration
            # when an iter throws it. So if the first iter throws it, the second
            # is *not* consumed. We rely on this, so don't change the order
            # without changing the logic.
            for val, field in zip(args, fields_iter):
                setattr(self, field.attname, val)
        else:
            # Slower, kwargs-ready version.
            fields_iter = iter(self._meta.local_fields)
            for val, field in zip(args, fields_iter):
                setattr(self, field.attname, val)
                kwargs.pop(field.name, None)

        # Now we're left with the unprocessed index_fields that *must* come from
        # keywords, or default.

        for field in fields_iter:
            if kwargs:
                try:
                    val = kwargs.pop(field.name)
                except KeyError:
                    # This is done with an exception rather than the
                    # default argument on pop because we don't want
                    # get_default() to be evaluated, and then not used.
                    # Refs #12057.
                    val = field.get_default()
            else:
                val = field.get_default()

            setattr(self, field.name, val)

        if kwargs:
            for prop in list(kwargs):
                try:
                    if isinstance(getattr(self.__class__, prop), property):
                        setattr(self, prop, kwargs.pop(prop))
                except AttributeError:
                    pass
            if kwargs:
                raise TypeError("'%s' is an invalid keyword argument for this function" % list(kwargs)[0])
        super(SpannerModel, self).__init__()

    def __str__(self):
        return '%s<%s>' % (self._class__, self._meta.table)

    @classmethod
    def from_db(cls, db, field_names, values):
        new = cls(**dict(zip(field_names, values)))
        new._state.adding = False
        new._state.db = db
        return new

    @classmethod
    def get_table_name(cls):
        if cls.Meta.table:
            return cls.Meta.table
        else:
            c = cls.__mro__[0]
            name = c.__module__ + "." + c.__name__
            return name

    def save(self, force_insert=False, force_update=False, using=None,
             update_fields=None):
        """
        Saves the current instance. Override this in a subclass if you want to
        control the saving process.

        The 'force_insert' and 'force_update' parameters can be used to insist
        that the "save" must be an SQL insert or update (or equivalent for
        non-SQL backends), respectively. Normally, they should not be set.
        """
        if force_insert and (force_update or update_fields):
            raise ValueError("Cannot force both insert and updating in model saving.")

        if update_fields is not None:
            # If update_fields is empty, skip the save. We do also check for
            # no-op saves later on for inheritance cases. This bailout is
            # still needed for skipping signal sending.
            if len(update_fields) == 0:
                return

            update_fields = frozenset(update_fields)
            field_names = set()
            for field in self._meta.local_fields:
                field_names.add(field.name)

            non_model_fields = update_fields.difference(field_names)

            if non_model_fields:
                raise ValueError("The following fields do not exist in this "
                                 "model or are m2m fields: %s"
                                 % ', '.join(non_model_fields))

        self.save_base(using=using, force_insert=force_insert,
                       force_update=force_update, update_fields=update_fields)

    def save_base(self, force_insert=False,
                  force_update=False, using=None, update_fields=None):
        """
        Handles the parts of saving which should be done only once per save,
        yet need to be done in raw saves, too. This includes some sanity
        checks and signal sending.

        """
        assert not (force_insert and (force_update or update_fields))
        assert update_fields is None or len(update_fields) > 0
        cls = origin = self.__class__
        # Skip proxies, but keep the origin as the proxy model.
        meta = cls._meta

        updated = self._save_table(cls, force_insert, force_update, using, update_fields)
        # Store the database on which the object was saved
        self._state.db = using
        # Once saved, this is no longer a to-be-added instance.
        self._state.adding = False

    def _save_table(self, cls=None, force_insert=False,
                    force_update=False, using=None, update_fields=None):
        """
        Does the heavy-lifting involved in saving. Updates or inserts the data
        for a single table.
        """
        meta = cls._meta
        pk_val = self._get_pk_val(meta)
        non_pks = [f for f in meta.local_fields if f.name not in pk_val['keys']]

        if update_fields:
            non_pks = [f for f in non_pks if f.name in update_fields]

        pk_set = not bool(pk_val['missing'])
        if not pk_set and (force_update or update_fields):
            raise ValueError("Cannot force an update in save() with no primary key.")

        updated = False
        # If possible, try an UPDATE. If that doesn't update anything, do an INSERT.
        # UPDATE
        if pk_set and not force_insert:
            forced_update = update_fields or force_update
            updated = self._do_update(using, pk_val, non_pks, forced_update)
            if force_update and not updated:
                raise ModelError("Forced update did not affect any rows.")

        # INSERT
        if not updated:
            # todo: support auto-generated pk values
            if not pk_set:
                raise ValueError("Can't insert value without primary key values! Missing: %s" % pk_val['missing'])
            self._do_insert(using, pk_val, non_pks)
        return updated

    def _do_update(self, using, pk_val, update_fields, forced_update):
        """
        This method will try to update the model. If the model was updated (in
        the sense that an update query was done and a matching row was found
        from the DB) the method will return True.
        """
        database = Connection.get(connection_id=using)
        with database.batch() as batch:

            # add primary keys to the updated columns
            columns = [pk_val['keys']] + [f.name for f in update_fields]

            result = batch.update(
                table=self._meta.table,
                columns=columns,
                values=pk_val['values'] + [f.to_db(self, False) for f in update_fields]
            )
            print (result)
            return True

    def _do_insert(self, using, pk_val, update_fields):
        """
        Do an INSERT. If update_pk is defined then this method should return
        the new pk for the model.
        """
        database = Connection.get(connection_id=using)
        with database.batch() as batch:
            # add primary keys to the updated columns
            columns = [pk_val['keys']] + [f.name for f in self.meta.local_fields]

            result = batch.insert(
                table=self._meta.table,
                columns=columns,
                values=pk_val['values'] + [f.to_db(self, True) for f in update_fields]
            )
            pass

    def delete(self, using=None, keep_parents=False):
        pk_val = self._get_pk_val()
        assert pk_val['missing'] is False, (
            "%s object can't be deleted because primary keys are missing: %s." %
            (self._meta.object_name, pk_val['missing'])
        )

        database = Connection.get(connection_id=using)
        with database.batch() as batch:
            batch.delete(
                table=self._meta.table,
                columns=pk_val['keys'],
                values=pk_val['values']
            )

        return True

    def _get_pk_val(self, meta=None):
        if not meta:
            meta = self._meta
        keys = meta.primary.fields
        values = [getattr(self, k) for k in keys]
        pk_data = {
            'keys': set(keys),
            'values': values,
            'missing': [keys[i] for i, v in enumerate(values) if not v],
        }

        return pk_data

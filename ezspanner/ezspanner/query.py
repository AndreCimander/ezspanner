# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
import copy
from collections import OrderedDict, defaultdict
import six

from ezspanner.query_utils import LOOKUP_SEP, Q, F
from .exceptions import ModelError, SpannerIndexError, QueryError
from .connection import Connection


# noinspection PyMethodFirstArgAssignment
class SpannerQuerySet(object):

    JOIN_LEFT = 'LEFT'
    JOIN_RIGHT = 'RIGHT'
    JOIN_INNER = 'INNER'
    JOIN_CROSS = 'CROSS'
    JOIN_FULL = 'FULL'

    def __init__(self, model=None):
        self.model = model
        self.conn = None
        self.joins = OrderedDict()
        # allows to force-select an index for the self.model table.
        self.selected_index = None
        # keeps track which fields from which models should be returned
        self.selected_fields = OrderedDict()
        # WHERE clause filter conditions
        self.where = None

        # param storage for later concrete value injection, keeps track of replacement_key and value.
        self.params = {}

        self.field_lookup = defaultdict(list)

        self._result_cache = None

        if model:
            self._discover_columns(model)

    def __deepcopy__(self, memo):
        """
        Deep copy of a QuerySet doesn't populate the cache
        """
        obj = self.__class__()
        for k, v in self.__dict__.items():
            if k == '_result_cache':
                obj.__dict__[k] = None
            else:
                obj.__dict__[k] = copy.deepcopy(v, memo)
        return obj

    def _discover_columns(self, model):
        for field in model._meta.local_fields:
            self.field_lookup[field.name].append(model)

    def is_column_ambiguous_or_unknown(self, column):
        """

        :param column:

        :raises QueryError: if column is ambiguous or not found.
        """
        if len(self.field_lookup[column]) == 0:
            raise QueryError("Field '%s' is unknown!" % column)
        if len(self.field_lookup[column]) > 1:
            raise QueryError("Field '%s' is ambiguous, you must specify a model/model alias!" % column)

    def get_model_for_column(self, column):
        """
        Return model instance for column. If the column exists in more than one queried model

        :param column:
        :raises QueryError: if column is ambiguous or not found.

        :rtype: ezspanner.models.SpannerModel
        """
        self.is_column_ambiguous_or_unknown(column)
        return self.field_lookup[column][0]

    def index(self, index_name):
        """
        Force index usage for base query model.

        :param index_name:
        :rtype: SpannerQuerySet
        """
        self = copy.deepcopy(self)
        # check if index exists
        if not self.model._meta.index_lookup.get(index_name) and index_name != '_BASE_TABLE':
            raise SpannerIndexError("invalid index specified! '%s' is not a valid index name for model '%s'" %
                                    (index_name, self.model))

        # set index for query
        self.selected_index = index_name
        return self

    def join(self, model, model_alias=None, join_type=JOIN_INNER, join_index=None, fields=None, on=None):
        """

        :param model:
        :param model_alias:
        :param join_type:
        :param join_index:
        :param fields:

        :type on: dict
        :param on: supply a dictionary|OrderedDict containing key, value pairs of join-fields.
        The dict's keys will be mapped to the joined Model, the value must be supplied as a F instance if you don't
        want to use a concrete value.

        Example:
        on=dict(id_a=F(OtherModel, 'id_a'))

        :rtype: SpannerQuerySet
        """
        self = copy.deepcopy(self)
        if not model:
            raise QueryError("You must specify a model as first parameter!")
        if self.joins.get(model) and not model_alias:
            raise QueryError("Model '%s' was already used for a join, supply a model_alias." % model)
        if model_alias is not None and (not isinstance(model_alias, six.string_types) or len(model_alias) == 0):
            raise QueryError("Model alias '%s' must be a non-empty string!" % model_alias)
        if model_alias and self.joins.get(model_alias):
            raise QueryError("Model alias '%s' already taken in this query!" % model_alias)

        if not isinstance(on, (dict, OrderedDict)) or not on:
            raise QueryError("You must supply a join dictionary for `on` ")

        # discover columns
        self._discover_columns(model)

        # get identifier
        model_identifier = model_alias or model

        # analyze on, construct Q query with correct F() fields
        join_on = []
        for key, value in six.iteritems(on):
            join_on.append((F(model_identifier, key), value))
        join_on = Q(*join_on)

        # analyze fields, if empty select all fields from joined model
        # if we haven't specified any fields we select all columns from the joined table
        self._set_values(model_identifier, fields)

        self.joins[model_identifier] = {
            'type': join_type,
            'index': join_index,
            'model': model,
            'alias': model_alias,
            'fields': fields,
            'join_on': join_on
        }

        return self

    def _reset_values(self, model=None, reset_all=False):
        if reset_all:
            self.selected_fields = OrderedDict()
        elif self.selected_fields.get(model) is not None:
            del self.selected_fields[model]

    def add_param(self, field, value):
        param_id = field.name
        i = 0
        while self.params.get(param_id) is not None:
            i += 1
            param_id = field.name+'_'+str(i)
        self.params[param_id] = {'value': value, 'type': field.get_spanner_type()}
        return param_id

    def values(self, *fields, **kwargs):
        """
        Set return values for this queryset.

        Reset fields with single None argument, e.g. .values(None), you can use the keyword argument reset_all=True to
        also reset all field selections from joins.

        :param fields:
        :param kwargs:

        :rtype: SpannerQuerySet
        """
        self = copy.deepcopy(self)
        self._set_values(fields, **kwargs)
        return self

    def _set_values(self, fields, model_or_alias=None, reset_all=False):
        """

        :type fields: list[F|unicode]|tuple[F|unicode]
        :param model_or_alias:
        :param reset_all:
        :return:
        """
        # check if model is already joined
        model = self._check_model_joined(model_or_alias or self.model)

        # empty fields -> reset value selection
        if len(fields) == 1 and fields[0] is None:
            self._reset_values(model=model, reset_all=reset_all)
            return self

        # make sure that selected fields exist
        new_fields = []
        for f in fields:
            # convert to column names to F instance
            if not isinstance(f, F):
                if not model._meta.field_lookup.get(f):
                    raise ModelError("'%s' is an invalid field for model '%s'" % (f, model))
                f = F(model_or_alias, f)
            new_fields.append(f)
        # append fields to existing selection
        existing_fields = self.selected_fields.get(model, [])
        self.selected_fields[model] = list(set(existing_fields + new_fields))

    def filter(self, *args, **kwargs):
        """
        Returns a new QuerySet instance with the args ANDed to the existing
        set.
        """
        return self.filter_and(*args, **kwargs)

    def filter_and(self, *args, **kwargs):
        """
        Returns a new QuerySet instance with the args ANDed to the existing
        set.
        """
        return self._filter_or_exclude(False, True, *args, **kwargs)

    def filter_or(self, *args, **kwargs):
        """
        Returns a new QuerySet instance with the args ORed to the existing
        set.
        """
        return self._filter_or_exclude(False, False, *args, **kwargs)

    def exclude(self, *args, **kwargs):
        """
        Returns a new QuerySet instance with NOT (args) ANDed to the existing
        set.
        """
        return self._filter_or_exclude(True, True, *args, **kwargs)

    def _filter_or_exclude(self, negate, op_and, *args, **kwargs):
        self = copy.deepcopy(self)
        if negate:
            q_obj = ~Q(*args, **kwargs)
        else:
            q_obj = Q(*args, **kwargs)

        # allow the Q instance to check for F fields to make sure that we have don't use ambiguous column names
        q_obj.verify(self)

        if not self.where:
            self.where = q_obj
        else:
            if op_and:
                self.where = self.where & q_obj
            else:
                self.where = self.where | q_obj

        return self

    def set_connection(self, connection_id):
        """
        Set connection id for query.

        :param connection_id:

        :rtype: SpannerQuerySet
        """
        self = copy.deepcopy(self)
        self.conn = Connection.get(connection_id)
        return self

    def group_by(self):
        """
        Add group by clause

        :rtype: SpannerQuerySet
        """
        self = copy.deepcopy(self)
        # todo: implement group by
        return self

    def annotate(self, *args, **kwargs):
        """

        :param args:
        :param kwargs:

        :rtype: SpannerQuerySet
        """
        self = copy.deepcopy(self)
        return self

    def aggregate(self, *args, **kwargs):
        """

        :param args:
        :param kwargs:

        :rtype: SpannerQuerySet
        """
        self = copy.deepcopy(self)
        return self

    def get(self, **filter_kwargs):
        if filter_kwargs:
            qs = self.filter(**filter_kwargs)

        # todo: return single instance or raise multiple objects
        qs.execute(fetch_one=True)

    def _check_model_joined(self, model):
        """

        :type model: ezspanner.models.SpannerModel|unicode
        :rtype: ezspanner.models.SpannerModel
        """
        if not model:
            raise QueryError("No mode specified?!")

        # base table is always joined :-)
        if model == self.model:
            return model

        # check model / alias
        model = self.joins.get(model)
        if model is None:
            raise QueryError("Model/alias '%s' is unknown")

        return model

    def _get_select_columns(self):

        # if base model fields are not defined: select all fields
        columns = []
        if self.selected_fields.get(self.model) is None:
            columns = [str(F(self.model._meta.table, f.name)) for f in self.model._meta.local_fields]

        # additional fields from joins
        for model_or_alias, columns in six.iteritems(self.selected_fields):
            columns.extend([str(f) for f in columns])

        return columns

    def _build_query(self):
        query_fragments = []

        # SELECT
        columns = self._get_select_columns()
        query_fragments.append("SELECT `%s`" % ','.join(columns))

        # FROM
        query_fragments.append("FROM %s" % self._build_table_name(self.model, self.selected_index))

        # JOIN
        if self.joins:
            pass

        # WHERE
        where = self._build_where()
        if where:
            query_fragments.append(where)

        return '\n'.join(query_fragments)

    def _build_table_name(self, model, index):
        table = '`%s`' % model._meta.table
        if index:
            table += '@{FORCE_INDEX=%s}' % index
        return table
    
    def _build_where(self):
        where = ''
        if self.where:
            where = 'WHERE ' + self._resolve_q(self.where)

        return where

    def _resolve_q(self, q):
        """
        Helper method to recursively resolve a Q tree.

        :type q: Q

        :rtype: unicode
        :return: filter condition string
        """
        connector = q.connector

        model = q.model or self.model

        sql_fragments = []
        for child in q.children:
            if isinstance(child, Q):
                sql_fragments.append('(' + self._resolve_q(child) + ')')
            else:
                column, value = child
                column_split = column.rsplit(LOOKUP_SEP, 1)

                # extract filter op type
                column = column_split[0]
                op = column_split[1] if len(column_split) == 2 else 'eq'
                op_type = FilterRegistry.registered_types.get(op)
                if not op_type:
                    raise QueryError("unregistered filter op type '%s'" % op)

                # get field
                field = model._meta.field_lookup[column]

                # build filter clause and append
                sql_fragments.append(op_type.as_sql(self, field, value))

        sql = (' %s ' % connector).join(sql_fragments)
        if q.negated:
            sql = ' NOT (%s) ' % sql
        return sql
    
    @property
    def query(self):
        """

        :rtype: unicode
        :return: return sql query for execution.
        """
        return self._build_query()

    def execute(self, connection_id=None, transaction=True, fetch_one=False):
        """

        :param connection_id:
        :param transaction:
        :param fetch_one:
        """
        self = self.set_connection(connection_id)

        if transaction:
            self.conn.run_in_transaction(self.run_in_transaction)

    def run_in_transaction(self, transaction):
        pass

#
# QUERY FILTERS
#


class FilterRegistry(object):

    registered_types = dict()

    @classmethod
    def register(cls, filter_class):
        if filter_class.operator is None:
            return

        if cls.registered_types.get(filter_class.operator) is not None:
            raise ValueError("Filter operator '%s' is already taken by '%s'" %
                             (filter_class.operator, cls.registered_types[filter_class.operator]))
        cls.registered_types[filter_class.operator] = filter_class


class FilterMeta(type):

    def __new__(cls, name, bases, attrs):
        super_new = super(FilterMeta, cls).__new__

        # Also ensure initialization is only performed for subclasses of FilterBase
        # (excluding Model class itself).
        parents = [b for b in bases if isinstance(b, FilterMeta)]
        if not parents:
            return super_new(cls, name, bases, attrs)

        # Create the class.
        module = attrs.pop('__module__')
        new_class = super_new(cls, name, bases, {'__module__': module})

        # add attributes
        for obj_name, obj in attrs.items():
            setattr(new_class, obj_name, obj)

        # register filter
        FilterRegistry.register(new_class)

        return new_class


class FilterBase(six.with_metaclass(FilterMeta)):
    operator = None

    @classmethod
    def as_sql(cls, qs, field, value):
        raise NotImplementedError


class FilterEquals(FilterBase):
    operator = 'eq'
    sql_op = '='

    @classmethod
    def as_sql(cls, qs, field, value):
        if isinstance(value, F):
            return '`{0}`.`{1}` {op} {2!s}'.format(field.model._meta.table, field.name, value, op=cls.sql_op)
        else:
            # todo: support alias of model in case of multiple joins
            param_placeholder = qs.add_param(field, value)
            return '`{0}`.`{1}` {op} @{2}'.format(field.model._meta.table, field.name, param_placeholder, op=cls.sql_op)


class FilterGte(FilterEquals):
    operator = 'gte'
    sql_op = '>='


class FilterGt(FilterEquals):
    operator = 'gt'
    sql_op = '>='


class FilterLte(FilterEquals):
    operator = 'lte'
    sql_op = '<='


class FilterLt(FilterEquals):
    operator = 'lt'
    sql_op = '<'

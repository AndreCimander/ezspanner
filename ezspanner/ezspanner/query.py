# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

import copy
from collections import OrderedDict

import six
from six import iteritems

from ezspanner.query_utils import LOOKUP_SEP, Q
from .exceptions import ModelError, SpannerIndexError, QueryError
from .connection import Connection


# noinspection PyMethodFirstArgAssignment
class SpannerQueryset(object):

    JOIN_LEFT = 'LEFT'
    JOIN_RIGHT = 'RIGHT'
    JOIN_INNER = 'INNER'
    JOIN_CROSS = 'CROSS'
    JOIN_FULL = 'FULL'

    def __init__(self, model=None):
        self.model = model
        self.conn = None
        self.joins = OrderedDict()
        self.selected_index = None
        self.selected_fields = OrderedDict()
        self.filter_conditions = OrderedDict()
        self.where = None
        self.params = {}
        self._result_cache = None

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

    def index(self, index_name):
        """
        Force index usage for base query model.

        :param index_name:
        :rtype: SpannerQueryset
        """
        self = copy.deepcopy(self)
        # check if index exists
        if not self.model._meta.index_lookup.get(index_name) and index_name != '_BASE_TABLE':
            raise SpannerIndexError("invalid index specified! '%s' is not a valid index name for model '%s'" %
                                    (index_name, self.model))

        # set index for query
        self.selected_index = index_name
        return self

    def join(self, model, model_alias=None, join_type=JOIN_INNER, join_index=None, *select_fields, **join_on_fields):
        """

        :param model:
        :param model_alias:
        :param join_type:
        :param join_index:
        :param select_fields:
        :param join_on_fields:

        :rtype: SpannerQueryset
        """
        self = copy.deepcopy(self)
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

        Reset fields with single None argument, e.g. .values(None), you can use the keyword argument reset_all=True to
        also reset all field selections from joins.

        :param fields:
        :param kwargs:

        :rtype: SpannerQueryset
        """
        self = copy.deepcopy(self)

        # check if model is already joined
        self._check_model_joined(kwargs.get('model'))
        model = kwargs.get('model') or self.model

        # empty fields -> reset value selection
        if len(fields) == 1 and fields[0] is None:
            self._reset_values(model=model, reset_all=kwargs.get('reset_all', False))
            return self

        for f in fields:
            if not model._meta.field_lookup.get(f):
                raise ModelError("'%s' is an invalid field for model '%s'" % (f, model))

        self.selected_fields[model] = set(fields)

        return self

    def filter_old(self, model=None, **kwargs):
        """

        :param model:
        :param kwargs:

        :rtype: SpannerQueryset
        """
        self = copy.deepcopy(self)

        # check if model is already joined
        self._check_model_joined(model)
        model = model or self.model
        
        if self.filter_conditions.get(model) is None:
            self.filter_conditions = []
        for key, value in iteritems(kwargs):
            key_split = key.rsplit(LOOKUP_SEP, 1)

            key = key.rsplit(LOOKUP_SEP, 1)[0]
            op = key_split[1] if len(key_split) == 2 else 'eq'
            op_type = FilterRegistry.registered_types.get(op)

            if not op_type:
                raise QueryError("unregistered filter op type '%s'" % op)

            self.filter_conditions[model].append({
                'field': key,
                'value': value,
                'type': op_type
            })

        return self

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

        :rtype: SpannerQueryset
        """
        self = copy.deepcopy(self)
        self.conn = Connection.get(connection_id)
        return self

    def annotate(self, *args, **kwargs):
        """

        :param args:
        :param kwargs:

        :rtype: SpannerQueryset
        """
        self = copy.deepcopy(self)
        return self

    def aggregate(self, *args, **kwargs):
        """

        :param args:
        :param kwargs:

        :rtype: SpannerQueryset
        """
        self = copy.deepcopy(self)
        return self

    def get(self, **filter_kwargs):
        if filter_kwargs:
            qs = self.filter(**filter_kwargs)

        # todo: return single instance or raise multiple objects
        qs.execute(fetch_one=True)

    def _check_model_joined(self, model):
        if model and model != self.model and self.joins.get(model) is None:
            raise QueryError("Model '%s' is not yet joined")

    def _get_select_columns(self):

        # if base model fields are not defined: select all fields
        if self.selected_fields.get(self.model) is None:
            columns = ['`%s`.`%s`' % (self.model._meta.table, f.name) for f in self.model._meta.local_fields]
        else:
            columns = []

        for model, columns in iteritems(self.selected_fields):
            columns.extend(['`%s`.`%s`' % (model._meta.table, f.name) for f in self.model._meta.local_fields])

        return columns

    def _build_query(self):
        columns = self._get_select_columns()

        query = "SELECT `%(columns)s` \n" \
                "FROM %(table)s \n" \
                "%(joins)s" \
                "%(where)s" \
                % {
                    'table': self._build_table_name(self.model, self.selected_index),
                    'columns': ','.join(columns),
                    'joins': '',
                    'where': '',
                }
        return query

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

        return (' %s ' % connector).join(sql_fragments)
    
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

    @classmethod
    def as_sql(cls, qs, field, value):
        # todo: add support for F(model|'model_alias', 'field_id')
        # todo: support alias of model in case of multiple joins
        param_placeholder = qs.add_param(field, value)
        return '`{0}`.`{1}` = @{2}'.format(field.model._meta.table, field.name, param_placeholder)


class FilterGte(FilterBase):
    operator = 'gte'


class FilterGt(FilterBase):
    operator = 'gt'

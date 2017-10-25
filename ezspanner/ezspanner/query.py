# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

import copy
from collections import OrderedDict
from six import iteritems

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

    def index(self, index_name):
        """
        Force index usage for base query model.

        :param index_name:
        :rtype: SpannerQueryset
        """
        self = copy.copy(self)
        # check if index exists
        if not self.model._meta.index_lookup.get(index_name) and index_name != '_BASE_TABLE':
            raise SpannerIndexError("invalid index specified! '%s' is not a valid index name for model '%s'" %
                                    (index_name, self.model))

        # set index for query
        self.selected_index = index_name
        return self

    def join(self, table, join_type=JOIN_INNER, join_index=None, *select_fields, **join_on_fields):
        """

        :param table:
        :param join_type:
        :param join_index:
        :param select_fields:
        :param join_on_fields:

        :rtype: SpannerQueryset
        """
        self = copy.copy(self)
        return self

    def _reset_values(self, model=None, reset_all=False):
        if reset_all:
            self.selected_fields = OrderedDict()
        elif self.selected_fields.get(model) is not None:
            del self.selected_fields[model]

    def values(self, *fields, **kwargs):
        """

        Reset fields with single None argument, e.g. .values(None), you can use the keyword argument reset_all=True to
        also reset all field selections from joins.

        :param fields:
        :param kwargs:

        :rtype: SpannerQueryset
        """
        self = copy.copy(self)

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

    def filter(self, model=None, **kwargs):
        """

        :param model:
        :param kwargs:

        :rtype: SpannerQueryset
        """
        self = copy.copy(self)

        # check if model is already joined
        self._check_model_joined(model)
        model = model or self.model

        # todo: implement filter query commands -> __gte, __gt, __lte, __lt, __in
        return self

    def set_connection(self, connection_id):
        """
        Set connection id for query.

        :param connection_id:

        :rtype: SpannerQueryset
        """
        self = copy.copy(self)
        self.conn = Connection.get(connection_id)
        return self

    def annotate(self, *args, **kwargs):
        """

        :param args:
        :param kwargs:

        :rtype: SpannerQueryset
        """
        self = copy.copy(self)
        return self

    def aggregate(self, *args, **kwargs):
        """

        :param args:
        :param kwargs:

        :rtype: SpannerQueryset
        """
        self = copy.copy(self)
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

        :rtype: SpannerQueryset
        """
        self = self.set_connection(connection_id)

        if transaction:
            self.conn.run_in_transaction(self.run_in_transaction)

        return self

    def run_in_transaction(self, transaction):
        pass

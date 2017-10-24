# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

import copy
from collections import OrderedDict

from .exceptions import ModelError, SpannerIndexError
from .connection import Connection


class SpannerQueryset(object):
    pass

    def __init__(self, model):
        self.model = model
        self.conn = None
        self.joins = OrderedDict()
        self.selected_index = None
        self.selected_fields = OrderedDict()

    def index(self, index_name):
        self = copy.copy(self)
        # check if index exists
        if not self.model._meta.index_lookup.get(index_name) and index_name != '_BASE_TABLE':
            raise SpannerIndexError("invalid index specified! '%s' is not a valid index name for model '%s'" %
                                    (index_name, self.model))

        # set index for query
        self.selected_index = index_name
        return self

    def join(self, table, join_type='LEFT', join_index=None, *select_fields, **join_on_fields):
        self = copy.copy(self)
        return self

    def _reset_values(self):
        self.selected_fields = OrderedDict()

    def values(self, *fields, **kwargs):
        self = copy.copy(self)
        # empty fields -> reset value selection
        if not fields:
            # todo: decide if we reset also selects from joins?
            self._reset_values()
            return self

        model = kwargs.get('model') or self.model
        for f in fields:
            if not model._meta.field_lookup.get(f):
                raise ModelError("'%s' is an invalid field for model '%s'" % (f, model))

        self.selected_fields[model] = set(fields)

        return self

    def filter(self, model=None, **kwargs):
        self = copy.copy(self)
        model = model or self.model
        return self

    def set_connection(self, connection_id):
        self = copy.copy(self)
        self.conn = Connection.get(connection_id)
        return self

    def _get_columns(self):
        columns = []

    def execute(self, connection_id=None, transaction=True):
        self = copy.copy(self)
        self.set_connection(connection_id)

        columns = self._get_columns()


        if transaction:
            self.conn.run_in_transaction(self.run_in_transaction)

        return self

    def run_in_transaction(self, transaction):
        pass

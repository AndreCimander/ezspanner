# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

from collections import OrderedDict

from .connection import Connection


class SpannerQueryset(object):
    pass

    def __init__(self, model):
        self.model = model
        self.conn = None
        self.joins = []
        self.selected_fields = OrderedDict()

    def index(self, index_name):
        return self

    def join(self, table, join_type='LEFT', join_index=None, *select_fields, **join_on_fields):
        return self

    def values(self, *fields):
        return self

    def filter(self, **kwargs):
        return self

    def set_connection(self, connection_id):
        self.conn = Connection.get(connection_id)

    def execute(self, connection_id=None, transaction=True):
        self.set_connection(connection_id)

        if transaction:
            self.conn.run_in_transaction(self.run_in_transaction)

        return self

    def run_in_transaction(self, transaction):
        pass

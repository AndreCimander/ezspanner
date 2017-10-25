# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
from future.builtins import *

from unittest import TestCase

from .helper import TestModelB
from ...exceptions import SpannerIndexError, ModelError


class SpannerQuerysetTests(TestCase):

    def test_valid_index(self):
        qs = TestModelB().objects
        qs = qs.index('over9000')
        self.assertEqual(qs.selected_index, 'over9000')

        self.assertEqual(qs._build_table_name(qs.model, qs.selected_index),
                         '`%s`@{FORCE_INDEX=over9000}' % TestModelB._meta.table)

    def test_invalid_index(self):
        qs = TestModelB().objects
        self.assertRaises(SpannerIndexError, qs.index, 'nope')

    def test_values(self):
        qs = TestModelB.objects

        # valid field
        qs = qs.values('value_field_x', 'value_field_y', 'value_field_x')
        self.assertEqual(len(qs.selected_fields.keys()), 1)
        self.assertEqual(len(qs.selected_fields[TestModelB]), 2)

        # test reset
        qs = qs.values(None)
        self.assertEqual(len(qs.selected_fields.keys()), 0)

        # test invalid field
        self.assertRaises(ModelError, qs.values, 'nope_field')

        # todo: test reset with joined model
        # todo: test reset with all

    def test_columns(self):
        qs = TestModelB.objects
        # if nothing is set: return all columns
        columns = qs._get_select_columns()
        self.assertEqual(len(columns), 5)
        self.assertEqual(columns[0], '`model_b`.`id_a`')
        self.assertEqual(columns[1], '`model_b`.`id_b`')
        self.assertEqual(columns[4], '`model_b`.`value_field_z`')

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
        qs = qs.values()
        self.assertEqual(len(qs.selected_fields.keys()), 0)

        # test invalid field
        self.assertRaises(ModelError, qs.values, 'nope_field')

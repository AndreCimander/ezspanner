# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
from future.builtins import *

from unittest import TestCase

from ezspanner import SpannerModelRegistry
from ezspanner.tests.v1.helper import TestModelA, TestModelB, TestModelC


class SpannerModelTests(TestCase):

    def test_get_registered_models_in_correct_order(self):
        sorted_models = list(SpannerModelRegistry.get_registered_models_in_correct_order())
        self.assertEqual(len(sorted_models), 3)
        self.assertIsInstance(sorted_models[0], TestModelA)
        self.assertIsInstance(sorted_models[1], TestModelB)
        self.assertIsInstance(sorted_models[2], TestModelC)

    def test_stmt_create(self):
        create_statements = SpannerModelRegistry.create_table_statements()
        pass

    def test_stmt_drop(self):
        pass

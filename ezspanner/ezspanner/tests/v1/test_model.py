# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
from future.builtins import *

from unittest import TestCase

from ezspanner import SpannerModelRegistry
from ezspanner.tests.v1.helper import TestModelA, TestModelB, TestModelC, TestModelD


class SpannerModelTests(TestCase):

    def test_get_registered_models_in_correct_order(self):
        sorted_models = list(SpannerModelRegistry.get_registered_models_in_correct_order())
        self.assertEqual(len(sorted_models), 3)
        self.assertIsInstance(sorted_models[0], TestModelA)
        self.assertIsInstance(sorted_models[1], TestModelB)
        self.assertIsInstance(sorted_models[2], TestModelC)

    def test_stmt_create(self):
        ddl_statements = SpannerModelRegistry.create_table_statements()
        self.assertEqual(len(ddl_statements), 6)

    def test_prio_dict(self):
        prio_dict = SpannerModelRegistry.get_registered_models_prio_dict()
        prio_dict_lookup = {}
        for i in range(0, 10):
            for cls in prio_dict[i]:
                prio_dict_lookup[cls] = i

        self.assertEqual(prio_dict_lookup[TestModelA], 0)
        self.assertEqual(prio_dict_lookup[TestModelB], 1)
        self.assertEqual(prio_dict_lookup[TestModelD], 1)
        self.assertEqual(prio_dict_lookup[TestModelC], 2)

    def test_stmt_delete(self):
        ddl_statements = SpannerModelRegistry.delete_table_statements()
        self.assertEqual(len(ddl_statements), 4)
        self.assertEqual(ddl_statements[0], 'DROP TABLE `model_a`')
        self.assertEqual(ddl_statements[1], 'DROP TABLE `model_b`')
        self.assertEqual(ddl_statements[2], 'DROP TABLE `model_d`')
        self.assertEqual(ddl_statements[3], 'DROP TABLE `model_c`')

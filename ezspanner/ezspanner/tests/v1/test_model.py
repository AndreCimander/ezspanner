# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
from future.builtins import *

from unittest import TestCase

from ... import SpannerModelRegistry
from .helper import TestModelA, TestModelB, TestModelC, TestModelD


class SpannerModelTests(TestCase):

    def test_get_registered_models_in_correct_order(self):
        sorted_models = list(SpannerModelRegistry.get_registered_models_in_correct_order())
        self.assertEqual(len(sorted_models), 3)
        self.assertIsInstance(sorted_models[0], TestModelA)
        self.assertIsInstance(sorted_models[1], TestModelB)
        self.assertIsInstance(sorted_models[2], TestModelC)

    def test_stmt_create(self):
        ddl_statements = SpannerModelRegistry.create_table_statements()
        self.assertEqual(len(ddl_statements), 10)

        # ModelA
        self.assertEqual(ddl_statements[0], """CREATE TABLE `model_a` (
`id_a` INT64 NOT NULL,
`field_int_not_null` INT64 NOT NULL,
`field_int_null` INT64 NULL,
`field_string_not_null` INT64 NOT NULL,
`field_string_null` STRING(200) NULL
) PRIMARY KEY (`id_a` );""")

        # interleave index test
        # fixme: move to own test

        self.assertEqual(ddl_statements[3], "CREATE INDEX `interleaved` ON `model_b` (`id_a` , `idb_b`  DESC, `value_field_x` , `value_field_y` ), INTERLEAVE IN `model_a`;")

        # ModelD
        self.assertEqual(ddl_statements[6], """CREATE TABLE `model_d` (
`id_a` INT64 NOT NULL,
`id_b` INT64 NOT NULL,
`value_field_x` INT64 NULL,
`value_field_y` INT64 NULL,
`value_field_z` INT64 NULL,
`id_d` INT64 NOT NULL
) PRIMARY KEY (`id_a` , `id_b` ) INTERLEAVE IN `model_a ` ON DELETE CASCADE;""")

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

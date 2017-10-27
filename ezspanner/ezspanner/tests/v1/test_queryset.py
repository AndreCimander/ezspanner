# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
from future.builtins import *

from unittest import TestCase

from ezspanner.query_utils import Q, F
from .helper import TestModelB, TestModelA
from ...exceptions import SpannerIndexError, ModelError, QueryError, QueryJoinError


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

    def test_q(self):
        pass

    def test_f(self):
        # test hashing to create sets
        f_set = {F('a'), F('a'), F(TestModelB, 'id_b'), F(TestModelB, 'id_b')}
        self.assertEqual(len(f_set), 2)

        # todo: test model/alias lookup
        pass

    def test_where(self):
        qs = TestModelB.objects.filter(Q(id_b=2, id_a=3) | ~Q(id_b=3, id_a=2), value_field_z=F('id_a'))
        where = qs._build_where()
        self.assertEqual(
            where,
            'WHERE ((`model_b`.`id_b` = @id_b AND `model_b`.`id_a` = @id_a) OR ( NOT (`model_b`.`id_b` = @id_b_1'
            ' AND `model_b`.`id_a` = @id_a_1) )) AND `model_b`.`value_field_z` = `model_b`.`id_a`')

    def test_join(self):
        qs = TestModelB.objects.join(TestModelA, on=dict(id_a=F(TestModelB, 'id_a')))

        self.assertEqual(len(qs.joins), 1)
        self.assertEqual(len(qs.selected_fields[TestModelA]), 5)
        self.assertEqual(qs.selected_fields[TestModelA], [
            F(TestModelA, 'id_a'),
            F(TestModelA, 'field_int_not_null'),
            F(TestModelA, 'field_int_null'),
            F(TestModelA, 'field_string_not_null'),
            F(TestModelA, 'field_string_null'),
        ])

        # same-model-join without alias fails
        self.assertRaises(QueryJoinError, qs.join, TestModelA, on=dict(id_a=F(TestModelB, 'id_a')))

        # same-model-join with alias
        qs = qs.join(TestModelA, alias='t', join_type=qs.JOIN_FULL, on=dict(id_a=F(TestModelB, 'id_a')))

        # test selected fields
        self.assertEqual(len(qs.joins), 2)
        self.assertEqual(len(qs.selected_fields[TestModelA]), 5)
        self.assertEqual(len(qs.selected_fields['t']), 5)
        self.assertEqual(qs.joins['t']['model'], TestModelA)

        self.assertEqual(
            qs._build_select_columns(),
            u'`model_b`.`id_a`, `model_b`.`id_b`, `model_b`.`value_field_x`, `model_b`.`value_field_y`, '
            u'`model_b`.`value_field_z`, `model_a`.`id_a`, `model_a`.`field_int_not_null`, `model_a`.`field_int_null`, '
            u'`model_a`.`field_string_not_null`, `model_a`.`field_string_null`, `t`.`id_a`, `t`.`field_int_not_null`, '
            u'`t`.`field_int_null`, `t`.`field_string_not_null`, `t`.`field_string_null`')

        self.assertEqual(qs._build_joins(), 'INNER JOIN `model_a` ON `model_a`.`id_a` = `model_b`.`id_a` '
                                            'FULL JOIN `model_a` AS `t` ON `t`.`id_a` = `model_b`.`id_a`')

# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
from future.builtins import *


import ezspanner


class TestModelA(ezspanner.SpannerModel):
    class Meta:
        table = 'model_a'
        pk = ['id_a']

    id_a = ezspanner.IntField()
    field_int_not_null = ezspanner.IntField()
    field_int_null = ezspanner.IntField(null=True)

    field_string_not_null = ezspanner.IntField(length=200)
    field_string_null = ezspanner.StringField(length=200, null=True)


class TestModelB(ezspanner.SpannerModel):
    class Meta:
        table = 'model_b'
        pk = ['id_b']
        parent = TestModelA
        indices = [
            ezspanner.SpannerIndex('over9000', fields=['-idb_b', 'value_field_x', '-value_field_y'])
        ]

    id_b = ezspanner.IntField()
    value_field_x = ezspanner.IntField()
    value_field_y = ezspanner.IntField()
    value_field_z = ezspanner.IntField()


class TestModelC(ezspanner.SpannerModel):
    class Meta:
        table = 'model_c'
        pk = ['id_c']
        parent = TestModelB

    id_c = ezspanner.IntField()


class TestModelD(TestModelB):
    class Meta:
        table = 'model_d'

    id_d = ezspanner.IntField()


class TestModelNotRegistered(TestModelC):
    class Meta:
        abstract = True

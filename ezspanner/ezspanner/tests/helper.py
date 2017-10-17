# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
from future.builtins import *


import ezspanner


@ezspanner.register()
class TestModelA(ezspanner.SpannerModel):
    class Meta:
        table = 'model_a'
        pk = ['id_a']

    id_a = ezspanner.IntField()
    field_int_not_null = ezspanner.IntField()
    field_int_null = ezspanner.IntField(null=True)

    field_string_not_null = ezspanner.IntField(length=200)
    field_string_null = ezspanner.StringField(length=200, null=True)


@ezspanner.register()
class TestModelB(ezspanner.SpannerModel):
    class Meta:
        table = 'model_b'
        pk = ['id_b']
        parent = TestModelA

    id_b = ezspanner.IntField()


@ezspanner.register()
class TestModelC(ezspanner.SpannerModel):
    class Meta:
        table = 'model_c'
        pk = ['id_c']
        parent = TestModelB

    id_c = ezspanner.IntField()


class TestModelNotRegistered(TestModelC):
    pass

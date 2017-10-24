# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
from future.builtins import *

from unittest import TestCase

from .helper import TestModelB
from ...exceptions import SpannerIndexError


class SpannerQuerysetTests(TestCase):

    def test_valid_index(self):
        qs = TestModelB().objects
        qs.index('over9000')
        self.assertEqual(qs.selected_index, 'over9000')

    def test_invalid_index(self):
        qs = TestModelB().objects
        self.assertRaises(SpannerIndexError, qs.index, 'nope')

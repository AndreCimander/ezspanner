# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

from unittest import TestCase

from future.builtins import *

from ezspanner.helper import get_valid_instance_from_class
from ezspanner.tests.v1.helper import TestModelA, TestModelB


class HelperGetValidInstanceTests(TestCase):

    def test_value_error(self):
        self.assertRaises(ValueError, get_valid_instance_from_class, None)
        self.assertRaises(ValueError, get_valid_instance_from_class, TestModelA, TestModelB)

    def test_already_instance(self):
        obj = get_valid_instance_from_class(TestModelA(), TestModelA)
        self.assertIsInstance(obj, TestModelA)

    def test_create_instance(self):
        obj = get_valid_instance_from_class(TestModelA, TestModelA)
        self.assertIsInstance(obj, TestModelA)

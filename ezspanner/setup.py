# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
from setuptools import setup

setup(name='ezspanner',
      version='0.0.1',
      description='A minimal wrapper around Google\'s Cloud Spanner to improve my quality of life. '
      'This package is inspired by Django\'s ORM, but follows a more basic approach since a) I\'m on my own '
      'b) Google hopefully releases ORM support soon.',
      url='http://github.com/ACimander/ezspanner',
      author='André Cimander',
      author_email='a.cimander+ezspanner@gmail.com',
      license='MIT',
      packages=['ezspanner'],
      zip_safe=False)

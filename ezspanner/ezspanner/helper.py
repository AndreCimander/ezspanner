# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
import inspect


def get_valid_instance_from_class(obj_or_class, valid_class_types=None, *instance_args, **instance_kwargs):
    """
    2-Step function to instantiate/validate a given parameter.

    First step: check if `obj_or_class` is a class or an instance.
    If it is a class, create a new instance of that class.

    Second step: (optional) validate if the instance is of type `valid_class_types`.


    :param obj_or_class:
    :param valid_class_types: list or tuple of valid class types.
    :param instance_args:
    :param instance_kwargs:

    :raise ValueError: if valid_class_types is given and isinstance fails.

    :return: instance of type `obj_or_class`/`obj_or_class.__class__`
    """
    if obj_or_class is None:
        raise ValueError("`obj_or_class` may not be None.")

    # check if we already have an instance
    if inspect.isclass(obj_or_class):
        instance = obj_or_class(*instance_args, **instance_kwargs)
    else:
        instance = obj_or_class

    # check if we need to validate for specific classes
    if valid_class_types and not isinstance(instance, valid_class_types):
        raise ValueError("%s is not of of type %s" % (instance, valid_class_types))

    return instance

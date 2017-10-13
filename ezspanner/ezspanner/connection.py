# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals
from google.cloud import spanner


class Connection(object):

    database = None

    connection_configs = {}

    @classmethod
    def add_config(cls, spanner_instance, spanner_database, connection_id='default'):
        """

        :param spanner_instance:
        :param spanner_database:
        :param connection_id:
        """
        cls.connection_configs[connection_id] = {
            'spanner_instance': spanner_instance,
            'spanner_database': spanner_database,
        }

    @classmethod
    def get(cls, connection_id=None, create_database=False):
        """


        :param connection_id:

        :rtype: google.cloud.spanner.database.Database
        """
        connection_id = connection_id or 'default'

        # verify connection id
        if connection_id not in cls.connection_configs:
            raise ValueError("[EZSpanner] invalid connection_id key!")

        # create client
        client = spanner.Client()
        instance = client.instance(cls.connection_configs[connection_id]['spanner_instance'])

        # create database connection
        database = instance.database(cls.connection_configs[connection_id]['spanner_database'])

        return database


"""

# set your connection information
Connection.add_config('your-instance-id', 'your-database-id')

# register your models

# get database connection, create database and registered models if it doesn't exist 
connection = Connection.get()

"""

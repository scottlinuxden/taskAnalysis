# coding=utf-8
""" Module Docstring
"""
__author__ = 'Ian Davis'


class DatabaseDriver(object):
    """ Define a basic interface for a database driver.

        :param filepath: The path to the database file to manage.
        :param table: The name of the table to set as the active table in the database.
    """

    def __init__(self, filepath, table=None):
        self.filepath = filepath
        self.table = table

    @staticmethod
    def create_connection(filepath):
        """ Create the underlying database connection for this driver.

            :param filepath: The path to the database file.
        """
        raise NotImplementedError('Abstract method declaration')

    def execute(self, command):
        """ Execute the given SQL command on the underlying database driver, returning any results.

            :param command: The SQL command to execute.
        """
        raise NotImplementedError('Abstract method declaration')

    def select(self, where=None):
        """ Execute a select query on the current database table, using the given where criteria to filter records.

            :param where: A dictionary of {column: value} pairs that will be used to generate a where SQL query.
        """
        raise NotImplementedError('Abstract method declaration')

    def select_all(self):
        """ Execute a select * query, returning all records in the current database table.
        """
        raise NotImplementedError('Abstract method declaration')

    def insert(self, data, replace=False):
        """ Execute an insert statement on the current database table, using the given data.

            :param data: A dictionary of {column: value} pairs that will be inserted as a new record.
            :param replace: A flag to tell the driver to optionally overwrite a record with the same primary key.
        """
        raise NotImplementedError('Abstract method declaration')

    def update(self, values, where=None):
        """ Execute an update statement on the current database table, using the given data.

            :param values: A dictionary of {column:value} pairs that will be written to the record.
            :param where: A dictionary of {column:value} pairs that will be used to query for the record(s) to update.
        """
        raise NotImplementedError('Abstract method declaration')

    def delete(self, where=None):
        """ Execute a delete statement on the current database table, using the given data.

            :param where: A dictionary of {column:value} pairs that will be used to query for the record(s) to delete.
                Defaults to None, in which case all records in the table will be deleted.
        """
        raise NotImplementedError('Abstract method declaration')

    def header(self):
        """ Query and return a descriptor of the header for the current database table.
        """
        raise NotImplementedError('Abstract method declaration')

    def tables(self):
        """ Query and return a list of all tables present in the database.
        """
        raise NotImplementedError('Abstract method declaration')

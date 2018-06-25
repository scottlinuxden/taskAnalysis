# coding=utf-8
import sql
import logging

from collections import OrderedDict


class DatabaseError(Exception):
    pass


class DatabaseWarning(UserWarning):
    pass


def format_value(value):
    # CSM-736: Only escape quotes if the incoming value is a string,
    # because otherwise this is an error and there's no point in doing it anyway.
    # The database expects string values though, so we just cast whatever it is as a string.
    # This shouldn't be an issue because any datatype getting put in the database needs to be sent as a string anyway.
    if isinstance(value, basestring):
        return value.replace("\"", "\"\"")
    else:
        return str(value)


class Database(object):
    _tableInfoTemplate = 'PRAGMA table_info({table})'
    _selectTemplate = 'SELECT {columns} FROM {table}'
    _insertTemplate = 'INSERT INTO {table} ({columns}) VALUES ({values})'
    _insertOrReplaceTemplate = 'INSERT OR REPLACE INTO {table} ({columns}) VALUES ({values})'
    _updateTemplate = 'UPDATE {table} SET {values}'
    _deleteTemplate = 'DELETE FROM {table}'

    _whereTemplate = ' WHERE '

    _mapClause = '"{column}"="{value}"'

    def __init__(self, databasePath, table=None):
        """ Database constructor.

            @attribute databasePath: string filepath pointing to the database to use.
        """
        self.databasePath = databasePath
        self._table = table

    def table(self, table=None):
        if table:
            self._table = table

        return self._table

    def execute(self, command):
        """ Run the given SQLite Command on the database, should
            generally not be called directly, instead use the API
            methods select, insert, update and delete.
        """
        try:
            status, results = sql.execute(self.databasePath, command)
        except Exception as err:
            logging.error(command)
            raise DatabaseError(str(err))

        if not status == 'success':
            errorMessage = ''.join(results)
            logging.error(command)
            raise DatabaseError(errorMessage)

        return results

    def generateColumnClause(self, columns):
        """ Used by SQLite command methods to generate a (col1, col2, col3, ...) formatted
            SQLite column specifier.
        """
        if not columns:
            return '*'

        formatted_columns = []

        for column in columns:
            formatted_columns.append(format_value(column))

        return '"{}"'.format('", "'.join(formatted_columns))

    def generateAssignments(self, map_, separator=', '):
        """ Used by SQLite command methods to generate a "col1"="val1", "col2"="val2", ...
            formatted SQLite value specifier.

            @param map_: A Col-Val Dictionary that is placed in the col1, val1, ... places.

            @param separator: A string that is placed inbetween each "col"="val" pair.
                -Default: ", "
        """
        equalities = []

        for column, value in map_.items():
            equalities.append(self._mapClause.format(column=format_value(column), value=format_value(value)))

        return separator.join(equalities)

    def generateWhereClause(self, where):
        """ Used by SQLite command methods to generate a WHERE "col1"="val1" AND "col2"="val2" ...
            formatted SQLite where clause.

            See: generateAssignments(map_=where, separator=' AND ')
        """
        if not where:
            return ''

        clause = self._whereTemplate + self.generateAssignments(map_=where, separator=' AND ')
        return clause

    def generateWhereLikeClause(self, where_like):
        if not where_like:
            return ''

        like_clause_pattern = "{column} LIKE '{pattern}'"
        like_clauses = []

        for column, pattern in where_like.iteritems():
            like_clauses.append(like_clause_pattern.format(column=column, pattern=pattern))

        return ' WHERE ' + ' AND '.join(like_clauses)

    def generateWhereNotClause(self, where_not):
        if not where_not:
            return ''

        not_clause_pattern = "NOT {column}='{value}'"
        not_clauses = []

        for column, value in where_not:
            not_clauses.append(not_clause_pattern.format(column=column, value=value))

        return ' WHERE ' + ' AND '.join(not_clauses)

    def generateLeftJoinClause(self, left_join):
        if not left_join:
            return ''

        left_join_template = ' LEFT JOIN {table} WHERE {where}'
        leftJoinClause = ''

        for descriptor in left_join:
            table = descriptor['table']
            whereClause = self.generateWhereClause(descriptor['where'])
            leftJoinClause += left_join_template.format(table=table, where=whereClause)

        return leftJoinClause

    def generateInnerJoinClause(self, inner_join):
        if not inner_join:
            return ''

        inner_join_template = ' INNER JOIN {table} WHERE {where}'
        innerJoinClause = ''

        for descriptor in inner_join:
            table = descriptor['table']
            whereClause = self.generateWhereClause(descriptor['where'])
            innerJoinClause += inner_join_template.format(table=table, where=whereClause)

        return innerJoinClause

    def generateValueClause(self, values):
        """ Used by SQLite command methods to generate a "col1"="val1", "col2"="val2, ...
            formatted SQLite value clause.

            See: generateAssignments(values)
        """
        if not values:
            raise DatabaseError('Incorrect or no values supplied to insert...')

        clause = self.generateAssignments(values)
        return clause

    def select(self, columns=None, where=None, where_like=None, where_not=None, inner_join=None, left_join=None):
        """ Execute a SQLite select statement and return a list of dictionary col-value
            results.

            :param columns: Any sequence of strings denoting what columns to select. Default: '*' (ALL in SQLite)
            :param where: A Col-Val dictionary denoting to select only records that match the values given.
            :param where_like: A Col-Val dictionary denoting to select only records that are "like" the values given.
            :param where_not: A Col-Val dictionary denoting to select only records that do not match the values given.
            :param inner_join: A list of dictionaries defining tables to join on and where dictionaries to join by.
            :param left_join: A list of dictionaries defining tables to join on and where dictionaries to join by.
        """
        columnClause = self.generateColumnClause(columns)
        whereClause = self.generateWhereClause(where)
        whereLikeClause = self.generateWhereLikeClause(where_like)
        whereNotClause = self.generateWhereNotClause(where_not)
        innerJoinClause = self.generateInnerJoinClause(inner_join)
        selectStatement = self._selectTemplate.format(table=self._table, columns=columnClause) + whereClause + whereLikeClause + whereNotClause + innerJoinClause
        data = self.execute(selectStatement)

        if not data:
            return

        records = []

        for record in data:
            records.append(dict(record))

        return records

    def update(self, values, where=None):
        """ Executes a SQLite update statement.

        @param values: A Col-Val dictionary with the values to update.
            @required

        @param where: A Col-Val dictionary denoting to update only records that match
                the values given.
            @Default: None
        """
        valueClause = self.generateValueClause(values)
        whereClause = self.generateWhereClause(where)
        updateStatement = self._updateTemplate.format(table=self._table, values=valueClause) + whereClause
        return self.execute(updateStatement)

    def insert_or_replace(self, values, columns=None):
        columnClause = self.generateColumnClause(columns)
        valueClause = self.generateColumnClause(values)
        insertStatement = self._insertOrReplaceTemplate.format(table=self._table, columns=columnClause, values=valueClause)
        return self.execute(insertStatement)

    def insert(self, values, columns=None, remove_unsupported=False):
        """ Execute a SQLite insert command.

            :param values: A required sequence of strings of the values to insert.
            :param columns: Any sequence of strings denoting what columns to insert.
                Note: If provided, the order of this sequence of strings should match
                    the order of the values given, as SQLite matches these up. If not
                    provided, the values sequence should match the length and order
                    of the database table header.
                Default: None (All columns will be considered)
            :param remove_unsupported: Flag to specify that the values and columns given should be filtered out to only columns supported by the active table.
        """
        if remove_unsupported:
            filtered_columns, filtered_values = self.filter_unsupported_columns(columns, values)
        else:
            filtered_columns = columns
            filtered_values = values

        columnClause = self.generateColumnClause(filtered_columns)
        valueClause = self.generateColumnClause(filtered_values)
        insertStatement = self._insertTemplate.format(table=self._table, columns=columnClause, values=valueClause)

        return self.execute(insertStatement)

    def delete(self, where=None):
        """ Execute a SQLite delete command.

        @param where: A Col-Val dictionary denoting to delete only records that match
                the values given.
            @Note: If where is NOT specified: ALL records in the table will be removed!
            @Default: None
        """
        whereClause = self.generateWhereClause(where)
        deleteStatement = self._deleteTemplate.format(table=self._table) + whereClause
        return self.execute(deleteStatement)

    def selectAll(self):
        return self.select()

    def header(self):
        _, pramga = sql.getTableInfo(self.databasePath, self._table)

        primaryKeys = OrderedDict()
        header = OrderedDict()

        for row in pramga:
            _, name, type_, _, _, primaryKey = row
            header[name] = type_

            if primaryKey:
                primaryKeys[name] = type_

        return primaryKeys, header

    def filter_unsupported_columns(self, columns, values):
        supported_columns = []
        supported_values = []
        _, database_header = self.header()

        for column, value in zip(columns, values):
            if column not in database_header:
                continue

            supported_columns.append(column)
            supported_values.append(value)

        return supported_columns, supported_values


class Header(object):
    def __init__(self):
        self._header = OrderedDict()

        self._load()

    def __iter__(self):
        for column in self._header:
            yield column

    def _load(self):
        pass


class SQLHeader(Header):
    def __init__(self, databasePath, table):
        self._databasePath = databasePath
        self._table = table

        self._primaryKeys = OrderedDict()

        Header.__init__(self)

    def _load(self):
        tableInfo = self._pragma()

        for row in tableInfo:
            cid, name, type_, notNull, defaultVal, primaryKey = row

            column = Column(name=name, type_=type_, primaryKey=primaryKey)

            self._header[name] = column

            if primaryKey:
                self._primaryKeys[name] = column

    def _pragma(self):
        return sql.getTableInfo(self._databasePath, self._table)


class Column(object):
    def __init__(self, name, type_, primaryKey):
        self.name = name
        self.type_ = type_
        self.primaryKey = primaryKey

    def name(self):
        return self.name

    def type(self):
        return self.type_

    def primaryKey(self):
        return self.primaryKey


import os
import sqlite3
import string

class SqlTable(object):
    def __init__(self, name, header=None, hlxCode='???', qntCode='???', deleteCode='???'):
        self.name = name

        if not header:
            self.header = []
        else:
            self.header = header

        self.rows = []
        self.hlxCode = hlxCode
        self.qntCode = qntCode
        self.deleteCode = deleteCode

    def getName(self):
        return self.name

    def getSqlName(self):
        return self.name.strip(' ').replace(' ', '')

    def getItems(self):
        return self.rows

    def getSqlItems(self):
        rowsParsed = []
        for row in self.rows:
            if row is not None and len(row) >= 1:
                thisRow = {}
                for columnName in row:
                    for dict in self.header:
                        if columnName == dict['name']:
                            if row[columnName] is not None and not row[columnName] == '':
                                thisRow[columnName] = row[columnName]
                rowsParsed.append(thisRow)

        return rowsParsed

    def getHeader(self):
        return self.header

    def getSqlHeader(self):
        parsedHeader = []
        if self.header is None or self.header == []:
            return '???'
        for dict in self.header:
            if 'type' in dict.keys():
                parsedHeader.append(dict['name'] + ' ' + dict['type'])
            else:
                parsedHeader.append(dict['name'] + ' text')
        parsedString = str('(' + ','.join(parsedHeader) + ')')
        return parsedString

    def getSqlHeaderByItems(self, itemsPresent):
        validHeader = []
        if self.header is None or self.header == []:
            return '???'
        for columnName in itemsPresent:
            foundHeader = False
            for headerDict in self.header:
                if columnName == headerDict['name']:
                    validHeader.append(columnName)
                    foundHeader = True
            if foundHeader == False:
                print('Could Not Find column: ' + columnName)
        headerString = str(','.join(validHeader))
        return headerString

    def getCode(self, type):
        if type == 'HLX':
            return 'RT' + self.hlxCode
        if type == 'QNT':
            return self.qntCode
        if type == 'Delete':
            return 'RT' + self.deleteCode

    def changeName(self, newName):
        self.name = newName

    def changeHeader(self, newHeader):
        self.header = newHeader

    def changeRows(self, rows):
        self.rows = rows

    def changeCode(self, type, newCode):
        if type == 'HLX':
            self.hlxCode = newCode
        else:
            self.qntCode = newCode

    def insertRow(self, items):
        self.rows.append(items)

    def insertHeaderColumn(self, headerDict):
        self.header.append(headerDict)

    def __str__(self):
        return self.getSqlName()

    def __repr__(self):
        return 'SQLTable({0})'.format(str(self))


def buildDb(SqlTables, databasePath):
    conn = sqlite3.connect(databasePath)
    conn.text_factory = str
    c = conn.cursor()

    creation_errors = []

    for table in SqlTables:
        if not table.getSqlHeader() is None and not table.getSqlHeader() == '???':
            tableName = table.getSqlName()
            productTextTables = ['Texts', 'prodTxt5Num', 'prodTxt6Num', 'prodTxt7Num', 'prodTxt8Num', 'prodTxt9Num']
            if tableName in productTextTables:
                tableName = 'ProductTexts'

            createTableCommand = 'CREATE TABLE ' + str(tableName) + ' ' + str(table.getSqlHeader())
            try:
                c.execute(createTableCommand)
#
            except Exception as err:
                pass
#
            items = table.getSqlItems()
            for rowDict in items:
                row = rowDict.values()
                for item in row:
                    if item is None or item == '':
                        row.remove(item)
                blankList = [''] * len(row)

                insertCommand = 'INSERT OR REPLACE INTO ' + tableName + \
                    ' (' + table.getSqlHeaderByItems(rowDict).replace(' text', '') + ') VALUES (' + '?,'.join(blankList) + '?)'

                if tableName == 'Prods':
                    print('inserting ' + str(row))
#
                try:
                    c.execute(insertCommand, row)
                    print('inserted row')
#
                except sqlite3.IntegrityError as error:
                    if 'Prods.PrNum' in str(error):
                        print('Prods.PrNum was Null, leaving blank value.')

                        insertCommand = 'INSERT OR REPLACE INTO ' + \
                            tableName + ' (' + table.getSqlHeaderByItems(rowDict).replace(' text', '') + ',PrNum)' \
                                + 'VALUES (' + '?,'.join(blankList) + '?,?)'
#
                        row.append('')

                        try:
                            c.execute(insertCommand, row)
#
                        except sqlite3.IntegrityError as error:
                            print('[DEBUG]-->ERR: ' + str(error))
#
                    elif 'ProductTexts.ProdTextType' in str(error):
                        tableTypes = {'Texts': 1, 'SMsgs': 2, 'Notes': 3, 'Mrquees': 4, 'prodTxt5Num': 5,
                                      'prodTxt6Num': 6, 'prodTxt7Num': 7, 'prodTxt8Num': 8, 'prodTxt9Num': 9}
#
                        tableType = tableTypes[table.getSqlName()]

                        row.append(tableType)

                        insertCommand = 'INSERT OR REPLACE INTO ' + \
                            tableName + ' (' + table.getSqlHeaderByItems(rowDict).replace(' text', '') + ',ProdTextType)' \
                                + 'VALUES (' + '?,'.join(blankList) + '?,?)'
#
                        try:
                            c.execute(insertCommand, row)
#
                        except sqlite3.IntegrityError as error:
                            print('[DEBUG]-->ERR: ' + str(error))
#
                except sqlite3.OperationalError as err:
                    if 'no such table: ProductTexts' in str(err):
                        statement = 'CREATE TABLE ProductTexts (ProdTextNum text, ProdTextDesc text)'
                        try:
                            c.execute(statement)
#
                        except Exception as err:
                            print(str(err))
                            continue

                        try:
                            c.execute(insertCommand, row)
#
                        except Exception as err:
                            print(str(err))
                            continue
#
                    else:
                        print(str(err))
                        print(insertCommand)
#
    conn.commit()
    conn.close()


def executeInsert(dbName, tableName, sqliteColumnPairs, primaryKeyPairs):
    status, tableInfo = getTableInfo(dbName,tableName)
    conn = sqlite3.connect(dbName)
    conn.row_factory = list
    conn.text_factory = str
    c = conn.cursor()
    insertCommand = 'INSERT INTO %s ' % tableName

    sqliteColumnNames = sqliteColumnPairs.keys()

    if tableName == 'Prods':
        if 'PrNum' in sqliteColumnNames:
            del sqliteColumnPairs['PrNum']

    insertCommand += '(%s) ' % ','.join(primaryKeyPairs.keys() + sqliteColumnPairs.keys())

    sqliteColumnValues = []

    for columnName in sqliteColumnPairs:
        if not sqliteColumnPairs[columnName] == '' and sqliteColumnPairs[columnName] is not None:
            sqliteColumnValues.append(sqliteColumnPairs[columnName])
        else:
            sqliteColumnValues.append(None)

    blankList = ['?'] * len(sqliteColumnValues)
    blankPrimaryKeysList = ['?'] * len(primaryKeyPairs.keys())
    insertCommand += 'VALUES (%s)' % (','.join(blankPrimaryKeysList) + ',' + ','.join(blankList))
    insertCommand = insertCommand.rstrip(',)') + ')'
    status = 'success'
    message = 'Successfully Excuted ' + insertCommand + '\nwith values ' + str(primaryKeyPairs.values() + sqliteColumnValues)
    try:
        c.execute(insertCommand, primaryKeyPairs.values() + sqliteColumnValues)
        conn.commit()
        print(message)
        print(len(blankList + blankPrimaryKeysList))
        print(len(sqliteColumnPairs.keys()))
    except Exception as error:
        print(str(error.message) + ' in ' + insertCommand + '\nwith values ' + str(primaryKeyPairs.values() + sqliteColumnValues))
        status = 'error'
        message = str(error) + ' in ' + insertCommand + ' with values ' + str(primaryKeyPairs.values() + sqliteColumnValues)
        conn.rollback()

    conn.close()
    return status, message


def getTableHeader(dbName, tableName):
    stauts, tableInfo = getTableInfo(dbName, tableName)
    return [columnInfo[1] for columnInfo in tableInfo]


def execute(dbName, statement, dictAccess=True):
    connection = None
    try:
        connection = sqlite3.connect(dbName)
        # connection.text_factory = lambda x: unicode(x, "latin_1", "replace")
        connection.text_factory = lambda x: str(x, "latin_1", "replace")

        if dictAccess:
            connection.row_factory = sqlite3.Row

        cursor = connection.cursor()
        cursor.execute(statement)
        # tokens = string.split(string.upper(string.strip(statement)), " ")
        tokens = statement.strip().upper().split(" ")

        if tokens[0] == 'SELECT' or tokens[0] == 'PRAGMA':
            rows = cursor.fetchall()
            result = ('success', rows)
        else:
            connection.commit()
            result = ('success',  None)
    except Exception as err:
        if connection:
            connection.rollback()

        result = ('error', '%s' % err)
    finally:
        if connection:
            connection.close()

    return result


def getTableInfo(dbName,  tableName):
    return execute(dbName, "PRAGMA table_info(%s)" % (tableName), False)


def getAllTableItemsAsText(dbName, tableName, columnOrder = None, orderBy = None):
    tableHeader = []
    data = []
    status = 'error'

    try:
        status,  result = getTableInfo(dbName, tableName)

        if status == 'success':
            tableDescriptor = []
            columnCasts = []

            for i in range(0, len(result)):
                tableDescriptor.append({'name': str(result[i][1]),  'type': str(result[i][2])})
#                columnCasts.append("CAST(%s as TEXT)" % (str(result[i][1])))
                columnCasts.append(str(result[i][1]))

            # columns = string.join(columnCasts, ",")
            columns = ','.join(columnCasts)

            if orderBy and columnOrder:
                status, data = execute(dbName = dbName,
                                       statement="SELECT %s FROM %s ORDER BY %s" % (columnOrder, tableName, orderBy),
                                       dictAccess=False)
            elif orderBy:
                status, data = execute(dbName=dbName, statement='SELECT %s FROM %s ORDER BY %s' % (columns,  tableName,  orderBy),  dictAccess=False)
            else:
                status, data = execute(dbName = dbName,
                                       statement="SELECT %s FROM %s" % (columns, tableName),
                                       dictAccess=False)
            # convert to None and all unicode to string
            convertedData = []

            for i in range(0, len(data)):
                rowData = []

                for j in range(0, len(data[i])):
                    if data[i][j] is None:
                        rowData.append('')
                    else:
                       rowData.append(data[i][j])

                convertedData.append(rowData)

        return status, tableDescriptor, convertedData
    except Exception as err:
        return 'error', err, None


def getAllDbTableNames(dbName):
    return execute(dbName, """
    SELECT name FROM sqlite_master
        WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%'
    UNION ALL
    SELECT name FROM sqlite_temp_master
        WHERE type IN ('table','view')
    ORDER BY 1;""", False)


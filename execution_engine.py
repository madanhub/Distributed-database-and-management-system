import os
import json
import datetime
import re
from column_data_type import ColumnDataType
from column_data_type import ColumnData
from datetime import datetime
from google.cloud import storage

class ExecutionEngine:
    global currentDatabase
    currentDatabase = ""
    currentDatabasePath = ""
    isLocal = True

    def __init__(self, queryString, identifier, rights):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'D:\Data warehousing 5408\Project\group12-5408\gcpKey.json'
        self.remoteFiles = []
        self.defaultPath = 'D:/DW/'
        self.metaDataFileName = 'metaData.txt'
        self.gddFileName = 'globalDataDictionary.txt'
        self.lddFileName = 'localDataDictionary.txt'
        self.logFile = 'logs.txt'
        self.sqlDumpFileName = 'sqlDump.txt'
        self.rights = rights
        self.createEvaluationPlan(queryString, identifier)

    def createEvaluationPlan(self, queryString, identifier):
        if identifier == 1:
            # create database
            if str(self.rights) == str(1):
                self.createDataBase(queryString)
            else:
                print("Execution Failed...Permission Denied!!");
        elif identifier == 2:
            # use database
            self.useDataBase(queryString)
        elif identifier == 3:
            # simple select query
            self.simpleSelectQuery(queryString)
        elif identifier == 4:
            # simple select with where
            self.simpleSelectWithWhereQuery(queryString)
        elif identifier == 5:
            # select with column name
            self.selectColumnQuery(queryString)
        elif identifier == 6:
            # select with column with where
            self.selectColumnWithWhereQuery(queryString)
        elif identifier == 7:
            # update with where
            self.updateColumnWithWhereQuery(queryString)
        elif identifier == 8:
            # delete with where
            self.deleteWithWhereQuery(queryString)
        elif identifier == 9:
            # simple insert
            self.simpleInsertQuery(queryString)
        elif identifier == 10:
            # drop table
            if str(self.rights) == str(1):
                self.dropTableQuery(queryString)
            else:
                print("Execution Failed...Permission Denied!!");
        elif identifier == 11:
            # create table
            if str(self.rights) == str(1):
                self.createQuery(queryString)
            else:
                print("Execution Failed...Permission Denied!!");

    def updateGDD(self, databaseName, databasePath):
        gddFilePath = os.path.join(self.defaultPath, self.gddFileName)
        fileWrite = open(gddFilePath, 'a')
        fileContent = {}
        fileContent[databaseName] = databasePath
        fileWrite.write(str(fileContent))
        fileWrite.write("*")
        fileWrite.close()

    def updateLDD(self, databaseName, databasePath):
        lddFilePath = os.path.join(self.defaultPath, self.lddFileName)
        fileWrite = open(lddFilePath, 'a')
        fileContent = {}
        fileContent[databaseName] = databasePath
        fileWrite.write(str(fileContent))
        fileWrite.write("*")
        fileWrite.close()

    def updateSQLDump(self, databaseName, databasePath, queryString):
        sqlDumpFile = databasePath + '/' + databaseName + self.sqlDumpFileName
        fileWrite = open(sqlDumpFile, 'a')
        fileWrite.write(queryString + '\n')
        fileWrite.close()

    def getGDD(self):
        return self.readFileFromBucket('remote-bucket', self.gddFileName)

    def createMetaDataFile(self, databasePath, databaseName: str):
        fileContent = {}
        fileContent[databaseName] = []
        metaDataFileName = databasePath + "/" + self.metaDataFileName
        fileWriting = open(metaDataFileName, 'w')
        fileWriting.write(str(fileContent))
        fileWriting.close()

    def getMetaDataFileContent(self, databasePath):
        metaDataFileName = databasePath + "/" + self.metaDataFileName
        fileReading = open(metaDataFileName, 'r')
        fileContent = fileReading.read()
        fileReading.close()
        return self.convertStringToDictionary(fileContent)

    def updateMetaDataFileContent(self, databasePath, fileContent):
        metaDataFileName = databasePath + "/" + self.metaDataFileName
        fileWriting = open(metaDataFileName, 'w')
        fileWriting.write(str(fileContent))
        fileWriting.close()

    def createDataBase(self, queryString):
        databaseName = ''
        listOfWords = queryString.split()

        # Find DBName
        for index in range(len(listOfWords)):
            if listOfWords[index].upper() == 'DATABASE':
                databaseName = listOfWords[index + 1]
                break

        # Generate Path
        databasePath = os.path.join(self.defaultPath, databaseName)
        print("Creating database at "+ databasePath+"...")

        # log entry for database creation
        self.enterLog("Database " + databaseName + " Created")

        # Create Database
        try:
            os.mkdir(databasePath)
            self.createMetaDataFile(databasePath, databaseName)
            print('Database Created Successfully!!')
            self.updateGDD(databaseName, databasePath)
            self.updateLDD(databaseName, databasePath)
            self.updateSQLDump(databaseName, databasePath, queryString)

            self.createBucket(databaseName)
            self.uploadFileToBucket(databaseName, self.metaDataFileName, databasePath + "/" + self.metaDataFileName)
            self.uploadFileToBucket('remote-bucket', self.gddFileName, self.defaultPath + "/" + self.gddFileName)
        except OSError as error:
            print(error)

    def useDataBase(self, queryString):
        global currentDatabase
        global currentDatabasePath
        databaseName = ''
        listOfWords = queryString.split()

        # Find DBName
        for index in range(len(listOfWords)):
            if listOfWords[index].upper() == 'DATABASE':
                databaseName = listOfWords[index + 1]
                break

        gddContent = self.getGDD()
        isValid = self.validateGDD(databaseName, gddContent)
        if isValid:
            currentDatabase = databaseName
            currentDatabasePath = self.fetchCurrentDatabasePath(gddContent, databaseName)
            # log entry for database creation
            self.enterLog("Database " + databaseName + " Accessed")
        else:
            print("Database does not exists!!")

    def createTableFileContent(self, databasePath: str, databaseName: str, tableName: str, tableColumns):
        fileContent = self.getMetaDataFileContent(databasePath)
        tableContent = {}
        tableContent[tableName] = []
        for column in tableColumns:
            if column:
                tableColumn = {}
                if column['columnData']:
                    tableColumn[column['columnName']] = column['columnData']
                tableContent[tableName].append(tableColumn)
        fileContent[databaseName].append(tableContent)
        return fileContent

    def createColumnData(self, dataType: str, length: int, isAllowNull:  bool, isAllowUnique: bool, isPrimaryKey: bool, isForeignKey: bool, fkConstraintName: str):
        columnDataType = ColumnDataType(dataType, length, isAllowNull, isAllowUnique, isPrimaryKey, isForeignKey, fkConstraintName)
        return columnDataType.__dict__

    def createColumn(self, columnName: str, dataType: str, length: int, isAllowNull:  bool, isAllowUnique: bool, isPrimaryKey: bool, isForeignKey: bool, fkConstraintName: str):
        columnDataType = self.createColumnData(dataType, length, isAllowNull, isAllowUnique, isPrimaryKey, isForeignKey, fkConstraintName)
        column = ColumnData(columnName, columnDataType)
        return column.__dict__

    def createQuery(self, queryString):
        global currentDatabasePath
        global currentDatabase
        if self.validateDatabaseSelected():
            queryArray = queryString.split()
            tableName = queryArray[2]

            pattern = "\((.*?)\)"
            columnSubString = re.search(pattern, queryString, re.IGNORECASE).group(1)

            columns = []
            columnKeys = []
            columnSubString = columnSubString.replace("(", "")
            before_keyword, keyword, after_keyword = columnSubString.partition("PRIMARY KEY")
            primaryKey = after_keyword

            allColumns = columnSubString.split(",")
            for column in allColumns:
                columnArray = column.split()
                columnName = columnArray[0]
                if columnName != 'PRIMARY':
                    columnKeys.append(columnName)
                    columnDataType = columnArray[1].upper()
                    columnLength = 'None'
                    isAllowNull = 'True'
                    isAllowUnique = 'False'

                    notNull = re.search("NOT NULL", column)
                    if notNull is not None:
                        isAllowNull = 'False'

                    unique = re.search("UNIQUE", column)
                    if unique is not None:
                        isAllowUnique = 'True'

                    isPrimaryKey = 'False'
                    isForeignKey = 'False'

                    if columnName == primaryKey:
                        isPrimaryKey = 'True'
                        isAllowUnique = 'True'
                        isAllowNull = 'False'

                    columnValue = self.createColumn(columnName, columnDataType, columnLength, isAllowNull, isAllowUnique, isPrimaryKey, isForeignKey, 'None')
                    columns.append(columnValue)

            fileContent = self.createTableFileContent(currentDatabasePath, currentDatabase, tableName, columns)
            print("Creating table at "+ currentDatabasePath+'/'+tableName)
            self.updateMetaDataFileContent(currentDatabasePath, fileContent)
            self.createTableFile(currentDatabasePath, tableName, columnKeys)
            # log entry for database creation
            self.enterLog("Table " + tableName + " Created")
            print("Table created successfully")
            self.updateSQLDump(currentDatabase, currentDatabasePath, queryString)

            self.uploadFileToBucket(currentDatabase, tableName, currentDatabasePath + '/' + tableName + '.txt')
            self.uploadFileToBucket(currentDatabase, self.metaDataFileName, currentDatabasePath + "/" + self.metaDataFileName)

    def createTableFile(self, databasePath, tableName, columnRow):
        columnRowData = []
        columnString = ""
        for colIndex in range(len(columnRow)):
            row = {}
            row['name'] = columnRow[colIndex]
            row['key'] = colIndex
            row['parentKey'] = colIndex - 1
            columnRowData.append(row)
            columnString += str(row) + "|"

        row = {}
        row['name'] = "timeStamp"
        row['key'] = len(columnRowData)
        row['parentKey'] = len(columnRowData) - 1
        columnRowData.append(row)
        columnString += str(row) + "|"

        row['name'] = "isDeleted"
        row['key'] = len(columnRowData)
        row['parentKey'] = len(columnRowData) - 1
        columnRowData.append(row)

        columnString += str(row)

        filePath = os.path.join(databasePath, tableName+".txt")
        fileWrite = open(filePath, "w")
        fileWrite.write(columnString)
        fileWrite.write('*')
        fileWrite.close()

    def dropTableQuery(self, queryString):
        global currentDatabase
        global currentDatabasePath
        queryArray = queryString.split()
        tableName = queryArray[2]
        filePath = os.path.join(currentDatabasePath, tableName+".txt")
        metaDataContent = self.getMetaDataFileContent(currentDatabasePath)
        tableDropped = False
        if metaDataContent[currentDatabase]:
            for tableIndex in range(len(metaDataContent[currentDatabase])):
                key = list(metaDataContent[currentDatabase][tableIndex].keys())
                if tableName in key:
                    tableDropped = True
                    metaDataContent[currentDatabase].pop(tableIndex)

        if tableDropped:
            print("Table dropping!!")
            self.updateMetaDataFileContent(currentDatabasePath, metaDataContent)
            os.remove(filePath)
            print("Table drop success!!")
            # log entry for database creation
            self.enterLog("Table " + tableName + " Dropped")
            self.updateSQLDump(currentDatabase, currentDatabasePath, queryString)
        else:
            print("Table not found!!")

    def simpleInsertQuery(self, queryString):
        if self.validateDatabaseSelected():
            global currentDatabasePath
            global currentDatabase
            queryArray = queryString.split()
            tableName = queryArray[2]
            pattern = "\((.*?)\)"
            columnSubString = re.search(pattern, queryString).group(1)
            columnValues = columnSubString.split(',')
            columnRow = []
            columnString = ""
            for colValue in range(len(columnValues)):
                row = {}
                row['data'] = columnValues[colValue]
                row['key'] = colValue
                columnRow.append(row)
                columnString += str(row) + "|"

            row = {}
            row['data'] = self.getCurrentTimeStampInUTC()
            row['key'] = len(columnRow)
            columnRow.append(row)
            columnString += str(row) + "|"

            row['data'] = "False"
            row['key'] = len(columnRow)
            columnRow.append(row)
            columnString += str(row)

            try:
                tableFilePath = os.path.join(currentDatabasePath, tableName+".txt")
                tableContent = self.readDataFromTableFile(tableFilePath)
                tableContent = tableContent + columnString + "*"
                print("Inserting in table" + tableName + "...")
                self.writeInTableFile(tableFilePath, tableContent)
                print("Inserted Successfully!!")
                # log entry for database creation
                self.enterLog("Insert " + tableName + " Values" + columnSubString)
                self.updateSQLDump(currentDatabase, currentDatabasePath, queryString)
            except Exception as error:
                print("Inserting in table" + tableName + "...")
            finally:
                self.uploadFileToBucket(currentDatabase, tableName + '.txt', tableFilePath)

    def readDataFromTableFile(self, filePath):
        fileRead = open(filePath, 'r')
        fileContent = fileRead.read()
        return fileContent

    def writeInTableFile(self, filePath, fileContent):
        fileWrite = open(filePath, 'w')
        fileWrite.write(fileContent)
        fileWrite.close()

    def simpleSelectQuery(self, queryString, displayFormatted = True):
        if self.validateDatabaseSelected():
            global currentDatabasePath
            global currentDatabase
            queryArray = queryString.split()
            tableName = queryArray[3]
            filePath = currentDatabasePath + '/' + tableName+'.txt'
            fileRead = open(filePath, 'r')
            fileContent = fileRead.read()
            if displayFormatted:
                self.formatSelectOutput(fileContent)
            else:
                return fileContent
        if currentDatabase == "":
            print("No database selected.")

    def simpleSelectWithWhereQuery(self, queryString):
        if self.validateDatabaseSelected():
            global currentDatabasePath;
            queryArray = queryString.split()
            tableName = queryArray[3]
            filePath = currentDatabasePath + '/' + tableName + '.txt'
            fileRead = open(filePath, 'r')
            fileContent = fileRead.read()

            conditionColumnPatten =  "WHERE\s(.*?)="
            conditionColumn = re.search(conditionColumnPatten, queryString, re.IGNORECASE).group(1)

            conditionColumnValuePattern = "\"(.*?)\""
            conditionColumnValue = re.search(conditionColumnValuePattern, queryString).group(1)

            filteredString = self.filterSelectOutput(conditionColumn, conditionColumnValue, fileContent)
            self.formatSelectOutput(filteredString)
        if currentDatabase == "":
            print("No database selected.")

    def filterSelectOutput(self, columnName, columnValue, fileContent):
        filteredString = ""
        columnKey = -1
        lines = fileContent.split("*")
        for lineIndex in range(len(lines)):
            rowData = lines[lineIndex].split("|")
            if lineIndex == 0:
                for row in rowData:
                    if row != "":
                        rowDictionary = self.convertStringToDictionary(row)
                        if str(rowDictionary['name']) == columnName:
                            columnKey = rowDictionary['key']
                filteredString += lines[lineIndex] + "*"
            else:
                for row in rowData:
                    if row != "":
                        rowDictionary = self.convertStringToDictionary(row)
                        if int(rowDictionary['key']) == int(columnKey):
                            if str(rowDictionary['data']).strip() == str(columnValue).strip():
                                filteredString += lines[lineIndex] + "*"
        return filteredString

    def selectColumnQuery(self, queryString):
        if self.validateDatabaseSelected():
            global currentDatabasePath;
            queryArray = queryString.split()
            tableName = ""
            columnNames = []
            # Find TABLE name
            for index in range(len(queryArray)):
                if queryArray[index].upper() == 'FROM':
                    tableName = queryArray[index + 1]
                    break

            columnNamePattern = "SELECT\s(.*?)\sFROM"
            columnNamesString = re.search(columnNamePattern, queryString, re.IGNORECASE).group(1)
            columnNamesString = columnNamesString.replace(" ", "")
            columnNames = columnNamesString.split(",")

            filePath = currentDatabasePath + '/' + tableName+'.txt'
            fileRead = open(filePath, 'r')
            fileContent = fileRead.read()

            self.formatSelectOutputWithColumns(fileContent, columnNames)
        if currentDatabase == "":
            print("No database selected.")

    def selectColumnWithWhereQuery(self, queryString):
        if self.validateDatabaseSelected():
            global currentDatabasePath;
            queryArray = queryString.split()
            tableName = ""
            columnNames = []
            # Find TABLE name
            for index in range(len(queryArray)):
                if queryArray[index].upper() == 'FROM':
                    tableName = queryArray[index + 1]
                    break

            columnNamePattern = "SELECT\s(.*?)\sFROM"
            columnNamesString = re.search(columnNamePattern, queryString, re.IGNORECASE).group(1)
            columnNamesString = columnNamesString.replace(" ", "")
            columnNames = columnNamesString.split(",")

            filePath = currentDatabasePath + '/' + tableName+'.txt'
            fileRead = open(filePath, 'r')
            fileContent = fileRead.read()

            conditionColumnPatten = "WHERE\s(.*?)="
            conditionColumn = re.search(conditionColumnPatten, queryString, re.IGNORECASE).group(1)

            conditionColumnValuePattern = "\"(.*?)\""
            conditionColumnValue = re.search(conditionColumnValuePattern, queryString).group(1)

            filteredString = self.filterSelectOutput(conditionColumn, conditionColumnValue, fileContent)
            self.formatSelectOutputWithColumns(filteredString, columnNames)
        if currentDatabase == "":
            print("No database selected.")

    def updateColumnWithWhereQuery(self, queryString):
        if self.validateDatabaseSelected():
            global currentDatabase
            global  currentDatabasePath
            queryArray = queryString.split()
            tableName = queryArray[1]
            conditionColumnPatten = "WHERE\s(.*?)="
            conditionColumnName = re.search(conditionColumnPatten, queryString, re.IGNORECASE).group(1)

            conditionColumnValuePattern = "\"(.*?)\""
            conditionColumnValue = re.findall(conditionColumnValuePattern, queryString, re.IGNORECASE)

            updateColumnPattern = "SET\s(.*?)="
            columnNameToBeUpdated = re.search(updateColumnPattern, queryString, re.IGNORECASE).group(1)

            selectQuery = self.createSelectQuery(queryString)
            selectResults = self.simpleSelectQuery(selectQuery, False)
            updatedRows = self.updateData(selectResults, columnNameToBeUpdated, conditionColumnValue[0],
                                          conditionColumnName, conditionColumnValue[1])
            selectResultAfterUpdate = self.simpleSelectQuery(selectQuery, False)
            error = self.validateConsistencyAndUpdate(selectResultAfterUpdate, updatedRows)
            if error == False:
                try:
                    tableFilePath = os.path.join(currentDatabasePath, tableName + ".txt")
                    print("Updating table " + tableName + "...")
                    self.writeInTableFile(tableFilePath, updatedRows)
                    print("Data updated successfully!!")
                    # log entry for database creation
                    self.enterLog("Updated " + tableName + " Column" + columnNameToBeUpdated + "Value" + conditionColumnValue[0])
                    self.updateSQLDump(currentDatabase, currentDatabasePath, queryString)
                except Exception as error:
                    print("Updating table " + tableName + "...")
                finally:
                    self.uploadFileToBucket(currentDatabase, tableName + '.txt', tableFilePath)
                    print("Data updated successfully!!")
        else:
            print("No database selected.")

    def createSelectQuery(self, queryString):
        selectQuery = "select * from "
        queryArray = queryString.split()
        tableName = queryArray[1]
        return selectQuery + tableName

    def createSelectQueryForDelete(self, queryString):
        selectQuery = "select * from "
        queryArray = queryString.split()
        tableName = queryArray[2]
        return selectQuery + tableName

    def softDeleteValues(self, selectResults, indicesToBeRemoved):
        lines = selectResults.split('*')
        deletedIndex = -1
        outputRows = ""
        for lineIndex in range(len(lines)):
            if lineIndex == 0:
                rowData = lines[lineIndex].split("|")
                for rowIndex in range(len(rowData)):
                    if rowData[rowIndex] != "":
                        rowDictionary = self.convertStringToDictionary(rowData[rowIndex])
                        if str(rowDictionary['name']).strip() == 'isDeleted':
                            deletedIndex = int(rowIndex)

            if lineIndex in indicesToBeRemoved:
                rowData = lines[lineIndex].split("|")
                for rowIndex in range(len(rowData)):
                    if rowData[rowIndex] != "":
                        rowDictionary = self.convertStringToDictionary(rowData[rowIndex])
                        if lineIndex != 0:
                            if rowIndex == deletedIndex:
                                rowDictionary['data'] = 'True'
                            outputRows += str(rowDictionary)
                            if rowIndex < (len(rowData) - 1):
                                outputRows += "|"
                outputRows += "*"
            else:
                outputRows += lines[lineIndex]
                if lineIndex < (len(lines) - 1):
                    outputRows += "*"
        return outputRows

    def deleteWithWhereQuery(self, queryString):
        if self.validateDatabaseSelected():
            queryArray = queryString.split()
            tableName = queryArray[2]
            conditionColumnPatten = "WHERE\s(.*?)="
            conditionColumnName = re.search(conditionColumnPatten, queryString, re.IGNORECASE).group(1)

            conditionColumnValuePattern = "\"(.*?)\""
            conditionColumnValue = re.findall(conditionColumnValuePattern, queryString, re.IGNORECASE)

            selectQuery = self.createSelectQueryForDelete(queryString)
            selectResults = self.simpleSelectQuery(selectQuery, False)
            indicesToBeRemoved = self.removeData(selectResults, conditionColumnName, conditionColumnValue[0])
            selectResultsAfterDelete = self.simpleSelectQuery(selectQuery, False)
            error = self.validateConsitencyAndDelete(selectResultsAfterDelete, selectResults, indicesToBeRemoved)
            if error == False:
                updatedRows = self.softDeleteValues(selectResultsAfterDelete, indicesToBeRemoved)
                tableFilePath = os.path.join(currentDatabasePath, tableName + ".txt")
                self.writeInTableFile(tableFilePath, updatedRows)
                # log entry for database creation
                self.enterLog(
                    "Deleted " + tableName + "Column " + conditionColumnName + "Value " + conditionColumnValue[0])
                self.updateSQLDump(currentDatabase, currentDatabasePath, queryString)
        else:
            print("No database selected.")

# *-*-*-*-*-*-*-*-*-*-*--*-*-*-*-*-*-*-*-*-*-*-*-*-*- VALIDATIONS -*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*

    def validateDatabaseSelected(self):
        global currentDatabase
        if currentDatabase == "":
            print("No database selected.")
            return False
        else:
            return True

    def validateGDD(self, databaseName, gddContent):
        allDatabases = gddContent.split("*")
        if len(allDatabases) == 1:
            if allDatabases[0] == '':
                return True
        for database in allDatabases:
            if (database):
                databaseDictionary = self.convertStringToDictionary(database)
                keys = list(databaseDictionary.keys())
                if databaseName in keys:
                    return True
        return False

    def fetchCurrentDatabasePath(self, gddContent, databaseName):
        allDatabases = gddContent.split("*")
        for database in allDatabases:
            if (database):
                databaseDictionary = self.convertStringToDictionary(database)
                key = list(databaseDictionary.keys())
                if databaseName in key:
                    return databaseDictionary[databaseName]
        return ""

    def convertStringToDictionary(self, fileContent):
        if fileContent == '':
            return fileContent
        else:
            doubleQuote = re.compile('(?<!\\\\)\"')
            withoutDoubleQuote = doubleQuote.sub('', fileContent)
            expression = re.compile('(?<!\\\\)\'')
            fileContent = expression.sub('\"', withoutDoubleQuote)
            fileDictionary = json.loads(fileContent)
            return fileDictionary

    def removeDeleteRecords(self, selectOutput):
        indicesToBeRemoved = self.removeData(selectOutput, 'isDeleted', 'True')
        lines = selectOutput.split("*")
        outputString = ""
        for lineIndex in range(len(lines)):
            if lineIndex not in indicesToBeRemoved:
                outputString += lines[lineIndex]
                if lineIndex < (len(lines) - 1):
                    outputString += "*"
        return outputString


    def formatSelectOutput(self, selectOutput):
        lines = self.removeDeleteRecords(selectOutput).split("*")
        outputString = ""
        timeStampIndex = -1
        deletedRowIndex = -1
        for lineIndex in range(len(lines)):
            rowData = lines[lineIndex].split("|")
            for rowIndex in range(len(rowData)):
                if rowData[rowIndex] != "":
                    rowDictionary = self.convertStringToDictionary(rowData[rowIndex])
                    if lineIndex == 0:
                        if rowDictionary['name'] == 'timeStamp':
                            timeStampIndex = rowIndex
                        elif rowDictionary['name'] == 'isDeleted':
                            deletedRowIndex = rowIndex
                        else:
                            outputString += rowDictionary['name'] + " "
                    else:
                        if rowIndex == timeStampIndex or rowIndex == deletedRowIndex:
                            outputString += ""
                        else:
                            outputString += rowDictionary['data'] + " "
            outputString += "\n"
        print(outputString, end='')

    def formatSelectOutputWithColumns(self, selectOutput, columnNames):
        lines = self.removeDeleteRecords(selectOutput).split("*")
        outputString = ""
        timeStampIndex = -1
        deletedRowIndex = -1
        columnKey = []
        for lineIndex in range(len(lines)):
            rowData = lines[lineIndex].split("|")
            for rowIndex in range(len(rowData)):
                if rowData[rowIndex] != "":
                    rowDictionary = self.convertStringToDictionary(rowData[rowIndex])
                    if lineIndex == 0:
                        if rowDictionary['name'] == 'timeStamp':
                            timeStampIndex = rowIndex
                        elif rowDictionary['name'] == 'isDeleted':
                            deletedRowIndex = rowIndex
                        else:
                            if str(rowDictionary['name']) in columnNames:
                                columnKey.append(rowDictionary['key'])
                                outputString += rowDictionary['name'] + " "
                    else:
                        if rowIndex == timeStampIndex or rowIndex == deletedRowIndex:
                            outputString += ""
                        else:
                            if rowDictionary['key'] in columnKey:
                                outputString += rowDictionary['data'] + " "
            outputString += "\n"
        print(outputString, end='')

    def updateData(self, fileContent, columnNameToBeUpdated, columnValueToBeUpdated, columnNameInWhere,
                   columnValueInWhere):
        lines = fileContent.split('*')
        timeStampIndex = -1
        columnKeyWhere = -1
        columnKeyUpdate = -1
        matchedRows = ""
        indicesToBeUpdated = []
        for lineIndex in range(len(lines)):
            rowData = lines[lineIndex].split("|")
            for rowIndex in range(len(rowData)):
                if rowData[rowIndex] != "":
                    rowDictionary = self.convertStringToDictionary(rowData[rowIndex])
                    if lineIndex == 0:
                        if rowDictionary['name'] == 'timeStamp':
                            timeStampIndex = rowIndex
                        else:
                            if str(rowDictionary['name']).strip() == str(columnNameInWhere).strip():
                                columnKeyWhere = rowDictionary['key']
                            if str(rowDictionary['name']).strip() == str(columnNameToBeUpdated).strip():
                                columnKeyUpdate = rowDictionary['key']
                    else:
                        if rowIndex != timeStampIndex:
                            if str(rowDictionary['key']).strip() == str(columnKeyWhere).strip():
                                if rowDictionary['data'].strip() == columnValueInWhere.strip():
                                    matchedRows += lines[lineIndex] + "*"
                                    indicesToBeUpdated.append(lineIndex)
        outputRows = ""
        for lineIndex in range(len(lines)):
            if lineIndex in indicesToBeUpdated:
                rowData = lines[lineIndex].split("|")
                for rowIndex in range(len(rowData)):
                    if rowData[rowIndex] != "":
                        rowDictionary = self.convertStringToDictionary(rowData[rowIndex])
                        if lineIndex != 0:
                            if rowDictionary['key'] == columnKeyUpdate:
                                rowDictionary['data'] = columnValueToBeUpdated
                            outputRows += str(rowDictionary)
                            if rowIndex < (len(rowData) - 1):
                                outputRows += "|"
                outputRows += "*"
            else:
                outputRows += lines[lineIndex]
                if lineIndex < (len(lines) - 1):
                    outputRows += "*"
        return outputRows;

    def removeData(self, fileContent, columnNameInWhere, columnValueInWhere):
        lines = fileContent.split('*')
        timeStampIndex = -1
        columnKeyWhere = -1
        indicesToBeDeleted = []
        for lineIndex in range(len(lines)):
            rowData = lines[lineIndex].split("|")
            for rowIndex in range(len(rowData)):
                if rowData[rowIndex] != "":
                    rowDictionary = self.convertStringToDictionary(rowData[rowIndex])
                    if lineIndex == 0:
                        if rowDictionary['name'] == 'timeStamp':
                            timeStampIndex = rowIndex
                        else:
                            if str(rowDictionary['name']).strip() == str(columnNameInWhere).strip():
                                columnKeyWhere = rowDictionary['key']
                    else:
                        if rowIndex != timeStampIndex:
                            if str(rowDictionary['key']).strip() == str(columnKeyWhere).strip():
                                if str(rowDictionary['data']).strip() == str(columnValueInWhere).strip():
                                    indicesToBeDeleted.append(lineIndex)
        return indicesToBeDeleted

    def validateConsistencyAndUpdate(self, selectResults, updateSelectResults):
        lines = selectResults.split('*')
        updatedLines = updateSelectResults.split('*')
        timeStampIndex = -1
        error = False
        for line in range(len(lines)):
            if line == 0:
                rowData = lines[line].split("|")
                for rowIndex in range(len(rowData)):
                    if rowData[rowIndex] != "":
                        rowDictionary = self.convertStringToDictionary(rowData[rowIndex])
                        if str(rowDictionary['name']).strip() == 'timeStamp':
                            timeStampIndex = int(rowIndex)
            elif lines[line] != updatedLines[line]:
                rowDataOriginal = lines[line].split("|")
                rowDataUpdated = updatedLines[line].split("|")
                if rowDataOriginal[timeStampIndex] != rowDataUpdated[timeStampIndex]:
                    error = True
                    print("Error: Record has been updated!!")
        return error

    def validateConsitencyAndDelete(self, selectResultsAfterDelete, selectResultsBeforeDelete, indicesToBeDeleted):
        lines = selectResultsBeforeDelete.split('*')
        deletedLines = selectResultsAfterDelete.split('*')
        timeStampIndex = -1
        deleteIndex = -1
        error = False
        for line in range(len(lines)):
            if line == 0:
                rowData = lines[line].split("|")
                for rowIndex in range(len(rowData)):
                    if rowData[rowIndex] != "":
                        rowDictionary = self.convertStringToDictionary(rowData[rowIndex])
                        if str(rowDictionary['name']).strip() == 'timeStamp':
                            timeStampIndex = int(rowIndex)
                        if str(rowDictionary['name']).strip() == 'isDeleted':
                            deleteIndex = int(rowIndex)
            elif line in indicesToBeDeleted:
                rowDataOriginal = lines[line].split("|")
                rowDataUpdated = deletedLines[line].split("|")
                if rowDataOriginal[timeStampIndex] != rowDataUpdated[timeStampIndex]:
                    error = True
                    print("Error: Record has been updated!!")
                elif str(rowDataOriginal[deleteIndex]) == str(True):
                    error = True
                    print("Error: Record has been Deleted!!")
        return error

    def readFileFromRemote(self):
        client = storage.Client()
        bucketName = '5408remote'
        buckets = client.get_bucket(bucketName)
        fileName = list(buckets.list_blobs(prefix=''))
        for name in fileName:
            self.remoteFiles.append(name.name)

    def uploadGDDInGCP(self, gddContent):
        gddFilePath = os.path.join(self.defaultPath, self.gddFileName)
        bucketName = '5408remote'
        client = storage.Client()
        self.deleteFileInGCP('globalDataDictionary.txt')
        bucket = client.get_bucket(bucketName)
        blob = bucket.blob(self.gddFileName)
        blob.upload_from_filename(gddFilePath)

    def updateMetaDataInGCP(self, filePath):
        bucketName = '5408remote/remoteStorage'
        client = storage.Client()
        bucket = client.get_bucket(bucketName)
        blob = bucket.blob(self.metaDataFileName)
        blob.delete()
        blob.upload_from_filename(filePath)

    def deleteFileInGCP(self, fileName):
        client = storage.Client()
        bucketName = '5408remote'
        bucket = client.get_bucket(bucketName)
        blobs = list(bucket.list_blobs(prefix=fileName))
        for blob in blobs:
            blob.delete()

    def createBucket(self, databaseName):
        client = storage.Client()
        bucket = client.bucket(databaseName)
        bucket.create()

    def uploadFileToBucket(self, bucketName, fileName, filePath):
        client = storage.Client()
        bucket = client.get_bucket(bucketName)
        blob = bucket.blob(fileName)
        blob.upload_from_filename(filePath)

    def readFileFromBucket(self, bucketName, fileName):
        client = storage.Client()
        bucket = client.get_bucket(bucketName)
        blob = bucket.get_blob(fileName)
        content = blob.download_as_string()
        return content.decode("utf-8")

    def enterLog(self, message):
        loggerPath = self.defaultPath + self.logFile
        fileWriting = open(loggerPath, 'a')
        fileWriting.write(
            "{0} -- {1}\n".format(datetime.utcnow().strftime("%Y-%m-%d %H:%M"),message))

    def getCurrentTimeStampInUTC(self):
        epoch = datetime.utcnow()
        currentUTCTimeInMilliSeconds = epoch.timestamp() * 1000
        return currentUTCTimeInMilliSeconds

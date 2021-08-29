import re
from execution_engine import ExecutionEngine;


class QueryParser:
    def __init__(self, query, rights):
        self.rights = rights
        self.queryString = query
        processedQuery = str(self.processQuery(self.queryString))
        self.validateAndTranslateQuery(processedQuery)

    def processQuery(self, queryString: str):
        spaceSeperation = queryString.split()
        queryArray = []
        for x in spaceSeperation:
            commaSeperation = x.split(',')
            properCommaSeperation = ""
            if len(commaSeperation) > 1:
                properCommaSeperation = ' , '.join(commaSeperation)
            else:
                properCommaSeperation = ''.join(commaSeperation)

            opeaningBracketSeperation = properCommaSeperation.split('(')
            properOpeaningBracketSeperation = ""
            if len(opeaningBracketSeperation) > 1:
                properOpeaningBracketSeperation = ' ( '.join(opeaningBracketSeperation)
            else:
                properOpeaningBracketSeperation = ''.join(opeaningBracketSeperation)

            closingBracketSeperation = properOpeaningBracketSeperation.split(')')
            properClosingBracketSeperation = ""
            if len(closingBracketSeperation) > 1:
                properClosingBracketSeperation = ' ) '.join(closingBracketSeperation)
            else:
                properClosingBracketSeperation = ''.join(closingBracketSeperation)
            queryArray.append(properClosingBracketSeperation)

        processedQuery = ' '.join(queryArray)
        finalQuery = processedQuery.split()
        finalProcessedQuery = ' '.join(finalQuery)
        return finalProcessedQuery

    def validateAndTranslateQuery(self, queryString: str):

        if re.match("CREATE\sDATABASE\s([a-zA-Z0-9_])\w+;", queryString, re.IGNORECASE) is not None:
            queryString = queryString.replace(";", "")
            executionEngine = ExecutionEngine(queryString, 1, self.rights)
            return True

        elif re.match("USE\sDATABASE\s([a-zA-Z0-9_])\w+;", queryString, re.IGNORECASE) is not None:
            queryString = queryString.replace(";", "")
            executionEngine = ExecutionEngine(queryString, 2, self.rights)
            return True


        # simple select
        elif re.match("SELECT\s\*\sFROM\s([a-zA-Z0-9_]+);", queryString, re.IGNORECASE) is not None:
            queryString = queryString.replace(";", "")
            executionEngine = ExecutionEngine(queryString, 3, self.rights)
            return True


        # simple select with where clause
        elif re.match("SELECT\s\*\sFROM\s([a-zA-Z0-9_])\w+\sWHERE\s([a-zA-Z0-9_])\w+=[\"]*\w+[\"]*;", queryString,
                      re.IGNORECASE) is not None:
            queryString = queryString.replace(";", "")
            executionEngine = ExecutionEngine(queryString, 4, self.rights)
            return True


        # select with columns
        elif re.match("SELECT\s([a-zA-Z0-9_]+\s[,\s]*)+FROM\s([a-zA-Z0-9_])+;", queryString, re.IGNORECASE) is not None:
            queryString = queryString.replace(";", "")
            executionEngine = ExecutionEngine(queryString, 5, self.rights)
            return True


        # select with columns with where clause
        elif re.match("SELECT\s(([a-zA-Z0-9_]+\s[,\s]*))+FROM\s([a-zA-Z0-9_]+\s)WHERE\s([a-zA-Z0-9_]+)=[\"]*([a-zA-Z0-9_]+)[\"]*;",
                      queryString, re.IGNORECASE) is not None:
            queryString = queryString.replace(";", "")
            executionEngine = ExecutionEngine(queryString, 6, self.rights)
            return True


        # update with where caluse
        elif re.match(
                "UPDATE\s([a-zA-Z0-9_]+\s)SET\s([a-zA-Z0-9_]+=([\"]*)([a-zA-Z0-9_]+([\"]*)([\s,]*)([\s]*)))+WHERE\s([a-zA-Z0-9_]+=[\"]*([a-zA-Z0-9_])+([\"]*));",
                queryString, re.IGNORECASE) is not None:
            queryString = queryString.replace(";", "")
            executionEngine = ExecutionEngine(queryString, 7, self.rights)
            return True


        # delete with where clause
        elif re.match("DELETE\sFROM\s([a-zA-Z0-9_]+)\sWHERE\s([a-zA-Z0-9_]+)=([\"]*)([a-zA-Z0-9_]+)([\"]*);", queryString,
                      re.IGNORECASE) is not None:
            queryString = queryString.replace(";", "")
            executionEngine = ExecutionEngine(queryString, 8, self.rights)
            return True


        # insert without columnName
        elif re.match("INSERT\sINTO\s([a-zA-Z0-9_]+\s)VALUES\s([\(])\s(([\"]*)([a-zA-Z0-9_])+([\"]*\s)([,\s]*))+([\)])\s;", queryString, re.IGNORECASE) is not None:
          queryString = queryString.replace(";", "")
          executionEngine = ExecutionEngine(queryString, 9, self.rights)
          return True


        # drop table query
        elif re.match("DROP\sTABLE\s([a-zA-Z0-9_]+);", queryString, re.IGNORECASE) is not None:
            queryString = queryString.replace(";", "")
            executionEngine = ExecutionEngine(queryString, 10, self.rights)
            return True


        # create table
        elif re.match("CREATE\sTABLE\s([a-zA-Z0-9_]+)\s([\(]\s)(([a-zA-Z0-9_]+)\s[INT]*[STRING]*\s[NOT NULL]*[UNIQUE]*([,\s]*))+(PRIMARY KEY\s\(\s([a-zA-Z0-9_]+)\s\))*(\s)*\)\s;", queryString, re.IGNORECASE) is not None:
            queryString = queryString.replace(";", "")
            executionEngine = ExecutionEngine(queryString, 11, self.rights)
            return True


        else:
            print("Query Invalid!!")

import sys
from query_parser import QueryParser

class QueryConsole:

    def __init__(self, rights):
        self.rights = rights
        self.query = ''
        self.getUserInput()

    def getUserInput(self):
        while True:
            self.query = input("Enter Query (Press 'Exit' to leave.): ")
            if self.query.upper() == 'EXIT':
                print("Exit")
                sys.exit()
            else:
                self.parseQuery(self.query)


    def parseQuery(self, queryInput):
        parser = QueryParser(queryInput, self.rights)

import sys
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from query_console import QueryConsole


class RegistrationConsole:
    def __init__(self):
        self.defaultPath = 'D:/DW/'
        self.metaDataFileName = 'userDetails.txt'
        self.getUserDetails()
        self.writeUserDetails()

    def writeUserDetails(self, username, password, rights):
        try:
            registrationString = username + ":'" + password + "," + rights + "'*"
            filePath = self.defaultPath + "/" + self.metaDataFileName
            fileWrite = open(filePath, "a")
            fileWrite.write(registrationString)
            fileWrite.close()
            print("Registration Completed!!!")
            console = QueryConsole(rights)

        except Exception as e:
            print(e)
            print("Exit")
            sys.exit()

    def getUserDetails(self):
        try:
            username = input("Enter username:")
            getUserName = username.replace(" ","")
            password = input("Enter password: ")
            getPassword = password.replace(" ","")

            encryptPassword = '';
            for getCharacter in getPassword:
                getVal = ord(getCharacter) + 1
                encryptPassword = encryptPassword + str(getVal) + "&"

            while True:
                rights = input("Select your rights:\n0 for User, 1 for Administrator:\n")
                if rights == '0' or rights == '1':
                    self.writeUserDetails(getUserName, encryptPassword, rights)
                else:
                    print("Invalid Input")

        except Exception as e:
            print(e)
            print("Exit")
            sys.exit()
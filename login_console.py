import sys
from query_console import QueryConsole

class LoginConsole:
    def __init__(self):
        self.defaultPath = 'D:/DW/'
        self.metaDataFileName = 'userDetails.txt'
        self.login()

    def login(self):
        try:
            inputUsername = input("Enter Username: ")
            getTrimUserName = inputUsername.replace(" ", "")
            inputPassword = input("Enter Password: ")
            getTrimPassword = inputPassword.replace(" ", "")

            encryptPassword = '';
            for getCharacter in getTrimPassword:
                getVal = ord(getCharacter) + 1
                encryptPassword = encryptPassword + str(getVal) + "&"


            getDetails = self.checkLoginCredentials(getTrimUserName, encryptPassword)
            # if getDetails == True:
            #     console = QueryConsole()
            # else:
            #     self.login()

        except Exception as e:
            print(e)
            print("Exit")
            sys.exit()


    def checkLoginCredentials(self, username, password):
        try:
            filePath = self.defaultPath + "/" + self.metaDataFileName
            fileRead = open(filePath, "r")
            fileContent = fileRead.read()
            fileRead.close()
            userExist = False
            fileArray=fileContent.split("*")
            for getDetails in fileArray:
                details=getDetails.split(":")
                if str(details[0]) == str(username):
                    userExist = True
                    details[1] = details[1].replace("'","")
                    getPassAndRights = details[1].split(",")
                    if str(getPassAndRights[0]) == str(password):
                        rights = getPassAndRights[1]
                        print("Login successful!!!")
                        console = QueryConsole(rights)
                    else:
                        print("Password Incorrect")
                        self.login()

            if userExist == False:
                print("User not registered")
                self.login()

        except Exception as e:
            print(e)
            print("Exit")
            sys.exit()



import sys
from login_console import LoginConsole
from registration_console import RegistrationConsole

class DBMS:
    def __init__(self):
        self.getRootInput()

    def getRootInput(self):
        while True:
            rootInput = input('Press 1 to Login, 2 to Register, 3 to Exit: ');
            if rootInput == '1':
                login = LoginConsole()
            elif rootInput == '2':
                registration = RegistrationConsole()
            elif rootInput == '3':
                sys.exit()
            else:
                print('Invalid Input!!')

dbms = DBMS()
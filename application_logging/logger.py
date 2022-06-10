from datetime import datetime


class App_Logger:
    def __init__(self):
        pass

    def log(self, logToFile, logMessage):
        self.x1 = datetime.now().date() # capture current date
        self.x3 = datetime.now().strftime('%H:%M:%S') # capture current in the format '%H:%M:%S'

        logToFile.write(str(self.x1) + '/' + str(self.x3) + '\t\t' + logMessage + '\n') #capture current date and time and write to a file
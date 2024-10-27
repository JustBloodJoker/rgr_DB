class View:
    def ProcessInputKey(self, message=''):
        return input(message)

    def ShowMessage(self, message):
        print(message)

    def ShowErrorMessage(self, error):
       self. ShowMessage(f"ERROR: {error}")

    def ShowTableResultFormat(self, table):
        for each in table:
            print(each)
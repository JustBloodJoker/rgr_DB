from Model import Model
from View import View

class Controller:
    def __init__(self):
        self.model = Model()
        self.view = View()

    def run(self):
        while True:
            choice = self.ShowMenu()
            if choice == 1:
                self.__RunTablesBaseMenu( self.model.nameEquipTypeTable, [self.AddEquipType, self.UpdateEquipType, self.DeleteEquipType, self.ShowEquipType] )
            if choice == 2:
                self.__RunTablesBaseMenu( self.model.nameEquipmentTable,[self.AddEquip, self.UpdateEquip, self.DeleteEquip, self.ShowEquip] )
            if choice == 3:
                self.__RunTablesBaseMenu( self.model.nameClientTable,[self.AddClient, self.UpdateClient, self.DeleteClient, self.ShowClient] )
            if choice == 4:
                self.__RunTablesBaseMenu( self.model.nameBookingTable,[self.AddBooking, self.UpdateBooking, self.DeleteBooking, self.ShowBooking] )
            if choice == 5:
                self.__RunTablesBaseMenu( self.model.nameRentalTable,[self.AddRental, self.UpdateRental, self.DeleteRental, self.ShowRental] )
            if choice == 6:
                self.Generate()
            if choice == 7:
                self.Search()
            if choice == 8:
                self.Reset()
            if choice == 9:
                break

    def __ProcessAnyDigitInput(self, message):
        while True:
            try:
                user_input = int( self.view.ProcessInputKey(message) )
                return user_input
            except ValueError:
                print("Please enter a valid integer.")

    def __ProcessInputWithController(self):
        return self.__ProcessAnyDigitInput("Enter your choice: ")

    def ShowMenu(self):
        self.view.ShowMessage("\nMenu:")
        self.view.ShowMessage("1. EquipmentType")
        self.view.ShowMessage("2. Equipment")
        self.view.ShowMessage("3. Client")
        self.view.ShowMessage("4. Booking")
        self.view.ShowMessage("5. Rental")
        self.view.ShowMessage("6. Generate")
        self.view.ShowMessage("7. Search")
        self.view.ShowMessage("8. Reset all")
        self.view.ShowMessage("9. Quit")
        return self.__ProcessInputWithController()

    def ShowSubMenu(self):
        self.view.ShowMessage("\nSubMenu:")
        self.view.ShowMessage("1. EquipmentType")
        self.view.ShowMessage("2. Equipment")
        self.view.ShowMessage("3. Client")
        self.view.ShowMessage("4. Booking")
        self.view.ShowMessage("5. Rental")
        self.view.ShowMessage("6. Quit")
        return self.__ProcessInputWithController()

    def ShowSearchMenu(self):
        self.view.ShowMessage("\nSearchMenu:")
        self.view.ShowMessage("1. Print the most rented equipment type")
        self.view.ShowMessage("2. Print the customer who booked the equipment most often")
        self.view.ShowMessage("3. Print the equipment that is most often booked")
        self.view.ShowMessage("4. Quit")
        return self.__ProcessInputWithController()


    def Generate(self):
        while True:
            error = 'incorrect input'
            choice = self.ShowSubMenu()
            if choice == 6:
                break

            number = self.__ProcessAnyDigitInput("Select the amount of data to generate.")
            if choice == 1:
                error = self.model.GenerateDataForEquipmentType(number)
            elif choice == 2:
                error = self.model.GenerateDataForEquipment(number)
            elif choice == 3:
                error = self.model.GenerateDataForClient(number)
            elif choice == 4:
                error = self.model.GenerateDataForBooking(number)
            elif choice == 5:
                error = self.model.GenerateDataForRental(number)

            self.view.ShowMessage(error)

    def Reset(self):
        errors = self.model.ResetTables()
        self.view.ShowMessage("Output reset result (if nothing output command was success): ")
        for each in errors:
            if each != self.model.s_sSuccess:
                self.view.ShowErrorMessage(each)

    def Search(self):
        while True:
            choice = self.ShowSearchMenu()
            result = ''
            error = 'Incorrect input'
            time = 0.0
            if choice == 1:
                error, result, time = self.model.FindingTheMostRentedEquipmentType()
            elif choice == 2:
                error, result , time = self.model.FindingTheCustomerWhoBookedTheEquipmentMostOften()
            elif choice == 3:
                error, result , time = self.model.FindingTheEquipmentThatIsMostOftenBooked()
            elif choice == 4:
                break

            if result:
                self.view.ShowMessage(f"Result: {result}")
                self.view.ShowMessage(f"Search request successfully done! Execution time: {time:.2f} milliseconds")
            else:
                self.view.ShowMessage(error)
                self.view.ShowErrorMessage(f"Unexpected error request DB query. Check input and tables data and try again.")

    def ShowBaseTablesMethods(self, name):
        self.view.ShowMessage(f"\nTable {name} manipulations menu:")
        self.view.ShowMessage("1. Insert to table")
        self.view.ShowMessage("2. Update table")
        self.view.ShowMessage("3. Delete from table")
        self.view.ShowMessage("4. Show table data")
        self.view.ShowMessage("5. Quit")
        return self.__ProcessInputWithController()

    def __RunTablesBaseMenu(self, name, methods):
        while True:
            choice = self.ShowBaseTablesMethods(name)
            if choice == 1:
                methods[0]()
            elif choice == 2:
                methods[1]()
            elif choice == 3:
                methods[2]()
            elif choice == 4:
                methods[3]()
            if choice == 5:
                break

    def __BaseTextBeforeOptionOfTableManip(self):
        self.view.ShowMessage("Input needed data to manipulate table (option fields marked (o) before field name")

    def __ProcessInputToField(self, fieldname, isopt: bool = False):
        mess = ''
        if isopt:
            mess += "(o) "
        mess += f"{fieldname}: "
        return self.view.ProcessInputKey(mess)

    #equip table
    def AddEquipType(self):
        self.__BaseTextBeforeOptionOfTableManip()
        id = self.__ProcessInputToField("equip_type_id")
        name = self.__ProcessInputToField("name")
        error = self.model.AddEquipType(id,name)
        self.view.ShowMessage(error)

    def UpdateEquipType(self):
        self.__BaseTextBeforeOptionOfTableManip()
        name = self.__ProcessInputToField("name")
        id = self.__ProcessInputToField("equip_type_id")
        error = self.model.UpdateInEquipType(name, id)
        self.view.ShowMessage(error)

    def DeleteEquipType(self):
        self.__BaseTextBeforeOptionOfTableManip()
        name = self.__ProcessInputToField("name",True)
        id = self.__ProcessInputToField("equip_type_id", True)
        error = self.model.DeleteFromEquipType(id, name)
        self.view.ShowMessage(error)

    def ShowEquipType(self):
        error, res = self.model.ShowDataInEquipTypeTable()
        self.view.ShowMessage(error)
        self.view.ShowTableResultFormat(res)

    #equip table
    def AddEquip(self):
        self.__BaseTextBeforeOptionOfTableManip()
        id = self.__ProcessInputToField("equip_id")
        equip_type_id = self.__ProcessInputToField("equip_type_id")
        name = self.__ProcessInputToField("name")
        error = self.model.AddEquipment(id,equip_type_id,name)
        self.view.ShowMessage(error)

    def UpdateEquip(self):
        self.__BaseTextBeforeOptionOfTableManip()
        name = self.__ProcessInputToField("name")
        id = self.__ProcessInputToField("equip_type_id")
        eq_id = self.__ProcessInputToField("equip_id")
        error = self.model.UpdateInEquipment(name, id, eq_id)
        self.view.ShowMessage(error)

    def DeleteEquip(self):
        self.__BaseTextBeforeOptionOfTableManip()
        eq_id = self.__ProcessInputToField("equip_id", True)
        name = self.__ProcessInputToField("name", True)
        id = self.__ProcessInputToField("equip_type_id", True)
        error = self.model.DeleteFromEquipment(eq_id, id, name)
        self.view.ShowMessage(error)

    def ShowEquip(self):
        error, res = self.model.ShowDataInEquipTable()
        self.view.ShowMessage(error)
        self.view.ShowTableResultFormat(res)

    #client table
    def AddClient(self):
        self.__BaseTextBeforeOptionOfTableManip()
        id = self.__ProcessInputToField("client_id")
        name = self.__ProcessInputToField("name")
        email = self.__ProcessInputToField("email")
        phone = self.__ProcessInputToField("phone", True)
        error = self.model.AddClient(id,name,email, phone)
        self.view.ShowMessage(error)

    def UpdateClient(self):
        self.__BaseTextBeforeOptionOfTableManip()
        name = self.__ProcessInputToField("name")
        email = self.__ProcessInputToField("email")
        phone = self.__ProcessInputToField("phone", True)
        client_id = self.__ProcessInputToField("client_id")
        error = self.model.UpdateInClient(name, email, phone, client_id)
        self.view.ShowMessage(error)

    def DeleteClient(self):
        self.__BaseTextBeforeOptionOfTableManip()
        client_id = self.__ProcessInputToField("client_id", True)
        email = self.__ProcessInputToField("email", True)
        phone = self.__ProcessInputToField("phone", True)
        error = self.model.DeleteFromClient(client_id, email, phone)
        self.view.ShowMessage(error)

    def ShowClient(self):
        error, res = self.model.ShowDataInClientTable()
        self.view.ShowMessage(error)
        self.view.ShowTableResultFormat(res)

    #booking table
    def AddBooking(self):
        self.__BaseTextBeforeOptionOfTableManip()
        id = self.__ProcessInputToField("booking_id")
        client_id = self.__ProcessInputToField("client_id")
        equip_id = self.__ProcessInputToField("equip_id")
        date_start = self.__ProcessInputToField("date_start")
        date_end = self.__ProcessInputToField("date_end")
        error = self.model.AddBooking(id, client_id, equip_id, date_start, date_end)
        self.view.ShowMessage(error)

    def UpdateBooking(self):
        self.__BaseTextBeforeOptionOfTableManip()

        client_id = self.__ProcessInputToField("client_id")
        equip_id = self.__ProcessInputToField("equip_id")
        date_start = self.__ProcessInputToField("date_start")
        date_end = self.__ProcessInputToField("date_end")
        booking_id = self.__ProcessInputToField("booking_id")
        error = self.model.UpdateInBooking(equip_id, client_id, date_start, date_end, booking_id)
        self.view.ShowMessage(error)

    def DeleteBooking(self):
        self.__BaseTextBeforeOptionOfTableManip()
        booking_id = self.__ProcessInputToField("booking_id", True)
        client_id = self.__ProcessInputToField("client_id", True)
        equip_id = self.__ProcessInputToField("equip_id", True)
        error = self.model.DeleteFromBooking(booking_id, client_id, equip_id)
        self.view.ShowMessage(error)

    def ShowBooking(self):
        error, res = self.model.ShowDataInBookingTable()
        self.view.ShowMessage(error)
        self.view.ShowTableResultFormat(res)

    #rental table
    def AddRental(self):
        self.__BaseTextBeforeOptionOfTableManip()
        id = self.__ProcessInputToField("rental_id")
        client_id = self.__ProcessInputToField("client_id")
        equip_id = self.__ProcessInputToField("equip_id")
        duration = self.__ProcessInputToField("duration")
        date_start = self.__ProcessInputToField("date_start")
        error = self.model.AddRental(id, client_id, equip_id, duration, date_start)
        self.view.ShowMessage(error)

    def UpdateRental(self):
        self.__BaseTextBeforeOptionOfTableManip()
        client_id = self.__ProcessInputToField("client_id")
        equip_id = self.__ProcessInputToField("equip_id")
        date_start = self.__ProcessInputToField("date_start")
        duration = self.__ProcessInputToField("duration")
        ren_id = self.__ProcessInputToField("rental_id")
        error = self.model.UpdateInRental(equip_id, client_id, date_start, duration, ren_id)
        self.view.ShowMessage(error)

    def DeleteRental(self):
        self.__BaseTextBeforeOptionOfTableManip()
        rental_id = self.__ProcessInputToField("rental_id", True)
        client_id = self.__ProcessInputToField("client_id", True)
        equip_id = self.__ProcessInputToField("equip_id", True)
        error = self.model.DeleteFromRental(rental_id, client_id, equip_id)
        self.view.ShowMessage(error)

    def ShowRental(self):
        error, res = self.model.ShowDataInRentalTable()
        self.view.ShowMessage(error)
        self.view.ShowTableResultFormat(res)

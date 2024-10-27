# coding=windows-1251
import psycopg
import time

class Model:

    nameEquipTypeTable = 'equipment_type'
    nameEquipmentTable = 'equipment'
    nameClientTable = 'client'
    nameBookingTable = 'booking'
    nameRentalTable = 'rental'
    s_sSuccess = "Success"

    def __del__(self):
        self.DBQuery.close()

    def __init__(self):
        connection_details = {
            "dbname": "postgres",
            "user": "postgres",
            "password": "2143",
            "host": "localhost",
            "port": 5432,
            "options": "-c lc_messages=en_US.UTF-8"
        }

        try:
            self.DBQuery = psycopg.connect(**connection_details)
            print("Connected successfully!")

            with self.DBQuery.cursor() as cursor:
                cursor.execute("SELECT version();")
                db_version = cursor.fetchone()
                print("Database version:", db_version)

        except psycopg.Error as e:
            print("Connection error:", e)

    def ResetTables(self):
        #drops tables
        errors = []
        errors.append(self.DropRentalTable())
        errors.append(self.DropBookingTable())
        errors.append(self.DropClientTable())
        errors.append(self.DropEquipTable())
        errors.append(self.DropEquipTypeTable())

        #create tables
        errors.append(self.CreateEquipmentTypeTable())
        errors.append(self.CreateEquipmentTable())
        errors.append(self.CreateClientTable())
        errors.append(self.CreateBookingTable())
        errors.append(self.CreateRentTable())

        return errors

    def __ExecuteDBCommand(self, command):
        with self.DBQuery.cursor() as cmd:
            try:
                cmd.execute(command)
                self.DBQuery.commit()
                return self.s_sSuccess
            except psycopg.Error as e:
                self.DBQuery.rollback()
                return f" Executing {command} make unexpected error: {e}"

    def DeleteFromDB(self, table, where=''):
        command = f"DELETE FROM {table}"
        if where != '':
            command += f" WHERE {where}"
        return self.__ExecuteDBCommand(command)

    def UpdateDBTable(self, table, set, where=''):
        command = f"UPDATE {table} SET {set}"
        if where != '':
            command += f" WHERE {where}"
        return self.__ExecuteDBCommand(command)

    def AddToDB(self, table, cols, values):
        command = f"INSERT INTO {table} ({cols}) VALUES ({values})"
        return self.__ExecuteDBCommand(command)

    def __TryCreateTable(self, tableName, params):
        try:
            with self.DBQuery.cursor() as c:
                c.execute(f'''
                     CREATE TABLE IF NOT EXISTS {tableName} (
                    {params}
                )
                ''')

                c.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{tableName}')")
                table_exists = c.fetchone()[0]

                if not table_exists:
                    c.execute(f'''
                         CREATE TABLE {tableName} (
                    {params}
                    )
                    ''')

                self.DBQuery.commit()
                return self.s_sSuccess
        except psycopg.Error as e:
            self.DBQuery.rollback()
            return f"Table creation error: {e}"

    def CreateEquipmentTypeTable(self):
        name = self.nameEquipTypeTable
        params = '''
                "equip_type_id" integer NOT NULL,
                "name" character varying(60) NOT NULL,
                CONSTRAINT "equipment_type_pkey" PRIMARY KEY ("equip_type_id"),
                CONSTRAINT name_un UNIQUE ("name") '''
        return self.__TryCreateTable(name, params)

    def CreateEquipmentTable(self):
        name = self.nameEquipmentTable
        params = '''
                "equip_id" integer NOT NULL,
                "name" character varying(40) NOT NULL,
                "equip_type_id" integer NOT NULL,
                CONSTRAINT "equipment_pkey" PRIMARY KEY ("equip_id"),
                CONSTRAINT "eqtypeTOeq" FOREIGN KEY ("equip_type_id") REFERENCES public."equipment_type" ("equip_type_id") MATCH SIMPLE
                '''
        return self.__TryCreateTable(name, params)

    def CreateClientTable(self):
        name = self.nameClientTable
        params = '''
                   "client_id" integer NOT NULL,
                   "name" character varying(20) NOT NULL,
                   "email" character varying(40) NOT NULL,
                   "phone" character varying(13),
                   CONSTRAINT "client_pkey" PRIMARY KEY ("client_id"),
                   CONSTRAINT email_un UNIQUE ("email"),
                   CONSTRAINT phone_un UNIQUE ("phone")
                   '''
        return self.__TryCreateTable(name, params)

    def CreateBookingTable(self):
        name = self.nameBookingTable
        params = '''
                  "booking_id" integer NOT NULL,
                  "client_id" integer NOT NULL,
                  "equip_id" integer NOT NULL,
                  "date_start" date NOT NULL,
                  "date_end" date NOT NULL,
                  CONSTRAINT "booking_pkey" PRIMARY KEY ("booking_id"),
                  CONSTRAINT "clientTOBook" FOREIGN KEY ("client_id") REFERENCES public."client" ("client_id") MATCH SIMPLE,
                  CONSTRAINT "equipTOBook" FOREIGN KEY ("equip_id") REFERENCES public."equipment" ("equip_id") MATCH SIMPLE
                  '''
        return self.__TryCreateTable(name, params)

    def CreateRentTable(self):
        name = self.nameRentalTable
        params = '''
                  "rental_id" integer NOT NULL,
                  "duration" interval NOT NULL,
                  "client_id" integer NOT NULL,
                  "equip_id" integer NOT NULL,
                  "date_start" date NOT NULL,
                  CONSTRAINT "rental_pkey" PRIMARY KEY ("rental_id"),
                  CONSTRAINT "clientToRental" FOREIGN KEY ("client_id") REFERENCES public."client" ("client_id") MATCH SIMPLE,
                  CONSTRAINT "equipTORental" FOREIGN KEY ("equip_id") REFERENCES public."equipment" ("equip_id") MATCH SIMPLE
                  '''
        return self.__TryCreateTable(name, params)

    def AddEquipType(self, equip_type_id, name):
        return self.AddToDB(self.nameEquipTypeTable, 'equip_type_id, name', f"{equip_type_id},'{name}'")

    def AddEquipment(self, equip_id, equip_type_id, name):
        return self.AddToDB(self.nameEquipmentTable, 'equip_id, equip_type_id, name', f"{equip_id},{equip_type_id},'{name}'")

    def AddClient(self, client_id, name, email, phone = ""):
        return self.AddToDB(self.nameClientTable, 'client_id, name, email, phone', f"{client_id},'{name}','{email}','{phone}'")

    def AddBooking(self, booking_id, client_id, equip_id, date_start, date_end):
        return self.AddToDB(self.nameBookingTable, 'booking_id, client_id, equip_id, date_start, date_end', f"{booking_id},{client_id},{equip_id},'{date_start}','{date_end}'")

    def AddRental(self, rental_id, client_id, equip_id, duration, date_start):
        return self.AddToDB(self.nameRentalTable, 'rental_id, client_id, equip_id, duration, date_start', f"{rental_id},{client_id},{equip_id},'{duration}','{date_start}'")

    def __AddToWhereForDB(self, add, where):
        if where!='':
            where +=','

        where += add
        return where

    def DeleteFromEquipType(self, equip_type_id = '', name=''):
        where = ''
        if equip_type_id!='':
            where = self.__AddToWhereForDB(f"equip_type_id = {equip_type_id}", where)
        if name!='':
            where = self.__AddToWhereForDB(f"name = '{name}'", where)

        return self.DeleteFromDB(self.nameEquipTypeTable, where)

    def DeleteFromEquipment(self, equip_id = '',equip_type_id = '', name = ''):
        where = ''
        if name!='':
            where = self.__AddToWhereForDB(f"name = '{name}'", where)
        if equip_id!='':
            where = self.__AddToWhereForDB(f"equip_id = {equip_id}", where)
        if equip_type_id != '':
            where = self.__AddToWhereForDB(f"equip_type_id = {equip_type_id}", where)

        return self.DeleteFromDB(self.nameEquipmentTable, where)

    def DeleteFromClient(self, client_id = '', email = '', phone = ''):
        where = ''
        if client_id != '':
            where = self.__AddToWhereForDB(f"client_id = {client_id}", where)
        if email != '':
            where = self.__AddToWhereForDB(f"email = '{email}'", where)
        if phone != '':
            where = self.__AddToWhereForDB(f"phone = '{phone}'", where)

        return self.DeleteFromDB(self.nameClientTable, where)

    def DeleteFromBooking(self, booking_id = '', client_id = '', equip_id = ''):
        where = ''
        if client_id != '':
            where = self.__AddToWhereForDB(f"client_id = {client_id}", where)
        if equip_id != '':
            where = self.__AddToWhereForDB(f"equip_id = {equip_id}", where)
        if booking_id != '':
            where = self.__AddToWhereForDB(f"booking_id = {booking_id}", where)

        return self.DeleteFromDB(self.nameBookingTable, where)

    def DeleteFromRental(self, rental_id = '', client_id = '', equip_id = ''):
        where = ''
        if client_id != '':
            where = self.__AddToWhereForDB(f"client_id = {client_id}", where)
        if equip_id != '':
            where = self.__AddToWhereForDB(f"equip_id = {equip_id}", where)
        if rental_id != '':
            where = self.__AddToWhereForDB(f"rental_id = {rental_id}", where)

        return self.DeleteFromDB(self.nameRentalTable, where)

    def UpdateInEquipType(self, name, equip_type_id=''):
        where = ''
        if equip_type_id != '':
            where = self.__AddToWhereForDB(f"equip_type_id = {equip_type_id}", where)

        return self.UpdateDBTable(self.nameEquipTypeTable,f"name = '{name}'", where)

    def UpdateInEquipment(self, name,equip_type_id, equip_id=''):
        where = ''
        if equip_id != '':
            where = self.__AddToWhereForDB(f"equip_id = {equip_id}", where)

        return self.UpdateDBTable(self.nameEquipmentTable,f"name = '{name}', equip_type_id='{equip_type_id}'", where)

    def UpdateInClient(self, name, email, phone='', client_id='' ):
        where = ''
        if client_id != '':
            where = self.__AddToWhereForDB(f"client_id = {client_id}", where)

        return self.UpdateDBTable(self.nameEquipTypeTable,f"name = '{name}', email = '{email}', phone = '{phone}'" ,where)

    def UpdateInBooking(self, equip_id, client_id, date_start, date_end, booking_id = ''):
        where = ''
        if booking_id != '':
            where = self.__AddToWhereForDB(f"booking_id = {booking_id}", where)

        return self.UpdateDBTable(self.nameEquipTypeTable, f"client_id='{client_id}',equip_id='{equip_id}',date_start='{date_start}',date_end='{date_end}'", where)

    def UpdateInRental(self,equip_id, client_id, date_start, duration, rental_id = ''):
        where = ''
        if rental_id != '':
            where = self.__AddToWhereForDB(f"rental_id = {rental_id}", where)

        return self.UpdateDBTable(self.nameEquipTypeTable, f"client_id='{client_id}',equip_id='{equip_id}',date_start='{date_start}',date_end='{duration}'", where)

    def __ExecuteGenerationForDBTable(self, start: int, count: int, command, force = False):
        end = start + count - 1
        command = f'''
                   DO $$
                   DECLARE
                       idx INT;
                   BEGIN
                       FOR idx IN {start}..{end}
                       LOOP
                       ''' + command
        command += '''
                       END LOOP;
                   END $$;
                    '''

        try:
            with self.DBQuery.cursor() as cur:
                cur.execute(command)
                self.DBQuery.commit()
                return self.s_sSuccess
        except psycopg.Error as e:
            self.DBQuery.rollback()
            return f" Generation command = {command} unexpected error: {e}"

    def GenerateDataForEquipmentType(self, count: int, start: int = 1):
        command = f"""
                    INSERT INTO public."equipment_type" ("equip_type_id", "name")
                    VALUES (
                      idx,  
                      'KRYTOI_TRAKTOR ' || idx::text
                    );
                    """
        return self.__ExecuteGenerationForDBTable(start, count, command)

    def GenerateDataForEquipment(self, count: int, start: int = 1):
        command = f"""
                    INSERT INTO public."equipment" ("equip_id", "name", "equip_type_id")
                    VALUES (
                      idx,  
                      'TRAKTOR ' || idx::text, 
                      (SELECT "equip_type_id" FROM public."equipment_type" ORDER BY random() LIMIT 1)
                    );
                    """
        return self.__ExecuteGenerationForDBTable(start, count, command)

    def GenerateDataForClient(self, count: int, start:int = 1):
        command = f"""
                    INSERT INTO public."client" ("client_id", "name", "email", "phone")
                    VALUES (
                      idx,  
                      'Client ' || idx::text,
                       substr(md5(random()::text), 1, 25) || '@example.com', 
                      '+' || LPAD((random()*100000000000)::bigint::text, 11, '0')
                    );
                    """
        return self.__ExecuteGenerationForDBTable(start, count, command)

    def GenerateDataForBooking(self, count: int, start: int = 1):
        command = f"""
                    INSERT INTO public."booking" ("booking_id", "client_id", "equip_id", "date_start", "date_end")
                    VALUES (
                        idx,    
                        (SELECT client_id FROM public."client" ORDER BY random() LIMIT 1),
                        (SELECT equip_id FROM public."equipment" ORDER BY random() LIMIT 1),
                        CURRENT_DATE - (random() * 30)::int,
                        CURRENT_DATE + (random() * 30)::int
                    );
                    """
        return self.__ExecuteGenerationForDBTable(start, count, command)

    def GenerateDataForRental(self, count: int, start: int = 1):
        command = f"""
                    INSERT INTO public."rental" ("rental_id", "client_id", "equip_id", "date_start", "duration")
                    VALUES (
                        idx,    
                        (SELECT client_id FROM public."client" ORDER BY random() LIMIT 1),
                        (SELECT equip_id FROM public."equipment" ORDER BY random() LIMIT 1),
                        CURRENT_DATE - (random() * 30)::int,
                        (floor(random() * 10) || ' days')::interval
                    );
                    """
        return self.__ExecuteGenerationForDBTable(start, count, command)

    def __DropDBTable(self, name):
        command = f"DROP TABLE IF EXISTS {name};"
        try:
            with self.DBQuery.cursor() as cur:
                cur.execute(command)
                self.DBQuery.commit()
                return self.s_sSuccess
        except psycopg.Error as e:
            self.DBQuery.rollback()
            return f" Drop table command = {command} unexpected error: {e}"

    def DropEquipTypeTable(self):
        return self.__DropDBTable(self.nameEquipTypeTable)

    def DropEquipTable(self):
        return self.__DropDBTable(self.nameEquipmentTable)

    def DropClientTable(self):
        return self.__DropDBTable(self.nameClientTable)

    def DropBookingTable(self):
        return self.__DropDBTable(self.nameBookingTable)

    def DropRentalTable(self):
        return self.__DropDBTable(self.nameRentalTable)

    def __ExecuteQueryWithResults(self, query, params=() ):
        try:
            with self.DBQuery.cursor() as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                return  self.s_sSuccess, results
        except psycopg.Error as e:
            self.DBQuery.rollback()
            return f"Execute query error: {e}", ''

    def __ExecuteQueryWithTimedResults(self, query, params=() ):
        start_time = time.time()
        error, results = self.__ExecuteQueryWithResults(query, params)
        elapsed_time = (time.time() - start_time) * 1000

        return error, results, elapsed_time

    def FindingTheMostRentedEquipmentType(self):
        query = '''
        SELECT et.name, COUNT(r.rental_id) AS rental_count
        FROM public.equipment AS e
        JOIN public.equipment_type AS et ON e.equip_type_id = et.equip_type_id
        JOIN public.rental AS r ON e.equip_id = r.equip_id
        GROUP BY et.name
        ORDER BY rental_count DESC
        LIMIT 1;
        '''
        error, results, elapsed_time = self.__ExecuteQueryWithTimedResults(query)
        return error, results, elapsed_time

    def FindingTheCustomerWhoBookedTheEquipmentMostOften(self):
        query = '''
                SELECT c.name, COUNT(b.booking_id) AS booking_count
                FROM public.client AS c
                JOIN public.booking AS b ON c.client_id = b.client_id
                GROUP BY c.name
                ORDER BY booking_count DESC
                LIMIT 1;
                '''
        error, results, elapsed_time = self.__ExecuteQueryWithTimedResults(query)
        return error, results, elapsed_time

    def FindingTheEquipmentThatIsMostOftenBooked(self):
        query = '''
                SELECT e.name, COUNT(b.booking_id) AS booking_count
                FROM public.equipment AS e
                JOIN public.booking AS b ON e.equip_id = b.equip_id
                GROUP BY e.name
                ORDER BY booking_count DESC
                LIMIT 1;
                '''
        error, results, elapsed_time = self.__ExecuteQueryWithTimedResults(query)
        return error, results, elapsed_time

    def ShowDataInTable(self, name):
        return self.__ExecuteQueryWithResults(f'SELECT * FROM "{name}"')

    def ShowDataInEquipTypeTable(self):
        return self.ShowDataInTable(self.nameEquipTypeTable)

    def ShowDataInEquipTable(self):
        return self.ShowDataInTable(self.nameEquipmentTable)

    def ShowDataInClientTable(self):
        return self.ShowDataInTable(self.nameClientTable)

    def ShowDataInBookingTable(self):
        return self.ShowDataInTable(self.nameBookingTable)

    def ShowDataInRentalTable(self):
        return self.ShowDataInTable(self.nameRentalTable)
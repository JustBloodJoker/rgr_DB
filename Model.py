# coding=windows-1251
from sqlalchemy import create_engine, Column, Integer, String, Date, Interval, ForeignKey, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect
import psycopg
import time

s_xBase = declarative_base()

connection_details = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "2143",
    "host": "localhost",
    "port": 5432,
    "options": "-c lc_messages=en_US.UTF-8"
}

connectionSQLAlchemyEngine = (
    f"postgresql://{connection_details['user']}:{connection_details['password']}"
    f"@{connection_details['host']}:{connection_details['port']}/{connection_details['dbname']}?"
    f"options={connection_details['options']}"
)

class EquipmentType(s_xBase):
    __tablename__ = 'equipment_type'

    equip_type_id = Column(Integer, primary_key=True)
    name = Column(String(60), unique=True, nullable=False)

class Equipment(s_xBase):
    __tablename__ = 'equipment'

    equip_id = Column(Integer, primary_key=True)
    name = Column(String(40), nullable=False)
    equip_type_id = Column(Integer, ForeignKey('equipment_type.equip_type_id'), nullable=False)

class Client(s_xBase):
    __tablename__ = 'client'

    client_id = Column(Integer, primary_key=True)
    name = Column(String(20), nullable=False)
    email = Column(String(40), unique=True, nullable=False)
    phone = Column(String(13), unique=True)

class Booking(s_xBase):
    __tablename__ = 'booking'

    booking_id = Column(Integer, primary_key=True)
    client_id = Column(Integer, ForeignKey('client.client_id'), nullable=False)
    equip_id = Column(Integer, ForeignKey('equipment.equip_id'), nullable=False)
    date_start = Column(Date, nullable=False)
    date_end = Column(Date, nullable=False)

class Rental(s_xBase):
    __tablename__ = 'rental'

    rental_id = Column(Integer, primary_key=True)
    duration = Column(Interval, nullable=False)
    client_id = Column(Integer, ForeignKey('client.client_id'), nullable=False)
    equip_id = Column(Integer, ForeignKey('equipment.equip_id'), nullable=False)
    date_start = Column(Date, nullable=False)

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
        try:
            self.DBQuery = psycopg.connect(**connection_details)
            print("Connected successfully!")

            with self.DBQuery.cursor() as cursor:
                cursor.execute("SELECT version();")
                db_version = cursor.fetchone()
                print("Database version:", db_version)

        except psycopg.Error as e:
            print("Connection error:", e)

        try:
            self.Engine = create_engine(connectionSQLAlchemyEngine)
            self.Session = sessionmaker(bind=self.Engine)()
            self.Base = s_xBase

            with self.Engine.connect() as connection:
                print("SQLAlchemy Connected successfully!")
        except Exception as e:
            print("SQLAlchemy connect error:", e)

    def ResetTables(self):
        outputs = []

        outputs.append(self.DropRentalTable())
        outputs.append(self.DropBookingTable())
        outputs.append(self.DropClientTable())
        outputs.append(self.DropEquipTable())
        outputs.append(self.DropEquipTypeTable())

        outputs.append(self.CreateEquipmentTypeTable())
        outputs.append(self.CreateEquipmentTable())
        outputs.append(self.CreateClientTable())
        outputs.append(self.CreateBookingTable())
        outputs.append(self.CreateRentTable())

        return outputs

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
        try:
            query = self.Session.query(table)
            if where is not None:
                query = query.filter(where)
            query.delete(synchronize_session=False)
            self.Session.commit()
            return self.s_sSuccess
        except SQLAlchemyError as e:
            self.Session.rollback()
            return f"Error deleting from {table.__tablename__}: {str(e)}"

    #method doesnt use now. just for tests
    def UpdateDBTable(self, table, set, where=''):
        command = f"UPDATE {table} SET {set}"
        if where != '':
            command += f" WHERE {where}"
        return self.__ExecuteDBCommand(command)

    def AddToDB(self, rowAdd):
        try:
            self.Session.add(rowAdd)
            self.Session.commit()
            return self.s_sSuccess
        except SQLAlchemyError as e:
            self.Session.rollback()
            return f"Error adding row: {str(e)}"

    def __TryCreateTable(self, tableClass):
        try:
            tableClass.__table__.create(self.Engine, checkfirst=True)
            return f"Table {tableClass.__tablename__} creation success"
        except SQLAlchemyError as e:
            self.Session.rollback()
            return f"Table creation error: {e}"

    def CreateEquipmentTypeTable(self):
        return self.__TryCreateTable(EquipmentType)

    def CreateEquipmentTable(self):
        return self.__TryCreateTable(Equipment)

    def CreateClientTable(self):
        return self.__TryCreateTable(Client)

    def CreateBookingTable(self):
        return self.__TryCreateTable(Booking)

    def CreateRentTable(self):
        return self.__TryCreateTable(Rental)

    def AddEquipType(self, equip_type_id, name):
        return self.AddToDB( EquipmentType(equip_type_id=equip_type_id, name=name) )

    def AddEquipment(self, equip_id, equip_type_id, name):
        return self.AddToDB( Equipment(equip_id=equip_id, equip_type_id=equip_type_id, name=name) )

    def AddClient(self, client_id, name, email, phone = ""):
        return self.AddToDB( Client(client_id=client_id, name=name, email=email, phone=phone) )

    def AddBooking(self, booking_id, client_id, equip_id, date_start, date_end):
        return self.AddToDB( Booking(booking_id=booking_id, client_id=client_id, equip_id=equip_id, date_start=date_start, date_end=date_end) )

    def AddRental(self, rental_id, client_id, equip_id, duration, date_start):
        return self.AddToDB( Rental(rental_id=rental_id, client_id=client_id, equip_id=equip_id, duration=duration, date_start=date_start) )

    def __AddToWhereForDB(self, add, where):
        if where!='':
            where +=','

        where += add
        return where

    def DeleteFromEquipType(self, equip_type_id='', name=''):
        filters = []
        if equip_type_id:
            filters.append(EquipmentType.equip_type_id == equip_type_id)
        if name:
            filters.append(EquipmentType.name == name)
        where_clause = and_(*filters) if filters else None
        return self.DeleteFromDB(EquipmentType, where_clause)

    def DeleteFromEquipment(self, equip_id='', equip_type_id='', name=''):
        filters = []
        if equip_id:
            filters.append(Equipment.equip_id == equip_id)
        if equip_type_id:
            filters.append(Equipment.equip_type_id == equip_type_id)
        if name:
            filters.append(Equipment.name == name)
        where_clause = and_(*filters) if filters else None
        return self.DeleteFromDB(Equipment, where_clause)

    def DeleteFromClient(self, client_id='', email='', phone=''):
        filters = []
        if client_id:
            filters.append(Client.client_id == client_id)
        if email:
            filters.append(Client.email == email)
        if phone:
            filters.append(Client.phone == phone)
        where_clause = and_(*filters) if filters else None
        return self.DeleteFromDB(Client, where_clause)

    def DeleteFromBooking(self, booking_id='', client_id='', equip_id=''):
        filters = []
        if booking_id:
            filters.append(Booking.booking_id == booking_id)
        if client_id:
            filters.append(Booking.client_id == client_id)
        if equip_id:
            filters.append(Booking.equip_id == equip_id)
        where_clause = and_(*filters) if filters else None
        return self.DeleteFromDB(Booking, where_clause)

    def DeleteFromRental(self, rental_id='', client_id='', equip_id=''):
        filters = []
        if rental_id:
            filters.append(Rental.rental_id == rental_id)
        if client_id:
            filters.append(Rental.client_id == client_id)
        if equip_id:
            filters.append(Rental.equip_id == equip_id)
        where_clause = and_(*filters) if filters else None
        return self.DeleteFromDB(Rental, where_clause)

    def UpdateInEquipType(self, name, equip_type_id=''):
        try:
            query = self.Session.query(EquipmentType)
            if equip_type_id:
                query = query.filter(EquipmentType.equip_type_id == equip_type_id)
            records = query.all()
            for record in records:
                record.name = name
            self.Session.commit()
            return self.s_sSuccess
        except SQLAlchemyError as e:
            self.Session.rollback()
            return f"Error updating EquipmentType: {e}"

    def UpdateInEquipment(self, name,equip_type_id, equip_id=''):
        try:
            query = self.Session.query(Equipment)
            if equip_id:
                query = query.filter(Equipment.equip_id == equip_id)
            records = query.all()
            for record in records:
                record.name = name
                record.equip_type_id = equip_type_id
            self.Session.commit()
            return self.s_sSuccess
        except SQLAlchemyError as e:
            self.Session.rollback()
            return f"Error updating Equipment: {e}"

    def UpdateInClient(self, name, email, phone='', client_id='' ):
        try:
            query = self.Session.query(Client)
            if client_id:
                query = query.filter(Client.client_id == client_id)
            records = query.all()
            for record in records:
                record.name = name
                record.email = email
                record.phone = phone
            self.Session.commit()
            return self.s_sSuccess
        except SQLAlchemyError as e:
            self.Session.rollback()
            return f"Error updating Client: {e}"

    def UpdateInBooking(self, equip_id, client_id, date_start, date_end, booking_id = ''):
        try:
            query = self.Session.query(Booking)
            if booking_id:
                query = query.filter(Booking.booking_id == booking_id)
            records = query.all()
            for record in records:
                record.equip_id = equip_id
                record.client_id = client_id
                record.date_start = date_start
                record.date_end = date_end
            self.Session.commit()
            return self.s_sSuccess
        except SQLAlchemyError as e:
            self.Session.rollback()
            return f"Error updating Booking: {e}"

    def UpdateInRental(self,equip_id, client_id, date_start, duration, rental_id = ''):
        try:
            query = self.Session.query(Rental)
            if rental_id:
                query = query.filter(Rental.rental_id == rental_id)
            records = query.all()
            for record in records:
                record.equip_id = equip_id
                record.client_id = client_id
                record.date_start = date_start
                record.duration = duration
            self.Session.commit()
            return self.s_sSuccess
        except SQLAlchemyError as e:
            self.Session.rollback()
            return f"Error updating Rental: {e}"

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

    def __DropDBTable(self, tableClass):
        try:
            if inspect(self.Engine).has_table(tableClass.__tablename__):
                tableClass.__table__.drop(self.Engine)
                return f"Table {tableClass.__tablename__} dropped"
            else:
                return f"Table {tableClass.__tablename__} does not exist"
        except SQLAlchemyError as e:
            self.Session.rollback()
            return f"Error dropping table {tableClass.__tablename__}: {e}"

    def DropEquipTypeTable(self):
        return self.__DropDBTable(EquipmentType)

    def DropEquipTable(self):
        return self.__DropDBTable(Equipment)

    def DropClientTable(self):
        return self.__DropDBTable(Client)

    def DropBookingTable(self):
        return self.__DropDBTable(Booking)

    def DropRentalTable(self):
        return self.__DropDBTable(Rental)

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
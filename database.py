#!/usr/bin/env python3
import psycopg2


#####################################################
##  Database Connection
#####################################################

class PythonClient:
    # connection parameters - ENTER YOUR LOGIN AND PASSWORD HERE
    userid = "y22s2c9120_akor6986"
    passwd = "Alexejko19929!"
    myHost = "soit-db-pro-2.ucc.usyd.edu.au"
    # instance variable for the database connection
    conn = None

    # Establishes a connection to the database.
    # The connection parameters are read from the instance variables above
    # (userid, passwd, and database).
    # @returns  true   on success and then the instance variable 'conn'
    #                  holds an open connection to the database.
    #           false  otherwise
    #
    def connectToDatabase(self):
        try:
            # connect to the database
            self.conn = psycopg2.connect(database=self.userid, user=self.userid, password=self.passwd, host=self.myHost)
            return True

        except psycopg2.Error as sqle:
            # TODO: add error handling #/
            print("psycopg2.Error : " + str(sqle.pgerror))
            return False

    # open ONE single database connection
    def openConnection(self):
        retval = True
        if self.conn is not None:
            print("You are already connected to the database no second connection is needed!")
        else:
            if self.connectToDatabase():
                print("You are successfully connected to the database.")
            else:
                print("Oops - something went wrong.")
                retval = False
        return retval

    # close the database connection again
    def closeConnection(self):
        if self.conn is None:
            print("You are not connected to the database!")
        else:
            try:
                self.conn.close()  # close the connection again after usage!
                self.conn = None
            except psycopg2.Error as sqle:
                # TODO: add error handling #/
                print("psycopg2.Error : " + sqle.pgerror)

    '''
    Validate administrator based on login and password
    '''


def checkAdmCredentials(login, password):
    uniDB = PythonClient()
    uniDB.openConnection()
    res = []
    try:

        curs = uniDB.conn.cursor()

        # TODO: Check if the login name should be case senstive or not
        # execute the query
        curs.execute(
            """SELECT * FROM checkadmCred(%(login)s,  %(password)s)""",
            {'login': login, 'password': password})

        #  loop through the resultset
        nr = 0
        # res = []
        row = curs.fetchone()
        while row is not None:
            nr += 1
            # print(str(row))
            res = [login, password, row[0], row[1], row[2], row[3]]
            row = curs.fetchone()

        if nr == 0:
            print("No entries found.")

        # clean up! (NOTE this really belongs in a finally block)
        curs.close()

    except psycopg2.Error as sqle:
        # TODO: add error handling #/
        print("psycopg2.Error : " + sqle.pgerror)
        curs.close()
    # print(res)
    uniDB.closeConnection()
    if res == []:
        return None
    else:
        return res


'''
List all the associated instructions in the database by administrator
'''


def findInstructionsByAdm(login):
    uniDB = PythonClient()
    uniDB.openConnection()
    res = []

    try:

        curs = uniDB.conn.cursor()

        # execute the query

        curs.execute(
            """SELECT InstructionId as instruction_id, Amount::float,\
            (CASE\
                WHEN Frequency = 'MTH' THEN 'Monthly' \
                WHEN Frequency = 'FTH' THEN 'Fortnightly' \
                ELSE '' END) as Frequency,\
             to_char(ExpiryDate, 'DD-MM-YYYY') as expirydate,\
             concat(firstname,' ', lastname) as fullname, Name, Coalesce(Notes,'') \
             FROM InvestInstruction JOIN Customer ON Customer = Login Join ETF Using(code) \
             WHERE Administrator =  %(login)s \
             ORDER BY \
			 	CASE WHEN Notes = '' or Notes = 'Instruction not renewed.'\
					THEN Notes END DESC, fullname DESC,\
				CASE WHEN Notes <> '' or Notes <> 'Instruction not renewed.'\
			 		THEN date(expirydate) END ASC, fullname DESC;""",
            {'login': login})
        ##to_date(to_char(ExpiryDate, 'DDMMYYYY'),'DDMMYYYY')
        ##to_char(ExpiryDate, 'DD-MM-YYYY')
        #  loop through the resultset
        nr = 0
        # res = []
        # inactive_instr = []
        row = curs.fetchone()
        while row is not None:
            nr += 1
            # print(str(row))

            res.append(
                {'instruction_id': row[0], 'amount': row[1], 'frequency': row[2], 'expirydate': row[3],
                 'customer': row[4],
                 'etf': row[5], 'notes': row[6]})
            row = curs.fetchone()

        if nr == 0:
            print("No entries found.")

        # clean up! (NOTE this really belongs in a finally block)
        curs.close()

    except psycopg2.Error as sqle:
        # TODO: add error handling #/
        print("psycopg2.Error : " + sqle.pgerror)
        curs.close()

    uniDB.closeConnection()
    # print(res)
    if res == []:
        return None
    else:
        return res


'''
Find a list of instructions based on the searchString provided as parameter
See assignment description for search specification
'''


def findInstructionsByCriteria(searchString):
    uniDB = PythonClient()
    uniDB.openConnection()
    res = []

    try:

        curs = uniDB.conn.cursor()

        # execute the query
        # if searchString == "" or searchString == " " or searchString is None:
        #    searchString = "%" ####need to work different Searching with a blank/empty keyword field will show all of the logged in
        ##3user’s associated instructions. how to get current login inside this function?

        # TODO: Check case sensitivity
        searchString = "%" + searchString + "%"
        curs.execute(
            """SELECT InstructionId as instruction_id, Amount::float,\
                    (CASE \
                    WHEN Frequency = 'MTH' THEN 'Monthly' \
                    WHEN Frequency = 'FTH' THEN 'Fortnightly' \
                    ELSE '' END) as Frequency, \
                    to_char(ExpiryDate, 'DD-MM-YYYY') as expirydate, \
                    concat(firstname,' ', lastname) as fullname, Name, Coalesce(Notes,'') as Notes, Administrator \
                FROM InvestInstruction JOIN Customer ON Customer = Login Join ETF Using(code) \
                WHERE (concat(firstname,' ', lastname) ILIKE(%(searchString)s) or name ILIKE(%(searchString)s) or Notes ILIKE(%(searchString)s)) and CURRENT_DATE<date(expirydate) \
                ORDER BY \
					CASE WHEN Administrator = '' or Administrator is NULL \
						THEN Administrator END ASC, date(expirydate) ASC, fullname DESC,\
					CASE WHEN Administrator <> '' or Administrator is not null\
						THEN date(expirydate) END ASC, fullname DESC;""",
            {'searchString': searchString})
        ##to_date(to_char(ExpiryDate, 'DDMMYYYY'),'DDMMYYYY')
        ##to_char(ExpiryDate, 'DD-MM-YYYY')
        #  loop through the resultset
        nr = 0
        # res = []
        # inactive_instr = []
        row = curs.fetchone()
        # print(str(row))
        while row is not None:
            nr += 1
            res.append(
                {'instruction_id': row[0], 'amount': row[1], 'frequency': row[2], 'expirydate': row[3],
                 'customer': row[4],
                 'etf': row[5], 'notes': row[6]})
            row = curs.fetchone()

        if nr == 0:
            print("No entries found.")

        # clean up! (NOTE this really belongs in a finally block)
        curs.close()

    except psycopg2.Error as sqle:
        # TODO: add error handling #/
        print("psycopg2.Error : " + str(sqle.pgerror))

        curs.close()

    uniDB.closeConnection()
    # print(res)
    if res == []:
        return None
    else:
        return res


'''
Add a new instruction
'''


def addInstruction(amount, frequency, customer, administrator, etf, notes):
    uniDB = PythonClient()
    uniDB.openConnection()
    print(amount, frequency, customer, administrator, etf, notes)
    print(type(amount), type(frequency), type(customer), type(administrator), type(etf), type(notes))
    try:

        curs = uniDB.conn.cursor()

        # TODO:
        # Frequency
        #
        # execute the query
        curs.execute(
            """
                INSERT INTO InvestInstruction(Amount, Frequency, ExpiryDate, Customer, Administrator, Code, Notes) 
                SELECT
                    %(amount)s, 
                    (SELECT FrequencyCode FROM Frequency WHERE UPPER(FrequencyDesc) = UPPER(%(frequency)s)),
                    CURRENT_DATE + INTERVAL '12 months', 
                    LOWER(%(customer)s), 
                    LOWER(%(administrator)s), 
                    UPPER(%(etf)s), 
                    %(notes)s
                ;
            """,
            {
                'amount': amount,
                'frequency': str(frequency),
                'customer': str(customer),
                'administrator': str(administrator),
                'etf': str(etf),
                'notes': str(notes)
            })
        uniDB.conn.commit()
        # record = curs.fetchone()
        # print(record)
        # print(curs.rowcount, "Record inserted successfully into InvestInstruction")

        # clean up! (NOTE this really belongs in a finally block)
        curs.close()

    except psycopg2.Error as sqle:
        uniDB.conn.rollback()
        # TODO: add error handling #/
        print("Failed to insert. psycopg2.Error : " + str(sqle.pgerror))
        curs.close()
        uniDB.closeConnection()
        return False

    uniDB.closeConnection()
    return True


'''
Update an existing instruction
'''


def updateInstruction(instructionid, amount, frequency, expirydate, customer, administrator, etf, notes):
    uniDB = PythonClient()
    uniDB.openConnection()
    print(amount, frequency, customer, administrator, etf, notes)
    print(type(amount), type(frequency), type(customer), type(administrator), type(etf), type(notes))
    try:
        curs = uniDB.conn.cursor()
        curs.callproc("UpdateInstruction", [instructionid, amount, frequency, expirydate, customer, administrator, etf, notes])

        uniDB.conn.commit()
        curs.close()

    except psycopg2.Error as sqle:
        uniDB.conn.rollback()
        # TODO: add error handling #/
        print("Failed to insert. psycopg2.Error : " + str(sqle.pgerror))
        curs.close()
        uniDB.closeConnection()
        return False

    uniDB.closeConnection()
    return True

# findInstructionsByAdm('ciori')

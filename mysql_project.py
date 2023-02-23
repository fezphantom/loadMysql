import mysql.connector as cnn
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
f = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fh = logging.FileHandler('mysql_activity.log')
fh.setFormatter(f)
logger.addHandler(fh)

class Database:
    def __init__(self,dbname, tablename) -> None:
        self.dbname = dbname
        self.tablename = tablename

    # @staticmethod
    def connect_db(self):
        db = cnn.connect(
            host = 'localhost',
            user = 'root',
            password = 'ovienunu1'
        )
        return db
    
    def create_db(self):
        """CREATE DATABASE"""
        logger.info("Creating database ....")
        mydb = self.connect_db()
        cur = mydb.cursor()
        try:
            query = f"CREATE DATABASE {self.dbname}"
        except Exception as e:
            logger.error(e)
        else:
            cur.execute(query)
            logger.info(f"Successfully created database: {self.dbname}")

    def create_table(self,head):
        logger.info("Creating table....")
        mydb = self.connect_db()
        cur = mydb.cursor()
        try:
            query = f""" create table {self.dbname}.{self.tablename} (
            {head[0]} int, 
            {head[1]} float, 
            {head[2]} float, 
            {head[3]} float,
            {head[4]} float, 
            {head[5]} float, 
            {head[6]} float,
            {head[7]} float,
            {head[8]} float, 
            {head[9]} float,
            {head[10]} int) """
            cur.execute(query)
        except Exception as e:
            logger.error(e)
        else:
            logger.info(f'Successfully created table: {self.tablename}')

    def insert(self,data):
        """
        This class method inserts  entry/ entries into a database
        ------------------
        Parameters
         data: entries for Database
        ------------------
        data_type: (tuple) for single entry or (list of tuples) for multiple entries
        
        """
        logger.info("Trying to insert data into Database....")
        mydb = self.connect_db()
        cur = mydb.cursor()
        try:
            if len(data) ==  1:
                query =f"INSERT INTO {self.dbname}.{self.tablename} (indx,RI,Na,Mg,Al,Si,K,Ca,Ba,Fe,class) VALUES {data}"
                cur.execute(query)
            else:
                for item in data:
                    query =f"INSERT INTO {self.dbname}.{self.tablename} (indx,RI,Na,Mg,Al,Si,K,Ca,Ba,Fe,class) VALUES {item}"
                    cur.execute(query)
        except Exception as e:
            logger.error(e)
        else:
            mydb.commit()
            logger.info("Successfuly populated table")

    def update(self,colname: str,new_value,con_colname:str,con_value):
        """
        This class method updates / alters entry in a database
        ------------------
        Parameters
        ------------------
        colname: Column name used for search paramter
        new_value: value to update entry
        con_colname: Column name used to find conditional search
        con_value: Data value used to search parameter to identify row(s) to delete
        """

        logger.info("Trying to update data in database....")
        mydb = self.connect_db()
        cur = mydb.cursor()
        try:
            query = f"""UPDATE {self.dbname}.{self.tablename} SET {colname} = {new_value}
            WHERE {con_colname} = {con_value}
             
               """
            cur.execute(query)
        except Exception as e:
            logger.error(e)
        else:
            mydb.commit()
            logger.info("Succesfully updated database entry")

    def delete(self,con_colname,con_value):
        """
        This class method deletes entry from a database
        ------------------
        Parameters
        ------------------
        con_colname: Column name used to find conditional search
        con_value: Data value used to search parameter to identify row(s) to delete
        """
        logger.info("Trying to delete data from database....")
        mydb = self.connect_db()
        cur = mydb.cursor()
        try:
            query = f"""DELETE FROM {self.dbname}.{self.tablename}
            WHERE {con_colname} = {con_value}
             
               """
            cur.execute(query)
        except Exception as e:
            logger.error(e)
        else:
            mydb.commit()
            logger.info("Succesfully deleted record from table")

def read_file(file_path:str) -> tuple:
    """
    Function to read a file into buffer
    Parameter: 
        file_path: directory to file
    Returns:
        tuple(header,body)
    """
    logger.info("Reading from file")
    try:
        with open(file_path, 'r') as file:
            data = file.readlines()
    except Exception as e:
        logger.error(e)
    else:
        body = list()
        for line in data:
            body.append(line.strip())
        body = [line.split(',') for line in body]
        header = body.pop(0)
        logger.info("Changing column name index to indx")
        header[0] = 'indx'
    return header, body

def convert_dtype(data) -> list:
    logger.info("Converting string datatype to int and float....")
    try:
        for item in data:
            for i in range(len(item)):
                if i == 0 or i== 10:
                    item[i] = int(item[i])
                else:
                    item[i] = float(item[i])
    except Exception as e:
        logger.error(e)
    else:
        data = [tuple(item) for item in data]
        logger.info("Data successfuly converted")

    return data


db_name = 'property'
db_table = 'glass'

# initate database
db = Database(db_name,db_table)

# create Database
# db.create_db()

# Extracting data from file
h,body = read_file('msql_db/glass.txt')

#convert body of glass data from string to int and float
body = convert_dtype(body)

# Create database table
db.create_table(h)

#Insert records into database
# db.insert(body)
# db.update('Na',9,'indx',214)
db.delete('Na',9)




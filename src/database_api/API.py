import sqlite3
import MasterConfig
from datetime import datetime


class DatabaseAPI:
    """ This class acts as an interface between Python and the weight-focused
     SQLite3 database stored on the hard disk. The API supports adding, removing, updating and searching.
    """
    def __init__(self):
        self.path = f'{MasterConfig.cwd}\\data\\weights.db'
        self.WEIGHT_TABLE_NAME = 'weights'
        self.connection = None
        self.cursor = None

    def _establish_connection(self):
        """ Establish connection to the database """
        self.connection = sqlite3.connect(self.path)
        self.cursor = self.connection.cursor()

    def _end_connection(self):
        """ Terminate connection to the database"""
        self.connection.close()
        self.connection = None
        self.cursor = None

    def _check_table_exists(self, table_name: str) -> bool:
        """ Check for the existence of a specific table in the database

        Args:
            table_name (str): the name of the table to search for

        Return:
            (bool) True if the table exists
        """
        if not self.connection:
            self._establish_connection()

        self.cursor.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name=?", (table_name,))

        if self.cursor.fetchone()[0] == 1:
            return True
        return False

    @staticmethod
    def to_date(date: int) -> str:
        """ Unix to date converter

        Args:
            date(int): the unix date to be converted

        Return:
            (str) the date value of the Unix date in YYYYY-mm-dd format
        """
        return datetime.fromtimestamp(date).strftime("%Y-%m-%d")

    @staticmethod
    def to_unix(date: str) -> int:
        """ Date to Unix converter

        Args:
            date(str): the date to be converted

        Return:
             (int) the Unix value of the date cast to integer

        """
        formatted_date = datetime.strptime(str(date), "%Y-%m-%d")
        unix_time_stamp = int(datetime.timestamp(formatted_date))
        return unix_time_stamp

    def entry_exists(self, date: str) -> bool:
        """ Checks the existence of a date in the database

        Args:
            date(str): the date of the desired entry in YYYY-mm-dd format

        Return:
            (bool) True if entry in the database exists with that date
        """
        unix_date = self.to_unix(date)

        if not self.connection:
            self._establish_connection()

        if not self._check_table_exists('weights'):
            self.cursor.execute("CREATE TABLE weights (date integer, weight real)")

        self.cursor.execute("SELECT 1 FROM weights WHERE date=?", (unix_date,))

        entry = self.cursor.fetchone()
        if entry:
            if entry[0] == 0:
                return False
        else:
            return False
        return True

    def get_entry_weight(self, date: str) -> float:
        """ Get the weight value of a specific entry

        Args:
            date(str): the date of the desired entry in YYYY-mm-dd format

        Return:
            (float) the weight value of the entry
        """
        unix_date = self.to_unix(date)

        if not self.connection:
            self._establish_connection()

        if not self._check_table_exists('weights'):
            self.cursor.execute("CREATE TABLE weights (date integer, weight real)")

        if not self.entry_exists(date):
            raise Exception(f'Entry at date {date} does not exist')

        self.cursor.execute("SELECT weight FROM weights WHERE date=?", (unix_date,))
        entry = self.cursor.fetchone()

        return float(entry[0])

    def get_all_weights_dates(self) -> list:
        """ Pull all the data from the database

        Return:
            (list) in format:
                list[
                    tuple(date, weight),
                    tuple(date, weight),
                    tuple(date, weight) ]
        """
        if not self.connection:
            self._establish_connection()

        if not self._check_table_exists('weights'):
            self.cursor.execute("CREATE TABLE weights (date integer, weight real)")

        self.cursor.execute("SELECT * FROM weights")
        return self.cursor.fetchall()

    def add_entry(self, date: str, weight: float):
        """ Add an entry to the database

        Args:
            date(str): the date of the new entry in YYY-mm-dd format
            weight(float): the weight value of the new entry
        """
        unix_date = self.to_unix(date)

        if not self.connection:
            self._establish_connection()

        if not self._check_table_exists('weights'):
            self.cursor.execute("CREATE TABLE weights (date integer, weight real)")

        if self.entry_exists(date):
            raise Exception(f'Entry for date: {date} already exists')

        self.cursor.execute("INSERT INTO weights VALUES (:date, :weight)", {'date': unix_date, 'weight': weight})
        self.connection.commit()
        self._end_connection()

    def remove_entry(self, date: str):
        """ Remove an entry from the database

        Args:
            date(str): the date of the desired entry in YYYY-mm-dd format
        """
        unix_date = self.to_unix(date)

        if not self.connection:
            self._establish_connection()

        if not self._check_table_exists('weights'):
            raise Exception('Weights table does not exist in the database')

        self.cursor.execute("DELETE FROM weights WHERE date=?", (unix_date,))
        self.connection.commit()
        self._end_connection()

    def update_entry(self, date: str, weight: float):
        """ Update an entry in the database

        Args:
            date(str): the date of the desired entry in YYYY-mm-dd format
            weight(float): the weight to update the entry with
        """
        unix_date = self.to_unix(date)

        if not self.connection:
            self._establish_connection()

        if not self._check_table_exists('weights'):
            raise Exception('Weights table does not exist in the database')

        self.cursor.execute("SELECT 1 FROM weights WHERE date=?", (unix_date,))
        if self.cursor.fetchone()[0] == 0:
            raise Exception('No matches found in the database')
        self.cursor.execute("UPDATE weights SET weight=? WHERE date=?", (weight, unix_date))

    def print_all(self):
        if not self.connection:
            self._establish_connection()

        self.cursor.execute("SELECT * FROM weights")
        print(self.cursor.fetchall())

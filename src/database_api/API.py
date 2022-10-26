import sqlite3
import MasterConfig
from datetime import datetime


class DatabaseAPI:
    def __init__(self):
        self.path = f'{MasterConfig.cwd}\\data\\weights.db'
        self.WEIGHT_TABLE_NAME = 'weights'
        self.connection = None
        self.cursor = None

    def _establish_connection(self):
        self.connection = sqlite3.connect(self.path)
        self.cursor = self.connection.cursor()

    def _end_connection(self):
        self.connection.close()
        self.connection = None
        self.cursor = None

    def _check_table_exists(self, table_name: str) -> bool:
        if not self.connection:
            self._establish_connection()

        self.cursor.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name=?", (table_name,))

        if self.cursor.fetchone()[0] == 1:
            return True
        return False

    @staticmethod
    def to_date(date):
        return datetime.fromtimestamp(date).strftime("%Y-%m-%d")

    @staticmethod
    def to_unix(date):
        formatted_date = datetime.strptime(str(date), "%Y-%m-%d")
        unix_time_stamp = int(datetime.timestamp(formatted_date))
        return unix_time_stamp

    def entry_exists(self, date) -> bool:
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

    def get_entry_weight(self, date) -> float:
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
        if not self.connection:
            self._establish_connection()

        if not self._check_table_exists('weights'):
            self.cursor.execute("CREATE TABLE weights (date integer, weight real)")

        self.cursor.execute("SELECT * FROM weights")
        return self.cursor.fetchall()

    def add_entry(self, date: str, weight: float):
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

    def remove_entry(self, date):
        unix_date = self.to_unix(date)

        if not self.connection:
            self._establish_connection()

        if not self._check_table_exists('weights'):
            raise Exception('Weights table does not exist in the database')

        self.cursor.execute("DELETE FROM weights WHERE date=?", (unix_date,))
        self.connection.commit()
        self._end_connection()

    def update_entry(self, date, weight):
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

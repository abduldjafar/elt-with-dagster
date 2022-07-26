import mariadb
import mariadb.cursors as cursors
from sys import exit
import os
from clickhouse_driver import connect
from clickhouse_driver.errors import Error
from pipelines.sql.clickhouse_query import ClickhouseQuery
import os

class ClickhouseServer(object):
    def __init__(self):

        self.ch_host = os.environ["CH_HOST"]
        self.conn = None
        self.cursor = None
        self.query = ClickhouseQuery()
    
    def init(self):
        try:
            self.conn = connect('clickhouse://' + self.ch_host)
            self.cursor = self.conn.cursor()
        except Error as e:
            print(e.message)

    def convert_data_type_from_mysql(self):

        return {
            "UNSIGNED TINYINT": "UInt8",
            "TINYINT": "Int8",
            "UNSIGNED SMALLINT": "UInt16",
            "SMALLINT": "Int16",
            "UNSIGNED INT": "UInt32",
            "UNSIGNED MEDIUMINT": "UInt32",
            "INT": "Int32",
            "UNSIGNED BIGINT": "UInt64",
            "BIGINT": "Int64",
            "FLOAT": "Float32",
            "DOUBLE": "Float64",
            "DATE": "Date",
            "DATETIME": "DateTime",
            "TIMESTAMP": "DateTime",
            "BINARY": "FixedString",
            "VARCHAR": "String",
            "CHAR": "String",
            "ENUM": "String",
            "MEDIUMTEXT":"String",
            "MEDIUMINT":"Int64",
            "TEXT": "String",
            "DECIMAL":"Float64",
            "YEAR":"Int32",
            "SET":"String",
            "BLOB":"String",
        }

    def execute_query(self, query):
        try:
            self.cursor.execute(query)
        except Error as e:
            print(e.message)

    def insert_data(
        self,
        db_name,
        tb_name,
        columns,
        data,
    ):
        truncate_query = self.query.truncate_table(db_name,tb_name)
        self.execute_query(truncate_query)

        query = self.query.insert_data_to_table(db_name, tb_name, columns)

        self.cursor.execute(query, data)

class MariadbServer(object):
    def __init__(
        self,
        user="root",
        password="toor",
        host="localhost",
        port=3306,
        database="database",
    ):
        self.user = os.environ["MYSQL_USER"] if 'MYSQL_USER' in os.environ else user
        self.password = os.environ["MYSQL_PASSWORD"] if 'MYSQL_PASSWORD' in os.environ else password
        self.host = os.environ["MYSQL_HOST"] if 'MYSQL_HOST' in os.environ else host
        self.port = os.environ["MYSQL_PORT"] if 'MYSQL_PORT' in os.environ else port
        self.database = database
        self.cursor = None

    def init(self):
        try:
            self.conn = mariadb.connect(
                user=(self.user if type(self.user) != tuple else self.user[0]),
                password=(
                    self.password if type(self.password) != tuple else self.password[0]
                ),
                host=(self.host if type(self.host) != tuple else self.host[0]),
                port=(
                    int(self.port) if type(self.port) != tuple else int(self.port[0])
                ),
                database=(
                    self.database if type(self.database) != tuple else self.database[0]
                ),
                
            )
            self.cursor = self.conn.cursor()
        except mariadb.Error as e:

            print(e)
            exit(1)
    
    def fetch_datas(self,tb_name="employees",columns=["first_name","last_name"],batch_size=1000):
        self.cursor.execute("select {} from {}".format(",".join(columns),tb_name))
        while True:
            rows = self.cursor.fetchmany(
                batch_size
            )  # this will fetch data in batches from the ready data in db
            if not rows:
                break

            rows = [tuple(None if item=='NULL' else item  for item in  data) for data in rows]

            yield from rows

if __name__ == "__main__":
    mariadbObj = MariadbServer(database="employees")
    columns = ["first_name","last_name"]
    chServer = ClickhouseServer()
    chServer.init()
    mariadbObj.init()
    data = mariadbObj.fetch_datas()
    print(type(data))

    chServer.insert_data("dwh","employees","({})".format(",".join(columns)),data)
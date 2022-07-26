#!/usr/bin/python
# -*- coding: utf-8 -*-
import mariadb
from sys import exit
from sql.mysql_query import MysqlQuery
import os


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
        self.mysql_query = MysqlQuery()

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

    def get_columns_name_and_data_type(self, table):
        result = {}

        self.cursor.execute(
            self.mysql_query.get_columns_name_and_data_type(table, self.database)
        )
        columns_and_datatype = self.cursor.fetchall()
    
        result["table_details"] = columns_and_datatype

        return result
    
    def get_spesific_columns_name_and_data_type(self, table,columns):
        result = {}


        columns_reformat = ["'{}'".format(column) for column in columns]
        columns_list_cleaned = " and column_name=".join(columns_reformat)
        self.cursor.execute(
            self.mysql_query.get_spesific_columns_name_and_data_type(table, self.database,columns_list_cleaned)
        )

        columns_and_datatype = self.cursor.fetchall()
        print(columns_and_datatype)

        data_types = {x[0]:[x[1],x[2]] for x in columns_and_datatype}
       
        columns_and_datatype = [(column,data_types[column][0],data_types[column][1]) for column in columns]

        result["table_details"] = columns_and_datatype
 


        return result

    def full_load_table(self, table):
        result = self.get_columns_name_and_data_type(table)

        self.cursor.execute(self.mysql_query.full_load_table(table, self.database))
        datas = self.cursor.fetchall()

        datas = [ tuple(None if item=='NULL' else item  for item in  data) for data in datas]

        result["datas"] = datas

        return result
    
    def full_load_table_with_batch(self, table,batch_size):
        self.cursor.execute(self.mysql_query.full_load_table(table, self.database))

        while True:
            rows = self.cursor.fetchmany(
                batch_size
            )  # this will fetch data in batches from the ready data in db
            if not rows:
                break
            rows = [tuple(None if item=='NULL' else item  for item in  data) for data in rows]
            yield from rows
        
    def full_load_table_with_spesific_columns_with_batch(self, table,columns,batch_size):

            str_columns = ",".join(columns)
            self.cursor.execute(self.mysql_query.full_load_table_with_spesific_columns(table, self.database,str_columns))
            
            while True:
                rows = self.cursor.fetchmany(
                    batch_size
                )  # this will fetch data in batches from the ready data in db
                if not rows:
                    break
                rows = [tuple(None if item=='NULL' else item  for item in  data) for data in rows]
                if table == "film":
                    print(rows)
                yield from rows
            
            
       
    
    def full_load_table_with_spesific_columns(self, table,columns):
        result = self.get_spesific_columns_name_and_data_type(table,columns)

        str_columns = ",".join(columns)
        self.cursor.execute(self.mysql_query.full_load_table_with_spesific_columns(table, self.database,str_columns))
        datas = self.cursor.fetchall()
        
        datas = [ tuple(None if item=='NULL' else item  for item in  data) for data in datas]


        result["datas"] = datas

        return result

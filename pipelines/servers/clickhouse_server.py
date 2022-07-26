#!/usr/bin/python
# -*- coding: utf-8 -*-
from clickhouse_driver import connect
from clickhouse_driver.errors import Error
from sql.clickhouse_query import ClickhouseQuery
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

    def create_table_with_columns(
        self,
        table,
        columns,
        db_name,
        order_by,
    ):
        query = self.query.create_table_with_columns(table, db_name, columns, order_by)

        if table == "sakila_customer_list":
            print(query)

        self.execute_query(query)

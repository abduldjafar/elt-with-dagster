#!/usr/bin/python
# -*- coding: utf-8 -*-


class MysqlQuery(object):
    def __init__(self):
        pass

    def full_load_table(self, table, database):
        return """
            select * from {}.{}
        """.format(
            database, table
        )
    
    def full_load_table_with_spesific_columns(self, table, database,columns):
        return """
            select {} from {}.{}
        """.format(
            columns,database, table
        )

    def get_columns_name_and_data_type(self, table, database):
        return """
            SELECT COLUMN_NAME,DATA_TYPE,IS_NULLABLE from INFORMATION_SCHEMA.COLUMNS 
            where table_schema = '{}' and table_name = '{}';
        """.format(
            database, table
        )

    def get_spesific_columns_name_and_data_type(self, table, database,columns):
        return """
            SELECT COLUMN_NAME,DATA_TYPE,IS_NULLABLE from INFORMATION_SCHEMA.COLUMNS 
            where table_schema = '{}' and table_name = '{}';
        """.format(
            database, table
        )

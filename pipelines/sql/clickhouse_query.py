#!/usr/bin/python
# -*- coding: utf-8 -*-


class ClickhouseQuery(object):
    def __init__(self):
        pass

    def create_table_with_columns(
        self,
        table_name,
        db_name,
        columns,
        order_by,
    ):
        return """
        CREATE TABLE IF NOT EXISTS {}.{} (
            {}
        ) ENGINE = MergeTree order by {} ;
        """.format(
            db_name, table_name, columns, order_by
        )

    def insert_data_to_table(
        self,
        db_name,
        tb_name,
        columns,
    ):
        return "INSERT INTO {}.{} {} VALUES ".format(db_name, tb_name, columns)
    
    def truncate_table(self, db_name,tb_name):
        return "TRUNCATE {}.{}".format(db_name,tb_name)

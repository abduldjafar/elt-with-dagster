#!/usr/bin/python
# -*- coding: utf-8 -*-
from dagster import op, Out
from servers.mariadb_server import MariadbServer
from services.clickhouse import ClickhouseServices

clickhouseSvc = ClickhouseServices()

def full_loads_with_batch(
    name="default_name",
    tb_name="default_tb_name",
    db_destination="default_db_destination",
    db_sources="default",
    columns=["*"],
    rdbmsObj=MariadbServer(),
    chServer=None
):
    @op(name=name, out={name: Out()})
    def full_loads_table():
        # Op logic here
        result = None
        if len(columns) == 1 and columns[0] == "*":
            result = rdbmsObj.get_columns_name_and_data_type(tb_name)
            datas = rdbmsObj.full_load_table_with_batch(tb_name, 1000)
        else:
            result = rdbmsObj.get_spesific_columns_name_and_data_type(tb_name,columns)
            datas = rdbmsObj.full_load_table_with_spesific_columns_with_batch(tb_name, columns,1000)
        
        result["datas"] = datas
        final_result = (result, tb_name, db_destination)
        return clickhouseSvc.insert_datas_from_mysqldatas_to_clickhouse(name,final_result,db_sources,chServer)

    return full_loads_table

@op
def summary_report(context, statuses):
    context.log.info(" ".join(statuses))

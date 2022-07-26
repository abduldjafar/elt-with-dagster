#!/usr/bin/python
# -*- coding: utf-8 -*-
from sqlalchemy import all_
from pipelines.ops.rdbms_ops import *
from servers.mariadb_server import MariadbServer
from servers.clickhouse_server import ClickhouseServer
from dagster import job
from pipelines.ops.rdbms_ops_config import mysql_employees_tables_config
from dagster_dbt import dbt_cli_resource, dbt_run_op
import os

mariadbObj = MariadbServer(database="employees")
ch_server = ClickhouseServer()
mariadbObj.init()
ch_server.init()

my_dbt_resource = dbt_cli_resource.configured(
    {
        "project_dir": "{}/pipelines/dbt_project".format(os.path.abspath(os.getcwd())),
        "profiles_dir": "{}/pipelines/dbt_project".format(os.path.abspath(os.getcwd())),
        "models": ["dwh","employees_cleanned","employees_schema"]
    }
)


@job(resource_defs={"dbt": my_dbt_resource})
def employees_elt_process():
    all_tables = []

    for tb_config in mysql_employees_tables_config():
        datas = full_loads_with_batch(
            name=tb_config["tb_name"],
            tb_name=tb_config["tb_name"],
            db_destination="dwh",
            db_sources="employees",
            columns=tb_config["tb_columns"],
            rdbmsObj=mariadbObj,
            chServer=ch_server
            
        )
        
        all_tables.append(datas())

    dbt_run_op(summary_report(all_tables))

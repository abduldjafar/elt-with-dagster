from dagster import job
from dagster_dbt import dbt_cli_resource, dbt_run_op,dbt_test_op
from pipelines.ops.rdbms_ops import summary_report
from pipelines.ops.mongo_ops import *
from pipelines.ops.mongo_ops_config import tables_config
import os



my_dbt_resource = dbt_cli_resource.configured(
    {
        "project_dir": "{}/pipelines/dbt_project".format(os.path.abspath(os.getcwd())),
        "profiles_dir": "{}/pipelines/dbt_project".format(os.path.abspath(os.getcwd())),
        "models": ["mongo_schema"]
    }
)

@job(resource_defs={"dbt": my_dbt_resource})
def mongo_elt_jobs():
    all_tables = []

    for tb_config in tables_config():
        datas = mongo_full_loads(
            name=tb_config["collection_name"],
            db_name=tb_config["db_name"],
            collection_name=tb_config["collection_name"],
            db_destination=tb_config["db_destination"],
        )
        process = clickhouse_insert_datas(
            name="dwh_{}".format(tb_config["collection_name"]), datas=datas
        )
        all_tables.append(process(datas()))
    
    dbt_test_op(dbt_run_op(summary_report(all_tables)))



from pipelines.ops.read_from_csv_ops import *
from pipelines.ops.rdbms_ops import summary_report
from dagster import job
from servers.clickhouse_server import ClickhouseServer
from dagster_dbt import dbt_cli_resource, dbt_run_op
import os

ch_server = ClickhouseServer()
ch_server.init()

my_dbt_resource = dbt_cli_resource.configured(
    {
        "project_dir": "{}/pipelines/dbt_project".format(os.path.abspath(os.getcwd())),
        "profiles_dir": "{}/pipelines/dbt_project".format(os.path.abspath(os.getcwd())),
        "models": ["sales_dataset"],
    }
)


@job(resource_defs={"dbt": my_dbt_resource})
def sales_elt_process():
    result = []
    full_load = full_loads_csv(
        name="load_sales_datas",
        db_name="dwh",
        tb_name="sales_datasets",
        order_by="order_id",
        chServer=ch_server,
        source_kind="file",
        path_file="/Users/abdulharisdjafar/Downloads/sales.csv",
    )

    result.append(full_load())

    dbt_run_op(summary_report(result))

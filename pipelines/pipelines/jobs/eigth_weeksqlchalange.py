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
        "models": ["eight_weeksqlchallenge"],
    }
)


@job(resource_defs={"dbt": my_dbt_resource})
def eigth_weeksqlchalange_dbt_process():
    dbt_run_op()

from numpy import source
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
        "models": ["covid_19_schema"],
    }
)


@job(resource_defs={"dbt": my_dbt_resource})
def covid_elt_process():
    result = []
    full_load = full_loads_csv(
        name="load_covid_datas",
        db_name="dwh",
        tb_name="covid_datasets",
        order_by="iso_code",
        url="https://covid.ourworldindata.org/data/owid-covid-data.csv",
        chServer=ch_server,
        source_kind="url",
    )

    result.append(full_load())

    dbt_run_op(summary_report(result))

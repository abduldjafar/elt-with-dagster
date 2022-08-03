#!/usr/bin/python
# -*- coding: utf-8 -*-
from dagster import repository

from pipelines.jobs.employees_data_extraction import employees_elt_process
from pipelines.jobs.eigth_weeksqlchalange import eigth_weeksqlchalange_dbt_process
from pipelines.jobs.covid_from_urls_extractor import covid_elt_process
from pipelines.jobs.sales_data_extraction import sales_elt_process
from pipelines.jobs.mongo_datas_extraction import mongo_elt_jobs
from pipelines.jobs.sakila_data_extractor import sakila_elt_process
from pipelines.schedules.my_hourly_schedule import my_hourly_schedule_employees


@repository
def pipelines():
    """
    The repository definition for this pipelines Dagster repository.

    For hints on building your Dagster repository, see our documentation overview on Repositories:
    https://docs.dagster.io/overview/repositories-workspaces/repositories
    """

    jobs = [
        employees_elt_process,
        mongo_elt_jobs,
        sakila_elt_process,
        covid_elt_process,
        sales_elt_process,
        eigth_weeksqlchalange_dbt_process,
    ]
    schedules = [my_hourly_schedule_employees]

    return jobs + schedules

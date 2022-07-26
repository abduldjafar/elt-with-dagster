from dagster import schedule
from pipelines.jobs.employees_data_extraction import employees_elt_process



@schedule(cron_schedule="0 * * * *", job=employees_elt_process, execution_timezone="US/Central")
def my_hourly_schedule_employees(_context):
    """
    A schedule definition. This example schedule runs once each hour.

    For more hints on running jobs with schedules in Dagster, see our documentation overview on
    schedules:
    https://docs.dagster.io/overview/schedules-sensors/schedules
    """
    run_config = {}
    return run_config

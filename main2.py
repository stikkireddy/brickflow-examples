# Databricks notebook source
# MAGIC %pip install -U git+https://github.com/stikkireddy/brickflow.git
# MAGIC %pip install -U python-dotenv

# COMMAND ----------
import logging
from datetime import timedelta

from brickflow.engine.task import TaskSettings, EmailNotifications
from brickflow.engine.utils import resolve_py4j_logging
from dotenv import load_dotenv

from dags.example_dag import dag

load_dotenv()  # take environment variables from .env.

# from brickflow.context import ctx
from brickflow.engine.project import Project
from brickflow.engine.workflow import Workflow, WorkflowPermissions, User

resolve_py4j_logging()

if __name__ == "__main__":
    workflows = []

    wf = Workflow(name=f"sri-airflow-workflow",
                  existing_cluster="1011-090100-bait793",
                  airflow_110_dag=dag,
                  default_task_settings=TaskSettings(
                      email_notifications=EmailNotifications(
                          on_failure=["sri.tikkireddy@databricks.com"]
                      ),
                      timeout_seconds=timedelta(hours=2).seconds
                  ),
                  tags={
                      "sample_pipeline": "hello world"
                  },
                  permissions=WorkflowPermissions(
                      # owner=User("sri.tikkireddy@databricks.com"),
                      can_manage=[User("tushar.madan@databricks.com")]
                  ))


    @wf.bind_airflow_task(name="start_task")
    def start_task():
        pass


    @wf.bind_airflow_task(name="branch_task", depends_on=[start_task])
    def branch_task():
        pass


    @wf.bind_airflow_task(name="continue_task", depends_on=[branch_task])
    def continue_task():
        pass


    @wf.bind_airflow_task(name="stop_task", depends_on=[branch_task])
    def stop_task():
        pass


    @wf.bind_airflow_task(name="continue2_task", depends_on=[continue_task])
    def continue2_task():
        pass


    @wf.bind_airflow_task(name="continue3_task", depends_on=[continue2_task])
    def continue3_task():
        pass


    @wf.bind_airflow_task(name="stop2_task", depends_on=[stop_task])
    def stop2_task():
        pass


    # TODO: dynamically fetch trigger rule from
    @wf.bind_airflow_task(name="join_task", depends_on=[stop2_task, continue2_task])
    def join_task():
        pass


    @wf.task(name="ending_task", depends_on=[join_task])
    def ending_task():
        logging.info("hello world 1234 ending task")


    with Project("sritestproject2",
                 # debug_execute_workflow="sri-workflow",
                 # debug_execute_task="dummy_task",
                 entry_point_path="main2",
                 ) as f:
        f.add_workflow(wf)

# COMMAND ----------

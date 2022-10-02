# Databricks notebook source
# MAGIC %pip install -U git+https://github.com/stikkireddy/brickflow.git
# MAGIC %pip install -U python-dotenv
# MAGIC %pip install apache-airflow==1.10.14 markupsafe==2.0.1

# COMMAND ----------

from dotenv import load_dotenv

from dags.example_dag import dag

load_dotenv()  # take environment variables from .env.

from brickflow.engine.context import Context
from brickflow.engine.project import Project
from brickflow.engine.workflow import Workflow

if __name__ == "__main__":
    ctx = Context()
    workflows = []

    wf = Workflow(name=f"sri-airflow-workflow", existing_cluster="1011-090100-bait793", airflow_110_dag=dag)


    @wf.bind_airflow_task(name="start_task")
    def start_task():
        pass


    @wf.bind_airflow_task(name="branch_task", depends_on=[start_task])
    def branch_task():
        pass


    @wf.bind_airflow_task(name="dummy_task", depends_on=[start_task])
    def dummy_task():
        pass


    @wf.bind_airflow_task(name="continue_task", depends_on=[branch_task, dummy_task])
    def continue_task():
        pass


    @wf.bind_airflow_task(name="stop_task", depends_on=[branch_task, dummy_task])
    def stop_task():
        pass


    @wf.bind_airflow_task(name="continue2_task", depends_on=[continue_task])
    def continue2_task():
        pass


    @wf.bind_airflow_task(name="stop2_task", depends_on=[stop_task])
    def stop2_task():
        pass


    # TODO: dynamically fetch trigger rule from
    @wf.bind_airflow_task(name="join_task", depends_on=[stop2_task, continue2_task])
    def join_task():
        pass


    with Project("sritestproject2",
                     # debug_execute_workflow="sri-workflow",
                     # debug_execute_task="dummy_task",
                     entry_point_path="main2",
                 ) as f:
        f.add_workflow(wf)


# COMMAND ----------

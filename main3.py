# Databricks notebook source
# MAGIC %pip install -U git+https://github.com/stikkireddy/brickflow.git
# MAGIC %pip install -U python-dotenv

# COMMAND ----------
import logging

from brickflow.context.context import ctx
from brickflow.engine.project import Project
from brickflow.engine.task import BrickflowTriggerRule
from brickflow.engine.workflow import Workflow

from dags.example_dag2 import dag

if __name__ == "__main__":
    wf = Workflow(name="sri-workflow-new-ops", existing_cluster="1011-090100-bait793",
                  # airflow_110_dag=dag
                  )


    # @wf.bind_airflow_task(name="start_task")
    # def start_task():
    #     pass
    #
    #
    # @wf.bind_airflow_task(name="branch_task", depends_on=[start_task])
    # def branch_task():
    #     pass
    #
    #
    # @wf.bind_airflow_task(name="is_ok", depends_on=[start_task])
    # def is_ok():
    #     pass
    #
    #
    # @wf.bind_airflow_task(name="continue_task", depends_on=[branch_task, is_ok])
    # def continue_task():
    #     pass
    #
    #
    # @wf.bind_airflow_task(name="stop_task", depends_on=[branch_task, ])
    # def stop_task():
    #     pass
    #
    #
    # @wf.bind_airflow_task(name="continue2_task", depends_on=[continue_task])
    # def continue2_task():
    #     pass
    #
    #
    # @wf.bind_airflow_task(name="continue3_task", depends_on=[continue2_task])
    # def continue3_task():
    #     pass
    #
    #
    # @wf.bind_airflow_task(name="stop2_task", depends_on=[stop_task])
    # def stop2_task():
    #     pass
    #
    #
    # @wf.bind_airflow_task(name="join_task", depends_on=[stop2_task, continue2_task])
    # def join_task():
    #     pass
    #
    #
    # @wf.task(name="ending_task", depends_on=[join_task])
    # def ending_task():
    #     logging.info("hello world 1234 ending task")
    #
    #
    # @wf.task(depends_on=[ending_task])
    # def ending_task2():
    #     ctx.skip_all_following()
    #     logging.info("hello world 1234 ending task 2")
    #
    #
    # @wf.task(depends_on=[ending_task2])
    # def ending_task3():
    #     ctx.skip_all_following()
    #     logging.info("hello world 1234 ending task 3")

    @wf.task
    def task_function(*, test="var"):
        return "hello world"


    @wf.task
    def task_function_no_deco_args(*, test="var"):
        return "hello world"


    @wf.task()
    def task_function_nokwargs():
        return "hello world"


    @wf.task(depends_on=task_function)
    def task_function_2():
        return "hello world"


    @wf.task(depends_on="task_function_2")
    def task_function_3():
        return "hello world"


    @wf.task(depends_on="task_function_3", trigger_rule=BrickflowTriggerRule.NONE_FAILED)
    def task_function_4():
        return "hello world"


    with Project("sritestproject3",
                 debug_execute_workflow="sri-workflow-no-ops",
                 debug_execute_task="dummy_task",
                 entry_point_path="main3",
                 ) as f:
        f.add_workflow(wf)

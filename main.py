# Databricks notebook source
# MAGIC %pip install -U git+https://github.com/stikkireddy/brickflow.git
# MAGIC %pip install -U python-dotenv

# COMMAND ----------

from dotenv import load_dotenv
load_dotenv()  # take environment variables from .env.

from sdk.engine.context import Context
from sdk.engine.project import Project
from sdk.engine.workflow import Workflow

if __name__ == "__main__":
    ctx = Context()
    workflows = []

    wf = Workflow(name=f"sri-workflow", existing_cluster="1011-090100-bait793")

    def read_csv(file):
        print("reading csv file")
        return file


    # @wf.bind_airflow_task(dag, "task_id")
    @wf.task()
    def dummy_task():
        print("dummy_task")
        read_csv("file://some other file")
        return "debug"

    @wf.task()
    # @wf.bind_airflow_task(dag, "task_1243")
    def read_csv_task():
        pass
        # print("dummy_task")
        # read_csv("file://some other file")
        # return "debug"


    @wf.task(depends_on=[read_csv_task])
    def analyze_table():
        dbutils.data.summarize(spark.table("diamonds"))


    read_tasks = [f"read_table_{i}"for i in range(3)]
    dq_checks = []
    for t in read_tasks:
        @wf.task(name=t, depends_on=[analyze_table])
        def read_table(*, test=1234):
            if t == "read_table_1":
                spark.table("diamonds").limit(10).display()


        dq_checks.append(f"dq_{t}")
        @wf.task(name=f"dq_{t}", depends_on=[t])
        def read_table2(*, test=1234):
            if t == "read_table_1":
                spark.table("diamonds").limit(10).display()

    @wf.task(depends_on=dq_checks)
    def write_table(*, test=1234):
        spark.table("diamonds").write.mode("overwrite").saveAsTable("sri_demo.diamonds_brickflow")



    with Project("sritestproject",
                     # debug_execute_workflow="sri-workflow",
                     # debug_execute_task="dummy_task",
                     entry_point_path="main",
                 ) as f:
        f.add_workflow(wf)


# COMMAND ----------


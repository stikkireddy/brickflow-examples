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

    wf = Workflow(name="sri-workflow", existing_cluster="1011-090100-bait793")


    @wf.task()
    def dummy_task():
        print("dummy_task")
        return "debug"

    @wf.task()
    def analyze_table():
        dbutils.data.summarize(spark.table("diamonds"))


    read_tasks = [f"read_table_{i}"for i in range(5)]
    for t in read_tasks:
        @wf.task(name=t, depends_on=[analyze_table])
        def read_table(*, test=1234):
            if t == "read_table_1":
                spark.table("diamonds").display()
            else:
                spark.table("diamonds").display()

    @wf.task(depends_on=read_tasks)
    def write_table(*, test=1234):
        spark.table("diamonds").write.mode("overwrite").saveAsTable("sri_demo.diamonds_brickflow")


    with Project("sritestproject",
                     # debug_execute_workflow="sri-workflow",
                     # debug_execute_task="dummy_task",
                     entry_point_path="main",
                 ) as f:
        f.add_workflow(wf)


# COMMAND ----------


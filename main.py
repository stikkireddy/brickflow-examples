# Databricks notebook source
# MAGIC %pip install -U git+https://github.com/stikkireddy/brickflow.git

# COMMAND ----------

from sdk.engine.project import Project, Stage
from sdk.engine.workflow import Workflow
from sdk.engine.context import Context

if __name__ == "__main__":
    # print("hello world")
    ctx = Context()

    wf = Workflow(name="sri-workflow", existing_cluster="1011-090100-bait793")


    @wf.task()
    def dummy_task():
        print("dummy_task")
        return "debug"

    @wf.task()
    def analyze_table():
        dbutils.data.summarize(spark.table("diamonds"))


    read_tasks = [f"read_table_{i}"for i in range(2)]
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
                 mode=Stage.execute,
                 debug_execute_workflow="sri-workflow",
                 debug_execute_task="dummy_task",
                 git_repo="https://github.com/stikkireddy/brickflow-examples",
                 entry_point_path="main",
                 provider="github",
                 git_reference="branch/main") as f:
        f.add_workflow(wf)

# COMMAND ----------



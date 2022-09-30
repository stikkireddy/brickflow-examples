# Databricks notebook source
# MAGIC %pip install -U git+https://github.com/stikkireddy/brickflow.git

# COMMAND ----------

from sdk.engine.project import Project, Stage
from sdk.engine.workflow import Workflow

if __name__ == "__main__":
    # print("hello world")

    wf = Workflow(name="sri-workflow", existing_cluster="1011-090100-bait793")


    @wf.task()
    def helloworld():
        print("hello world")


    @wf.task()
    def helloworld2(*, test=1234):
        print("hello world2")


    with Project("sritestproject", mode=Stage.execute,
                 execute_workflow="sri-workflow",
                 execute_task="helloworld2",
                 git_repo="https://github.com/stikkireddy/brickflow-examples",
                 entry_point_path="/test",
                 provider="github",
                 git_reference="branch/main") as f:
        f.add_workflow(wf)

# COMMAND ----------



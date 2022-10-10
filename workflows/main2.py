import logging
from datetime import timedelta

from brickflow.engine.compute import Cluster
from brickflow.engine.task import TaskSettings, EmailNotifications


from brickflow.engine.workflow import Workflow, WorkflowPermissions, User



workflows = []

wf = Workflow(f"sri-airflow-workflow",
              clusters=[Cluster.from_existing_cluster("1011-090100-bait793")],
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
                  owner=User("sri.tikkireddy@databricks.com"),
                  can_manage=[User("tushar.madan@databricks.com")]
              ))


@wf.task
def start_task():
    pass


@wf.task(name="branch_task", depends_on=[start_task])
def branch_task():
    pass


@wf.task(name="continue_task", depends_on=[branch_task])
def continue_task():
    pass


@wf.task(name="stop_task", depends_on=[branch_task])
def stop_task():
    pass


@wf.task(name="continue2_task", depends_on=[continue_task])
def continue2_task():
    pass


@wf.task(name="continue3_task", depends_on=[continue2_task])
def continue3_task():
    pass


@wf.task(name="stop2_task", depends_on=[stop_task])
def stop2_task():
    pass


@wf.task(name="join_task", depends_on=[stop2_task, continue2_task])
def join_task():
    pass


@wf.task(name="ending_task", depends_on=[join_task])
def ending_task():
    logging.info("hello world 1234 ending task")


# COMMAND ----------

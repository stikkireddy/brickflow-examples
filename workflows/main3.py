from brickflow.engine.compute import Cluster
from brickflow.engine.task import BrickflowTriggerRule
from brickflow.engine.workflow import Workflow


wf = Workflow("sri-workflow-new-ops", clusters=[Cluster.from_existing_cluster("1011-090100-bait793")],
              # airflow_110_dag=dag
              )

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


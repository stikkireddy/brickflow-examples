from brickflow.context import ctx
from brickflow.engine.compute import Cluster

from brickflow.engine.workflow import Workflow


workflows = []

wf = Workflow("sri-workflow", clusters=[Cluster.from_existing_cluster("1011-090100-bait793")])


def read_csv(file):
    print("reading csv file")
    return file


@wf.task()
def dummy_task():
    print("dummy_task")
    read_csv("file://some other file")
    return "debug"


@wf.task()
def read_csv_task():
    # pass
    print("dummy_task")
    read_csv("file://some other file")
    return "debug"


@wf.task(depends_on=[read_csv_task])
def analyze_table():
    ctx.dbutils.data.summarize(ctx.spark.table("diamonds"))


# @wf.task
# class DLTPipeline(WF.DLTPIPELINE):
#     @dlt.table
#     def ...

read_tasks = [f"read_table_{i}" for i in range(3)]
dq_checks = []
for t in read_tasks:
    @wf.task(name=t, depends_on=[analyze_table])
    def read_table(*, test=1234):
        if t == "read_table_1":
            ctx.spark.table("diamonds").limit(10).display()


    dq_checks.append(f"dq_{t}")


    @wf.task(name=f"dq_{t}", depends_on=[t])
    def run_dq(*, test=1234):
        if t == "read_table_1":
            ctx.spark.table("diamonds").limit(10).display()


@wf.task(depends_on=dq_checks)
def write_table(*, test=1234):
    # print(test)
    ctx.spark.table("diamonds").write.mode("overwrite").saveAsTable("sri_demo.diamonds_brickflow2")

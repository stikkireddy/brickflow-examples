import logging

from brickflow.context import ctx
from brickflow.engine.compute import Cluster
from brickflow.engine.workflow import Workflow

wf = Workflow("srt-workflow-withscripts", clusters=[Cluster.from_existing_cluster("1011-090100-bait793")],)

@wf.task()
def show_tables(*,
                exec_dt="1245",
                s3_dest_path="s3_path",
                target_db="abc",
                integrated_db="123",
                reporting_region_cd="235",
                reference_db="21431"):
    from scripts import spark_script_1
    args = [exec_dt,
            s3_dest_path,
            target_db,
            integrated_db,
            reporting_region_cd,
            reference_db]
    logging.info(f"Recieved Args: {args}")
    spark_script_1.main(args)


@wf.task(depends_on=show_tables)
def show_tables_2(*,
                exec_dt="1245",
                s3_dest_path="s3_path",
                target_db="abc",
                integrated_db="123",
                reporting_region_cd="235",
                reference_db="21431"):
    from scripts import spark_script_1
    args = [exec_dt,
            s3_dest_path,
            target_db,
            integrated_db,
            reporting_region_cd,
            reference_db]
    logging.info(f"Recieved Args: {args}")
    spark_script_1.main(args)
    return "FAILED"


@wf.task(depends_on=show_tables)
def show_tables_3(*,
                  exec_dt="1245",
                  s3_dest_path="s3_path",
                  target_db="abc",
                  integrated_db="123",
                  reporting_region_cd="235",
                  reference_db="21431"):
    from scripts import spark_script_1
    args = [exec_dt,
            s3_dest_path,
            target_db,
            integrated_db,
            reporting_region_cd,
            reference_db]
    logging.info(f"Recieved Args: {args}")
    spark_script_1.main(args)


@wf.task(depends_on=[show_tables_3, show_tables_2])
def show_tables_4(*,
                  exec_dt="1245",
                  s3_dest_path="s3_path",
                  target_db="abc",
                  integrated_db="123",
                  reporting_region_cd="235",
                  reference_db="21431"):
    from scripts import spark_script_1
    logging.info("SHOW TABLES 2 returned: %s", ctx.get_return_value(show_tables_2))
    args = [exec_dt,
            s3_dest_path,
            target_db,
            integrated_db,
            reporting_region_cd,
            reference_db]
    logging.info(f"Recieved Args: {args}")
    spark_script_1.main(args)

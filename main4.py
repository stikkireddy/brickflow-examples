# Databricks notebook source
# MAGIC %pip install -U git+https://github.com/stikkireddy/brickflow.git
# MAGIC %pip install -U python-dotenv

# COMMAND ----------

import logging

from brickflow.engine.project import Project
from brickflow.engine.workflow import Workflow

if __name__ == "__main__":
    wf = Workflow(name="srt-workflow-withscripts", existing_cluster="1011-090100-bait793")

    # TO

    # exec_dt = argv[0]
    # s3_dest_path = argv[1]
    # target_db = argv[2]
    # integrated_db = argv[3]
    # reporting_region_cd = argv[4]
    # reference_db = argv[5]
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


    with Project("sritestproject3", ) as f:
        f.add_workflow(wf)
        # f.add_dir()

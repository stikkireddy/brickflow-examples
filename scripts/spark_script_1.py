import sys


from brickflow.context import ctx


# imports required for PySpark
try:
    from pyspark import SparkConf
    from pyspark.sql import SparkSession
    from pyspark.sql import HiveContext
    from pyspark.sql.functions import col
    from pyspark.sql.functions import max
    from pyspark.sql.functions import coalesce
    from pyspark.sql.types import *
    from pyspark.sql.functions import *
    from pyspark.sql.window import Window
except ImportError as e:
    print("Error importing Spark Modules", e)
    sys.exit(1)


def main(argv):
    # Input s3 path to store the output
    exec_dt = argv[0]
    s3_dest_path = argv[1]
    target_db = argv[2]
    integrated_db = argv[3]
    reporting_region_cd = argv[4]
    reference_db = argv[5]

    spark = ctx.spark

    print(argv)
    spark.sql("SHOW TABLES")


if __name__ == "__main__":
    if len(sys.argv) != 7:
        raise Exception('Incorrect number of arguments passed')
    print('Number of arguments:', len(sys.argv), 'arguments.')
    print('Argument List:', str(sys.argv))
    main(sys.argv[1:])

import os
import sys
from pyspark.sql import SparkSession
import logging
import logging.config

logging.config.fileConfig(fname="../util/login_to_file.conf")
logger=logging.getLogger("create_objects")
import get_all_variables as gav
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def get_spark_object(envn,appName):
    try:
        logger.info(f"get_spark_object is started. {envn} is used")
        if(envn=='TEST'):
            master="local"
        else:
            master="yarn"
        spark = SparkSession.builder\
                .master(master)\
                .appName(appName)\
                .getOrCreate()
    except Exception as exp:
        logger.error("Error in method -get_spark_object(). Please check the stack trace. "+str(exp),exc_info=True)
    else:
        logger.info("Spark is obj created")
    return spark


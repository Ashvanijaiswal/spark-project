from pyspark.sql.functions import current_date
import logging
import pandas as pd
import logging.config
logging.config.fileConfig("../util/login_to_file.conf")
logger=logging.getLogger("validation")

def get_curr_date(spark):
    try:
        opDF=spark.sql("""select current_date """)
        logger.info("current date is "+str(opDF.collect()))
    except Exception as exp:
        logger.error("Exception Occurs in get_cur_date() method. please check the stack trace"+ str(exp),exc_info=True)
        raise
    else:
        logger.info("spark object is validated. Spark object is ready.")

def df_count(df,dfname):
    try:
        logger.info(f"The dataframe validation by count of df_count() is started for dataframe {dfname} ")
        df_count=df.count()
        logger.info(f"The dataframe count is {df_count}.")
    except Exception as exp:
        logger.error("Error in the method - df_count(). Please check the stack trace "+str(exp),exc_info=True)
        raise
    else:
        logger.info("The dataframe validation by count df_count() is completed.")

def get_top10_rec(df,df_name):
    try:
        logger.info(f"The dataframe validation by getting 10 records get_10_rec() is started for dataframe {df_name} ")
        logger.info("Start reading of 10 records.")
        df_pandas=df.limit(10).toPandas()
        logger.info( df_pandas.to_string(index=False))
    except Exception as exp:
        logger.error("Error in method - get_top10_rec(). Please check print stack trace "+str(exp),exc_info=True)
        raise
    else:
        logger.info("The dataframe validation get_top10_rec() is completed.")


def df_print_schema(df,dfname):
    try:
        logger.info(f"The dataframe schema validation for dataframe {dfname}.")
        sch=df.schema.fields
        logger.info("The dataframe schema is.")
        for row in sch:
            logger.info(f"\t{row}")
    except Exception as exp:
        logger.error("Error in method- df_print_schema(). Please check the stack trace."+str(exp),exc_info=True)
        raise
    else:
        logger.info("df_print_schema() is complete")
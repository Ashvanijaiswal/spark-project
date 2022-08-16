import logging
import logging.config

logging.config.fileConfig("../util/login_to_file.conf")
logger=logging.getLogger("presc_run_data_ingest")
def load_files(spark,file_dir,file_format,header,inferSchema):
    try:
        if(file_format=='parquet'):
            df=spark.read\
               .format(file_format)\
               .load(file_dir)
        elif(file_format=='csv'):
            df=spark.read\
              .format(file_format)\
              .option("header",header)\
              .option("inferSchema",inferSchema)\
              .load(file_dir)
    except Exception as exp:
        logger.error("Exception occurs while readng file in dataframe"+str(exp),exc_info=True)
        raise
    else:
        logger.info(f"The input file {file_dir} is loaded in dataframe. The load_file() function is completed." )
    return df


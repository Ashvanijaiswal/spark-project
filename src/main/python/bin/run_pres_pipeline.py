import get_all_variables as gav
from create_object import get_spark_object
from validations import get_curr_date,df_count,get_top10_rec,df_print_schema
import sys
import logging
import logging.config
import os
from presc_run_data_ingest import load_files
from pres_run_data_preprocess import perform_data_clean
from presc_run_data_transform import city_report, top_5_presc

logging.config.fileConfig(fname="../util/login_to_file.conf")

def main():
#     Get all Variable-------Completed
    logging.info("pres_run_pipeline is started")
# Get Spark Object
    try:
        spark=get_spark_object(gav.envn, gav.appName)
        logging.info('Spark object is created')
        get_curr_date(spark)
        logging.info("pres_run_pipeline is completed")
    except Exception as exp:
        logging.error("Error Occur in main() method .Please check the stack Trace, go to the respetive module and fix it"+ str(exp),\
                      exc_info=True)
        sys.exit(1)
# Initiate run_prescribe_data_ingest script
    for file in os.listdir(gav.staging_dim_city):
        file_dir=gav.staging_dim_city+'/'+file
        print(file_dir)
        if(file.split('.')[1]=='csv'):
            file_format='csv'
            header=gav.header
            inferSchema=gav.inferSchema
        elif(file.split('.')[1]=='parquet'):
            file_format='parquet'
            header='NA'
            inferSchema='NA'


    df_city = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header, inferSchema=inferSchema)

# Load the prescriber Fact file
    for file in os.listdir(gav.staging_fact):
        file_dir=gav.staging_fact+'/'+file
        print(file_dir)
        if(file.split('.')[1]=='csv'):
            file_format='csv'
            header=gav.header
            inferSchema=gav.inferSchema
        elif(file.split('.')[1]=='parquet'):
            file_format='parquet'
            header='NA'
            inferSchema='NA'

    df_fact = load_files(spark=spark, file_dir=file_dir, file_format=file_format, header=header, inferSchema=inferSchema)
#
# Validation of City file
    df_count(df_city,'df_city')
    get_top10_rec(df_city,'df_city')
#
# Validation of Fact file
    df_count(df_fact,'df_fact')
    get_top10_rec(df_fact,'df_fact')

#     Validate preprocessing for df_city
    df_city_sel,df_fact_sel=perform_data_clean(df_city,df_fact)
    get_top10_rec(df_city_sel,"df_city_sel")
    get_top10_rec(df_fact_sel,"df_fact_sel")
    df_print_schema(df_fact_sel,"df_fact_sel")
# Initiate  presc_run_data_transform script
    df_city_final=city_report(df_city_sel,df_fact_sel)
    df_presc_final=top_5_presc(df_fact_sel)
#     Validate for df_fact_final
    get_top10_rec(df_presc_final,"df_presc_final")
    df_print_schema(df_presc_final,"df_presc_final")




if __name__=='__main__':
    logging.info("main method is started...")
    main()
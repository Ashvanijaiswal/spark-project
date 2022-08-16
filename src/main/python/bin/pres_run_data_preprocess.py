import logging
import logging.config
from pyspark.sql.functions import upper,lit,regexp_extract,col,concat_ws,avg,coalesce,round
from pyspark.sql.window import Window

logging.config.fileConfig("../util/login_to_file.conf")
logger=logging.getLogger(__name__)

def perform_data_clean(df1,df2):
    try:
        logger.info(f"perform_data_clean() is started...")
        df_city_sel=df1.select(upper(df1.city).alias("city"),
                                  df1.state_id,
                                  upper(df1.state_name).alias("state_name"),
                                  upper(df1.county_name).alias("county_name"),
                                  df1.population,
                                  df1.zips)
        logger.info(f"perform_data_clean() is started for df_fact dataframe...")
        df_fact_sel = df2.select(df2.npi.alias("presc_id"),df2.nppes_provider_last_org_name.alias("presc_lname"), \
                             df2.nppes_provider_first_name.alias("presc_fname"),df2.nppes_provider_city.alias("presc_city"), \
                             df2.nppes_provider_state.alias("presc_state"),df2.specialty_description.alias("presc_spclt"), df2.years_of_exp, \
                             df2.drug_name,df2.total_claim_count.alias("trx_cnt"),df2.total_day_supply, \
                             df2.total_drug_cost)
        df_fact_sel.withColumn("country_name",lit("USA"))
        pattern='\d+'
        idx=0
        df_fact_sel.withColumn("years_of_exp",regexp_extract(col("years_of_exp"),pattern,idx))
        df_fact_sel.withColumn('years_of_exp',col("years_of_exp").cast("int"))
        df_fact_sel=df_fact_sel.withColumn("full_name",concat_ws(' ',col("presc_fname"),col("presc_lname")))
        df_fact_sel=df_fact_sel.dropna(how='any',subset=["presc_id","drug_name"])
        spec=Window.partitionBy("presc_id")
        df_fact_sel=df_fact_sel.withColumn("trx_cnt",coalesce(col("trx_cnt"),round(avg("trx_cnt").over(spec))))
        df_fact_sel=df_fact_sel.withColumn("trx_cnt",col("trx_cnt").cast("int"))




    except Exception as exp:
        logger.error("Error in the method -perform_data_clean().Please check stack trace."+str(exp),exc_info=True)
        raise
    else:
        logger.info("perform_data_clean() is completed.")
    return df_city_sel,df_fact_sel



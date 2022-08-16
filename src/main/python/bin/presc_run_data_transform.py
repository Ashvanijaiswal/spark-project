from pyspark.sql.functions import upper,size,countDistinct,sum,dense_rank,col
from pyspark.sql.window import Window
import logging
import logging.config
from udfs import column_split_cnt
logging.config.fileConfig("../util/login_to_file.conf")
logger=logging.getLogger(__name__)


def city_report(df_city_sel,df_fact_sel):
    try:
        logger.info(f"Transform - city_report() is started...")
        df_city_split = df_city_sel.withColumn('zip_counts', column_split_cnt(df_city_sel.zips))
        df_fact_grp = df_fact_sel.groupBy(df_fact_sel.presc_state, df_fact_sel.presc_city).agg(
            countDistinct("presc_id").alias("presc_counts"), sum("trx_cnt").alias("trx_counts"))
        df_city_join = df_city_split.join(df_fact_grp, (df_city_split.state_id == df_fact_grp.presc_state) & (
                    df_city_split.city == df_fact_grp.presc_city), 'inner')
        df_city_final = df_city_join.select("city", "state_name", "county_name", "population", "zip_counts",
                                            "trx_counts", "presc_counts")
    except Exception as exp:
        logger.error("Error in the method - city_report(). Please check the Stack Trace. " + str(exp), exc_info=True)
        raise
    else:
        logger.info("Transform - city_report() is completed...")
    return df_city_final

def top_5_presc(df_fact_sel):
    try:
        logger.info("top_5_presc() method started.")
        spec=Window.partitionBy("presc_id").orderBy(col("trx_counts").desc())
        df_presc_final=df_fact_sel.filter((df_fact_sel.years_of_exp>=20) &(df_fact_sel.years_of_exp<=50))\
        .withColumn("dense_rank",dense_rank().over(spec))\
        .filter("dense_rank<=5")\
        .select("presc_id","full_name","presc_state","country_name","years_of_exp","trx_cnt","total_day_supply","total_drug_cost")
    except Exception as exp:
        logger.error("Error in method - top_5_presc().Please check stack trace.")
        raise
    else:
        logger.info("Transform- top_5_presc() is completed.")
    return df_presc_final



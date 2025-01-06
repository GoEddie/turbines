from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f

from .config import JSON_DAILY_SUMMARY_OUTPUT_DIR, JSON_CLEANED_DATA_OUTPUT_DIR

def write_summary_to_database(spark: SparkSession, df_daily_statistics: DataFrame):
    
    df_output = (df_daily_statistics
                    .withColumn("year", f.year(f.col("date")))
                    .withColumn("month", f.month(f.col("date")))
                    .withColumn("day", f.dayofmonth(f.col("date")))
    )

    df_output.show()

    df_output.write.partitionBy("year", "month", "day").mode("overwrite").json(JSON_DAILY_SUMMARY_OUTPUT_DIR)


def write_cleaned_data_to_database(spark: SparkSession, df_clean_data: DataFrame):
    
    df_output = (df_clean_data
                    .withColumn("year", f.year(f.col("timestamp")))
                    .withColumn("month", f.month(f.col("timestamp")))
                    .withColumn("day", f.dayofmonth(f.col("timestamp")))
    )

    df_output.show()

    df_output.write.partitionBy("year", "month", "day").mode("overwrite").json(JSON_CLEANED_DATA_OUTPUT_DIR)
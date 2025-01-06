from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f

# To run this without a postgres container, comment out the next line and uncomment the line afterwards
from .pg_database import write_summary_to_database, write_cleaned_data_to_database
# from .json_database import write_summary_to_database, write_cleaned_data_to_database

from .config import CSV_SOURCE_DIR, CSV_OUTLIERS_OUTPUT_DIR


def main(start_date: datetime, end_date: datetime):

    spark = (SparkSession
                .builder
                .appName("Pipeline")
                .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
                .getOrCreate())

    # Read the source data, filtering to the specified daterange
    df_source = get_data(spark, start_date, end_date) 

    # Clean the source data
    df_clean = clean_data(spark, df_source, start_date, end_date)

    # Calculate daily statistics
    df_daily_statistics = calculate_daily_statistics(df_clean)
    
    # Write the daily summary to teh database
    write_summary_to_database(spark, df_daily_statistics)

    # Write the cleaned data to the database
    write_cleaned_data_to_database(spark, df_clean)

    # Find and write outliers
    find_and_write_outliers(df_clean)
    
    
def get_data(spark: SparkSession, start: datetime, end: datetime) -> DataFrame:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return (spark
            .read
            .option("inferSchema", True)
            .csv(CSV_SOURCE_DIR, header=True)
            .filter(f.col("timestamp").between(start, end))
    )


def check_for_rows_with_missing_columns(df: DataFrame):
    """
        If any rows exist with one or more null column then fail the pipeline, this isn't currently handled
    """
    missing_data = df.filter(
            (f.col('timestamp').isNull()) | (f.col('turbine_id')).isNull() | (f.col('wind_speed').isNull()) | (f.col('wind_direction').isNull()) | (f.col('power_output').isNull())
        )

    missing_data_count = missing_data.count()
    
    if missing_data_count > 0:
        missing_data.show()
        raise Exception(f"{missing_data_count} rows with missing data")


def get_expected_timeslots(spark: SparkSession, start_date: datetime, end_date: datetime) -> DataFrame:
    """
        Spark 3.5 doesn't have a nice way to add hours to a timestamp so convert to a unix timestamp and add the hours before converting back to a timestamp.

        The initial range is to create the offset in hours from the start, making sure we include midnight on the first day and edn at 23:00 on the last day.
    """
    
    return (spark
                .range(0,(end_date - start_date).days * 24)
                .withColumn("unix_timestamp", f.to_unix_timestamp(f.lit(start_date)))
                .withColumn("unix_timestamp", f.col("unix_timestamp") + (f.col("id") * f.lit(3600)))
                .withColumn("timestamp", f.from_unixtime(f.col("unix_timestamp")))
                .drop(f.col("unix_timestamp"))
                .drop(f.col("id"))
                .withColumn("hour", f.hour(f.col("timestamp")))
            )


def fill_missing_timeslots_with_turbine_hourly_mean(df_source: DataFrame, df_expected_timeslots: DataFrame) -> DataFrame:
    """
        1. Get a list of turbines
        2. Get a list of all the expected timeslots
        3. Join expected timeslots and actual timeslots for each turbine to create missing rows
        4. Calculate the hourly mean for each turbine
        5. Join the hourly mean to the missing rows
        6. Fill in missing values with the hourly mean using coalesce
    """
    df_turbines = df_source.select('turbine_id').distinct()

    df_all_timeslots = df_expected_timeslots.crossJoin(df_turbines)
    df_data_including_missing_rows = df_all_timeslots.join(df_source, on=["timestamp", "turbine_id"], how="left")
    
    df_hourly_turbine_mean = (df_data_including_missing_rows
                                .groupBy("turbine_id", "hour")
                                .agg(
                                    f.mean("wind_speed").alias("wind_speed_mean"),
                                    f.mean("wind_direction").alias("wind_direction_mean"),
                                    f.mean("power_output").alias("power_output_mean"),

                                    f.stddev("wind_speed").alias("wind_speed_stddev"),
                                    f.stddev("wind_direction").alias("wind_direction_stddev"),
                                    f.stddev("power_output").alias("power_output_stddev"),
                                    )
                            )

    df_data_including_hourly_means = (df_data_including_missing_rows
                                        .join(df_hourly_turbine_mean, on=["turbine_id", "hour"], how="left"))
    
    return (df_data_including_hourly_means
                .withColumn("wind_speed", f.coalesce(f.col("wind_speed"), f.col("wind_speed_mean")))
                .withColumn("wind_direction", f.coalesce(f.col("wind_direction"), f.col("wind_direction_mean")))
                .withColumn("power_output", f.coalesce(f.col("power_output"), f.col("power_output_mean")))
            )


def overwrite_outliers_with_means(df_data: DataFrame) -> DataFrame:
    """
        1. Calculate z-scores for each column
        2. Replace values with the hourly mean if the z-score is greater than 3 (3 standard deviations from mean)
    """
    df_with_zscores = (df_data
                        .withColumn("zscore_wind_speed", f.abs((f.col("wind_speed") - f.col("wind_speed_mean")) / f.col("wind_speed_stddev")))
                        .withColumn("zscore_wind_direction", f.abs((f.col("wind_direction") - f.col("wind_direction_mean")) / f.col("wind_direction_stddev")))
                        .withColumn("zscore_power_output", f.abs((f.col("power_output") - f.col("power_output_mean")) / f.col("power_output_stddev")))    
                    )
    
    df_cleaned = (df_with_zscores
                    .withColumn("wind_speed", f.when(f.col("zscore_wind_speed") > 3, f.col("wind_speed_mean")).otherwise(f.col("wind_speed")))
                    .withColumn("wind_direction", f.when(f.col("zscore_wind_direction") > 3, f.col("wind_direction_mean")).otherwise(f.col("wind_direction")))
                    .withColumn("power_output", f.when(f.col("zscore_power_output") > 3, f.col("power_output_mean")).otherwise(f.col("power_output")))
                )
    
    return (df_cleaned
                .drop(f.col("zscore_wind_speed"), f.col("zscore_wind_direction"), f.col("zscore_power_output")) 
            )


def clean_data(spark: SparkSession, df_source: DataFrame, start_date: datetime, end_date: datetime) -> DataFrame:
    """
        1. Validate all rows have a value for each column, if not throw an exception - this isn't an expected state
        2. Replace any missing rows with the mean of the turbine and hour
        3. Replace any outliers (3 standard deviations from mean) with the mean of the turbine and hour
    
    """
    check_for_rows_with_missing_columns(df_source)
    df_expected_timeslots = get_expected_timeslots(spark, start_date, end_date)
    df_data = fill_missing_timeslots_with_turbine_hourly_mean(df_source, df_expected_timeslots)
    df_cleaned = overwrite_outliers_with_means(df_data)

    return df_cleaned   


def calculate_daily_statistics(df_clean: DataFrame) -> DataFrame:
    """
        1. Calculate the daily mean, min and max for power_output column
    """
    df_daily_statistics = (df_clean
                            .withColumn("date", f.to_date(f.col("timestamp")))
                            .groupBy(f.col("date"), f.col("turbine_id"))
                            .agg(
                                f.mean("power_output").alias("power_output_mean"),
                                f.min("power_output").alias("power_output_min"),
                                f.max("power_output").alias("power_output_max"),
                                f.stddev("power_output").alias("power_output_stddev"),
                            )
                        )

    return df_daily_statistics


def find_and_write_outliers(df_clean: DataFrame) -> None:
    """
        Outliers are any power_output's that are 2 standard deviations from the mean of power_output from that turbine for that hour slot
    """
    df_outliers = (df_clean
                    .withColumn("zscore_power_output", f.abs((f.col("power_output") - f.col("power_output_mean")) / f.col("power_output_stddev")))    
                    .withColumn("outlier", f.when(f.col("zscore_power_output") > 2, f.lit(True)).otherwise(f.lit(False))) 
                    .filter(f.col("outlier") == f.lit(True))
                )
    
    df_outliers.write.option("header", "true").csv(CSV_OUTLIERS_OUTPUT_DIR, mode="overwrite")


if __name__ == "__main__":
    main(datetime(2022, 3, 1), datetime(2022, 4, 1))

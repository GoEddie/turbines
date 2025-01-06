from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as f

from .config import JDBC_URL, JDBC_USER, JDBC_PASSWORD, JDBC_SSLMODE


def execute_query(spark: SparkSession, query: str) -> None:

    connection_properties = {
        "user": JDBC_USER,
        "password": JDBC_PASSWORD,
        "driver": "org.postgresql.Driver",
        "sslmode": JDBC_SSLMODE
    }
    
    conn = spark._jvm.java.sql.DriverManager.getConnection(
        JDBC_URL, 
        connection_properties["user"], 
        connection_properties["password"]
    )
    
    try:
        stmt = conn.createStatement()
        stmt.execute(query)
    finally:
        conn.close()


def ensure_summary_target_table_exists(spark: SparkSession) -> None:
    create_table_query = """
        CREATE TABLE IF NOT EXISTS turbine_daily_summary (
            "date" DATE,
            turbine_id INT,
            power_output_mean FLOAT,
            power_output_min FLOAT,
            power_output_max FLOAT,
            power_output_stddev FLOAT,
            PRIMARY KEY ("date", turbine_id)
        );
    """
    
    execute_query(spark, create_table_query)


def ensure_cleaned_data_target_table_exists(spark: SparkSession) -> None:
    create_table_query = """
        CREATE TABLE IF NOT EXISTS turbine_hourly_cleaned
        (
            turbine_id INT,
            hour INT,
            "timestamp" TIMESTAMP,
            wind_speed DOUBLE PRECISION,
            wind_direction DOUBLE PRECISION,
            power_output DOUBLE PRECISION,
            wind_speed_mean DOUBLE PRECISION,
            wind_direction_mean DOUBLE PRECISION,
            power_output_mean DOUBLE PRECISION,
            wind_speed_stddev DOUBLE PRECISION,
            wind_direction_stddev DOUBLE PRECISION,
            power_output_stddev DOUBLE PRECISION,
            PRIMARY KEY (timestamp, turbine_id)
        );
    """
    
    execute_query(spark, create_table_query)


def run_daily_summary_upsert(spark: SparkSession) -> None:
    upsert_query = """
    INSERT INTO turbine_daily_summary (
        "date", turbine_id, power_output_mean, power_output_min, power_output_max, power_output_stddev
    )
    SELECT 
        "date", turbine_id, power_output_mean, power_output_min, power_output_max, power_output_stddev 
    FROM turbine_daily_summary_load_table
    ON CONFLICT ("date", turbine_id) 
    DO UPDATE SET 
        power_output_mean = EXCLUDED.power_output_mean,
        power_output_min = EXCLUDED.power_output_min,
        power_output_max = EXCLUDED.power_output_max,
        power_output_stddev = EXCLUDED.power_output_stddev;
    """

    execute_query(spark, upsert_query)


def run_cleaned_data_upsert(spark: SparkSession) -> None:
    upsert_query = """
    INSERT INTO public.turbine_hourly_cleaned ( turbine_id, hour, "timestamp", wind_speed, wind_direction, power_output, wind_speed_mean, wind_direction_mean, power_output_mean, wind_speed_stddev, wind_direction_stddev, power_output_stddev
    )
    SELECT  turbine_id, hour, "timestamp", wind_speed, wind_direction, power_output, wind_speed_mean, wind_direction_mean, power_output_mean, wind_speed_stddev, wind_direction_stddev, power_output_stddev
    FROM turbine_hourly_cleaned_load_table
    ON CONFLICT ("timestamp", turbine_id) 
    DO UPDATE SET 
        hour = EXCLUDED.hour,
        wind_speed = EXCLUDED.wind_speed,
        wind_direction = EXCLUDED.wind_direction,
        power_output = EXCLUDED.power_output,
        wind_speed_mean = EXCLUDED.wind_speed_mean,
        wind_direction_mean = EXCLUDED.wind_direction_mean,
        power_output_mean = EXCLUDED.power_output_mean,
        wind_speed_stddev = EXCLUDED.wind_speed_stddev,
        wind_direction_stddev = EXCLUDED.wind_direction_stddev,
        power_output_stddev = EXCLUDED.power_output_stddev;
    """

    execute_query(spark, upsert_query)

def write_summary_to_database(spark: SparkSession, df_daily_statistics: DataFrame) -> None:

    (df_daily_statistics
        .write
        .format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", JDBC_URL)
        .option("dbtable", "turbine_daily_summary_load_table")
        .option("sslmode", JDBC_SSLMODE)
        .option("user", JDBC_USER)
        .option("password", JDBC_PASSWORD)
        .mode("overwrite")
        .save()
    )
    
    ensure_summary_target_table_exists(spark)
    run_daily_summary_upsert(spark)
    

def write_cleaned_data_to_database(spark: SparkSession, df_clean_data: DataFrame):

    (df_clean_data
        .withColumn("timestamp", f.col("timestamp").cast("timestamp"))
        .write
        .format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", JDBC_URL)
        .option("dbtable", "turbine_hourly_cleaned_load_table")
        .option("sslmode", JDBC_SSLMODE)
        .option("user", JDBC_USER)
        .option("password", JDBC_PASSWORD)
        .mode("overwrite")
        .save()
    )

    ensure_cleaned_data_target_table_exists(spark)
    run_cleaned_data_upsert(spark)
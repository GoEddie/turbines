import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, DoubleType, StringType, FloatType
from datetime import datetime

from ..pipeline.pipeline import check_for_rows_with_missing_columns, get_expected_timeslots, fill_missing_timeslots_with_turbine_hourly_mean, overwrite_outliers_with_means, calculate_daily_statistics


SPARK = SparkSession.builder.appName("test").getOrCreate()


def test__check_for_rows_with_missing_columns___throws_when_missing_columns():
    # Arrange
    schema = StructType([
                            StructField("timestamp", TimestampType(), True),
                            StructField("turbine_id", IntegerType(), True),
                            StructField("wind_speed", DoubleType(), True),
                            StructField("wind_direction", DoubleType(), True),
                            StructField("power_output", DoubleType(), True)
                        ])
    
    df_with_missing_columns = SPARK.createDataFrame(
                [(None, 1, 10.5, None, 100.0)], schema
    )

    # Act & Assert
    assert pytest.raises(Exception, check_for_rows_with_missing_columns, df_with_missing_columns)


def test__check_for_rows_with_missing_columns___does_not_throw_when_no_missing_columns():
    # Arrange
    schema = StructType([
                            StructField("timestamp", TimestampType(), True),
                            StructField("turbine_id", IntegerType(), True),
                            StructField("wind_speed", DoubleType(), True),
                            StructField("wind_direction", DoubleType(), True),
                            StructField("power_output", DoubleType(), True)
                        ])
    
    df_with_missing_columns = SPARK.createDataFrame(
            [(datetime.now(), 1, 10.5, 123.0, 100.0)], schema
    )

    # Act & Assert
    check_for_rows_with_missing_columns(df_with_missing_columns)


def test__get_expected_timeslots__creates_all_hourly_timeslots_between_two_dates():
    #Arrange
    start_date = datetime(2021, 1, 1, 0, 0, 0)
    end_date = datetime(2021, 1, 2, 0, 0, 0)
    
    # Act
    expected_timeslots = get_expected_timeslots(SPARK, start_date, end_date)
    
    # Assert
    assert expected_timeslots.count() == 24
    assert expected_timeslots.collect()[0].timestamp == '2021-01-01 00:00:00'
    assert expected_timeslots.collect()[-1].timestamp == '2021-01-01 23:00:00'


def test__fill_missing_timeslots_with_turbine_hourly_mean__fills_missing_values_with_mean_values():
    #Arrange
    schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("turbine_id", IntegerType(), True),
        StructField("wind_speed", FloatType(), True),
        StructField("wind_direction", FloatType(), True),
        StructField("power_output", FloatType(), True)
    ])

    data = [
        (datetime(2022, 3, 1, 0, 0), 1, 10.0, 180.0, 100.0),
        (datetime(2022, 3, 1, 1, 0), 1, 12.0, 190.0, 110.0),
        (datetime(2022, 3, 1, 2, 0), 1, 11.0, 200.0, 120.0)
    ]

    df_source = SPARK.createDataFrame(data, schema)
    df_expected_timeslots = get_expected_timeslots(SPARK, datetime(2022, 3, 1, 0, 0), datetime(2022, 3, 2, 0, 0))

    # Act
    df_result = fill_missing_timeslots_with_turbine_hourly_mean(df_source, df_expected_timeslots)
    
    # Assert
    assert df_result.count() == 24
    
    
def test_overwrite_outliers_with_means():
    # Arrange
    data = [
        # Normal values
        (1, "2022-01-01 00:00:00", 10.0, 45.0, 100.0, 10.0, 45.0, 100.0, 2.0, 5.0, 20.0, 0),
        # Outlier values (>3 std dev)
        (1, "2022-01-01 01:00:00", 20.0, 90.0, 200.0, 10.0, 45.0, 100.0, 2.0, 5.0, 20.0, 0)
    ]
    
    columns = ["turbine_id", "timestamp", "wind_speed", "wind_direction", "power_output",
              "wind_speed_mean", "wind_direction_mean", "power_output_mean",
              "wind_speed_stddev", "wind_direction_stddev", "power_output_stddev", "hour"]
    
    df = SPARK.createDataFrame(data, columns)
    
    # Act
    result_df = overwrite_outliers_with_means(df)
    
    # Assert
    results = result_df.collect()
    
    # First row should remain unchanged (within 3 std dev)
    assert results[0]["wind_speed"] == 10.0
    assert results[0]["wind_direction"] == 45.0
    assert results[0]["power_output"] == 100.0
    
    # Second row should be replaced with means (>3 std dev)
    assert results[1]["wind_speed"] == 10.0  # mean value
    assert results[1]["wind_direction"] == 45.0  # mean value
    assert results[1]["power_output"] == 100.0  # mean value
    
    # Check z-score columns were dropped
    assert "zscore_wind_speed" not in result_df.columns
    assert "zscore_wind_direction" not in result_df.columns 
    assert "zscore_power_output" not in result_df.columns


def test_calculate_daily_statistics():
    # Arrange
    test_data = [
        ("2022-03-01 00:00:00", 1, 100.0),
        ("2022-03-01 01:00:00", 1, 200.0),
        ("2022-03-01 02:00:00", 1, 300.0),
        ("2022-03-02 00:00:00", 1, 400.0),
        ("2022-03-02 01:00:00", 1, 500.0),
        ("2022-03-01 00:00:00", 2, 150.0),
        ("2022-03-01 01:00:00", 2, 250.0)
    ]
    
    df = SPARK.createDataFrame(
        test_data,
        ["timestamp", "turbine_id", "power_output"]
    )

    # Act
    result = calculate_daily_statistics(df)
    result_dict = {
        (row.date.strftime("%Y-%m-%d"), row.turbine_id): {
            "mean": row.power_output_mean,
            "min": row.power_output_min,
            "max": row.power_output_max
        }
        for row in result.collect()
    }

    # Assert
    assert len(result_dict) == 3  # 2 days for turbine 1, 1 day for turbine 2
    
    # Check turbine 1, day 1
    assert result_dict[("2022-03-01", 1)]["mean"] == 200.0
    assert result_dict[("2022-03-01", 1)]["min"] == 100.0
    assert result_dict[("2022-03-01", 1)]["max"] == 300.0
    
    # Check turbine 1, day 2
    assert result_dict[("2022-03-02", 1)]["mean"] == 450.0
    assert result_dict[("2022-03-02", 1)]["min"] == 400.0
    assert result_dict[("2022-03-02", 1)]["max"] == 500.0
    
    # Check turbine 2, day 1
    assert result_dict[("2022-03-01", 2)]["mean"] == 200.0
    assert result_dict[("2022-03-01", 2)]["min"] == 150.0
    assert result_dict[("2022-03-01", 2)]["max"] == 250.0
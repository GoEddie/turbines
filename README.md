# Turbine Pipeline

## Overview

I focused on writing the code and tests rather than the application structure and followed the steps in the document rather than creating a data lake with different stages.

## Database

The instructions said to write to a database so I used a local docker container with postgres:

```bash
 docker run   -p 5432:5432 -e POSTGRES_PASSWORD=password -e POSTGRES_DB=colibri   postgres:latest
```

To run this, either create the postgres container or you can instead write to json on disk by commenting out one or the other db write commands in `pipeline.py`:

```python
from pg_database import write_summary_to_database
# from json_database import write_summary_to_database
```

## Config 

To change the paths to the input or output files, or the jdbc settings see `src/pipeline/config.py`

## Running the Pipeline 

Steps to run the pipeline:

1. Create a virtual environment
1. Install requirements `pip install -r requirements.txt`
1. Change to the src directory
1. `python3 -m pipeline.pipeline`

## Running the Tests 
 
Steps to run the tests:

1. Create a virtual environment
1. Install requirements `pip install -r requirements.txt`
1. Change to the src/tests directory
1. `pytest -vv`

## Process

### Clean data

#### Missing Values

There were no instances in the test data of a turbine supplying data for an hour but one of the columns being null so I added a check to ensure that was true but don't handle rows where some of the columns are null.

To handle missing hours, this creates a table of all of the hourly timeslots between two dates, it then joins that to the list of the turbine id's which gives us a table of all of the expected turbines + hourly timeslots. 

The expected timeslots is joined to the actual data and any missing values are populated with the mean for that turbine's hourly timeslot (the mean is of the other timeslots for each hour). This doesn't handle where a turbine doesn't supply any data at all, it is possible to use the mean of another turbine but I didn't think that made sense.

#### Outliers 

In the cleaning stage, any outliers that are outside of 3 standard deviations are replaced with the mean for that turbine's hourly timeslot. The instructions said to remove or impute outliers so I chose to use the mean of the other values for that timeslot. I chose 3 standard deviations as the instructions didn't specify a value and later the 2 standard deviation value is used to find outliers.

### Calculate Summary

This is a straightforward aggregation on the date (excluding the time) and turbine id. The mean, max, min, and standard deviation are calculated for each of the columns.

This is then written to the database as a table. The `pg_database.py` file includes code to do an upsert so that the process can be re-run without duplicating data.

### Identify Anomalies

In this phase we use the standard deviations already created in the clean step and filter to rows where the `power_output` is outside of 2 standard deviations. The file is then written to a csv file, the instructions didn't specify where or how the anomalies should be shown so I wrote them to disk.

## Tests

I have written unit tests for the core processing functions, given time constraints I didn't include integration tests or data quality tests but as part of a pipeline I would normally include those.
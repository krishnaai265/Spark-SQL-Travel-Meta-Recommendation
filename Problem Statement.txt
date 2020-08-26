# Travel Meta Recommendation Revisited

Some time has passed since we have implemented the application for Motels.home. Meanwhile the guys at Databricks 
were busy and they came out with SparkSQL. The management of Motels.home are aware of new technologies, and they 
realized that it is mandatory to update the application because of the benefits that SparkSQL provides.

This time the tasks is to update the application and use SparkSQL instead of SparkCore API while leaving the business
logic intact. Again you have to replace the ??? with your implementation.

## Requirements
- work with DataFrames - don't use the old RDD API
- using DataFrame DSL is strongly recommended, although you can use SQL statements if you get stuck somewhere

## Hints  
- recommend you to look at [examples project](../examples)
- you can use join's for both datasets (exchange and motels as well)
- for finding the maximum price you can use window functions (rank - HiveContext needed)
- some input's are in Parquet format which integrates better with SparkSQL. This input is the same as in the previous
homework, so you can switch back and use those during development if that's more convenient. Although at the end you 
have to read from Parquet files.
- if you run the app in local mode the application will exit with a huge exception, please ignore it. It's a bug for 
SparkSQL in local mode but it doesn't have any impact in our logic as it will happen during the closing of the related
SparkContext after all data are written to disk.

## Acceptance criteria:
* missing parts are filled in
* screenshot from local run
  - IntelliJ console - please include Spark logs as well (modify log4j.properties)
  - screenshot of each directory in local filesystem having the output files
* screenshots from HDP Hortonworks
  - screenshot of each directory in HDFS having the output files
  - screenshot of SparkUI showing the various stages and tasks
  - duration of the job in seconds
@ hint: 
  - you can utilize cluster_submit.sh to launch the app in HDP Sandbox

